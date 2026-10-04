# 设备固件分批灰度发布与回滚（本机可复现）

一套完全运行在本机的设备固件灰度发布模拟系统：服务端为线程安全的内存状态机 +
FastAPI 本地 HTTP 接口；"设备侧"由同进程内的 `DeviceClient`（可多线程并发）
模拟。**不依赖外部服务、真实硬件或任何凭据。**

## 目录结构

```
fleet/
  models.py      # 领域类型：设备/发布/上报/命令/配置/枚举
  versioning.py  # 版本号解析与比较（malformed -> 未知版本）
  registry.py    # 已知固件版本目录（白名单）
  store.py       # 核心：单锁保护的状态机（上报去重、批次、安全阀、回滚）
  service.py     # 门面：编排 store / registry / 设备客户端
  client.py      # 模拟设备进程：序列号、拉取并执行命令、回报结果
  api.py         # FastAPI 本地 HTTP 接口
scripts/demo.py  # 一键并发端到端演示（成功 + 失败触发回滚）
tests/           # 并发/重复/乱序上报、批次边界、失败率回滚、HTTP 用例
main.py          # uvicorn 入口（127.0.0.1，随机端口）
```

## 快速开始

```bash
uv sync --group dev        # 安装依赖
uv run pytest -s           # 运行全部测试（-s 可查看每个用例打印的输入与判定依据）
uv run python scripts/demo.py   # 并发设备进程的端到端演示
uv run python main.py            # 启动 HTTP 服务（控制台打印实际端口）
```

## 设备状态机

设备状态：`unknown → online ⇄ offline`，另有 `updating`、`failed`。

```
                 register / heartbeat / version(正常版本)
   unknown ───────────────────────────────────────────────▶ online
      ▲                                                        │  │
      │ 未注册上报被拒绝(unknown_device)                        │  │ 超过 offline_timeout_s 未心跳
      │◀──────────────────────────────────────────────────────┘  │ mark_offline_devices()
      │                                                          ▼
      │                                                       offline
      │
  发布下发 update 命令
 online/offline ──────────────▶ updating
                                   │  update_succeeded(==目标版本)
                                   ├──────────────────────────▶ online（版本=目标）
                                   │  update_failed
                                   └──────────────────────────▶ failed
 回滚命令 rollback（仅发布处于 rolled_back）
 online/updating ─────────────▶ updating ──rollback_done(==稳定版本)──▶ online（版本=稳定版）
```

关键规则：

- **心跳不掩盖状态**：`updating` / `failed` 期间的心跳只刷新 `last_seen`，不会把设备拉回 `online`。
- **离线**：仅对 `online` 设备做超时扫描（`POST /maintenance/offline-sweep`），更新中/失败设备不受影响。
- **失败保持**：设备进入 `failed` 后不会被普通心跳自动恢复，需人工处置后随新发布重新下发。

## 上报的幂等与乱序防护

每条上报携带：

- `seq`：设备内单调递增计数器；服务端记录已应用的最大 `seq`。
  - `seq < 已应用` → 拒绝 `stale_counter`（乱序旧消息）
  - `seq == 已应用`（>0）→ 拒绝 `duplicate`（重复投递）
  - `seq > 已应用+1` → 接受但在 ack 中标注 seq gap（跳过号，不影响结论）
- `report_id`（可选）：客户端生成的幂等键，重放同一 id → 拒绝 `duplicate`。
- 所有判定与状态变更在 **同一把可重入锁** 内完成，因此并发上报不会出现
  重复计数（如成功集合 +1 两次）或状态回退（旧版本号覆盖新版本号）。

版本类上报先过两道闸门，**绝不静默忽略**：

1. 版本不在固件目录或格式非法 → `unknown_version`
2. 版本低于 `min_known_version` → `version_too_old`

拒绝原因枚举 `RejectReason`：`unknown_device / unknown_version /
version_too_old / stale_counter / duplicate / not_expected`，HTTP 层
分别以 409（操作冲突）或 422（上报被拒）返回，`reason` 字段可机读区分。

## 发布状态机与批次推进

发布状态：`pending → active → completed`，任何 active 阶段可转入终态
`rolled_back`，`pending/active` 可转入终态 `cancelled`（取消，立即释放设备）。
多个计划可并行处于 `active`，数量受 `max_active_releases` 约束。

```
 create_release        start_release            最后一批达标
 pending ─────────▶ pending ──────────────▶ active ──────────▶ completed
     │                   │                      │  ▲
     │ cancel            │ cancel               │  │ advance_release()
     ▼                   ▼                      │  └ 健康率 >= health_threshold
 cancelled         cancelled      每批结束后     │  ┌ cancel_release()
                                  失败率超阈值    ▼  ▼
                                           rolled_back ──全部 rollback_done──▶ (设备释放)
```

批次规则（配置 `batch_percentages`，如 `(10, 30, 60, 100)`）：

- 第 *k* 批设备数 = `ceil(比例_k × 总设备数)`，按注册顺序切片；批次边界由
  `Release.batch_size()/batch_members()` 固定，与推进次数无关。
- `start_release` 下发第 0 批；`advance_release` 只能在**当前批全部有结论**
  （成功+失败 == 批次人数）后调用，否则拒绝 `batch_pending`。
- 当前批健康率 `成功数 / 批次人数` **低于** `health_threshold` 时拒绝推进
  （`health_not_met`）；最后一批达标则发布 `completed`。
- **设备互斥**：设备同一时刻最多绑定一个进行中的发布；目标设备已被占用时
  整次 start **原子失败**（先全部校验再绑定），不会半绑定、不会跳批。
- **并发安全**：start/advance 先校验再变更并在锁内完成；并发调用只有一个生效，
  其余被 `not_pending / not_active / batch_pending` 拒绝，同一设备不会被
  重复下发，批次不会被跳过。

## 安全阀与回滚规则

- 每当一批内收到成功/失败结论，立即重算**当前批**失败率
  `失败数 / 批次人数`；一旦 **> `failure_threshold`**，自动回滚（只触发一次，
  触发原因写入发布历史，如 `batch 0 failure rate 0.40 > 0.30`）。
- 也可手动调用 `rollback_release(reason)`。
- 回滚目标是每台设备在发布前记录的 **stable_version**（各自不同也正确）。
  回滚时按设备处境分别处理：
  - **已成功升级**（在新版本上）或 **updating 中**（正前往新版本）：下发
    `rollback` 命令，设备回报 `rollback_done` 且版本必须等于 stable_version，
    否则以 `not_expected` 拒绝；
  - **已失败**：设备从未离开 stable_version，触发时即记入回滚历史并解绑，
    状态保留 `failed`，不重复下发；
  - **已下发但未开始**：直接就地解决（版本不变，回到 online）；
  - **尚未下发的后续批次**：从未绑定，完全不受影响。
- 回滚途中的迟到/乱序消息不静默丢弃：
  - 迟到的 `update_succeeded` 会被接受、计入成功历史，并**重新补一条 rollback
    命令**，保证最终版本仍回到 stable_version；
  - 回滚后迟到的 `update_failed` 幂等接受（设备本就在 stable_version）；
  - 回滚后重复/旧 seq 消息仍按 `duplicate / stale_counter` 区分拒绝。
- 回滚为终态：`rolled_back` 的发布调用 advance 一律拒绝（`rolled_back`）。
- 发布历史 `Release.history` 为**追加写**事件流（created / started /
  dispatched / batch_advanced / device_updated / device_failed /
  rollback_triggered / rollback_dispatched / device_rolled_back /
  completed），相同输入序列可复现相同事件序列（见
  `test_rollback_history_is_append_only_and_reproducible`）。

## 多计划并行与冲突准入

多个发布计划可以**同时进行**：每个计划有独立的目标设备、批次划分、阈值与
状态机，各自推进/判定/回滚，一个计划回滚或卡在门槛上**不影响任何其他计划**。

### 准入规则（`start_release` 时两道闸门）

启动前全部校验完成后才做任何状态变更，因此被拒时**不存在半截状态**，已有计划
照常运行。拒绝原因与结构化详情同时返回（`RolloutError.reason/details`，
HTTP 409 `detail.reason/detail.details`）：

1. **并行上限**：进行中的计划数达到 `max_active_releases`（默认 3）即拒绝
   `active_release_limit`，`details` 带 `limit` 与当前 `in_progress` 列表。
   - “进行中” = `active` / `paused`，以及回滚命令尚未全部回报的
     `rolled_back`（回滚未完成前设备仍被占用）；`pending` 与已结束计划不计入。
2. **设备冲突**：目标设备集合与**任何进行中计划所占用的设备**有重叠即拒绝
   `device_busy`。
   - **冲突判定范围是新计划的全部目标设备**（所有批次），不是仅首批。某台设备
     要到后面批次才下发，也必须在启动时就归属于本计划，因此与后续批次设备冲突
     也会在开始前整体拒绝，绝不会走到一半才发现。
   - `details = {"devices": [冲突设备...], "conflicts": {设备: [计划id...]}}`，
     明确指出是哪些设备、跟哪个计划冲突。

设备占用口径：

| 计划状态 | 占用的设备 |
|---|---|
| `active` / `paused` | **全部目标设备**（含尚未下发的后续批次） |
| `rolled_back`（回滚进行中） | 仍绑定、等待 rollback 回报的已下发设备 |
| `rolled_back`（全部回报完成） | 无 |
| `completed` / `cancelled` / `pending` | 无 |

### 计划结束与设备释放

计划以三种方式结束后都会**真正释放设备**，之后可被新计划收编：

- **正常完成** `completed`：最后一批健康达标收尾，全部设备解绑（版本停留在目标版）。
- **回滚完成**：每台设备回报 `rollback_done` 后逐一解绑；全部完成前计划仍计入
  “进行中”、其设备仍被占用（避免新计划与尚未回到稳定版的设备冲突）。
- **取消** `cancelled`（`cancel_release(reason)` /
  `POST /releases/{id}/cancel`）：可从 `pending`（未启动）或 `active`/`paused`
  取消；取消不做版本回滚——已成功设备留在目标版、失败设备保留 `failed`，
  已下发但未成功的设备回到 `stable_version`，**所有设备立即解绑**。取消后到达
  的迟到上报按 `not_expected` 拒绝，不会让已取消的计划复活。

### 状态查询

- `store.list_releases(state=None)` / `GET /releases?state=active`：列出所有
  进行中与已结束计划，每个计划一条汇总；整个列表在**同一把锁内**计算，并发、
  重复、迟到上报期间热读也不会出现条目间自相矛盾。
- `store.release_summary(id)` / `GET /releases/{id}/summary`：单计划汇总。
- 汇总字段包括：

| 字段 | 含义 |
|---|---|
| `state` / `in_progress` | 计划状态；是否仍占用并行名额 |
| `current_batch` / `batch_count` | 当前批次（-1 未启动）/ 总批次数 |
| `total_devices` / `dispatched` / `decided` | 总设备 / 已下发 / 已有结论 |
| `succeeded` / `failed` / `rolled_back` | 成功 / 失败 / 已回滚设备数 |
| `pending_decisions` | 当前批尚待上报结论的设备数 |
| `current_batch_*` | 当前批人数/成功/失败（门槛与安全阀的计算口径） |
| `batch_health_rate` / `batch_failure_rate` | 当前批健康率 / 失败率 |
| `overall_health_rate` / `overall_failure_rate` | 全计划口径健康率 / 失败率 |
| `gate` | 下一步判定：`waiting_for_reports`（停住等上报）/ `ready_to_advance`（可推进）/ `blocked_health_below_threshold`（健康不足被挡住）/ `final_batch_healthy_completable`（可收尾）/ `rollback_in_flight` / `rollback_complete` / `paused` / `not_started` / `done_completed` / `done_cancelled` |
| `consistent` / `mismatches` | 汇总结论与每台设备实际版本/绑定关系的交叉校验；任何对不上（如已成功设备版本≠目标版、已结束仍绑定）都会列出 |

## 配置项（`fleet.models.RolloutConfig`）

| 配置项 | 默认值 | 含义 |
|---|---|---|
| `batch_percentages` | `(10, 30, 60, 100)` | 各批次累计覆盖比例（%），决定批次边界 |
| `health_threshold` | `0.8` | 推进下一批所需的当前批最低健康率 |
| `failure_threshold` | `0.3` | 当前批失败率超过该值即触发安全阀回滚 |
| `offline_timeout_s` | `30.0` | online 设备多久无心跳判为离线 |
| `min_known_version` | `"1.0.0"` | 可接受的最低版本，更低则 `version_too_old` |
| `max_active_releases` | `3` | 同时进行中的计划数上限，超限启动被整体拒绝 |

固件白名单由 `FirmwareRegistry` 维护（默认已知 `0.9.0 / 1.0.0 / 1.1.0 /
1.2.0`）；未知或无法按 `数字.数字…` 解析的版本一律拒绝。

## HTTP 接口（仅监听 127.0.0.1）

| 方法与路径 | 说明 |
|---|---|
| `POST /devices` `{device_id, version}` | 注册（幂等；未知版本 409） |
| `GET /devices/{id}` | 设备快照 |
| `POST /devices/{id}/reports` | 上报 `{kind, seq, version?, report_id?}`；接受 200，拒绝 422 且带 `reason` |
| `POST /releases` | 创建发布 |
| `GET /releases?state=` | 计划列表与汇总（可按状态过滤；含进行中与已结束） |
| `GET /releases/{id}` | 发布快照（含追加写历史） |
| `GET /releases/{id}/summary` | 单计划汇总（批次/健康失败比例/gate/一致性校验） |
| `POST /releases/{id}/start` | 启动并下发第 0 批；超并行上限（`active_release_limit`）或全目标设备冲突（`device_busy`）时 409 整体拒绝 |
| `POST /releases/{id}/advance` | 健康门槛校验后推进下一批 / 收尾 |
| `POST /releases/{id}/rollback` | 手动回滚 |
| `POST /releases/{id}/cancel` | 取消计划（pending/active 均可），立即释放设备 |
| `POST /maintenance/offline-sweep` | 离线扫描 |

`kind` 取值：`register / heartbeat / version / update_started /
update_succeeded / update_failed / rollback_done`。

curl 小例：

```bash
curl -s -X POST localhost:<port>/devices -H 'content-type: application/json' \
  -d '{"device_id":"d1","version":"1.0.0"}'
curl -s -X POST localhost:<port>/devices/d1/reports -H 'content-type: application/json' \
  -d '{"kind":"heartbeat","seq":1}'
```

## 本地验证方法

```bash
# 单元 + 并发 + HTTP 测试（-s 打印每个用例的输入与判定依据）
uv run pytest -s

# 只看某一类场景
uv run pytest -s tests/test_device_reports.py   # 并发/重复/乱序/版本闸门
uv run pytest -s tests/test_rollout_batches.py  # 批次边界/并发不跳批
uv run pytest -s tests/test_rollback.py         # 失败率回滚/版本自洽/历史复现
uv run pytest -s tests/test_multi_releases.py   # 多计划并行/冲突拒绝无副作用/回滚隔离/释放复用/并发查询一致
uv run pytest -s tests/test_api.py              # HTTP：冲突 409、列表/汇总、取消复用

# 多线程设备进程端到端演示（首批 3 成功 2 失败 -> 自动回滚 -> 全部回到 1.0.0）
uv run python scripts/demo.py
```

测试覆盖对照：

- **并发上报**：20 线程原子领 seq 心跳（计数精确为 20）；10 线程并发同一成功
  事件（只计一次）。
- **重复 / 乱序**：相同 seq、相同 `report_id` 拒绝为 `duplicate`；旧 seq
  拒绝为 `stale_counter`，且旧版本号不覆盖新版本。
- **失败率触发回滚**：首批 3 成功/2 失败（0.4 > 0.3）精确触发一次，校验
  成功设备回滚、失败设备就地解决、后续批次未绑定、最终版本全部自洽。
- **批次边界**：10 台 × (50%,100%) 恰好 5+5；未决不可推进；健康率 0.6 < 0.8
  禁止收尾；并发 start/advance 不跳批、不重复下发。
- **多计划并行**（`test_multi_releases.py`）：两个不相交计划独立推进；后续批次
  设备冲突在启动时整体拒绝（`device_busy` + 冲突设备/计划详情）且无任何副作用；
  超 `max_active_releases` 拒绝 `active_release_limit`；一个计划 50% 失败自动
  回滚不影响隔壁计划完成；设备在完成/回滚完成/取消后释放并可被新计划收编（三种
  结束方式参数化覆盖）；并发 + 重复 + 迟到上报期间热读 `list_releases()`，
  每个快照 `consistent=True` 且成功数不重不漏。
- 每个用例均 `print` 输入与判定依据，并断言具体的、可互相区分的失败原因。
