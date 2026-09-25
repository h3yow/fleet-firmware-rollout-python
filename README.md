# 设备固件分批灰度发布与回滚（本地可复现）

一套**纯本机可运行**的设备固件灰度发布系统：FastAPI 服务 + 多线程模拟设备，
无外部服务、真实硬件或凭据依赖。支持设备注册/心跳/版本上报、按批次推进的
灰度发布、健康度闸门、失败率安全阀自动回滚，并对重复、乱序、并发上报做了
幂等与防回退处理。

## 1. 目录结构

```
fleetrollout/
  models.py    # 领域类型：Device / FirmwareRelease / Batch / 状态枚举
  errors.py    # 可区分的业务错误码（断言失败原因用）
  config.py    # 可由环境变量覆盖的配置项
  versions.py  # 已知版本白名单与语义化版本比较
  store.py     # 线程安全内存存储（单一 RLock）
  devices.py   # 注册 / 心跳 / 在线判定 / 幂等防乱序上报
  release.py   # 批次编排 / 健康判定 / 安全阀回滚
  api.py       # HTTP 路由
  app.py       # 依赖装配
simulator/
  device_client.py  # 模拟设备（后台线程心跳 + 轮询下发 + 刷写回报 + 版本对账）
  run_demo.py       # 端到端演示脚本（随机端口启动真实 uvicorn）
tests/         # 18 个 pytest 用例（核心逻辑 / HTTP / 端到端）
main.py        # 固定 8000 端口的服务入口
```

## 2. 快速开始

```bash
uv sync                       # 安装依赖（fastapi / pytest / httpx / uvicorn）
uv run pytest                 # 运行全部测试
uv run python -m simulator.run_demo   # 端到端演示（自动起停本机服务）
uv run python main.py         # 启动常驻服务 http://127.0.0.1:8000，文档见 /docs
                              # 端口被占用时：ROLLOUT_PORT=8011 uv run python main.py
```

演示脚本会：在随机端口启动真实 HTTP 服务，用多线程模拟 9 台设备，依次跑
“健康发布完成”和“第一批失败率超阈值 → 自动回滚 → 版本对账 → 迟到上报被拒”
两个场景，并打印每台设备的输入、每次判定依据和完整发布历史。

## 3. 设备状态机

设备状态（`DeviceStatus`）：

```
                注册成功 / 心跳到达
 OFFLINE ───────────────────────▶ ONLINE
   ▲  │  心跳超时(>offline_timeout_s)   │ 发布下发(批次激活)
   │  └───────────────────────────────▶│
   │                                   ▼
   │                                UPDATING
   │                          success ╱     ╲ failure
   │                          ONLINE ◀       ▶ UPDATE_FAILED
   └─（UPDATE_FAILED / UPDATING 不被心跳改写成 ONLINE，状态不回退）

回滚发生时：该发布涉及的所有设备强制回到 ONLINE，current_version 恢复基线。
```

要点：

- 心跳只负责 `OFFLINE ↔ ONLINE`；`UPDATING` / `UPDATE_FAILED` 只能由发布
  流程和上报结果迁移，避免心跳把“更新失败”刷回“在线”。
- 心跳超时判定是惰性的：调用 `refresh_status` / `refresh_all` 或设备查询前刷新。

## 4. 上报的幂等、乱序与并发处理

每次结果上报携带 `seq`（单调序号）与 `dedup_key`（报文唯一键）：

| 情况 | 处理 | 错误码 |
| --- | --- | --- |
| `dedup_key` 已处理过 | 直接返回当前设备，**不计数、不改版本/状态** | （幂等成功） |
| `seq < 已处理最大 seq` | 拒绝 | `stale_report` (HTTP 409) |
| 版本不在白名单 | 拒绝，绝不静默忽略 | `unknown_version` (422) |
| 版本 < 最低支持版本 | 拒绝，绝不静默忽略 | `unsupported_version` (422) |
| 引用不存在的发布 | 拒绝 | `release_not_found` (404) |
| 发布已回滚后迟到的目标版本上报 | 拒绝，设备版本保持基线 | `stale_report` (409) |

所有设备与批次变更都在存储的同一把 `RLock` 内原子完成；上报采用
“**发布侧前置校验 → 记录幂等键/序号 → 设备状态迁移 → 发布侧记账**”的顺序，
因此回滚后的迟到成功上报不可能先把版本改成目标版再被回滚。

## 5. 发布与批次推进规则

发布状态（`ReleaseStatus`）：

```
PENDING ──start──▶ IN_PROGRESS ──最后一批健康 advance──▶ COMPLETED
                       │  │
        手动 rollback  │  │ 当前批次失败率超阈值（收齐时 / advance 时）
                       ▼  ▼
                  ROLLED_BACK（经 ROLLING_BACK 瞬态）
```

批次状态（`BatchStatus`）：`pending → active → healthy`；
失败安全阀触发时当前批次为 `failed`，其余未完成批次为 `skipped`
（手动回滚时在途批次无失败证据，也记为 `skipped`）。

推进闸门（只有调用 `advance` 或批次刚收齐时判定）：

1. **必须收齐**：当前批次 `successes + failures == 批内设备数`，
   未收齐一律 `batch_not_ready`，不会下发下一批。
2. **失败率优先（安全阀）**：`failures / total > failure_ratio_threshold`
   → 立即自动回滚。注意是**严格大于**，恰好等于阈值不触发。
3. **健康率闸门**：`successes / total >= healthy_ratio_threshold`
   （恰好等于阈值即达标）才允许推进。
4. 健康率不足但失败率也未超阈值：保持在途，可等待人工处置/手动回滚。

占用规则：

- `start` 时对发布全部设备做原子占用（`active_release_id`），任一设备已在
  其他在途发布中或当前版本不等于基线，则**整体拒绝**（全有或全无）。
- 因此同一设备不可能同时参与两个进行中的发布，并发 `start` 也不会重复占用。
- 成功或失败的设备在回报后退出占用；回滚会清空该发布全部设备的占用。

## 6. 回滚规则与自洽性

- 回滚目标 = 创建发布时声明的**基线版本**（`baseline_version`，即上一稳定版本）。
- 回滚把发布涉及的**全部**设备（包括此前已成功进入目标版本的健康批次）的
  `current_version` 与 `last_known_good_version` 恢复为基线，状态置 `ONLINE`，
  清空在途发布占用。
- 每个发布维护不可变追加的 `history`：创建参数、每批下发、每台设备成功/失败、
  进入回滚原因与回滚完成，因此同一输入序列可完整复现判定过程。
- 模拟设备轮询时若发现本地版本与服务端分歧（如刚成功就被回滚），会按服务端
  权威版本做一次 `installed` 对账上报，保持设备侧与服务侧一致。

## 7. 配置项（环境变量）

| 环境变量 | 默认值 | 含义 |
| --- | --- | --- |
| `ROLLOUT_OFFLINE_TIMEOUT_S` | `30.0` | 心跳多久无响应判定离线 |
| `ROLLOUT_HEARTBEAT_INTERVAL_S` | `5.0` | （模拟器用）心跳间隔 |
| `ROLLOUT_BATCH_SIZE` | `3` | 默认每批设备数（建发布时可覆盖） |
| `ROLLOUT_HEALTHY_RATIO` | `0.8` | 默认推进健康率阈值（`>=` 达标） |
| `ROLLOUT_FAILURE_RATIO` | `0.3` | 默认回滚失败率阈值（`>` 触发） |
| `ROLLOUT_MIN_REPORTS` | `1` | 预留：最少回报数 |
| `ROLLOUT_MIN_VERSION` | `1.0.0` | 最低支持版本，更旧的上报拒绝 |

已知固件版本白名单当前为 `["1.0.0", "1.1.0", "2.0.0"]`（见
`fleetrollout/app.py` 的 `DEFAULT_KNOWN_VERSIONS`，可在装配时注入）。

## 8. HTTP 接口摘要

| 方法 & 路径 | 说明 |
| --- | --- |
| `POST /devices/register` | `{device_id, version}` 注册 |
| `POST /devices/heartbeat` | `{device_id}` 心跳 |
| `POST /devices/report` | `{device_id, version, release_id, outcome, seq, dedup_key}` |
| `GET /devices` / `GET /devices/{id}` | 设备列表 / 详情（含状态、版本、在途发布） |
| `POST /releases/create` | 指定版本、设备清单、批次大小、两个阈值 |
| `POST /releases/{id}/start` | 原子占用并激活第 0 批（下发） |
| `POST /releases/{id}/advance` | 收齐且健康才推进；失败率超阈值则自动回滚 |
| `POST /releases/{id}/rollback` | 手动回滚，body 可带 `reason` |
| `GET /releases` / `GET /releases/{id}` | 发布详情，含批次计数与完整 history |

错误响应统一为 `{"detail": {"code": "...", "message": "..."}}`，业务错误码：
`device_not_found` / `release_not_found` / `device_already_registered` /
`device_busy` / `release_state_error` / `batch_not_ready` /
`unknown_version` / `unsupported_version` / `stale_report`。

## 9. 本地手工验证示例（curl）

```bash
uv run python main.py &
for i in 0 1 2; do
  curl -s -X POST localhost:8000/devices/register \
    -H 'content-type: application/json' \
    -d "{\"device_id\":\"d$i\",\"version\":\"1.0.0\"}"
done
curl -s -X POST localhost:8000/releases/create -H 'content-type: application/json' -d '{
  "release_id":"r1","target_version":"2.0.0","baseline_version":"1.0.0",
  "device_ids":["d0","d1","d2"],"batch_size":3}'
curl -s -X POST localhost:8000/releases/r1/start -d '{}' -H 'content-type: application/json'
for i in 0 1 2; do
  curl -s -X POST localhost:8000/devices/report -H 'content-type: application/json' -d "{
    \"device_id\":\"d$i\",\"version\":\"2.0.0\",\"release_id\":\"r1\",
    \"outcome\":\"success\",\"seq\":1,\"dedup_key\":\"k$i\"}"
done
curl -s -X POST localhost:8000/releases/r1/advance -d '{}' -H 'content-type: application/json'
```

## 10. 测试覆盖说明

- **并发上报**：20 台 × 8 线程并发同键上报，断言成功计数恰好为 20。
- **重复上报**：相同 `dedup_key` 多次提交只计数一次。
- **乱序上报**：高 seq 先到后，低 seq 报文被 `stale_report` 拒绝，版本不回退。
- **批次边界**：未收齐不得推进、不得提前下发下一批；健康率恰好等于阈值达标、
  失败率恰好等于阈值**不**回滚。
- **失败率回滚**：2/3 失败（>0.3）收齐即自动回滚，全部设备版本与占用自洽。
- **失败原因可区分**：对每种业务错误断言独立 `code`。
- 每个用例均打印【输入】与【判定依据】，便于人工核对。
