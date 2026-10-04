"""Multi-plan parallel rollout: admission, isolation, release and consistency.

Covers:
- several independent releases running and advancing in parallel;
- start-admission conflicts over the *whole* target set, rejected wholesale
  with no side effects;
- configurable cap on in-progress plans;
- one plan rolling back (auto / manual) cannot affect any other plan;
- devices are truly released on completion / rollback-complete / cancel and
  can join a new plan;
- state queries stay consistent with device versions under concurrent,
  duplicate and late reports.

Every case logs its inputs and the exact decision basis via ``show_case``.
"""

from __future__ import annotations

import threading

import pytest

from fleet.models import (
    DeviceReport,
    DeviceStatus,
    ReleaseState,
    ReportKind,
    RolloutConfig,
)
from fleet.registry import FirmwareRegistry
from fleet.store import FleetStore, RolloutError

from conftest import show_case


def _succeed(store, device_id, first_seq, version):
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_STARTED, first_seq))
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_SUCCEEDED,
                     first_seq + 1, version))


def _fail(store, device_id, first_seq):
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_STARTED, first_seq))
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_FAILED, first_seq + 1))


def _finish_rollback(store, device_id, stable, seq):
    store.pending_commands(device_id)
    store.apply_report(
        DeviceReport(device_id, ReportKind.ROLLBACK_DONE, seq, stable))


@pytest.fixture
def multi_store():
    return FleetStore(
        FirmwareRegistry({"1.0.0", "1.1.0", "1.2.0"}),
        RolloutConfig(batch_percentages=(50, 100),
                      health_threshold=0.8, failure_threshold=0.3,
                      max_active_releases=3),
    )


def _provision(store, ids, version="1.0.0"):
    for d in ids:
        store.register_device(d, version)


# ------------------------------------------------------------- parallel plans

def test_two_disjoint_plans_run_and_advance_independently(multi_store):
    s = multi_store
    a, b = ["a0", "a1", "a2"], ["b0", "b1", "b2"]
    _provision(s, a + b)
    s.create_release("rA", "1.1.0", a)
    s.create_release("rB", "1.2.0", b)
    items_a = s.start_release("rA")
    items_b = s.start_release("rB")
    show_case(
        "多计划并行：互不相交的两个计划同时启动",
        f"rA 目标 {a} -> 1.1.0; rB 目标 {b} -> 1.2.0",
        f"首批各下发 {len(items_a)}/{len(items_b)} 台; 两者都应为 active, "
        f"进行中计数=2",
    )
    assert len(items_a) == 2 and len(items_b) == 2
    assert s.release_snapshot("rA")["state"] == ReleaseState.ACTIVE.value
    assert s.release_snapshot("rB")["state"] == ReleaseState.ACTIVE.value
    assert s.list_releases()["in_progress_count"] == 2

    # rA advances first; rB must be unaffected by that.
    _succeed(s, "a0", 1, "1.1.0")
    _succeed(s, "a1", 1, "1.1.0")
    s.advance_release("rA")
    summ_b = s.release_summary("rB")
    show_case(
        "独立推进：rA 进第二批时 rB 不被带动",
        "rA 首批 2/2 成功 -> advance; rB 尚无上报",
        f"rA current_batch=1; rB current_batch=0 gate={summ_b['gate']} "
        f"pending={summ_b['pending_decisions']}",
    )
    assert s.release_snapshot("rA")["current_batch"] == 1
    assert summ_b["current_batch"] == 0
    assert summ_b["gate"] == "waiting_for_reports"


# --------------------------------------------------------------- admission ---

def test_conflict_checks_whole_target_set_and_is_side_effect_free(multi_store):
    s = multi_store
    ids = [f"d{i}" for i in range(6)]
    _provision(s, ids)
    # batch (50,100) over 4 devices -> d0,d1 dispatched, d2,d3 still waiting.
    s.create_release("r1", "1.1.0", ["d0", "d1", "d2", "d3"])
    s.start_release("r1")
    s.create_release("r2", "1.2.0", ["d2", "d4"])  # d2 only in later batch
    with pytest.raises(RolloutError) as exc:
        s.start_release("r2")
    show_case(
        "冲突准入：判定范围是计划全部设备而非首批",
        "r1 目标 d0..d3（d2 尚未下发）; start r2 目标 [d2,d4]",
        f"拒绝原因={exc.value.reason}; 详情={exc.value.details} 必须指出 d2 与 r1 冲突",
    )
    assert exc.value.reason == "device_busy"
    assert exc.value.details["devices"] == ["d2"]
    assert exc.value.details["conflicts"] == {"d2": ["r1"]}

    # No side effects: r2 stays pending, d4 never bound, r1 untouched.
    assert s.release_snapshot("r2")["state"] == ReleaseState.PENDING.value
    assert s.device_snapshot("d4")["active_release_id"] is None
    assert s.device_snapshot("d2")["active_release_id"] is None
    assert s.list_releases()["in_progress_count"] == 1
    assert s.pending_commands("d4") == []

    # disjoint plan starts fine right after the rejection
    s.create_release("r3", "1.1.0", ["d4", "d5"])
    items = s.start_release("r3")
    show_case(
        "被拒后不影响其他计划：不相交的 r3 可正常启动",
        "r2 被拒后, 启动 r3=[d4,d5]",
        f"r3 首批下发 {len(items)} 台 (2 台 × 50% ceil=1), r1 继续 active",
    )
    assert {i.device_id for i in items} == {"d4"}
    assert s.release_snapshot("r1")["state"] == ReleaseState.ACTIVE.value


def test_active_release_cap_is_enforced_without_half_state(multi_store):
    s = multi_store
    ids = [f"d{i}" for i in range(9)]
    _provision(s, ids)
    for name, devs in [("r1", ids[0:3]), ("r2", ids[3:6]),
                       ("r3", ids[6:9])]:
        s.create_release(name, "1.1.0", devs)
        s.start_release(name)
    s.register_device("x0", "1.0.0")
    s.create_release("r4", "1.2.0", ["x0"])
    with pytest.raises(RolloutError) as exc:
        s.start_release("r4")
    show_case(
        "并行上限：max_active_releases=3, 第 4 个计划整体拒绝",
        "r1/r2/r3 均 active; start r4=[x0]（无设备冲突）",
        f"拒绝原因={exc.value.reason}, 详情={exc.value.details}; r4 必须保持 pending",
    )
    assert exc.value.reason == "active_release_limit"
    assert exc.value.details["limit"] == 3
    assert set(exc.value.details["in_progress"]) == {"r1", "r2", "r3"}
    assert s.release_snapshot("r4")["state"] == ReleaseState.PENDING.value
    assert s.device_snapshot("x0")["active_release_id"] is None


# ------------------------------------------------------------- isolation -----

def test_rollback_of_one_plan_does_not_touch_other_plans(multi_store):
    s = multi_store
    _provision(s, ["a0", "a1", "a2", "b0", "b1", "b2"])
    # 4 devices per plan would also work; use 3 -> ceil(50%*3)=2 in batch 0.
    s.create_release("rA", "1.1.0", ["a0", "a1", "a2"])
    s.create_release("rB", "1.2.0", ["b0", "b1", "b2"])
    s.start_release("rA")
    s.start_release("rB")

    # a0 succeeds, a1 fails -> batch 0 rate 1/2 = 0.5 > 0.3 -> auto rollback.
    _succeed(s, "a0", 1, "1.1.0")
    _fail(s, "a1", 1)
    show_case(
        "回滚隔离：rA 失败率 1/2=0.5 > 0.3 触发自动回滚",
        "rA: a0 成功, a1 失败; rB 同步进行中",
        "rA=rolled_back; rB 必须仍 active, b0/b1 绑定关系与命令不变",
    )
    assert s.release_snapshot("rA")["state"] == \
        ReleaseState.ROLLED_BACK.value
    snap_b = s.release_snapshot("rB")
    assert snap_b["state"] == ReleaseState.ACTIVE.value
    assert snap_b["current_batch"] == 0
    assert s.device_snapshot("b0")["active_release_id"] == "rB"

    # finish rA rollback: a0 returns to 1.0.0; B-side versions untouched.
    _finish_rollback(s, "a0", "1.0.0", 3)
    show_case(
        "回滚完成只影响本计划设备",
        "a0 回报 rollback_done(1.0.0)",
        "a0=1.0.0 且解绑; b0/b1 仍是 1.0.0 但仍绑定 rB; rA 不再计为进行中",
    )
    assert s.device_snapshot("a0")["firmware_version"] == "1.0.0"
    assert s.device_snapshot("a0")["active_release_id"] is None
    assert s.release_summary("rA")["gate"] == "rollback_complete"
    assert s.list_releases()["in_progress_count"] == 1
    assert s.release_summary("rB")["in_progress"] is True

    # rB can still complete normally: batch0 (b0,b1) then batch1 adds b2.
    _succeed(s, "b0", 1, "1.2.0")
    _succeed(s, "b1", 1, "1.2.0")
    s.advance_release("rB")
    _succeed(s, "b2", 3, "1.2.0")
    s.advance_release("rB")
    assert s.release_snapshot("rB")["state"] == \
        ReleaseState.COMPLETED.value
    for d in ("b0", "b1", "b2"):
        assert s.device_snapshot(d)["firmware_version"] == "1.2.0"


# ------------------------------------------------------------- release/reuse -

@pytest.mark.parametrize("ending", ["completed", "rolled_back", "cancelled"])
def test_devices_are_released_and_reusable_after_plan_ends(
        multi_store, ending):
    s = multi_store
    _provision(s, ["d0", "d1", "d2"])
    s.create_release("r1", "1.1.0", ["d0", "d1", "d2"])
    s.start_release("r1")
    if ending == "completed":
        _succeed(s, "d0", 1, "1.1.0")
        _succeed(s, "d1", 1, "1.1.0")
        s.advance_release("r1")
        _succeed(s, "d2", 3, "1.1.0")
        s.advance_release("r1")
        expected_state = ReleaseState.COMPLETED.value
    elif ending == "rolled_back":
        _succeed(s, "d0", 1, "1.1.0")
        _fail(s, "d1", 1)
        _finish_rollback(s, "d0", "1.0.0", 3)
        expected_state = ReleaseState.ROLLED_BACK.value
    else:  # cancelled
        s.cancel_release("r1", reason="operator abort")
        expected_state = ReleaseState.CANCELLED.value

    unbound = [d for d in ("d0", "d1", "d2")
               if s.device_snapshot(d)["active_release_id"] is None]
    show_case(
        f"设备释放（{ending}）：结束后设备可被新计划收编",
        f"r1 以 {expected_state} 结束",
        f"解绑设备={unbound}; 之后 start r2 收编全部三台必须成功",
    )
    assert unbound == ["d0", "d1", "d2"]
    assert s.release_summary("r1")["in_progress"] is False

    s.create_release("r2", "1.2.0", ["d0", "d1", "d2"])
    items = s.start_release("r2")
    assert len(items) == 2
    assert s.release_snapshot("r2")["state"] == ReleaseState.ACTIVE.value
    for d in ("d0", "d1"):
        assert s.device_snapshot(d)["active_release_id"] == "r2"


# ------------------------------------------------------------- consistency ---

def test_listing_consistent_under_concurrent_duplicate_late_reports(
        multi_store):
    s = multi_store
    _provision(s, [f"d{i}" for i in range(6)])
    s.create_release("rA", "1.1.0", ["d0", "d1", "d2"])
    s.create_release("rB", "1.2.0", ["d3", "d4", "d5"])
    s.start_release("rA")
    s.start_release("rB")

    errors: list[str] = []

    def worker(device_id, target, seq_pair):
        try:
            # duplicate starts: same seq twice; only one can bind state.
            s.apply_report(DeviceReport(
                device_id, ReportKind.UPDATE_STARTED, seq_pair[0]))
            s.apply_report(DeviceReport(
                device_id, ReportKind.UPDATE_STARTED, seq_pair[0]))
            s.apply_report(DeviceReport(
                device_id, ReportKind.UPDATE_SUCCEEDED,
                seq_pair[1], target))
        except Exception as exc:  # pragma: no cover - diagnostic only
            errors.append(repr(exc))

    threads = [
        threading.Thread(target=worker, args=("d0", "1.1.0", (1, 2))),
        threading.Thread(target=worker, args=("d1", "1.1.0", (1, 2))),
        threading.Thread(target=worker, args=("d3", "1.2.0", (1, 2))),
        threading.Thread(target=worker, args=("d4", "1.2.0", (1, 2))),
    ]
    # concurrent listing while reports fly: every snapshot must be coherent
    stop = threading.Event()

    def observer():
        while not stop.is_set():
            listing = s.list_releases()
            for e in listing["releases"]:
                assert e["consistent"], f"inconsistent summary: {e}"
                assert e["succeeded"] <= e["total_devices"]

    obs = threading.Thread(target=observer)
    obs.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    obs.join()
    assert not errors

    # late, out-of-order duplicate after success must not change any counts.
    late = s.apply_report(DeviceReport(
        "d0", ReportKind.UPDATE_SUCCEEDED, 2, "1.1.0"))
    stale = s.apply_report(DeviceReport(
        "d0", ReportKind.HEARTBEAT, 1))
    listing = s.list_releases()
    summaries = {e["release_id"]: e for e in listing["releases"]}
    show_case(
        "并发/重复/迟到上报下查询一致",
        "4 设备各自双线程重复 started+成功; 观测线程并发热读 list_releases; "
        "再投重复成功(seq=2)与旧心跳(seq=1)",
        f"重复 ack 原因={late.reason}, 旧消息原因={stale.reason}; "
        f"rA/rB 成功数={summaries['rA']['succeeded']}/"
        f"{summaries['rB']['succeeded']}, 全部 consistent=True, 版本与目标一致",
    )
    assert late.reason.value == "duplicate"
    assert stale.reason.value == "stale_counter"
    for name, target in (("rA", "1.1.0"), ("rB", "1.2.0")):
        e = summaries[name]
        assert e["consistent"] is True
        assert e["succeeded"] == 2
        assert e["failed"] == 0
        members = s.release_snapshot(name)["target_device_ids"]
        for d in members[:2]:
            assert s.device_snapshot(d)["firmware_version"] == target

    # advance both plans; gate verdicts must reflect reality and stay disjoint.
    s.advance_release("rA")
    s.advance_release("rB")
    listing = s.list_releases()
    gates = {e["release_id"]: e["gate"] for e in listing["releases"]}
    show_case(
        "并发推进后判定仍自洽",
        "rA/rB 首批均 2/2 成功 -> 各自 advance 到第二批",
        f"gate={gates}; 两计划均 waiting_for_reports, consistent=True",
    )
    for e in listing["releases"]:
        assert e["consistent"] is True
    assert gates == {"rA": "waiting_for_reports",
                     "rB": "waiting_for_reports"}
