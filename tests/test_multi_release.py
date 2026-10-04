"""Multiple release plans running in parallel.

Covers: independent parallel progress, whole-plan conflict admission with no
side effects, the configurable concurrency cap, rollback isolation between
plans, device reuse after a plan ends, and summary-query consistency under
concurrent / duplicate / late reports.
"""

from __future__ import annotations

import threading

import pytest

from fleet.models import (
    DeviceReport,
    DeviceStatus,
    ReportKind,
    ReleaseState,
    RolloutConfig,
)
from fleet.registry import FirmwareRegistry
from fleet.store import FleetStore, RolloutError

from conftest import KNOWN, show_case


def _make_store(**overrides) -> FleetStore:
    config = RolloutConfig(
        batch_percentages=(50, 100),
        health_threshold=0.8,
        failure_threshold=0.3,
        **overrides,
    )
    return FleetStore(FirmwareRegistry(set(KNOWN)), config)


def _versions(store, ids):
    return {d: store.device_snapshot(d)["firmware_version"] for d in ids}


# ---------------------------------------------------------------- parallel


def test_parallel_releases_advance_independently(service):
    r1_ids = [f"a{i}" for i in range(4)]
    r2_ids = [f"b{i}" for i in range(6)]
    for d in r1_ids + r2_ids:
        service.provision_device(d, "1.0.0")
    store = service.store

    store.create_release("r1", "1.1.0", r1_ids)
    store.create_release("r2", "1.2.0", r2_ids)
    store.start_release("r1")
    store.start_release("r2")
    service.tick_clients()  # batch 0 of both plans succeeds

    store.advance_release("r1")  # r1 -> batch 1; r2 untouched
    mid1 = store.release_summary("r1")
    mid2 = store.release_summary("r2")
    show_case(
        "多计划并行：各自独立推进",
        "r1(a0..a3 -> 1.1.0) 与 r2(b0..b5 -> 1.2.0) 同时开始; 仅推进 r1",
        f"r1: state={mid1['state']} batch={mid1['current_batch']}; "
        f"r2: state={mid2['state']} batch={mid2['current_batch']} (必须仍=0)",
    )
    assert mid1["state"] == ReleaseState.ACTIVE.value
    assert mid1["current_batch"] == 1
    assert mid2["state"] == ReleaseState.ACTIVE.value
    assert mid2["current_batch"] == 0

    service.tick_clients()
    store.advance_release("r1")  # r1 completes
    assert store.release_summary("r1")["state"] == ReleaseState.COMPLETED.value
    # r2 is still mid-flight and finishes on its own schedule
    assert store.release_summary("r2")["state"] == ReleaseState.ACTIVE.value
    store.advance_release("r2")
    service.tick_clients()
    store.advance_release("r2")

    s1, s2 = store.release_summary("r1"), store.release_summary("r2")
    print(f"  终态: r1={s1['state']} health={s1['health']}; "
          f"r2={s2['state']} health={s2['health']}")
    assert s1["state"] == s2["state"] == ReleaseState.COMPLETED.value
    assert s1["succeeded"] == 4 and s1["health"] == 1.0
    assert s2["succeeded"] == 6 and s2["health"] == 1.0
    assert _versions(store, r1_ids) == {d: "1.1.0" for d in r1_ids}
    assert _versions(store, r2_ids) == {d: "1.2.0" for d in r2_ids}


# --------------------------------------------------------------- admission


def test_conflicting_release_rejected_without_side_effects(service):
    ids = [f"d{i}" for i in range(6)]
    for d in ids:
        service.provision_device(d, "1.0.0")
    store = service.store

    store.create_release("r1", "1.1.0", ids[:4])
    store.start_release("r1")
    # d4 is only reserved by a *pending* plan: reservation covers the whole
    # target set, not just dispatched batches.
    store.create_release("r2", "1.1.0", ["d4", "d5"])

    with pytest.raises(RolloutError) as exc:
        store.create_release("r3", "1.2.0", ["d2", "d3", "d4"])
    show_case(
        "冲突准入：与进行中计划重叠即整体拒绝",
        "r1(active, d0-d3) + r2(pending, d4-d5); 新建 r3(d2,d3,d4)",
        f"拒绝原因={exc.value.reason}; 冲突明细={exc.value.conflicts}",
    )
    assert exc.value.reason == "device_conflict"
    assert exc.value.conflicts == {"d2": "r1", "d3": "r1", "d4": "r2"}

    # no side effects: r3 does not exist, no device got bound to it,
    # and the pre-existing plans still run.
    with pytest.raises(KeyError):
        store.release_snapshot("r3")
    for d in ids:
        bound = store.device_snapshot(d)["active_release_id"]
        assert bound in (None, "r1"), f"{d} unexpectedly bound to {bound}"
    service.tick_clients()
    store.advance_release("r1")
    service.tick_clients()
    store.advance_release("r1")
    assert store.release_summary("r1")["state"] == ReleaseState.COMPLETED.value
    print("  副作用检查: r3 未创建; r1 照常推进到 completed; "
          "d4/d5 仍只被 r2 预留")


def test_max_active_releases_limit():
    store = _make_store(max_active_releases=2)
    for i in range(6):
        store.register_device(f"d{i}", "1.0.0")
    store.create_release("r1", "1.1.0", ["d0", "d1"])
    store.create_release("r2", "1.1.0", ["d2", "d3"])

    with pytest.raises(RolloutError) as exc:
        store.create_release("r3", "1.2.0", ["d4", "d5"])
    show_case(
        "并发上限：第 3 个进行中计划被拒",
        "max_active_releases=2, 已有 r1/r2 进行中, 再建 r3(d4,d5)",
        f"拒绝原因={exc.value.reason}: {exc.value.message}",
    )
    assert exc.value.reason == "too_many_releases"
    with pytest.raises(KeyError):
        store.release_snapshot("r3")

    # a freed slot (cancelled pending plan) admits the next plan
    store.cancel_release("r1", reason="superseded")
    store.create_release("r3", "1.2.0", ["d4", "d5"])
    summary = store.release_summary("r3")
    print(f"  释放名额后: r1 state=cancelled, r3 准入成功 "
          f"state={summary['state']}")
    assert store.release_snapshot("r1")["state"] == ReleaseState.CANCELLED.value
    assert summary["state"] == ReleaseState.PENDING.value


def test_cancel_pending_release_frees_devices():
    store = _make_store()
    for d in ("d0", "d1"):
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ["d0", "d1"])
    store.cancel_release("r1", reason="plan withdrawn")

    store.create_release("r2", "1.2.0", ["d0", "d1"])
    store.start_release("r2")
    show_case(
        "取消未开始的计划：设备立即可复用",
        "r1(d0,d1) 创建后取消; 同设备新建 r2 并 start",
        f"r1 state={store.release_snapshot('r1')['state']}; "
        f"r2 已 start, 目标设备数="
        f"{len(store.release_snapshot('r2')['target_device_ids'])}",
    )
    assert store.release_snapshot("r1")["state"] == ReleaseState.CANCELLED.value
    assert store.release_summary("r2")["state"] == ReleaseState.ACTIVE.value
    # cancelling an *active* plan is refused: rollback is the safe ending
    with pytest.raises(RolloutError) as exc:
        store.cancel_release("r2")
    assert exc.value.reason == "not_pending"
    print(f"  active 计划取消被拒: reason={exc.value.reason} (应走回滚)")


# -------------------------------------------------------------- isolation


def test_rollback_of_one_release_does_not_affect_others(service):
    r1_ids = ["a0", "a1", "a2", "a3"]
    r2_ids = ["b0", "b1", "b2", "b3"]
    service.provision_device("a0", "1.0.0")                 # will succeed
    service.provision_device("a1", "1.0.0", fail_rate=1.0)  # will fail
    for d in ("a2", "a3"):
        service.provision_device(d, "1.0.0")
    for d in r2_ids:
        service.provision_device(d, "1.0.0")
    store = service.store

    store.create_release("r1", "1.1.0", r1_ids)
    store.create_release("r2", "1.2.0", r2_ids)
    store.start_release("r1")
    store.start_release("r2")
    # batch 0 of r1 = {a0 ok, a1 fail} -> failure rate 0.5 > 0.3 -> rollback;
    # batch 0 of r2 all succeed in the same tick.
    service.tick_clients()
    service.tick_clients()  # drain r1's rollback command to a0

    s1 = store.release_summary("r1")
    s2 = store.release_summary("r2")
    show_case(
        "回滚隔离：r1 触发安全阀, r2 不受影响",
        "r1 首批 a0 成功/a1 失败 (失败率 0.5 > 0.3); r2 首批全部成功",
        f"r1: state={s1['state']} rolled_back_devices={s1['rolled_back_devices']}"
        f" bound={s1['bound_devices']}; r2: state={s2['state']} "
        f"health={s2['health']}",
    )
    assert s1["state"] == ReleaseState.ROLLED_BACK.value
    assert s1["bound_devices"] == 0  # rollback fully drained
    assert s2["state"] == ReleaseState.ACTIVE.value
    assert s2["health"] == 1.0
    # r1 devices are all back on their stable versions
    assert _versions(store, r1_ids) == {d: "1.0.0" for d in r1_ids}
    assert store.device_snapshot("a1")["status"] == DeviceStatus.FAILED.value

    # r2 keeps advancing to completion
    store.advance_release("r2")
    service.tick_clients()
    store.advance_release("r2")
    s2 = store.release_summary("r2")
    print(f"  r2 终态: state={s2['state']} succeeded={s2['succeeded']}")
    assert s2["state"] == ReleaseState.COMPLETED.value
    assert _versions(store, r2_ids) == {d: "1.2.0" for d in r2_ids}
    r2_events = {e["event"] for e in store.release_snapshot("r2")["history"]}
    assert "rollback_triggered" not in r2_events


def test_devices_reusable_after_completion_and_rollback(service):
    for d in ("c0", "c1", "c2", "c3"):
        service.provision_device(d, "1.0.0")
    store = service.store

    # r1 completes normally
    store.create_release("r1", "1.1.0", ["c0", "c1"])
    store.start_release("r1")
    service.tick_clients()
    store.advance_release("r1")
    service.tick_clients()
    store.advance_release("r1")
    assert store.release_summary("r1")["state"] == ReleaseState.COMPLETED.value

    # r2 rolls back and drains
    store.create_release("r2", "1.1.0", ["c2", "c3"])
    store.start_release("r2")
    store.rollback_release("r2", reason="manual")
    service.tick_clients()  # no-op for c2/c3: resolved in place (never started)
    assert store.release_summary("r2")["state"] == ReleaseState.ROLLED_BACK.value
    assert store.release_summary("r2")["bound_devices"] == 0

    # both plans ended -> every device is free for a brand-new plan
    store.create_release("r3", "1.2.0", ["c0", "c1", "c2", "c3"])
    store.start_release("r3")
    service.tick_clients()
    store.advance_release("r3")
    service.tick_clients()
    store.advance_release("r3")
    s3 = store.release_summary("r3")
    show_case(
        "设备释放后可复用",
        "r1 完成(c0,c1@1.1.0), r2 回滚(c2,c3@1.0.0); 新建 r3 收编全部 4 台",
        f"r3 state={s3['state']} succeeded={s3['succeeded']} "
        f"health={s3['health']}",
    )
    assert s3["state"] == ReleaseState.COMPLETED.value
    assert s3["succeeded"] == 4
    assert _versions(store, ["c0", "c1", "c2", "c3"]) == {
        d: "1.2.0" for d in ("c0", "c1", "c2", "c3")
    }


# ------------------------------------------------------------- consistency


def test_summaries_consistent_under_concurrent_reports(service):
    r1_ids = [f"p{i}" for i in range(4)]
    r2_ids = [f"q{i}" for i in range(4)]
    for d in r1_ids + r2_ids:
        service.provision_device(d, "1.0.0")
    store = service.store
    store.create_release("r1", "1.1.0", r1_ids)
    store.create_release("r2", "1.2.0", r2_ids)
    store.start_release("r1")
    store.start_release("r2")

    errors: list[AssertionError] = []

    def check_invariants(rounds: int) -> None:
        for _ in range(rounds):
            for s in store.list_releases():
                try:
                    assert 0 <= s["succeeded"] <= s["dispatched"] <= s["targets"]
                    assert 0 <= s["failed"] <= s["dispatched"]
                    assert s["succeeded"] + s["failed"] <= s["dispatched"]
                    assert s["bound_devices"] >= 0
                    assert s["state"] in {st.value for st in ReleaseState}
                except AssertionError as exc:  # pragma: no cover
                    errors.append(exc)

    def work(device_id: str) -> None:
        client = service.client(device_id)
        for _ in range(3):
            client.run_commands()
            client.heartbeat()

    threads = [threading.Thread(target=work, args=(d,))
               for d in r1_ids + r2_ids]
    watcher = threading.Thread(target=check_invariants, args=(200,))
    watcher.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    watcher.join()

    # finish both plans
    store.advance_release("r1")
    store.advance_release("r2")
    service.tick_clients()
    store.advance_release("r1")
    store.advance_release("r2")

    # late + duplicate reports after completion must not corrupt summaries
    late = store.apply_report(DeviceReport(
        device_id="p0", kind=ReportKind.UPDATE_SUCCEEDED, seq=2,
        version="1.1.0"))
    dup = store.apply_report(DeviceReport(
        device_id="p1", kind=ReportKind.UPDATE_SUCCEEDED,
        seq=store.device_snapshot("p1")["report_counter"],
        version="1.1.0"))

    summaries = {s["release_id"]: s for s in store.list_releases()}
    show_case(
        "并发上报下状态查询一致性",
        "8 台设备分属 r1/r2, 8 线程并发执行命令+心跳, 期间 200 次轮询 "
        "list_releases; 收尾后补迟到(seq 倒退)与重复上报",
        f"轮询不变量违例数={len(errors)}; 迟到上报 accepted={late.accepted} "
        f"reason={late.reason and late.reason.value}; 重复上报 "
        f"accepted={dup.accepted} reason={dup.reason and dup.reason.value}; "
        f"r1={summaries['r1']['state']}/{summaries['r1']['succeeded']}台 "
        f"r2={summaries['r2']['state']}/{summaries['r2']['succeeded']}台",
    )
    assert not errors
    assert not late.accepted and late.reason.value == "stale_counter"
    assert not dup.accepted
    for rid, ids, ver in (("r1", r1_ids, "1.1.0"), ("r2", r2_ids, "1.2.0")):
        s = summaries[rid]
        assert s["state"] == ReleaseState.COMPLETED.value
        assert s["succeeded"] == len(ids) and s["health"] == 1.0
        assert s["failed"] == 0 and s["bound_devices"] == 0
        # summary agrees with the actual device states
        for d in ids:
            snap = store.device_snapshot(d)
            assert snap["firmware_version"] == ver
            assert snap["active_release_id"] is None
            assert snap["status"] == DeviceStatus.ONLINE.value
    print("  汇总与设备实际版本逐台核对一致")
