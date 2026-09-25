"""Batched canary rollout: gating, boundaries and dispatch exclusivity."""

from __future__ import annotations

import threading

import pytest

from fleet.models import (
    DeviceReport,
    DeviceStatus,
    ReportKind,
    ReleaseState,
)
from fleet.store import RolloutError

from conftest import show_case


def _succeed(store, device_id, started_seq, version):
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_STARTED, started_seq))
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_SUCCEEDED,
                     started_seq + 1, version))


def _fail(store, device_id, started_seq):
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_STARTED, started_seq))
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_FAILED, started_seq + 1))


def test_batch_boundaries_match_percentages(store):
    # batch_percentages=(50,100) over 10 devices -> 5 then 5
    ids = [f"d{i}" for i in range(10)]
    for d in ids:
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ids)

    first = store.start_release("r1")
    first_ids = {i.device_id for i in first}
    show_case(
        "批次边界：50% 首批",
        "10 台设备, 批次配置 (50%,100%)",
        f"首批下发 {len(first)} 台 (必须=5); 未下发设备仍为 online",
    )
    assert len(first) == 5
    assert first_ids == set(ids[:5])
    assert store.device_snapshot("d5")["status"] == DeviceStatus.ONLINE.value
    # cannot advance before the batch is decided
    with pytest.raises(RolloutError) as exc:
        store.advance_release("r1")
    assert exc.value.reason == "batch_pending"

    for d in ids[:5]:
        store.pending_commands(d)
        _succeed(store, d, 1, "1.1.0")
    second = store.advance_release("r1")
    release = store.release_snapshot("r1")
    show_case(
        "批次推进：首批 5/5 健康后进入第二批",
        "首批全部成功后 advance",
        f"第二批下发 {len(second)} 台 (必须=5), current_batch={release['current_batch']}",
    )
    assert len(second) == 5
    assert release["current_batch"] == 1
    assert set(i.device_id for i in second) == set(ids[5:])

    for d in ids[5:]:
        store.pending_commands(d)
        _succeed(store, d, 1, "1.1.0")
    assert store.advance_release("r1") == []
    release = store.release_snapshot("r1")
    show_case(
        "批次收尾：最后一批达标后发布完成",
        "第二批 5/5 成功后 advance",
        f"state={release['state']} (必须 completed); 设备全部解绑",
    )
    assert release["state"] == ReleaseState.COMPLETED.value
    assert all(
        store.device_snapshot(d)["firmware_version"] == "1.1.0" for d in ids
    )
    assert all(
        store.device_snapshot(d)["active_release_id"] is None for d in ids
    )


def test_cannot_advance_when_health_below_threshold(registry):
    # Isolate the health gate from the safety valve: disable auto rollback.
    from fleet.models import RolloutConfig
    from fleet.store import FleetStore
    store = FleetStore(registry, RolloutConfig(
        batch_percentages=(50, 100), health_threshold=0.8,
        failure_threshold=1.0))
    # batch of 5: 4 success / 1 failure -> health 0.8 meets threshold exactly.
    # then batch of 5: 3 success / 2 failure -> health 0.6 < 0.8 -> blocked.
    ids = [f"d{i}" for i in range(10)]
    for d in ids:
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ids)
    store.start_release("r1")
    for d in ids[:5]:
        store.pending_commands(d)
    for d in ids[:4]:
        _succeed(store, d, 1, "1.1.0")
    _fail(store, ids[4], 1)
    second = store.advance_release("r1")  # 0.8 exactly -> allowed
    assert len(second) == 5

    for d in ids[5:]:
        store.pending_commands(d)
    for d in ids[5:8]:
        _succeed(store, d, 1, "1.1.0")
    for d in ids[8:]:
        _fail(store, d, 1)

    with pytest.raises(RolloutError) as exc:
        store.advance_release("r1")
    release = store.release_snapshot("r1")
    show_case(
        "健康门槛：第二匹健康率 0.6 < 0.8，禁止推进/收尾",
        "首批 4/5 成功(health=0.8 达标); 第二批 3/5 成功(health=0.6)",
        f"advance 被拒原因={exc.value.reason}: {exc.value.message}; "
        f"发布停留在 state={release['state']}, batch={release['current_batch']}",
    )
    assert exc.value.reason == "health_not_met"
    assert release["state"] == ReleaseState.ACTIVE.value
    assert release["current_batch"] == 1


def test_device_can_only_join_one_active_release(store):
    store.register_device("d1", "1.0.0")
    store.register_device("d2", "1.0.0")
    store.create_release("r1", "1.1.0", ["d1"])
    store.create_release("r2", "1.2.0", ["d1", "d2"])
    store.start_release("r1")
    with pytest.raises(RolloutError) as exc:
        store.start_release("r2")
    show_case(
        "设备互斥：同一设备不能同时参与两个进行中的发布",
        "r1 占用 d1 后, 尝试 start r2(包含 d1)",
        f"start(r2) 被拒原因={exc.value.reason}: {exc.value.message}",
    )
    assert exc.value.reason == "device_busy"
    # r1 remains the sole active release; d2 untouched
    assert store.release_snapshot("r1")["state"] == ReleaseState.ACTIVE.value
    assert store.release_snapshot("r2")["state"] == ReleaseState.PENDING.value


def test_concurrent_start_does_not_skip_batch_or_double_dispatch(store):
    ids = [f"d{i}" for i in range(4)]
    for d in ids:
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ids)
    results: list[list] = []
    errors: list[RolloutError] = []
    barrier = threading.Barrier(4)

    def worker():
        barrier.wait()
        try:
            results.append(store.start_release("r1"))
        except RolloutError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    release = store.release_snapshot("r1")
    bound = [
        d for d in ids
        if store.device_snapshot(d)["active_release_id"] == "r1"
    ]
    show_case(
        "并发下发：4 线程同时 start 同一发布",
        "barrier 放行 4 x start_release(r1), 首批 50%=2 台",
        f"成功 start 次数={len(results)} (必须 1); 拒绝={len(errors)} "
        f"原因={errors[0].reason if errors else '-'}; "
        f"实际绑定设备={sorted(bound)} (必须恰好 2, 不跳批/不重复)",
    )
    assert len(results) == 1
    assert len(results[0]) == 2
    assert all(e.reason == "not_pending" for e in errors)
    assert sorted(bound) == ["d0", "d1"]
    assert release["current_batch"] == 0


def test_concurrent_advance_never_double_dispatches_next_batch(store):
    ids = [f"d{i}" for i in range(4)]
    for d in ids:
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ids)
    store.start_release("r1")
    for d in ids[:2]:
        store.pending_commands(d)
        _succeed(store, d, 1, "1.1.0")

    results: list[list] = []
    errors: list[RolloutError] = []
    barrier = threading.Barrier(4)

    def worker():
        barrier.wait()
        try:
            results.append(store.advance_release("r1"))
        except RolloutError as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    release = store.release_snapshot("r1")
    show_case(
        "并发推进：4 线程同时 advance",
        "首批完成后 barrier 放行 4 x advance",
        f"成功推进次数={len(results)} (必须 1), 该次下发="
        f"{sorted(i.device_id for i in results[0])}; "
        f"拒绝原因={errors[0].reason if errors else '-'}; "
        f"dispatched 总数={len(release['dispatched'])} (必须 4)",
    )
    assert len(results) == 1
    assert sorted(i.device_id for i in results[0]) == ["d2", "d3"]
    assert errors and {e.reason for e in errors} <= {
        "batch_pending", "not_active", "rolled_back"}
    assert len(release["dispatched"]) == 4
