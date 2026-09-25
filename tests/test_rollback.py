"""Safety-valve rollback: trigger, version consistency and history replay."""

from __future__ import annotations

from fleet.models import (
    DeviceReport,
    DeviceStatus,
    EventKind,
    ReportKind,
    RejectReason,
    ReleaseState,
    RolloutConfig,
)
from fleet.registry import FirmwareRegistry
from fleet.store import FleetStore

from conftest import show_case


def _succeed(store, device_id, started_seq, version):
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_STARTED, started_seq))
    return store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_SUCCEEDED,
                     started_seq + 1, version))


def _fail(store, device_id, started_seq):
    store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_STARTED, started_seq))
    return store.apply_report(
        DeviceReport(device_id, ReportKind.UPDATE_FAILED, started_seq + 1))


def _make_store(registry, failure_threshold=0.3, percentages=(50, 100)):
    return FleetStore(registry, RolloutConfig(
        batch_percentages=percentages, health_threshold=0.8,
        failure_threshold=failure_threshold))


def test_failure_rate_exceeding_threshold_triggers_rollback(registry):
    store = _make_store(registry)
    ids = [f"d{i}" for i in range(10)]
    for d in ids:
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ids)
    store.start_release("r1")
    batch0 = ids[:5]
    for d in batch0:
        store.pending_commands(d)

    acks = {}
    for d in batch0[:3]:
        acks[d] = _succeed(store, d, 1, "1.1.0")
    for d in batch0[3:]:
        acks[d] = _fail(store, d, 1)

    release = store.release_snapshot("r1")
    tripped_acks = [a.detail for a in acks.values() if "safety valve" in a.detail]
    show_case(
        "安全阀：首批失败率 2/5=0.4 > 0.3 自动回滚",
        "首批 5 台: 3 成功 / 2 失败",
        f"第 2 个失败上报的 detail='{tripped_acks[0]}'; "
        f"发布状态={release['state']} (必须 rolled_back); "
        f"未下发的第二批设备={ids[5:]} 全部仍未绑定",
    )
    assert release["state"] == ReleaseState.ROLLED_BACK.value
    assert len(tripped_acks) == 1  # tripped exactly once
    for d in ids[5:]:
        snap = store.device_snapshot(d)
        assert snap["active_release_id"] is None
        assert snap["firmware_version"] == "1.0.0"

    # succeeded devices get rollback commands back to their stable version
    for d in batch0[:3]:
        commands = store.pending_commands(d)
        assert [c.command for c in commands] == ["rollback"]
        assert commands[0].version == "1.0.0"
        seq = store.device_snapshot(d)["report_counter"]
        ack = store.apply_report(
            DeviceReport(d, ReportKind.ROLLBACK_DONE, seq + 1, "1.0.0"))
        assert ack.accepted
        assert store.device_snapshot(d)["firmware_version"] == "1.0.0"

    # failed devices were resolved at trigger time and stay FAILED on 1.0.0
    for d in batch0[3:]:
        snap = store.device_snapshot(d)
        assert snap["firmware_version"] == "1.0.0"
        assert snap["status"] == DeviceStatus.FAILED.value
        assert snap["active_release_id"] is None
        assert store.pending_commands(d) == []

    release = store.release_snapshot("r1")
    show_case(
        "回滚后版本自洽",
        "3 台成功设备执行 rollback_done; 2 台失败设备触发时即解决",
        f"rolled_back_devices={sorted(release['rolled_back_devices'])} "
        f"(必须覆盖首批全部 5 台)",
    )
    assert sorted(release["rolled_back_devices"]) == sorted(batch0)


def test_rollback_history_is_append_only_and_reproducible(registry):
    store = _make_store(registry)
    ids = [f"d{i}" for i in range(4)]
    for d in ids:
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ids)
    store.start_release("r1")
    for d in ids[:2]:
        store.pending_commands(d)
    _succeed(store, ids[0], 1, "1.1.0")
    _fail(store, ids[1], 1)

    history = store.release_snapshot("r1")["history"]
    kinds = [e["event"] for e in history]
    trigger = next(e for e in history
                   if e["event"] == EventKind.ROLLBACK_TRIGGERED.value)
    show_case(
        "发布历史：追加写、含触发原因、可复现",
        "2 台首批 (1 成功 1 失败, 失败率 0.5) 触发回滚",
        f"事件序列={kinds}; 触发原因='{trigger['reason']}'",
    )
    assert kinds[0] == EventKind.CREATED.value
    assert kinds.count(EventKind.ROLLBACK_TRIGGERED.value) == 1
    assert "failure rate" in trigger["reason"]
    # running a second identical scenario yields the same event shape
    store2 = _make_store(registry)
    for d in ids:
        store2.register_device(d + "b", "1.0.0")
    ids2 = [d + "b" for d in ids]
    store2.create_release("r1b", "1.1.0", ids2)
    store2.start_release("r1b")
    for d in ids2[:2]:
        store2.pending_commands(d)
    _succeed(store2, ids2[0], 1, "1.1.0")
    _fail(store2, ids2[1], 1)
    kinds2 = [e["event"] for e in store2.release_snapshot("r1b")["history"]]
    assert kinds2 == kinds


def test_manual_rollback_restores_previous_stable_version(registry):
    store = _make_store(registry, failure_threshold=1.0)
    ids = ["d0", "d1"]
    for d in ids:
        store.register_device(d, "1.0.5")
    store.create_release("r1", "1.1.0", ids)
    store.start_release("r1")
    for d in ids:
        store.pending_commands(d)
        _succeed(store, d, 1, "1.1.0")
    commands = store.rollback_release("r1", reason="manual safety stop")
    show_case(
        "手动回滚：回到发布前各自的稳定版本",
        "2 台设备从 1.0.5 升到 1.1.0 后手动 rollback",
        f"下发命令={[(c.device_id, c.command, c.version) for c in commands]}",
    )
    assert all(c.version == "1.0.5" for c in commands)
    for d in ids:
        seq = store.device_snapshot(d)["report_counter"]
        store.apply_report(
            DeviceReport(d, ReportKind.ROLLBACK_DONE, seq + 1, "1.0.5"))
        assert store.device_snapshot(d)["firmware_version"] == "1.0.5"
    assert (store.release_snapshot("r1")["state"]
            == ReleaseState.ROLLED_BACK.value)


def test_late_success_during_rollback_is_re_rolled_back(registry):
    store = _make_store(registry, percentages=(100,))
    ids = ["d0", "d1"]
    for d in ids:
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ids)
    store.start_release("r1")
    for d in ids:
        store.pending_commands(d)
    # d0 starts updating but has not reported a result; d1 fails -> rollback
    store.apply_report(
        DeviceReport("d0", ReportKind.UPDATE_STARTED, 1))
    _fail(store, "d1", 1)
    assert store.release_snapshot("r1")["state"] == ReleaseState.ROLLED_BACK.value

    # d0 is still UPDATING and already has a rollback command queued; now a
    # late success arrives.
    d0_seq = store.device_snapshot("d0")["report_counter"]
    late = store.apply_report(
        DeviceReport("d0", ReportKind.UPDATE_SUCCEEDED,
                     d0_seq + 1, "1.1.0"))
    commands = store.pending_commands("d0")
    show_case(
        "回滚途中迟到成功：不丢、不静默，自动重新下发回滚",
        "rollback 已触发, d0 迟到 update_succeeded(1.1.0)",
        f"ack='{late.detail}'; 队列命令={[(c.command, c.version) for c in commands]}",
    )
    assert late.accepted
    # original rollback command + re-dispatched rollback after late success
    rollback_cmds = [c for c in commands if c.command == "rollback"]
    assert len(rollback_cmds) >= 1
    assert all(c.version == "1.0.0" for c in rollback_cmds)
    done = store.apply_report(
        DeviceReport("d0", ReportKind.ROLLBACK_DONE, d0_seq + 2, "1.0.0"))
    assert done.accepted
    assert store.device_snapshot("d0")["firmware_version"] == "1.0.0"


def test_rollback_done_with_wrong_version_is_rejected(registry):
    store = _make_store(registry)
    ids = ["d0", "d1"]
    for d in ids:
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ids)
    store.start_release("r1")
    for d in ids:
        store.pending_commands(d)
    _succeed(store, "d0", 1, "1.1.0")
    _fail(store, "d1", 1)
    d0_seq = store.device_snapshot("d0")["report_counter"]
    bad = store.apply_report(
        DeviceReport("d0", ReportKind.ROLLBACK_DONE, d0_seq + 1, "1.2.0"))
    show_case(
        "回滚版本必须与稳定版本一致",
        f"rollback_done(version=1.2.0) 而稳定版本=1.0.0",
        f"accepted={bad.accepted}, reason={bad.reason.value}",
    )
    assert not bad.accepted
    assert bad.reason is RejectReason.NOT_EXPECTED
    assert store.device_snapshot("d0")["firmware_version"] == "1.1.0"


def test_rolled_back_release_cannot_advance(registry):
    from fleet.store import RolloutError
    store = _make_store(registry, failure_threshold=1.0)
    ids = ["d0", "d1", "d2", "d3"]
    for d in ids:
        store.register_device(d, "1.0.0")
    store.create_release("r1", "1.1.0", ids)
    store.start_release("r1")
    store.rollback_release("r1", reason="manual")
    try:
        store.advance_release("r1")
        raise AssertionError("advance should fail")
    except RolloutError as exc:
        show_case(
            "已回滚发布不可再推进",
            "release=rolled_back 后调用 advance",
            f"拒绝原因={exc.reason}",
        )
        assert exc.reason == "rolled_back"
