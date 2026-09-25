"""Device registration, heartbeat, version reports and online/offline state."""

from __future__ import annotations

import threading

import pytest

from fleet.models import (
    DeviceReport,
    DeviceStatus,
    ReportAck,
    RejectReason,
    ReportKind,
)
from fleet.store import RolloutError

from conftest import show_case


def _report(device_id, kind, seq, version=None, report_id=None):
    return DeviceReport(
        device_id=device_id, kind=kind, seq=seq,
        version=version, report_id=report_id,
    )


# ---------------------------------------------------------------- registration

def test_register_is_idempotent_and_rejects_unknown_version(store):
    store.register_device("d1", "1.0.0")
    first = store.device_snapshot("d1")

    dup = store.register_device("d1", "1.0.0")
    assert dup.report_counter == 0

    with pytest.raises(RolloutError) as exc:
        store.register_device("d2", "9.9.9")
    show_case(
        "注册：幂等 + 未知版本拒绝",
        "register(d1,1.0.0) 两次; register(d2,9.9.9)",
        f"重复注册不重置状态(counter={dup.report_counter}); "
        f"d2 被拒原因={exc.value.reason}",
    )
    assert exc.value.reason == RejectReason.UNKNOWN_VERSION.value
    assert first["registered"] is True


def test_unregistered_device_report_is_rejected(store):
    ack = store.apply_report(
        _report("ghost", ReportKind.HEARTBEAT, 1))
    show_case(
        "未注册设备上报",
        "heartbeat(device=ghost, seq=1)",
        f"accepted={ack.accepted}, reason={ack.reason.value}",
    )
    assert not ack.accepted
    assert ack.reason is RejectReason.UNKNOWN_DEVICE


# ------------------------------------------------------------------- duplicates

def test_duplicate_seq_is_rejected_and_not_double_counted(store):
    store.register_device("d1", "1.0.0")
    store.create_release("r1", "1.1.0", ["d1"])
    store.start_release("r1")
    store.pending_commands("d1")

    store.apply_report(_report("d1", ReportKind.UPDATE_STARTED, 1))
    ok1 = store.apply_report(
        _report("d1", ReportKind.UPDATE_SUCCEEDED, 2, "1.1.0"))
    ok2 = store.apply_report(
        _report("d1", ReportKind.UPDATE_SUCCEEDED, 2, "1.1.0"))

    release = store.release_snapshot("r1")
    show_case(
        "重复上报：同一成功事件投递两次",
        "update_succeeded(seq=2) x2",
        f"第一次 accepted={ok1.accepted}; 第二次 reason={ok2.reason.value}; "
        f"succeeded 计数={len(release['succeeded'])} (必须为 1)",
    )
    assert ok1.accepted
    assert not ok2.accepted
    assert ok2.reason is RejectReason.DUPLICATE
    assert len(release["succeeded"]) == 1


def test_report_id_idempotency(store):
    store.register_device("d1", "1.0.0")
    a = store.apply_report(
        _report("d1", ReportKind.HEARTBEAT, 1, report_id="h-1"))
    b = store.apply_report(
        _report("d1", ReportKind.HEARTBEAT, 2, report_id="h-1"))
    show_case(
        "幂等键：相同 report_id 重放",
        "heartbeat(report_id=h-1) 两次 (seq 1,2)",
        f"首次 accepted={a.accepted}; 重放 reason={b.reason.value}",
    )
    assert a.accepted
    assert not b.accepted
    assert b.reason is RejectReason.DUPLICATE


# ------------------------------------------------------------- out of ordering

def test_out_of_order_reports_do_not_regress_state(store):
    store.register_device("d1", "1.0.0")
    store.apply_report(_report("d1", ReportKind.HEARTBEAT, 3))
    stale = store.apply_report(_report("d1", ReportKind.HEARTBEAT, 2))
    future = store.apply_report(
        _report("d1", ReportKind.VERSION, 5, "1.1.0"))
    middle = store.apply_report(
        _report("d1", ReportKind.VERSION, 4, "1.0.5"))

    device = store.device_snapshot("d1")
    show_case(
        "乱序上报：seq 3 -> 2 -> 5 -> 4",
        "heartbeat@3, heartbeat@2(旧), version=1.1.0@5, version=1.0.5@4(旧)",
        f"seq2 拒绝原因={stale.reason.value}; seq4 拒绝原因={middle.reason.value}; "
        f"最终版本={device['firmware_version']} (必须停在 1.1.0, 不回退)",
    )
    assert not stale.accepted and stale.reason is RejectReason.STALE_COUNTER
    assert not middle.accepted and middle.reason is RejectReason.STALE_COUNTER
    assert future.accepted
    assert device["firmware_version"] == "1.1.0"
    assert device["report_counter"] == 5


# ------------------------------------------------------------------ concurrency

def test_concurrent_heartbeats_count_once(store):
    store.register_device("d1", "1.0.0")
    threads_results: list[ReportAck] = []
    barrier = threading.Barrier(20)
    # Atomic per-device sequence allocator, mirroring a real client that
    # stamps each outbound report under a send-lock.
    seq_lock = threading.Lock()
    counter = {"v": 0}

    def worker():
        barrier.wait()
        with seq_lock:
            counter["v"] += 1
            seq = counter["v"]
        ack = store.apply_report(_report("d1", ReportKind.HEARTBEAT, seq))
        threads_results.append(ack)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [a for a in threads_results if a.accepted]
    rejected = [a for a in threads_results if not a.accepted]
    device = store.device_snapshot("d1")
    show_case(
        "并发上报：20 线程同时心跳",
        "barrier 同时放行; 客户端原子领取 seq 后并发上报",
        f"accepted={len(accepted)}, rejected={len(rejected)}, "
        f"最终 counter={device['report_counter']} (必须恰好为 20), "
        f"状态={device['status']}",
    )
    assert len(accepted) == 20
    assert rejected == []
    assert device["report_counter"] == 20
    assert device["status"] == DeviceStatus.ONLINE.value


def test_concurrent_duplicate_update_events_count_once(store):
    store.register_device("d1", "1.0.0")
    store.create_release("r1", "1.1.0", ["d1"])
    store.start_release("r1")
    store.pending_commands("d1")
    store.apply_report(_report("d1", ReportKind.UPDATE_STARTED, 1))

    results: list[ReportAck] = []
    barrier = threading.Barrier(10)

    def worker():
        barrier.wait()
        results.append(store.apply_report(
            _report("d1", ReportKind.UPDATE_SUCCEEDED, 2, "1.1.0")))

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    accepted = [a for a in results if a.accepted]
    release = store.release_snapshot("r1")
    show_case(
        "并发重复：10 线程同时发同一成功事件 seq=2",
        "10 x update_succeeded(seq=2,1.1.0)",
        f"accepted={len(accepted)} (必须为 1), "
        f"succeeded 集合={sorted(release['succeeded'])}",
    )
    assert len(accepted) == 1
    assert len(release["succeeded"]) == 1


# ----------------------------------------------------------- version validation

def test_unknown_and_too_old_versions_are_not_silently_ignored(store):
    store.register_device("d1", "1.0.0")
    unknown = store.apply_report(
        _report("d1", ReportKind.VERSION, 1, "7.7.7"))
    malformed = store.apply_report(
        _report("d1", ReportKind.VERSION, 2, "not-a-version"))
    too_old = store.apply_report(
        _report("d1", ReportKind.VERSION, 3, "0.9.0"))
    ok = store.apply_report(
        _report("d1", ReportKind.VERSION, 4, "1.0.5"))

    device = store.device_snapshot("d1")
    show_case(
        "版本闸门：未知/非法/过旧版本上报",
        "version=7.7.7, not-a-version, 0.9.0, 1.0.5 (min=1.0.0)",
        f"7.7.7->{unknown.reason.value}; 非法->{malformed.reason.value}; "
        f"0.9.0->{too_old.reason.value}; 1.0.5 accepted={ok.accepted}; "
        f"最终版本={device['firmware_version']}",
    )
    assert not unknown.accepted
    assert unknown.reason is RejectReason.UNKNOWN_VERSION
    assert not malformed.accepted
    assert malformed.reason is RejectReason.UNKNOWN_VERSION
    assert not too_old.accepted
    assert too_old.reason is RejectReason.VERSION_TOO_OLD
    assert ok.accepted
    assert device["firmware_version"] == "1.0.5"


# ------------------------------------------------------- heartbeat does not mask

def test_heartbeat_does_not_clear_updating_or_failed(store):
    store.register_device("d1", "1.0.0")
    store.create_release("r1", "1.1.0", ["d1"])
    store.start_release("r1")
    store.pending_commands("d1")
    store.apply_report(_report("d1", ReportKind.UPDATE_STARTED, 1))
    hb = store.apply_report(_report("d1", ReportKind.HEARTBEAT, 2))
    assert store.device_snapshot("d1")["status"] == DeviceStatus.UPDATING.value
    store.apply_report(_report("d1", ReportKind.UPDATE_FAILED, 3))
    hb2 = store.apply_report(_report("d1", ReportKind.HEARTBEAT, 4))
    status = store.device_snapshot("d1")["status"]
    show_case(
        "心跳不能掩盖 更新中/失败 状态",
        "updating 后 heartbeat@2; failed 后 heartbeat@4",
        f"更新中心跳 accepted={hb.accepted} 状态保持 updating; "
        f"失败后心跳 accepted={hb2.accepted} 最终状态={status} (保持 failed)",
    )
    assert hb.accepted and hb2.accepted
    assert status == DeviceStatus.FAILED.value


# --------------------------------------------------------------- offline sweep

def test_offline_sweep(store):
    store.register_device("d1", "1.0.0")
    store._devices["d1"].last_seen = 0.0  # device last seen at t=0
    went = store.mark_offline_devices(now=10_000.0)
    device = store.device_snapshot("d1")
    show_case(
        "离线检测：超过 offline_timeout 未心跳",
        f"sweep at now=10000 (timeout=30s)",
        f"转离线设备={went}; 状态={device['status']}",
    )
    assert went == ["d1"]
    assert device["status"] == DeviceStatus.OFFLINE.value
