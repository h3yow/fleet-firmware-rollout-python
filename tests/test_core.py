"""核心单元测试。

每个用例都打印【输入】与【判定依据】，并对业务错误的 code 做断言，
保证“失败原因可区分”。
"""
from __future__ import annotations

import concurrent.futures as cf

import pytest

from fleetrollout.errors import (
    BatchNotReadyError,
    DeviceBusyError,
    DeviceAlreadyRegisteredError,
    ReleaseNotFoundError,
    ReleaseStateError,
    StaleReportError,
    UnknownVersionError,
    UnsupportedVersionError,
)
from fleetrollout.models import BatchStatus, DeviceStatus, ReleaseStatus


# ---------- 设备注册 / 心跳 / 状态 ------------------------------------


def test_duplicate_registration_rejected(services):
    print("\n[输入] 同一 device_id='d0' version='1.0.0' 连续注册两次")
    services.devices.register("d0", "1.0.0", 0.0)
    with pytest.raises(DeviceAlreadyRegisteredError) as exc:
        services.devices.register("d0", "1.0.0", 1.0)
    print(f"[判定依据] 第二次注册抛出 code={exc.value.code}")
    assert exc.value.code == "device_already_registered"


def test_heartbeat_marks_offline_back_online_without_state_rollback(services):
    print("\n[输入] d0 注册后超过超时时间无心跳，再心跳；随后置为更新失败后再心跳")
    services.devices.register("d0", "1.0.0", 0.0)
    status = services.devices.refresh_status("d0", 100.0)
    print(f"[判定依据] 心跳超时 -> {status.value}")
    assert status == DeviceStatus.OFFLINE

    d = services.devices.heartbeat("d0", 101.0)
    assert d.status == DeviceStatus.ONLINE

    d.status = DeviceStatus.UPDATE_FAILED
    d = services.devices.heartbeat("d0", 102.0)
    print(f"[判定依据] 更新失败状态下心跳 -> {d.status.value}（不得回退）")
    assert d.status == DeviceStatus.UPDATE_FAILED


def test_unknown_and_too_old_versions_not_silently_ignored(services):
    print("\n[输入] 注册未知版本 9.9.9；设备上报过旧版本 0.9.0（最低支持 1.0.0）")
    with pytest.raises(UnknownVersionError) as e1:
        services.devices.register("x", "9.9.9", 0.0)
    print(f"[判定依据] 未知版本 -> code={e1.value.code}")
    services.devices.register("d0", "1.0.0", 0.0)
    with pytest.raises(UnsupportedVersionError) as e2:
        services.devices.report_version("d0", "0.9.0", None, "installed", 1.0, seq=1)
    print(f"[判定依据] 过旧版本 -> code={e2.value.code}")
    assert services.devices.get("d0").current_version == "1.0.0"


# ---------- 重复 / 乱序 / 并发上报 ------------------------------------


def test_duplicate_report_with_same_dedup_key_counts_once(services):
    print("\n[输入] d0..d2 参与发布，同一 dedup_key 上报成功 3 次")
    for i in range(3):
        services.devices.register(f"d{i}", "1.0.0", 0.0)
    services.releases.create_release("r1", "2.0.0", "1.0.0",
                                     [f"d{i}" for i in range(3)], 0.0)
    services.releases.start("r1", 0.0)
    for _ in range(3):
        services.devices.report_version(
            "d0", "2.0.0", "r1", "success", 1.0, seq=1, dedup_key="same-key")
    batch = services.releases.get_release("r1").batches[0]
    print(f"[判定依据] 批次计数 successes={batch.successes}（期望 1），reported={len(batch.reported)}")
    assert batch.successes == 1
    assert len(batch.reported) == 1
    assert services.devices.get("d0").last_report_seq == 1


def test_out_of_order_report_rejected(services):
    print("\n[输入] 先以 seq=2 成功上报，再以 seq=1 迟到上报另一个结果")
    services.devices.register("d0", "1.0.0", 0.0)
    services.devices.report_version("d0", "1.1.0", None, "installed", 2.0, seq=2, dedup_key="s2")
    with pytest.raises(StaleReportError) as exc:
        services.devices.report_version("d0", "1.0.0", None, "installed", 1.0, seq=1, dedup_key="s1")
    print(f"[判定依据] seq=1 的旧报文 -> code={exc.value.code}；版本保持 {services.devices.get('d0').current_version}")
    assert exc.value.code == "stale_report"
    assert services.devices.get("d0").current_version == "1.1.0"


def test_concurrent_duplicate_reports_no_double_counting(services):
    print("\n[输入] 20 台设备，每台 8 线程并发以相同 dedup_key 上报成功")
    n = 20
    for i in range(n):
        services.devices.register(f"d{i}", "1.0.0", 0.0)
    services.releases.create_release(
        "r1", "2.0.0", "1.0.0", [f"d{i}" for i in range(n)], 0.0,
        batch_size=n, failure_ratio=0.99)
    services.releases.start("r1", 0.0)

    def one(i):
        return services.devices.report_version(
            f"d{i}", "2.0.0", "r1", "success", 1.0, seq=1, dedup_key=f"k{i}")

    with cf.ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(one, list(range(n)) * 8))

    batch = services.releases.get_release("r1").batches[0]
    print(f"[判定依据] successes={batch.successes} failures={batch.failures}（期望 {n}/0）")
    assert batch.successes == n
    assert batch.failures == 0
    assert len(batch.reported) == n


def test_concurrent_conflicting_release_starts_never_double_assign(services):
    print("\n[输入] d0..d11 已被 rel-1 占用；8 线程并发启动仅含 d10 的第二个发布")
    for i in range(12):
        services.devices.register(f"d{i}", "1.0.0", 0.0)
    services.releases.create_release(
        "r1", "2.0.0", "1.0.0", [f"d{i}" for i in range(12)], 0.0, batch_size=10)
    services.releases.start("r1", 0.0)
    services.releases.create_release("r2", "2.0.0", "1.0.0", ["d10"], 0.0)

    codes: list[str] = []
    def try_start(_):
        try:
            services.releases.start("r2", 0.0)
        except DeviceBusyError as exc:
            codes.append(exc.code)

    with cf.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(try_start, range(8)))
    print(f"[判定依据] {len(codes)}/8 次并发启动被拒, code 集合={set(codes)}")
    assert len(codes) == 8
    assert set(codes) == {"device_busy"}
    assert services.store.devices()["d10"].active_release_id == "r1"


def test_report_unknown_release_is_not_silent(services):
    print("\n[输入] 设备上报关联一个不存在的发布 nope")
    services.devices.register("d0", "1.0.0", 0.0)
    with pytest.raises(ReleaseNotFoundError) as exc:
        services.devices.report_version("d0", "2.0.0", "nope", "success", 1.0, seq=1, dedup_key="x")
    print(f"[判定依据] code={exc.value.code}")
    assert exc.value.code == "release_not_found"


# ---------- 批次边界 --------------------------------------------------


def test_cannot_advance_before_batch_fully_reported(services):
    print("\n[输入] 4 台分 2 批（每批 2）；批次 0 只有 1/2 成功就尝试推进")
    for i in range(4):
        services.devices.register(f"d{i}", "1.0.0", 0.0)
    services.releases.create_release("r1", "2.0.0", "1.0.0",
                                     [f"d{i}" for i in range(4)], 0.0, batch_size=2)
    services.releases.start("r1", 0.0)
    services.devices.report_version("d0", "2.0.0", "r1", "success", 1.0, seq=1, dedup_key="a")
    with pytest.raises(BatchNotReadyError) as exc:
        services.releases.advance("r1", 2.0)
    print(f"[判定依据] 推进被拒 code={exc.value.code}；批次 1 设备 d2 状态="
          f"{services.devices.get('d2').status.value}（不得提前下发）")
    assert exc.value.code == "batch_not_ready"
    assert services.devices.get("d2").status == DeviceStatus.ONLINE


def test_healthy_threshold_boundary(services):
    print("\n[输入] 4 台一批，健康阈值=0.75：3/4 成功恰好达标可推进；2/4 不可推进")
    # 子场景 1：3 成功 1 失败 = 0.75 健康、0.25 失败 -> 达标
    for i in range(4):
        services.devices.register(f"a{i}", "1.0.0", 0.0)
    services.releases.create_release(
        "ra", "2.0.0", "1.0.0", [f"a{i}" for i in range(4)], 0.0,
        batch_size=4, healthy_ratio=0.75, failure_ratio=0.5)
    services.releases.start("ra", 0.0)
    for i, outcome in enumerate(["success", "success", "success", "failure"]):
        services.devices.report_version(
            f"a{i}", "2.0.0" if outcome == "success" else "1.0.0",
            "ra", outcome, 1.0, seq=1, dedup_key=f"a{i}")
    rel = services.releases.advance("ra", 2.0)
    print(f"[判定依据] 3/4 成功，健康率=0.75>=0.75 且失败率=0.25<=0.5 -> {rel.status.value}")
    assert rel.status == ReleaseStatus.COMPLETED

    # 子场景 2：2 成功 2 失败 = 0.5 健康，失败率 0.5（阈值 0.5，>0.5 才回滚）-> 不达标也不回滚
    for i in range(4):
        services.devices.register(f"b{i}", "1.0.0", 0.0)
    services.releases.create_release(
        "rb", "2.0.0", "1.0.0", [f"b{i}" for i in range(4)], 0.0,
        batch_size=4, healthy_ratio=0.75, failure_ratio=0.5)
    services.releases.start("rb", 0.0)
    for i, outcome in enumerate(["success", "success", "failure", "failure"]):
        services.devices.report_version(
            f"b{i}", "2.0.0" if outcome == "success" else "1.0.0",
            "rb", outcome, 1.0, seq=1, dedup_key=f"b{i}")
    with pytest.raises(BatchNotReadyError) as exc:
        services.releases.advance("rb", 2.0)
    print(f"[判定依据] 2/4 成功健康率=0.5<0.75 且失败率 0.5 未超阈值 -> code={exc.value.code}，保持在途")
    assert exc.value.code == "batch_not_ready"
    assert services.releases.get_release("rb").status == ReleaseStatus.IN_PROGRESS


# ---------- 失败率回滚 / 安全阀 ---------------------------------------


def test_failure_ratio_triggers_auto_rollback(services):
    print("\n[输入] 3 台一批，2 台失败（失败率 0.667 > 阈值 0.3）")
    for i in range(3):
        services.devices.register(f"d{i}", "1.0.0", 0.0)
    services.releases.create_release("r1", "2.0.0", "1.0.0",
                                     [f"d{i}" for i in range(3)], 0.0, failure_ratio=0.3)
    services.releases.start("r1", 0.0)
    services.devices.report_version("d0", "2.0.0", "r1", "success", 1.0, seq=1, dedup_key="0")
    services.devices.report_version("d1", "1.0.0", "r1", "failure", 1.0, seq=1, dedup_key="1")
    rel = services.releases.get_release("r1")
    print(f"[判定依据] 仅 2/3 回报时状态={rel.status.value}（不提前下结论）")
    assert rel.status == ReleaseStatus.IN_PROGRESS
    services.devices.report_version("d2", "1.0.0", "r1", "failure", 1.0, seq=1, dedup_key="2")
    rel = services.releases.get_release("r1")
    print(f"[判定依据] 收齐后失败率 2/3=67%>30% -> 状态={rel.status.value}，原因={rel.rollback_reason}")
    assert rel.status == ReleaseStatus.ROLLED_BACK
    assert rel.batches[0].status == BatchStatus.FAILED
    for i in range(3):
        d = services.devices.get(f"d{i}")
        assert d.current_version == "1.0.0"
        assert d.last_known_good_version == "1.0.0"
        assert d.active_release_id is None


def test_late_report_after_rollback_rejected_and_versions_stay_consistent(services):
    print("\n[输入] 回滚完成后，迟到一条目标版本成功上报 seq=9")
    for i in range(3):
        services.devices.register(f"d{i}", "1.0.0", 0.0)
    services.releases.create_release("r1", "2.0.0", "1.0.0",
                                     [f"d{i}" for i in range(3)], 0.0, failure_ratio=0.3)
    services.releases.start("r1", 0.0)
    for i, outcome in enumerate(["failure", "failure", "success"]):
        services.devices.report_version(
            f"d{i}", "2.0.0" if outcome == "success" else "1.0.0",
            "r1", outcome, 1.0, seq=1, dedup_key=f"k{i}")
    assert services.releases.get_release("r1").status == ReleaseStatus.ROLLED_BACK
    with pytest.raises(StaleReportError) as exc:
        services.devices.report_version(
            "d2", "2.0.0", "r1", "success", 2.0, seq=9, dedup_key="late")
    print(f"[判定依据] 迟到上报 -> code={exc.value.code}")
    assert exc.value.code == "stale_report"
    assert services.devices.get("d2").current_version == "1.0.0"


def test_manual_rollback_restores_baseline_and_skips_remaining(services):
    print("\n[输入] 6 台分 2 批，第一批健康，手动回滚；第二批应被跳过且全员恢复基线")
    for i in range(6):
        services.devices.register(f"d{i}", "1.0.0", 0.0)
    services.releases.create_release("r1", "2.0.0", "1.0.0",
                                     [f"d{i}" for i in range(6)], 0.0, batch_size=3)
    services.releases.start("r1", 0.0)
    for i in range(3):
        services.devices.report_version(f"d{i}", "2.0.0", "r1", "success", 1.0, seq=1, dedup_key=f"{i}")
    services.releases.advance("r1", 2.0)
    rel = services.releases.rollback("r1", 3.0, "人工中止")
    print(f"[判定依据] 手动回滚后状态={rel.status.value}；批次1状态={rel.batches[1].status.value}")
    assert rel.status == ReleaseStatus.ROLLED_BACK
    assert rel.batches[1].status == BatchStatus.SKIPPED
    versions = {d.device_id: d.current_version for d in services.devices.list_devices()}
    assert all(v == "1.0.0" for v in versions.values()), versions
    with pytest.raises(ReleaseStateError) as exc:
        services.releases.advance("r1", 4.0)
    print(f"[判定依据] 回滚后再推进 -> code={exc.value.code}")


def test_device_cannot_join_second_concurrent_release(services):
    print("\n[输入] r1 在途时尝试把 d0 纳入 r2")
    for i in range(3):
        services.devices.register(f"d{i}", "1.0.0", 0.0)
    services.releases.create_release("r1", "2.0.0", "1.0.0",
                                     [f"d{i}" for i in range(3)], 0.0)
    services.releases.start("r1", 0.0)
    services.releases.create_release("r2", "2.0.0", "1.0.0", ["d0"], 0.0)
    with pytest.raises(DeviceBusyError) as exc:
        services.releases.start("r2", 0.0)
    print(f"[判定依据] code={exc.value.code}，d0 仍归属 r1")
    assert exc.value.code == "device_busy"
    assert services.devices.get("d0").active_release_id == "r1"
