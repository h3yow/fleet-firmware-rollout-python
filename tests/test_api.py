"""End-to-end HTTP checks through the FastAPI app on localhost TestClient."""

from __future__ import annotations

from fastapi.testclient import TestClient

from fleet.api import create_app

from conftest import show_case


def _service():
    from fleet.models import RolloutConfig
    from fleet.service import RolloutService
    svc = RolloutService(RolloutConfig(batch_percentages=(100,)))
    return svc


def test_api_full_flow_and_distinct_rejection_reasons():
    client = TestClient(create_app(_service()))

    r = client.post("/devices", json={"device_id": "d1", "version": "1.0.0"})
    assert r.status_code == 200

    # unknown version at registration -> 409 unknown_version
    r = client.post("/devices", json={"device_id": "x", "version": "5.0.0"})
    assert r.status_code == 409
    reg_reason = r.json()["detail"]["reason"]

    # duplicate heartbeat -> 422 duplicate
    client.post("/devices/d1/reports",
                json={"kind": "heartbeat", "seq": 1})
    r = client.post("/devices/d1/reports",
                    json={"kind": "heartbeat", "seq": 1})
    dup_reason = r.json()["reason"]

    # stale counter -> 422 stale_counter
    r = client.post("/devices/d1/reports",
                    json={"kind": "heartbeat", "seq": 9})
    assert r.status_code == 200
    r = client.post("/devices/d1/reports",
                    json={"kind": "heartbeat", "seq": 2})
    stale_reason = r.json()["reason"]

    # too-old version -> 422 version_too_old
    r = client.post("/devices/d1/reports",
                    json={"kind": "version", "seq": 10, "version": "0.9.0"})
    old_reason = r.json()["reason"]

    show_case(
        "HTTP 拒绝原因可区分",
        "未知版本注册 / 重复心跳 / 旧 seq / 过旧版本上报",
        f"409 reason={reg_reason}; 422 reasons={dup_reason}, "
        f"{stale_reason}, {old_reason} (四者必须互不相同)",
    )
    assert len({reg_reason, dup_reason, stale_reason, old_reason}) == 4

    # rollout lifecycle over HTTP
    r = client.post("/releases", json={
        "release_id": "r1", "to_version": "1.1.0", "device_ids": ["d1"]})
    assert r.status_code == 200
    r = client.post("/releases/r1/start")
    assert r.status_code == 200 and len(r.json()["dispatched"]) == 1
    client.post("/devices/d1/reports",
                json={"kind": "update_started", "seq": 11})
    client.post("/devices/d1/reports",
                json={"kind": "update_succeeded", "seq": 12,
                      "version": "1.1.0"})
    r = client.post("/releases/r1/advance")
    assert r.status_code == 200 and r.json()["state"] == "completed"
    snap = client.get("/devices/d1").json()
    assert snap["firmware_version"] == "1.1.0"
    show_case(
        "HTTP 全流程",
        "注册->心跳->创建发布->启动->更新成功->收尾",
        f"最终设备版本={snap['firmware_version']}, 状态={snap['status']}",
    )


def test_api_rollback_endpoint():
    client = TestClient(create_app(_service()))
    client.post("/devices", json={"device_id": "d1", "version": "1.0.0"})
    client.post("/releases", json={
        "release_id": "r1", "to_version": "1.1.0", "device_ids": ["d1"]})
    client.post("/releases/r1/start")
    client.post("/devices/d1/reports",
                json={"kind": "update_started", "seq": 1})
    client.post("/devices/d1/reports",
                json={"kind": "update_succeeded", "seq": 2,
                      "version": "1.1.0"})
    r = client.post("/releases/r1/rollback", json={"reason": "manual"})
    assert r.status_code == 200 and r.json()["state"] == "rolled_back"
    cmd = r.json()["dispatched"][0]
    client.post("/devices/d1/reports",
                json={"kind": "rollback_done", "seq": 3,
                      "version": cmd["version"]})
    snap = client.get("/devices/d1").json()
    show_case(
        "HTTP 手动回滚",
        "升到 1.1.0 后 /rollback, 设备回传 rollback_done",
        f"下发命令={cmd['command']}->{cmd['version']}; "
        f"最终版本={snap['firmware_version']}",
    )
    assert snap["firmware_version"] == "1.0.0"


def test_api_multi_plan_admission_listing_and_cancel():
    from fleet.models import RolloutConfig
    from fleet.service import RolloutService

    svc = RolloutService(RolloutConfig(batch_percentages=(100,),
                                       max_active_releases=1))
    client = TestClient(create_app(svc))
    for d in ("d1", "d2", "d3"):
        client.post("/devices", json={"device_id": d, "version": "1.0.0"})

    client.post("/releases", json={
        "release_id": "r1", "to_version": "1.1.0",
        "device_ids": ["d1", "d2"]})
    r = client.post("/releases/r1/start")
    assert r.status_code == 200

    # overlapping start -> 409 device_busy with structured details
    client.post("/releases", json={
        "release_id": "r2", "to_version": "1.2.0", "device_ids": ["d2"]})
    r = client.post("/releases/r2/start")
    body = r.json()["detail"]
    show_case(
        "HTTP 冲突准入",
        "r1 占用 d1,d2; start r2=[d2] (上限=1 之外也有设备冲突)",
        f"status={r.status_code} reason={body['reason']} "
        f"details={body.get('details')}",
    )
    assert r.status_code == 409
    assert body["reason"] in ("device_busy", "active_release_limit")

    # listing reports one in-progress plan and is self-consistent
    r = client.get("/releases")
    data = r.json()
    show_case(
        "HTTP 计划列表与汇总",
        "GET /releases（含进行中与已结束计划）",
        f"进行中={data['in_progress_count']}/{data['max_active_releases']}; "
        f"每计划字段含 batch/比例/gate/consistent",
    )
    assert data["in_progress_count"] == 1
    r1 = next(e for e in data["releases"] if e["release_id"] == "r1")
    assert r1["consistent"] is True
    for key in ("current_batch", "batch_health_rate", "batch_failure_rate",
                "overall_health_rate", "overall_failure_rate", "gate",
                "state", "succeeded", "failed"):
        assert key in r1

    # state filter works too
    pending = client.get("/releases", params={"state": "pending"}).json()
    assert [e["release_id"] for e in pending["releases"]] == ["r2"]

    # single-plan summary endpoint
    r = client.get("/releases/r1/summary")
    assert r.status_code == 200 and r.json()["gate"] in (
        "waiting_for_reports", "final_batch_healthy_completable")

    # cancel r1 and the devices are released
    r = client.post("/releases/r1/cancel", json={"reason": "ops stop"})
    assert r.status_code == 200 and r.json()["state"] == "cancelled"
    assert set(r.json()["released_devices"]) == {"d1", "d2"}
    r = client.post("/releases/r2/start")
    show_case(
        "HTTP 取消后释放复用",
        "cancel r1 后启动之前被拒的 r2=[d2]",
        f"status={r.status_code}; r2 state={r.json().get('state')}",
    )
    assert r.status_code == 200
