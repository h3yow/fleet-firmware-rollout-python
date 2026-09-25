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
