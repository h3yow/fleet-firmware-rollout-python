"""HTTP API 集成测试：校验状态码与可区分的错误 code。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from fleetrollout.app import create_app


@pytest.fixture
def client():
    with TestClient(create_app()) as c:
        yield c


def _register(client, ids, version="1.0.0"):
    for did in ids:
        r = client.post("/devices/register", json={"device_id": did, "version": version})
        assert r.status_code == 200, r.text


def _create(client, rid, ids, **kw):
    payload = {
        "release_id": rid,
        "target_version": "2.0.0",
        "baseline_version": "1.0.0",
        "device_ids": ids,
    }
    payload.update(kw)
    r = client.post("/releases/create", json=payload)
    assert r.status_code == 200, r.text


def test_full_healthy_release_over_http(client):
    print("\n[输入] HTTP 注册 3 台 -> 建发布 -> start -> 3 条成功上报 -> advance")
    _register(client, ["d0", "d1", "d2"])
    _create(client, "r1", ["d0", "d1", "d2"], batch_size=3)
    assert client.post("/releases/r1/start", json={}).status_code == 200
    for i in range(3):
        r = client.post("/devices/report", json={
            "device_id": f"d{i}", "version": "2.0.0", "release_id": "r1",
            "outcome": "success", "seq": 1, "dedup_key": f"k{i}"})
        assert r.status_code == 200, r.text
    r = client.post("/releases/r1/advance", json={})
    body = r.json()["release"]
    print(f"[判定依据] 发布状态={body['status']}，批次状态={body['batches'][0]['status']}")
    assert body["status"] == "completed"


def test_http_error_codes_are_distinguishable(client):
    print("\n[输入] 覆盖重复注册/未知版本/占用冲突/未收齐推进/乱序上报的 HTTP 错误码")
    _register(client, ["d0", "d1", "d2"])

    r = client.post("/devices/register", json={"device_id": "d0", "version": "1.0.0"})
    assert r.status_code == 409 and r.json()["detail"]["code"] == "device_already_registered"
    print("[判定依据] 重复注册 -> 409 device_already_registered")

    r = client.post("/devices/register", json={"device_id": "x", "version": "9.9.9"})
    assert r.status_code == 422 and r.json()["detail"]["code"] == "unknown_version"
    print("[判定依据] 未知版本 -> 422 unknown_version")

    _create(client, "r1", ["d0", "d1", "d2"], batch_size=3)
    client.post("/releases/r1/start", json={})
    _create(client, "r2", ["d0"], batch_size=1)
    r = client.post("/releases/r2/start", json={})
    assert r.status_code == 409 and r.json()["detail"]["code"] == "device_busy"
    print("[判定依据] 设备占用冲突 -> 409 device_busy")

    r = client.post("/devices/report", json={
        "device_id": "d0", "version": "2.0.0", "release_id": "r1",
        "outcome": "success", "seq": 2, "dedup_key": "s2"})
    assert r.status_code == 200
    r = client.post("/devices/report", json={
        "device_id": "d0", "version": "2.0.0", "release_id": "r1",
        "outcome": "success", "seq": 1, "dedup_key": "s1"})
    assert r.status_code == 409 and r.json()["detail"]["code"] == "stale_report"
    print("[判定依据] 乱序上报 -> 409 stale_report")

    # 批次只收 1/3，不能推进
    r = client.post("/releases/r1/advance", json={})
    assert r.status_code == 409 and r.json()["detail"]["code"] == "batch_not_ready"
    print("[判定依据] 批次未收齐推进 -> 409 batch_not_ready")


def test_http_rollback_safety_valve_and_history(client):
    print("\n[输入] HTTP 3 台一批 2 台失败，收齐后自动回滚，检查历史与设备版本")
    _register(client, ["d0", "d1", "d2"])
    _create(client, "r1", ["d0", "d1", "d2"], batch_size=3, failure_ratio=0.3)
    client.post("/releases/r1/start", json={})
    reports = [
        ("d0", "2.0.0", "success"),
        ("d1", "1.0.0", "failure"),
        ("d2", "1.0.0", "failure"),
    ]
    for did, ver, outcome in reports:
        client.post("/devices/report", json={
            "device_id": did, "version": ver, "release_id": "r1",
            "outcome": outcome, "seq": 1, "dedup_key": did})
    rel = client.get("/releases/r1").json()
    print(f"[判定依据] status={rel['status']} reason={rel['rollback_reason']}")
    assert rel["status"] == "rolled_back"
    assert "失败率" in rel["rollback_reason"]
    assert rel["batches"][0]["status"] == "failed"
    # 历史可复现：包含创建、下发、3 条回报、回滚进入与完成
    joined = "\n".join(rel["history"])
    for token in ["创建发布", "下发", "更新失败", "回滚完成"]:
        assert token in joined, token
    print("[判定依据] 历史事件链完整，含失败与回滚记录")
    for did in ["d0", "d1", "d2"]:
        d = client.get(f"/devices/{did}").json()
        assert d["current_version"] == "1.0.0" and d["active_release_id"] is None
