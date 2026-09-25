"""端到端本地演示（无需任何外部依赖）。

启动一个绑定随机端口的本机 uvicorn 服务，在同一进程里用多线程模拟设备：
  场景 A：3 台设备一批，全部健康，发布完成；
  场景 B：6 台设备分两批，第一批失败率超阈值，安全阀自动回滚，
          随后设备与服务端版本完成对账，并验证迟到/乱序上报被拒绝。

运行：uv run python -m simulator.run_demo
"""
from __future__ import annotations

import socket
import threading
import time

import httpx
import uvicorn

from fleetrollout.app import create_app
from simulator.device_client import SimulatedDevice


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_server() -> tuple[uvicorn.Server, str]:
    port = _free_port()
    config = uvicorn.Config(
        create_app(), host="127.0.0.1", port=port, log_level="warning", lifespan="off"
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{port}"
    # 等待端口就绪
    for _ in range(100):
        try:
            if httpx.get(f"{base_url}/healthz", timeout=0.5).status_code == 200:
                break
        except httpx.TransportError:
            time.sleep(0.02)
    else:
        raise RuntimeError("本地演示服务启动失败")
    return server, base_url


def _wait_batch_settled(client: httpx.Client, release_id: str, expected: int, timeout: float = 5.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(f"/releases/{release_id}")
        r.raise_for_status()
        rel = r.json()
        b = rel["batches"][rel["active_batch_index"]] if rel["active_batch_index"] is not None else rel["batches"][0]
        if b["reported"] >= expected:
            return rel
        time.sleep(0.02)
    raise TimeoutError("批次回报等待超时")


def _print_release(title: str, rel: dict) -> None:
    print(f"\n===== {title} =====")
    print(f"发布 {rel['release_id']}: {rel['baseline_version']} -> {rel['target_version']} "
          f"状态={rel['status']} 回滚原因={rel['rollback_reason'] or '-'}")
    for b in rel["batches"]:
        print(f"  批次{b['index']} {b['status']}: {b['reported']}/{b['size']} 回报, "
              f"成功 {b['successes']}, 失败 {b['failures']}")


def main() -> None:
    server, base_url = _start_server()
    print(f"本地服务已启动: {base_url}")

    try:
        # ---------------- 场景 A：健康发布 ----------------
        with httpx.Client(base_url=base_url, timeout=5.0) as client:
            devices_a = [
                SimulatedDevice(base_url, f"a{i}", "1.0.0", fail_rate=0.0,
                                seed=i, client=client, verbose=True)
                for i in range(3)
            ]
            for d in devices_a:
                d.register()
                d.start()

            print("\n输入: 创建发布 rel-a（1.0.0 -> 2.0.0，3 台 1 批，健康阈值 0.8）")
            client.post("/releases/create", json={
                "release_id": "rel-a", "target_version": "2.0.0",
                "baseline_version": "1.0.0", "device_ids": [f"a{i}" for i in range(3)],
                "batch_size": 3, "healthy_ratio": 0.8,
            }).raise_for_status()
            client.post("/releases/rel-a/start", json={}).raise_for_status()
            print("判定依据: 等待批次 0 收齐 3/3 回报")
            rel = _wait_batch_settled(client, "rel-a", 3)
            adv = client.post("/releases/rel-a/advance", json={})
            print(f"推进响应 code={adv.status_code}")
            rel = client.get("/releases/rel-a").json()
            _print_release("场景 A 结果", rel)
            assert rel["status"] == "completed"
            for d in devices_a:
                d.stop()

        # ---------------- 场景 B：失败率触发回滚 ----------------
        with httpx.Client(base_url=base_url, timeout=5.0) as client:
            # b0,b1 必失败（fail_rate=1.0），b2 成功 => 失败率 2/3 > 0.3
            rates = {"b0": 1.0, "b1": 1.0, "b2": 0.0, "b3": 0.0, "b4": 0.0, "b5": 0.0}
            devices_b = [
                SimulatedDevice(base_url, name, "1.0.0", fail_rate=rates[name],
                                seed=hash(name) & 0xFFFF, client=client, verbose=True)
                for name in rates
            ]
            for d in devices_b:
                d.register()
                d.start()

            print("\n输入: 创建发布 rel-b（1.0.0 -> 2.0.0，6 台每批 3，失败阈值 0.3）")
            client.post("/releases/create", json={
                "release_id": "rel-b", "target_version": "2.0.0",
                "baseline_version": "1.0.0", "device_ids": list(rates),
                "batch_size": 3, "failure_ratio": 0.3,
            }).raise_for_status()
            client.post("/releases/rel-b/start", json={}).raise_for_status()
            print("判定依据: 等待批次 0 收齐 3/3；若失败率 > 0.30 自动回滚")
            rel = _wait_batch_settled(client, "rel-b", 3)
            time.sleep(0.3)  # 等待设备完成回滚对账
            rel = client.get("/releases/rel-b").json()
            _print_release("场景 B 结果（安全阀）", rel)
            assert rel["status"] == "rolled_back", rel["status"]
            assert rel["batches"][0]["status"] == "failed"
            assert rel["batches"][1]["status"] == "skipped"

            # 历史自洽：所有设备版本回到基线
            devices = client.get("/devices").json()["devices"]
            for d in devices:
                if d["device_id"].startswith("b"):
                    assert d["current_version"] == "1.0.0", d
                    assert d["active_release_id"] is None
            print("版本自洽检查: 6 台设备 current_version 均为 1.0.0 且无在途发布")

            # 迟到的成功上报必须被拒绝（失败原因可区分）
            late = client.post("/devices/report", json={
                "device_id": "b2", "version": "2.0.0", "release_id": "rel-b",
                "outcome": "success", "seq": 999, "dedup_key": "late-1",
            })
            print(f"迟到上报判定: HTTP {late.status_code} code={late.json()['detail']['code']}")
            assert late.status_code == 409 and late.json()["detail"]["code"] == "stale_report"

            # 已回滚发布不能再推进
            adv = client.post("/releases/rel-b/advance", json={})
            print(f"回滚后推进判定: HTTP {adv.status_code} code={adv.json()['detail']['code']}")
            assert adv.json()["detail"]["code"] == "release_state_error"

            for d in devices_b:
                d.stop()

        print("\n发布历史（rel-b）:")
        with httpx.Client(base_url=base_url) as client:
            for line in client.get("/releases/rel-b").json()["history"]:
                print("  " + line)

        print("\n全部本地演示断言通过。")
    finally:
        server.should_exit = True


if __name__ == "__main__":
    main()
