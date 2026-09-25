"""本地设备模拟器。

一个 SimulatedDevice 实例代表一台设备；它在后台线程中周期性发送心跳，
并轮询自身状态。服务端把设备置为 updating 即表示“固件已下发”，设备按
fail_rate 随机成功/失败，再以带 dedup_key + seq 的上报回报结果。

不依赖任何真实硬件或外部服务，只与本机 HTTP API 交互。
"""
from __future__ import annotations

import itertools
import random
import threading
import time
from typing import Any

import httpx


class SimulatedDevice:
    def __init__(
        self,
        base_url: str,
        device_id: str,
        version: str,
        fail_rate: float = 0.0,
        heartbeat_interval: float = 0.2,
        poll_interval: float = 0.05,
        seed: int | None = None,
        verbose: bool = True,
        client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.device_id = device_id
        self.version = version
        self.fail_rate = fail_rate
        self.heartbeat_interval = heartbeat_interval
        self.poll_interval = poll_interval
        self.verbose = verbose
        self._rng = random.Random(seed)
        self._client = client or httpx.Client(base_url=self.base_url, timeout=5.0)
        self._owns_client = client is None
        self._seq = itertools.count(1)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._dispatched: set[str] = set()  # 已处理过下发的 release，防重复下发
        self.last_error: dict[str, Any] | None = None

    def log(self, msg: str) -> None:
        if self.verbose:
            print(f"[device {self.device_id}] {msg}", flush=True)

    # ---- 同步 API（也可单步调用，便于测试与演示脚本编排） ------------

    def register(self) -> None:
        r = self._client.post(
            "/devices/register", json={"device_id": self.device_id, "version": self.version}
        )
        if r.status_code == 409 and r.json()["detail"]["code"] == "device_already_registered":
            self.log("已注册过，跳过")
            return
        r.raise_for_status()
        self.log(f"注册成功，初始版本 {self.version}")

    def heartbeat_once(self) -> None:
        r = self._client.post("/devices/heartbeat", json={"device_id": self.device_id})
        r.raise_for_status()

    def poll_once(self) -> dict[str, Any] | None:
        """检查服务端是否向本设备下发了更新；若有则模拟刷写并上报。"""
        r = self._client.get(f"/devices/{self.device_id}")
        r.raise_for_status()
        me = r.json()
        if me["status"] != "updating":
            self._resync_if_needed(me)
            return None
        release_id = me["active_release_id"]
        if release_id is None or release_id in self._dispatched:
            return None
        return self._handle_dispatch(me)

    def _resync_if_needed(self, me: dict[str, Any]) -> None:
        """回滚后服务端版本可能已退回基线，本地按服务端权威版本对账。"""
        server_version = me["current_version"]
        if server_version == self.version:
            return
        seq = next(self._seq)
        dedup_key = f"{self.device_id}:resync:{seq}"
        self.log(f"检测到版本分歧（本地 {self.version} / 服务端 {server_version}），按服务端同步")
        self.version = server_version
        r = self._client.post(
            "/devices/report",
            json={
                "device_id": self.device_id,
                "version": server_version,
                "release_id": None,
                "outcome": "installed",
                "seq": seq,
                "dedup_key": dedup_key,
            },
        )
        r.raise_for_status()

    def _handle_dispatch(self, me: dict[str, Any]) -> dict[str, Any]:
        release_id = me["active_release_id"]
        rel = self._client.get(f"/releases/{release_id}")
        rel.raise_for_status()
        target = rel.json()["target_version"]
        self._dispatched.add(release_id)
        self.log(f"收到下发 {release_id}: {self.version} -> {target}")

        failed = self._rng.random() < self.fail_rate
        outcome = "failure" if failed else "success"
        # 成功才切版本；失败保持原版本（模拟刷写失败）。
        reported_version = self.version if failed else target
        seq = next(self._seq)
        dedup_key = f"{self.device_id}:{release_id}:{seq}"
        payload = {
            "device_id": self.device_id,
            "version": reported_version,
            "release_id": release_id,
            "outcome": outcome,
            "seq": seq,
            "dedup_key": dedup_key,
        }
        self.log(f"刷写结果: {outcome}（版本 {reported_version}），上报 seq={seq} key={dedup_key}")
        r = self._client.post("/devices/report", json=payload)
        if r.status_code >= 400:
            self.last_error = r.json().get("detail")
            self.log(f"上报被拒绝: {self.last_error}")
            return {"accepted": False, "detail": self.last_error, "payload": payload}
        body = r.json()
        if not failed:
            self.version = target
        return {"accepted": True, "outcome": outcome, "payload": payload, "response": body}

    # ---- 后台线程模式（模拟常驻设备进程） ------------------------------

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name=f"dev-{self.device_id}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)
        if self._owns_client:
            self._client.close()

    def _run(self) -> None:
        last_hb = 0.0
        while not self._stop.is_set():
            now = time.monotonic()
            try:
                if now - last_hb >= self.heartbeat_interval:
                    self.heartbeat_once()
                    last_hb = now
                self.poll_once()
            except Exception as exc:  # 模拟进程不退出，只记录
                self.log(f"循环异常: {exc}")
            time.sleep(self.poll_interval)
