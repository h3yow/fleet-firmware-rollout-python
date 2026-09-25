#!/usr/bin/env python3
"""Local end-to-end demonstration of staged rollout + safety-valve rollback.

Everything runs in this one process with simulated device clients - no
network calls, hardware or credentials required.

Run:  uv run python scripts/demo.py
"""

from __future__ import annotations

import pathlib
import sys
import threading

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from fleet.client import DeviceClient
from fleet.models import RolloutConfig
from fleet.service import RolloutService
from fleet.store import RolloutError


def heading(text: str) -> None:
    print("\n" + "=" * 68)
    print(text)
    print("=" * 68)


def main() -> None:
    # 10 devices, two batches (50% -> 100%); safety valve at 30% failures,
    # advance gate at 80% healthy.
    svc = RolloutService(RolloutConfig(
        batch_percentages=(50, 100),
        health_threshold=0.8,
        failure_threshold=0.3,
    ))
    store = svc.store

    heading("1) 注册 10 台设备 (初始版本 1.0.0)")
    clients: dict[str, DeviceClient] = {}
    for i in range(10):
        # devices 3 and 4 in batch 1 are programmed to fail the update
        fail_rate = 1.0 if i in (3, 4) else 0.0
        clients[f"d{i}"] = svc.provision_device(
            f"d{i}", "1.0.0", fail_rate=fail_rate, seed=i)
    print("已注册:", [c.device_id for c in clients.values()])

    heading("2) 创建并启动发布 r1: 1.0.0 -> 1.1.0 (批次 50%, 100%)")
    store.create_release("r1", "1.1.0", [f"d{i}" for i in range(10)])
    first = store.start_release("r1")
    print("首批下发:", sorted(i.device_id for i in first))

    heading("3) 首批设备并发执行更新 (d3,d4 被配置为必然失败)")
    barrier = threading.Barrier(5)

    def run_concurrently(device_id: str) -> None:
        barrier.wait()  # release all five device "processes" at once
        clients[device_id].run_commands()

    threads = [threading.Thread(target=run_concurrently, args=(f"d{i}",))
               for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    release = store.release_snapshot("r1")
    print(f"发布状态: {release['state']}")
    print(f"成功: {sorted(release['succeeded'])}")
    print(f"失败: {sorted(release['failed'])}")
    trigger = next(e for e in release["history"]
                   if e["event"] == "rollback_triggered")
    print(f"判定: 失败率 2/5=0.40 > 阈值 0.30 -> {trigger['reason']}")

    heading("4) 成功设备并发执行回滚命令 (失败设备已就地解决)")
    barrier = threading.Barrier(len(release["succeeded"]))
    threads = [
        threading.Thread(target=run_concurrently, args=(d,))
        for d in sorted(release["succeeded"])
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    heading("5) 最终设备版本 (必须全部回到 1.0.0, 状态自洽)")
    for i in range(10):
        snap = store.device_snapshot(f"d{i}")
        print(f"  {snap['device_id']}: version={snap['firmware_version']:<7} "
              f"status={snap['status']:<8} release={snap['active_release_id']}")
    release = store.release_snapshot("r1")
    print(f"已回滚设备: {sorted(release['rolled_back_devices'])}")

    heading("6) 已回滚发布禁止再推进")
    try:
        store.advance_release("r1")
    except RolloutError as exc:
        print(f"advance 被正确拒绝: reason={exc.reason} ({exc.message})")

    heading("7) 发布历史 (追加写, 可复现)")
    for event in release["history"]:
        print(" ", event)


if __name__ == "__main__":
    main()
