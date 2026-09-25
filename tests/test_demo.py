"""端到端演示冒烟测试：以子进程运行 simulator.run_demo（真实本机 HTTP）。"""
from __future__ import annotations

import subprocess
import sys


def test_run_demo_end_to_end():
    print("\n[输入] 运行 `python -m simulator.run_demo`（随机端口、多线程模拟设备）")
    proc = subprocess.run(
        [sys.executable, "-m", "simulator.run_demo"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    print(proc.stdout[-2000:])
    print(f"[判定依据] 退出码={proc.returncode}")
    if proc.returncode != 0:
        print(proc.stderr)
    assert proc.returncode == 0
    assert "全部本地演示断言通过" in proc.stdout
    assert "触发安全阀" in proc.stdout
