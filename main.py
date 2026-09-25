"""本地服务入口：uv run python main.py

默认监听 127.0.0.1:8000；端口被占用时可用环境变量覆盖：
    ROLLOUT_PORT=8011 uv run python main.py
"""
from __future__ import annotations

import os

import uvicorn

from fleetrollout.app import create_app

app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("ROLLOUT_PORT", "8000"))
    host = os.environ.get("ROLLOUT_HOST", "127.0.0.1")
    uvicorn.run(app, host=host, port=port)
