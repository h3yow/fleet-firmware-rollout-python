"""全局配置项（均可通过环境变量覆盖）。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw is not None else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw is not None else default


@dataclass(frozen=True)
class Settings:
    offline_timeout_s: float = 30.0
    heartbeat_interval_s: float = 5.0
    default_batch_size: int = 3
    default_healthy_ratio: float = 0.8
    default_failure_ratio: float = 0.3
    min_reports_to_judge: int = 1
    min_supported_version: str = "1.0.0"


def load_settings() -> Settings:
    return Settings(
        offline_timeout_s=_env_float("ROLLOUT_OFFLINE_TIMEOUT_S", 30.0),
        heartbeat_interval_s=_env_float("ROLLOUT_HEARTBEAT_INTERVAL_S", 5.0),
        default_batch_size=_env_int("ROLLOUT_BATCH_SIZE", 3),
        default_healthy_ratio=_env_float("ROLLOUT_HEALTHY_RATIO", 0.8),
        default_failure_ratio=_env_float("ROLLOUT_FAILURE_RATIO", 0.3),
        min_reports_to_judge=_env_int("ROLLOUT_MIN_REPORTS", 1),
        min_supported_version=os.environ.get("ROLLOUT_MIN_VERSION", "1.0.0"),
    )
