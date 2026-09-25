"""领域类型定义：设备、版本上报、固件发布、批次。"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class DeviceStatus(str, Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    UPDATING = "updating"
    UPDATE_FAILED = "update_failed"


class ReleaseStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    COMPLETED = "completed"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"


class BatchStatus(str, Enum):
    PENDING = "pending"
    ACTIVE = "active"
    HEALTHY = "healthy"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class VersionReport:
    device_id: str
    version: str
    release_id: str | None
    outcome: str  # success | failure | installed
    timestamp: float
    dedup_key: str


@dataclass
class Device:
    device_id: str
    current_version: str = ""
    last_known_good_version: str = ""
    status: DeviceStatus = DeviceStatus.OFFLINE
    last_heartbeat: float = 0.0
    active_release_id: str | None = None
    seen_report_keys: set[str] = field(default_factory=set)
    last_report_seq: int = 0
    registered_at: float = 0.0


@dataclass
class Batch:
    index: int
    device_ids: list[str]
    status: BatchStatus = BatchStatus.PENDING
    successes: int = 0
    failures: int = 0
    reported: set[str] = field(default_factory=set)


@dataclass
class FirmwareRelease:
    release_id: str
    target_version: str
    baseline_version: str
    batch_size: int
    healthy_ratio_threshold: float
    failure_ratio_threshold: float
    status: ReleaseStatus = ReleaseStatus.PENDING
    batches: list[Batch] = field(default_factory=list)
    active_batch_index: int | None = None
    created_at: float = 0.0
    history: list[str] = field(default_factory=list)
    rollback_reason: str = ""
    device_ids: list[str] = field(default_factory=list)
