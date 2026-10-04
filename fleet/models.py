"""Domain types for the fleet firmware rollout service.

Only plain data classes and enums live here; all state-machine logic lives in
:mod:`fleet.store` and is protected by a single lock so that concurrent,
duplicate and out-of-order reports are handled deterministically.
"""

from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field
from typing import Optional


class DeviceStatus(str, enum.Enum):
    """Lifecycle state of a single device."""

    UNKNOWN = "unknown"
    ONLINE = "online"
    OFFLINE = "offline"
    UPDATING = "updating"
    FAILED = "failed"


class ReleaseState(str, enum.Enum):
    """State of a batched firmware release."""

    PENDING = "pending"
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ROLLED_BACK = "rolled_back"
    CANCELLED = "cancelled"


#: States in which a release still holds its devices and blocks admission of
#: overlapping releases. Everything else (completed / rolled_back / cancelled)
#: is terminal and releases its devices for reuse.
IN_PROGRESS_STATES = frozenset({
    ReleaseState.PENDING,
    ReleaseState.ACTIVE,
    ReleaseState.PAUSED,
})


class ReportKind(str, enum.Enum):
    """Kinds of reports a device client may send."""

    REGISTER = "register"
    HEARTBEAT = "heartbeat"
    VERSION = "version"
    UPDATE_STARTED = "update_started"
    UPDATE_SUCCEEDED = "update_succeeded"
    UPDATE_FAILED = "update_failed"
    ROLLBACK_DONE = "rollback_done"


class RejectReason(str, enum.Enum):
    """Why a report was rejected instead of changing state.

    Distinct values let tests assert *why* a report was refused, rather than
    only asserting that it was refused.
    """

    UNKNOWN_DEVICE = "unknown_device"
    UNKNOWN_VERSION = "unknown_version"
    VERSION_TOO_OLD = "version_too_old"
    STALE_COUNTER = "stale_counter"
    DUPLICATE = "duplicate"
    NOT_EXPECTED = "not_expected"


class EventKind(str, enum.Enum):
    """Audit events recorded in a release's history (append-only)."""

    CREATED = "created"
    STARTED = "started"
    CANCELLED = "cancelled"
    DISPATCHED = "dispatched"
    BATCH_ADVANCED = "batch_advanced"
    PAUSED = "paused"
    COMPLETED = "completed"
    ROLLBACK_TRIGGERED = "rollback_triggered"
    ROLLBACK_DISPATCHED = "rollback_dispatched"
    DEVICE_UPDATED = "device_updated"
    DEVICE_FAILED = "device_failed"
    DEVICE_ROLLED_BACK = "device_rolled_back"


@dataclass(frozen=True)
class RolloutConfig:
    """Knobs for the safety valve and batch gating.

    ``health_threshold`` is the minimum healthy-device ratio a batch needs to
    advance. ``failure_threshold`` is the failed-device ratio at or above
    which the safety valve aborts and rolls the release back.
    """

    batch_percentages: tuple[int, ...] = (10, 30, 60, 100)
    health_threshold: float = 0.8
    failure_threshold: float = 0.3
    offline_timeout_s: float = 30.0
    min_known_version: str = "1.0.0"
    # Maximum number of concurrently in-progress releases (pending/active/
    # paused). Creating one more is rejected outright with no side effects.
    max_active_releases: int = 4


@dataclass
class Device:
    device_id: str
    firmware_version: str = ""
    status: DeviceStatus = DeviceStatus.UNKNOWN
    last_seen: float = 0.0
    # Monotonic per-device counter supplied by the client; protects against
    # duplicate and out-of-order reports.
    report_counter: int = 0
    # Release this device is currently bound to (at most one active release).
    active_release_id: Optional[str] = None
    target_version: Optional[str] = None
    # stable version is the version the device was on before the current
    # release touched it; rollback restores this version.
    stable_version: Optional[str] = None
    last_report_id: Optional[str] = None
    registered: bool = False


@dataclass
class Release:
    release_id: str
    from_version: str
    to_version: str
    target_device_ids: list[str]
    batch_percentages: tuple[int, ...]
    state: ReleaseState = ReleaseState.PENDING
    current_batch: int = -1  # -1 = not started
    dispatched: set[str] = field(default_factory=set)
    succeeded: set[str] = field(default_factory=set)
    failed: set[str] = field(default_factory=set)
    rolled_back_devices: set[str] = field(default_factory=set)
    created_at: float = field(default_factory=time.time)
    history: list[dict] = field(default_factory=list)

    def batch_size(self, batch_index: int) -> int:
        """Number of devices that should be dispatched through ``batch_index``."""
        if batch_index < 0 or batch_index >= len(self.batch_percentages):
            return 0
        pct = self.batch_percentages[batch_index]
        total = len(self.target_device_ids)
        # ceil(pct * total / 100)
        return -(-pct * total // 100)

    def batch_members(self, batch_index: int) -> list[str]:
        """Slice of target devices assigned to a single batch (0-based)."""
        start = self.batch_size(batch_index - 1) if batch_index > 0 else 0
        end = self.batch_size(batch_index)
        return self.target_device_ids[start:end]


@dataclass
class DeviceReport:
    """One inbound message from a (simulated) device."""

    device_id: str
    kind: ReportKind
    seq: int
    version: Optional[str] = None
    report_id: Optional[str] = None
    ts: float = field(default_factory=time.time)


@dataclass
class ReportAck:
    accepted: bool
    device_id: str
    status: Optional[DeviceStatus] = None
    reason: Optional[RejectReason] = None
    detail: str = ""


@dataclass
class DispatchItem:
    """A command the platform wants the local device client to execute."""

    device_id: str
    release_id: str
    command: str  # "update" | "rollback"
    version: str
