"""Thread-safe in-memory store containing all state-machine logic."""

from __future__ import annotations

import threading

from .models import (
    Device,
    DeviceReport,
    DispatchItem,
    Release,
    ReportAck,
    RolloutConfig,
)
from .registry import FirmwareRegistry


class FleetStore:
    def __init__(
        self,
        registry: FirmwareRegistry,
        config: RolloutConfig | None = None,
    ) -> None:
        raise NotImplementedError

    def register_device(self, device_id: str, version: str = "") -> Device:
        raise NotImplementedError

    def apply_report(self, report: DeviceReport) -> ReportAck:
        raise NotImplementedError

    def mark_offline_devices(self, now: float | None = None) -> list[str]:
        raise NotImplementedError

    def create_release(
        self, release_id: str, to_version: str, device_ids: list[str],
        from_version: str | None = None,
    ) -> Release:
        raise NotImplementedError

    def start_release(self, release_id: str) -> list[DispatchItem]:
        raise NotImplementedError

    def advance_release(self, release_id: str) -> list[DispatchItem]:
        raise NotImplementedError

    def rollback_release(
        self, release_id: str, reason: str = "manual"
    ) -> list[DispatchItem]:
        raise NotImplementedError

    def pending_commands(self, device_id: str) -> list[DispatchItem]:
        raise NotImplementedError
