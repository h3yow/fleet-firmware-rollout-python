"""Simulated on-device agent (local process; no real hardware)."""

from __future__ import annotations

from .models import DeviceReport, ReportKind
from .store import FleetStore


class DeviceClient:
    def __init__(
        self,
        device_id: str,
        initial_version: str,
        store: FleetStore,
        fail_rate: float = 0.0,
    ) -> None:
        raise NotImplementedError

    def send(self, kind: ReportKind, version: str | None = None) -> object:
        raise NotImplementedError

    def run_commands(self) -> list[object]:
        raise NotImplementedError
