"""Simulated on-device agent.

A :class:`DeviceClient` is a plain local object standing in for a physical
device: it owns a monotonic send sequence, drains commands queued by the
platform, and reports progress back through the store. No network, hardware
or credentials are involved.
"""

from __future__ import annotations

import random
import uuid
from typing import Optional

from .models import DeviceReport, ReportAck, ReportKind
from .store import FleetStore


class DeviceClient:
    def __init__(
        self,
        device_id: str,
        initial_version: str,
        store: FleetStore,
        fail_rate: float = 0.0,
        seed: Optional[int] = None,
    ) -> None:
        self.device_id = device_id
        self.version = initial_version
        self._store = store
        self.fail_rate = fail_rate
        self._rng = random.Random(seed)
        self._seq = 0

    def send(self, kind: ReportKind, version: str | None = None,
             report_id: str | None = None) -> ReportAck:
        self._seq += 1
        report = DeviceReport(
            device_id=self.device_id,
            kind=kind,
            seq=self._seq,
            version=version,
            report_id=report_id,
        )
        return self._store.apply_report(report)

    def register(self) -> ReportAck:
        return self.send(ReportKind.REGISTER, self.version)

    def heartbeat(self) -> ReportAck:
        return self.send(ReportKind.HEARTBEAT)

    def report_version(self) -> ReportAck:
        return self.send(ReportKind.VERSION, self.version)

    def run_commands(self) -> list[ReportAck]:
        """Drain queued platform commands and execute them locally."""
        acks: list[ReportAck] = []
        for command in self._store.pending_commands(self.device_id):
            if command.command == "update":
                acks.append(self.send(ReportKind.UPDATE_STARTED))
                if self._rng.random() < self.fail_rate:
                    acks.append(self.send(ReportKind.UPDATE_FAILED))
                else:
                    self.version = command.version
                    acks.append(
                        self.send(ReportKind.UPDATE_SUCCEEDED, command.version)
                    )
            elif command.command == "rollback":
                # rollbacks always succeed locally
                self.version = command.version
                acks.append(
                    self.send(
                        ReportKind.ROLLBACK_DONE, command.version,
                        report_id=f"rb-{uuid.uuid4().hex[:8]}",
                    )
                )
        return acks
