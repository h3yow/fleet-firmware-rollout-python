"""Thread-safe in-memory store containing all state-machine logic.

A single :class:`threading.RLock` guards every mutation. Device reports carry
a monotonic per-device ``seq`` and an optional idempotency ``report_id`` so
that concurrent, duplicate and out-of-order deliveries can never cause double
counting or state regression.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from copy import deepcopy
from dataclasses import asdict

from .models import (
    Device,
    DeviceReport,
    DeviceStatus,
    DispatchItem,
    EventKind,
    Release,
    ReleaseState,
    ReportAck,
    ReportKind,
    RejectReason,
    RolloutConfig,
)
from .registry import FirmwareRegistry
from .versioning import compare_version


class RolloutError(Exception):
    """Operation rejected by the state machine; ``reason`` is machine-readable."""

    def __init__(self, reason: str, message: str):
        super().__init__(message)
        self.reason = reason
        self.message = message


def _now() -> float:
    return time.time()


class FleetStore:
    def __init__(
        self,
        registry: FirmwareRegistry,
        config: RolloutConfig | None = None,
        clock=_now,
    ) -> None:
        self._registry = registry
        self._config = config or RolloutConfig()
        self._clock = clock
        self._lock = threading.RLock()
        self._devices: dict[str, Device] = {}
        self._releases: dict[str, Release] = {}
        self._commands: dict[str, deque[DispatchItem]] = {}

    # ------------------------------------------------------------------ utils

    def _record(self, release: Release, kind: EventKind, **fields) -> None:
        release.history.append(
            {"ts": self._clock(), "event": kind.value, **fields}
        )

    def device_snapshot(self, device_id: str) -> dict:
        with self._lock:
            device = self._devices[device_id]
            return asdict(device)

    def release_snapshot(self, release_id: str) -> dict:
        with self._lock:
            return asdict(self._releases[release_id])

    # ------------------------------------------------------------- registration

    def register_device(self, device_id: str, version: str = "") -> Device:
        """Register a device. Idempotent; never resets an existing device."""
        with self._lock:
            existing = self._devices.get(device_id)
            if existing is not None:
                return existing
            if version and not self._registry.is_known(version):
                raise RolloutError(
                    RejectReason.UNKNOWN_VERSION.value,
                    f"cannot register {device_id} with unknown version "
                    f"{version!r}",
                )
            device = Device(
                device_id=device_id,
                firmware_version=version,
                status=DeviceStatus.ONLINE,
                last_seen=self._clock(),
                registered=True,
            )
            self._devices[device_id] = device
            self._commands[device_id] = deque()
            return device

    def mark_offline_devices(self, now: float | None = None) -> list[str]:
        """Flip ONLINE devices past the offline timeout to OFFLINE."""
        now = self._clock() if now is None else now
        flipped: list[str] = []
        with self._lock:
            for device in self._devices.values():
                if (
                    device.status is DeviceStatus.ONLINE
                    and device.last_seen
                    and now - device.last_seen > self._config.offline_timeout_s
                ):
                    device.status = DeviceStatus.OFFLINE
                    flipped.append(device.device_id)
        return flipped

    # ----------------------------------------------------------------- reports

    def _reject(self, device_id: str, reason: RejectReason, detail: str,
                device: Device | None) -> ReportAck:
        return ReportAck(
            accepted=False,
            device_id=device_id,
            status=device.status if device else None,
            reason=reason,
            detail=detail,
        )

    def apply_report(self, report: DeviceReport) -> ReportAck:
        with self._lock:
            device = self._devices.get(report.device_id)
            if device is None or not device.registered:
                return self._reject(
                    report.device_id, RejectReason.UNKNOWN_DEVICE,
                    "device is not registered", device,
                )

            # idempotency via client-generated report id
            if (
                report.report_id is not None
                and device.last_report_id is not None
                and report.report_id == device.last_report_id
            ):
                return self._reject(
                    device.device_id, RejectReason.DUPLICATE,
                    f"report_id {report.report_id} already applied", device,
                )

            # ordering / duplicate via monotonic sequence number
            if report.seq < device.report_counter:
                return self._reject(
                    device.device_id, RejectReason.STALE_COUNTER,
                    f"seq {report.seq} < last {device.report_counter}", device,
                )
            if report.seq == device.report_counter and device.report_counter > 0:
                return self._reject(
                    device.device_id, RejectReason.DUPLICATE,
                    f"seq {report.seq} already applied", device,
                )
            seq_gap = report.seq > device.report_counter + 1

            # version validation (never silently accept garbage)
            if report.version is not None:
                if not self._registry.is_known(report.version):
                    return self._reject(
                        device.device_id, RejectReason.UNKNOWN_VERSION,
                        f"version {report.version!r} is not in the registry",
                        device,
                    )
                if compare_version(
                    report.version, self._config.min_known_version
                ) < 0:
                    return self._reject(
                        device.device_id, RejectReason.VERSION_TOO_OLD,
                        f"version {report.version} older than minimum "
                        f"{self._config.min_known_version}", device,
                    )

            release = (
                self._releases.get(device.active_release_id)
                if device.active_release_id is not None
                else None
            )

            handler = getattr(
                self, f"_on_{report.kind.value}", None
            )
            if handler is None:  # pragma: no cover - enum is closed
                return self._reject(
                    device.device_id, RejectReason.NOT_EXPECTED,
                    f"unsupported report kind {report.kind}", device,
                )

            ack = handler(device, release, report)
            if ack.accepted:
                device.report_counter = report.seq
                if report.report_id is not None:
                    device.last_report_id = report.report_id
                device.last_seen = report.ts or self._clock()
                if seq_gap:
                    ack.detail = (
                        f"{ack.detail}; seq gap accepted "
                        f"({device.report_counter}->{report.seq})"
                    ).strip("; ")
            return ack

    # -- report handlers -----------------------------------------------------

    def _accept(self, device: Device, detail: str = "") -> ReportAck:
        return ReportAck(
            accepted=True, device_id=device.device_id,
            status=device.status, detail=detail,
        )

    def _on_register(self, device, release, report) -> ReportAck:
        if report.version:
            device.firmware_version = report.version
        device.status = DeviceStatus.ONLINE
        return self._accept(device, "registered/seen")

    def _on_heartbeat(self, device, release, report) -> ReportAck:
        # UPDATING/FAILED are sticky: a heartbeat must not mask them.
        if device.status in (DeviceStatus.ONLINE, DeviceStatus.OFFLINE,
                             DeviceStatus.UNKNOWN):
            device.status = DeviceStatus.ONLINE
        return self._accept(device, "heartbeat")

    def _on_version(self, device, release, report) -> ReportAck:
        device.firmware_version = report.version
        if device.status in (DeviceStatus.ONLINE, DeviceStatus.OFFLINE,
                             DeviceStatus.UNKNOWN):
            device.status = DeviceStatus.ONLINE
        if (
            device.status is DeviceStatus.UPDATING
            and release is not None
            and report.version == release.to_version
        ):
            return self._complete_success(device, release, "reported via version")
        return self._accept(device, f"version={report.version}")

    def _on_update_started(self, device, release, report) -> ReportAck:
        if release is None or device.target_version is None:
            return self._reject(
                device.device_id, RejectReason.NOT_EXPECTED,
                "update_started without a dispatched release", device,
            )
        device.status = DeviceStatus.UPDATING
        return self._accept(device, f"updating->{device.target_version}")

    def _on_update_succeeded(self, device, release, report) -> ReportAck:
        if release is None or device.target_version is None:
            return self._reject(
                device.device_id, RejectReason.NOT_EXPECTED,
                "update_succeeded without a dispatched release", device,
            )
        version = report.version or release.to_version
        if version != release.to_version:
            return self._reject(
                device.device_id, RejectReason.NOT_EXPECTED,
                f"succeeded version {version} != target {release.to_version}",
                device,
            )
        if device.device_id in release.succeeded:
            return self._reject(
                device.device_id, RejectReason.DUPLICATE,
                "device already counted as succeeded", device,
            )
        device.firmware_version = release.to_version

        # Late success arriving while the release is already rolling back:
        # record the fact and re-dispatch rollback to keep history consistent.
        if release.state is ReleaseState.ROLLED_BACK:
            release.succeeded.add(device.device_id)
            self._record(release, EventKind.DEVICE_UPDATED,
                         device_id=device.device_id,
                         version=release.to_version, late=True)
            device.status = DeviceStatus.UPDATING
            self._enqueue(
                device.device_id, release.release_id,
                "rollback", device.stable_version,
            )
            return self._accept(
                device, "late success; rollback re-dispatched"
            )

        return self._complete_success(device, release, "update succeeded")

    def _complete_success(self, device, release, why: str) -> ReportAck:
        device.status = DeviceStatus.ONLINE
        release.succeeded.add(device.device_id)
        self._record(release, EventKind.DEVICE_UPDATED,
                     device_id=device.device_id, version=release.to_version)
        tripped = self._maybe_rollback(release)
        detail = why + ("; safety valve tripped" if tripped else "")
        return self._accept(device, detail)

    def _on_update_failed(self, device, release, report) -> ReportAck:
        if release is None:
            rolled = self._find_rolled_back(device.device_id)
            if rolled is not None and device.device_id in rolled.failed:
                device.status = DeviceStatus.FAILED
                return self._accept(
                    device, "late failure after rollback; already resolved"
                )
            return self._reject(
                device.device_id, RejectReason.NOT_EXPECTED,
                "update_failed without a dispatched release", device,
            )
        if device.target_version is None:
            return self._reject(
                device.device_id, RejectReason.NOT_EXPECTED,
                "update_failed without a dispatched release", device,
            )
        if device.device_id in release.failed:
            return self._reject(
                device.device_id, RejectReason.DUPLICATE,
                "device already counted as failed", device,
            )
        device.status = DeviceStatus.FAILED
        release.failed.add(device.device_id)
        self._record(release, EventKind.DEVICE_FAILED,
                     device_id=device.device_id)
        tripped = self._maybe_rollback(release)
        return self._accept(
            device,
            "update failed" + ("; safety valve tripped" if tripped else ""),
        )

    def _on_rollback_done(self, device, release, report) -> ReportAck:
        if release is None or release.state is not ReleaseState.ROLLED_BACK:
            return self._reject(
                device.device_id, RejectReason.NOT_EXPECTED,
                "rollback_done while release is not rolling back", device,
            )
        expected = device.stable_version
        if report.version != expected:
            return self._reject(
                device.device_id, RejectReason.NOT_EXPECTED,
                f"rollback version {report.version} != stable {expected}",
                device,
            )
        self._resolve_rollback(device, release, report.version)
        return self._accept(device, f"rolled back to {expected}")

    def _resolve_rollback(self, device, release, version: str | None = None,
                          keep_status: bool = False):
        device.firmware_version = version or device.stable_version
        if not keep_status:
            device.status = DeviceStatus.ONLINE
        release.rolled_back_devices.add(device.device_id)
        self._record(release, EventKind.DEVICE_ROLLED_BACK,
                     device_id=device.device_id,
                     version=device.firmware_version)
        device.active_release_id = None
        device.target_version = None
        device.stable_version = None

    # --------------------------------------------------------------- releases

    def create_release(
        self, release_id: str, to_version: str, device_ids: list[str],
        from_version: str | None = None,
    ) -> Release:
        with self._lock:
            if release_id in self._releases:
                raise RolloutError("release_exists",
                                   f"release {release_id} already exists")
            if not self._registry.is_known(to_version):
                raise RolloutError(RejectReason.UNKNOWN_VERSION.value,
                                   f"target version {to_version!r} unknown")
            unknown = [d for d in device_ids if d not in self._devices]
            if unknown:
                raise RolloutError(RejectReason.UNKNOWN_DEVICE.value,
                                   f"unregistered devices: {unknown}")
            if not device_ids:
                raise RolloutError("empty_targets", "release has no devices")
            release = Release(
                release_id=release_id,
                from_version=from_version or "",
                to_version=to_version,
                target_device_ids=list(device_ids),
                batch_percentages=self._config.batch_percentages,
            )
            self._releases[release_id] = release
            self._record(release, EventKind.CREATED,
                         to_version=to_version,
                         batch_percentages=list(release.batch_percentages))
            return release

    def _bind_and_dispatch(self, release: Release, members: list[str],
                           command: str, version: str | None) -> list[DispatchItem]:
        items: list[DispatchItem] = []
        for device_id in members:
            device = self._devices[device_id]
            if command == "update":
                if device.active_release_id is not None:
                    raise RolloutError(
                        "device_busy",
                        f"device {device_id} already in active release "
                        f"{device.active_release_id}",
                    )
                if device.status is DeviceStatus.UPDATING:
                    raise RolloutError(
                        "device_busy",
                        f"device {device_id} is already updating",
                    )
                device.active_release_id = release.release_id
                device.target_version = release.to_version
                device.stable_version = device.firmware_version
                release.dispatched.add(device_id)
            items.append(DispatchItem(
                device_id=device_id, release_id=release.release_id,
                command=command,
                version=version if version is not None else release.to_version,
            ))
            self._commands[device_id].append(items[-1])
        return items

    def _enqueue(self, device_id: str, release_id: str,
                 command: str, version: str) -> None:
        item = DispatchItem(device_id=device_id, release_id=release_id,
                            command=command, version=version)
        self._commands[device_id].append(item)

    def start_release(self, release_id: str) -> list[DispatchItem]:
        with self._lock:
            release = self._releases.get(release_id)
            if release is None:
                raise RolloutError("not_found", f"release {release_id} missing")
            if release.state is not ReleaseState.PENDING:
                raise RolloutError("not_pending",
                                   f"release is {release.state.value}")
            release.state = ReleaseState.ACTIVE
            release.current_batch = 0
            self._record(release, EventKind.STARTED)
            members = release.batch_members(0)
            items = self._bind_and_dispatch(release, members, "update", None)
            self._record(release, EventKind.DISPATCHED, batch=0,
                         device_ids=list(members))
            return items

    def _find_rolled_back(self, device_id: str) -> Release | None:
        for release in self._releases.values():
            if (
                release.state is ReleaseState.ROLLED_BACK
                and device_id in release.dispatched
            ):
                return release
        return None

    def _batch_stats(self, release: Release, batch: int) -> tuple[int, int, int]:
        """Return (members, succeeded, failed) restricted to one batch."""
        members = release.batch_members(batch)
        ok = sum(1 for d in members if d in release.succeeded)
        bad = sum(1 for d in members if d in release.failed)
        return len(members), ok, bad

    def advance_release(self, release_id: str) -> list[DispatchItem]:
        """Gate: current batch must be fully decided and healthy enough."""
        with self._lock:
            release = self._releases.get(release_id)
            if release is None:
                raise RolloutError("not_found", f"release {release_id} missing")
            if release.state is ReleaseState.ROLLED_BACK:
                raise RolloutError("rolled_back",
                                   "release has been rolled back")
            if release.state is not ReleaseState.ACTIVE:
                raise RolloutError("not_active",
                                   f"release is {release.state.value}")
            batch = release.current_batch
            total, ok, bad = self._batch_stats(release, batch)
            decided = ok + bad
            if decided < total:
                raise RolloutError(
                    "batch_pending",
                    f"batch {batch}: {decided}/{total} devices decided",
                )
            health = ok / total
            if health < self._config.health_threshold:
                raise RolloutError(
                    "health_not_met",
                    f"batch {batch} health {health:.2f} < "
                    f"{self._config.health_threshold:.2f}",
                )

            # all batches done -> complete
            if batch == len(release.batch_percentages) - 1:
                release.state = ReleaseState.COMPLETED
                for device_id in release.target_device_ids:
                    device = self._devices[device_id]
                    if device.active_release_id == release_id:
                        device.active_release_id = None
                        device.target_version = None
                        device.stable_version = None
                self._record(release, EventKind.COMPLETED,
                             succeeded=sorted(release.succeeded))
                return []

            next_batch = batch + 1
            members = release.batch_members(next_batch)
            items = self._bind_and_dispatch(release, members, "update", None)
            release.current_batch = next_batch
            self._record(release, EventKind.BATCH_ADVANCED,
                         from_batch=batch, to_batch=next_batch,
                         health=health)
            self._record(release, EventKind.DISPATCHED, batch=next_batch,
                         device_ids=list(members))
            return items

    def _maybe_rollback(self, release: Release) -> bool:
        """Safety valve over the currently dispatched batch."""
        if release.state is not ReleaseState.ACTIVE:
            return False
        batch = release.current_batch
        total, ok, bad = self._batch_stats(release, batch)
        if total == 0 or bad == 0:
            return False
        rate = bad / total
        if rate <= self._config.failure_threshold:
            return False
        self.rollback_release(
            release.release_id,
            reason=(f"batch {batch} failure rate {rate:.2f} > "
                    f"{self._config.failure_threshold:.2f}"),
        )
        return True

    def rollback_release(self, release_id: str, reason: str = "manual"
                         ) -> list[DispatchItem]:
        with self._lock:
            release = self._releases.get(release_id)
            if release is None:
                raise RolloutError("not_found", f"release {release_id} missing")
            if release.state not in (ReleaseState.ACTIVE, ReleaseState.PAUSED):
                raise RolloutError("not_active",
                                   f"release is {release.state.value}")
            release.state = ReleaseState.ROLLED_BACK
            self._record(release, EventKind.ROLLBACK_TRIGGERED, reason=reason)

            items: list[DispatchItem] = []
            need_command: list[str] = []
            for device_id in release.target_device_ids:
                device = self._devices[device_id]
                if device.active_release_id != release_id:
                    continue
                if device_id in release.succeeded or (
                    device.status is DeviceStatus.UPDATING
                ):
                    # actually on (or heading to) the new version -> roll back
                    need_command.append(device_id)
                else:
                    # failed device never left stable version: resolve now,
                    # keep FAILED status; same for not-started devices.
                    keep = device.status is DeviceStatus.FAILED
                    self._resolve_rollback(device, release, keep_status=keep)
            for device_id in need_command:
                device = self._devices[device_id]
                device.status = DeviceStatus.UPDATING
                items.append(DispatchItem(
                    device_id=device_id, release_id=release_id,
                    command="rollback", version=device.stable_version,
                ))
                self._commands[device_id].append(items[-1])
            self._record(release, EventKind.ROLLBACK_DISPATCHED,
                         device_ids=need_command,
                         auto_resolved=sorted(
                             release.rolled_back_devices), reason=reason)
            return items

    def pending_commands(self, device_id: str) -> list[DispatchItem]:
        """Drain and return queued commands for one (simulated) device."""
        with self._lock:
            if device_id not in self._devices:
                raise RolloutError(RejectReason.UNKNOWN_DEVICE.value,
                                   f"unknown device {device_id}")
            queue = self._commands[device_id]
            items = list(queue)
            queue.clear()
            return items
