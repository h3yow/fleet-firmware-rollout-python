"""线程安全的内存存储。"""
from __future__ import annotations

import threading

from .models import Device, FirmwareRelease


class InMemoryStore:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._devices: dict[str, Device] = {}
        self._releases: dict[str, FirmwareRelease] = {}

    @property
    def lock(self) -> threading.RLock:
        return self._lock

    def devices(self) -> dict[str, Device]:
        return self._devices

    def releases(self) -> dict[str, FirmwareRelease]:
        return self._releases

    def reset(self) -> None:
        with self._lock:
            self._devices.clear()
            self._releases.clear()
