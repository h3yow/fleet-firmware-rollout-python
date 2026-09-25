"""共享夹具：装配一套线程安全的内存服务。"""
from __future__ import annotations

import types

import pytest

from fleetrollout.config import load_settings
from fleetrollout.devices import DeviceService
from fleetrollout.release import ReleaseService
from fleetrollout.store import InMemoryStore
from fleetrollout.versions import VersionRegistry

KNOWN_VERSIONS = ["1.0.0", "1.1.0", "2.0.0", "0.9.0"]


@pytest.fixture
def services():
    settings = load_settings()
    store = InMemoryStore()
    versions = VersionRegistry(KNOWN_VERSIONS)
    devices = DeviceService(store, versions, settings)
    releases = ReleaseService(store, versions, settings)
    devices.add_report_listener(releases.handle_device_report)
    return types.SimpleNamespace(
        settings=settings,
        store=store,
        versions=versions,
        devices=devices,
        releases=releases,
    )
