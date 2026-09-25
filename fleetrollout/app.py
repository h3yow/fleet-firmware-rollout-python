"""应用装配：把内存服务单例挂载到 FastAPI。"""
from __future__ import annotations

from fastapi import FastAPI

from .api import build_router
from .config import Settings, load_settings
from .devices import DeviceService
from .release import ReleaseService
from .store import InMemoryStore
from .versions import VersionRegistry

DEFAULT_KNOWN_VERSIONS = ["1.0.0", "1.1.0", "2.0.0"]


def create_app(
    settings: Settings | None = None,
    store: InMemoryStore | None = None,
    versions: VersionRegistry | None = None,
) -> FastAPI:
    settings = settings or load_settings()
    store = store or InMemoryStore()
    versions = versions or VersionRegistry(DEFAULT_KNOWN_VERSIONS)

    device_service = DeviceService(store, versions, settings)
    release_service = ReleaseService(store, versions, settings)
    # 上报先经发布侧校验，再由发布侧记账/触发安全阀。
    device_service.add_report_listener(release_service.handle_device_report)

    app = FastAPI(title="Fleet Firmware Rollout", version="0.1.0")
    app.state.settings = settings
    app.state.store = store
    app.state.versions = versions
    app.state.device_service = device_service
    app.state.release_service = release_service

    @app.get("/")
    def root():
        return {
            "service": "fleet-firmware-rollout",
            "known_versions": sorted(versions._known),
            "min_supported_version": settings.min_supported_version,
        }

    app.include_router(build_router(device_service, release_service))
    return app
