"""HTTP surface of the service, served entirely on localhost."""

from __future__ import annotations

from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .models import DeviceReport, ReportKind
from .service import RolloutService
from .store import RolloutError


class RegisterIn(BaseModel):
    device_id: str
    version: str = ""


class ReportIn(BaseModel):
    kind: ReportKind
    seq: int = Field(ge=1)
    version: Optional[str] = None
    report_id: Optional[str] = None


class ReleaseIn(BaseModel):
    release_id: str
    to_version: str
    device_ids: list[str]
    from_version: Optional[str] = None


class RollbackIn(BaseModel):
    reason: str = "manual"


def create_app(service: RolloutService | None = None) -> FastAPI:
    service = service or RolloutService()
    store = service.store
    app = FastAPI(title="Fleet firmware rollout (local simulation)")

    def _raise(exc: RolloutError) -> None:
        raise HTTPException(status_code=409, detail={
            "reason": exc.reason, "message": exc.message,
        })

    def _ack_payload(ack) -> dict:
        return {
            "accepted": ack.accepted,
            "device_id": ack.device_id,
            "status": ack.status.value if ack.status else None,
            "reason": ack.reason.value if ack.reason else None,
            "detail": ack.detail,
        }

    @app.get("/health")
    def health():
        return {"status": "ok"}

    @app.post("/devices")
    def register_device(body: RegisterIn):
        try:
            device = store.register_device(body.device_id, body.version)
        except RolloutError as exc:
            _raise(exc)
        return {"device_id": device.device_id,
                "firmware_version": device.firmware_version,
                "status": device.status.value}

    @app.get("/devices/{device_id}")
    def get_device(device_id: str):
        try:
            return store.device_snapshot(device_id)
        except KeyError:
            raise HTTPException(404, detail={"reason": "unknown_device"})

    @app.post("/devices/{device_id}/reports")
    def report(device_id: str, body: ReportIn):
        ack = store.apply_report(DeviceReport(
            device_id=device_id, kind=body.kind, seq=body.seq,
            version=body.version, report_id=body.report_id,
        ))
        # rejected reports get 422 and a machine-readable rejection reason
        return JSONResponse(status_code=200 if ack.accepted else 422,
                            content=_ack_payload(ack))

    @app.post("/releases")
    def create_release(body: ReleaseIn):
        try:
            release = store.create_release(
                body.release_id, body.to_version, body.device_ids,
                body.from_version,
            )
        except RolloutError as exc:
            _raise(exc)
        return {"release_id": release.release_id,
                "state": release.state.value}

    @app.get("/releases/{release_id}")
    def get_release(release_id: str):
        try:
            return store.release_snapshot(release_id)
        except KeyError:
            raise HTTPException(404, detail={"reason": "not_found"})

    @app.post("/releases/{release_id}/start")
    def start_release(release_id: str):
        try:
            items = store.start_release(release_id)
        except RolloutError as exc:
            _raise(exc)
        return {"dispatched": [vars(i) for i in items]}

    @app.post("/releases/{release_id}/advance")
    def advance_release(release_id: str):
        try:
            items = store.advance_release(release_id)
        except RolloutError as exc:
            _raise(exc)
        snap = store.release_snapshot(release_id)
        return {"dispatched": [vars(i) for i in items],
                "state": snap["state"],
                "current_batch": snap["current_batch"]}

    @app.post("/releases/{release_id}/rollback")
    def rollback_release(release_id: str, body: RollbackIn):
        try:
            items = store.rollback_release(release_id, body.reason)
        except RolloutError as exc:
            _raise(exc)
        return {"dispatched": [vars(i) for i in items],
                "state": "rolled_back"}

    @app.post("/maintenance/offline-sweep")
    def offline_sweep():
        return {"went_offline": store.mark_offline_devices()}

    return app
