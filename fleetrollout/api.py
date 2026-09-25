"""FastAPI 路由：设备注册、心跳、版本上报、发布管理、推进、回滚。

所有响应都带明确的判定/错误 code，便于测试区分失败原因。
"""
from __future__ import annotations

import time

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from .errors import RolloutError
from .models import DeviceStatus, ReleaseStatus
from .release import ReleaseService
from .devices import DeviceService

# ---- 请求 / 响应模型 ------------------------------------------------


class RegisterIn(BaseModel):
    device_id: str
    version: str
    ts: float | None = None


class HeartbeatIn(BaseModel):
    device_id: str
    ts: float | None = None


class ReportIn(BaseModel):
    device_id: str
    version: str
    release_id: str | None = None
    outcome: str = Field(description="success | failure | installed")
    seq: int = 0
    dedup_key: str | None = None
    ts: float | None = None


class CreateReleaseIn(BaseModel):
    release_id: str
    target_version: str
    baseline_version: str
    device_ids: list[str]
    batch_size: int | None = None
    healthy_ratio: float | None = None
    failure_ratio: float | None = None
    ts: float | None = None


class ReleaseActionIn(BaseModel):
    ts: float | None = None
    reason: str = "manual"


# ---- 序列化辅助 ------------------------------------------------------


def _device_dict(d) -> dict:
    return {
        "device_id": d.device_id,
        "current_version": d.current_version,
        "last_known_good_version": d.last_known_good_version,
        "status": d.status.value,
        "last_heartbeat": d.last_heartbeat,
        "active_release_id": d.active_release_id,
        "last_report_seq": d.last_report_seq,
    }


def _release_dict(r) -> dict:
    return {
        "release_id": r.release_id,
        "target_version": r.target_version,
        "baseline_version": r.baseline_version,
        "status": r.status.value,
        "active_batch_index": r.active_batch_index,
        "batch_size": r.batch_size,
        "healthy_ratio_threshold": r.healthy_ratio_threshold,
        "failure_ratio_threshold": r.failure_ratio_threshold,
        "rollback_reason": r.rollback_reason,
        "batches": [
            {
                "index": b.index,
                "status": b.status.value,
                "size": len(b.device_ids),
                "successes": b.successes,
                "failures": b.failures,
                "reported": len(b.reported),
                "device_ids": b.device_ids,
            }
            for b in r.batches
        ],
        "history": list(r.history),
    }


# ---- 错误码映射（断言时可区分失败原因） ------------------------------

_STATUS_BY_CODE = {
    "device_not_found": 404,
    "release_not_found": 404,
    "device_already_registered": 409,
    "device_busy": 409,
    "release_state_error": 409,
    "batch_not_ready": 409,
    "unknown_version": 422,
    "unsupported_version": 422,
    "stale_report": 409,
}


def _raise(exc: RolloutError) -> None:
    raise HTTPException(
        status_code=_STATUS_BY_CODE.get(exc.code, 400),
        detail={"code": exc.code, "message": str(exc)},
    )


def build_router(device_service: DeviceService, release_service: ReleaseService) -> APIRouter:
    router = APIRouter()

    def now(ts: float | None) -> float:
        return ts if ts is not None else time.time()

    # ---- 设备 --------------------------------------------------------

    @router.post("/devices/register")
    def register_device(body: RegisterIn):
        try:
            d = device_service.register(body.device_id, body.version, now(body.ts))
        except RolloutError as exc:
            _raise(exc)
        return {"code": "registered", "device": _device_dict(d)}

    @router.post("/devices/heartbeat")
    def heartbeat(body: HeartbeatIn):
        try:
            d = device_service.heartbeat(body.device_id, now(body.ts))
        except RolloutError as exc:
            _raise(exc)
        return {"code": "heartbeat_ok", "device": _device_dict(d)}

    @router.post("/devices/report")
    def report(body: ReportIn):
        try:
            d = device_service.report_version(
                device_id=body.device_id,
                version=body.version,
                release_id=body.release_id,
                outcome=body.outcome,
                now=now(body.ts),
                seq=body.seq,
                dedup_key=body.dedup_key,
            )
        except RolloutError as exc:
            _raise(exc)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"code": "bad_request", "message": str(exc)})
        release_state = None
        if body.release_id:
            try:
                r = release_service.get_release(body.release_id)
                release_state = r.status.value
            except RolloutError:
                release_state = None
        return {"code": "report_accepted", "device": _device_dict(d), "release_status": release_state}

    @router.get("/devices")
    def list_devices():
        return {"devices": [_device_dict(d) for d in device_service.list_devices()]}

    @router.get("/devices/{device_id}")
    def get_device(device_id: str):
        try:
            d = device_service.get(device_id)
        except RolloutError as exc:
            _raise(exc)
        return _device_dict(d)

    # ---- 发布 --------------------------------------------------------

    @router.post("/releases/create")
    def create_release(body: CreateReleaseIn):
        try:
            r = release_service.create_release(
                release_id=body.release_id,
                target_version=body.target_version,
                baseline_version=body.baseline_version,
                device_ids=body.device_ids,
                now=now(body.ts),
                batch_size=body.batch_size,
                healthy_ratio=body.healthy_ratio,
                failure_ratio=body.failure_ratio,
            )
        except RolloutError as exc:
            _raise(exc)
        return {"code": "release_created", "release": _release_dict(r)}

    @router.post("/releases/{release_id}/start")
    def start_release(release_id: str, body: ReleaseActionIn | None = None):
        body = body or ReleaseActionIn()
        try:
            r = release_service.start(release_id, now(body.ts))
        except RolloutError as exc:
            _raise(exc)
        return {"code": "release_started", "release": _release_dict(r)}

    @router.post("/releases/{release_id}/advance")
    def advance_release(release_id: str, body: ReleaseActionIn | None = None):
        body = body or ReleaseActionIn()
        try:
            r = release_service.advance(release_id, now(body.ts))
        except RolloutError as exc:
            _raise(exc)
        return {"code": "release_advanced", "release": _release_dict(r)}

    @router.post("/releases/{release_id}/rollback")
    def rollback_release(release_id: str, body: ReleaseActionIn | None = None):
        body = body or ReleaseActionIn()
        try:
            r = release_service.rollback(release_id, now(body.ts), body.reason)
        except RolloutError as exc:
            _raise(exc)
        return {"code": "release_rolled_back", "release": _release_dict(r)}

    @router.get("/releases")
    def list_releases():
        return {"releases": [_release_dict(r) for r in release_service.list_releases()]}

    @router.get("/releases/{release_id}")
    def get_release(release_id: str):
        try:
            r = release_service.get_release(release_id)
        except RolloutError as exc:
            _raise(exc)
        return _release_dict(r)

    @router.get("/healthz")
    def healthz():
        return {"status": "ok"}

    return router
