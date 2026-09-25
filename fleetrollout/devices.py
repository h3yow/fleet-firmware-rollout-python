"""设备注册、心跳、状态判定与版本上报处理。

并发模型：所有状态变更都在 store.lock 同一把可重入锁内完成，保证
注册/心跳/上报相互原子；上报通过 dedup_key 幂等、seq 防乱序。
"""
from __future__ import annotations

from collections.abc import Callable

from .config import Settings
from .errors import (
    DeviceAlreadyRegisteredError,
    DeviceNotFoundError,
    StaleReportError,
    UnsupportedVersionError,
)
from .models import Device, DeviceStatus
from .store import InMemoryStore
from .versions import VersionRegistry, is_older

# 上报处理回调签名：(device, release_id, outcome, version, now) -> None
# phase="validate" 在设备状态迁移前调用，只允许抛异常拒绝上报；
# phase="apply" 在迁移后调用，用于批次记账与安全阀。
ReportListener = Callable[[Device, str | None, str, str, float, str], None]


class DeviceService:
    def __init__(
        self,
        store: InMemoryStore,
        versions: VersionRegistry,
        settings: Settings,
    ) -> None:
        self.store = store
        self.versions = versions
        self.settings = settings
        self._report_listeners: list[ReportListener] = []

    def add_report_listener(self, listener: ReportListener) -> None:
        self._report_listeners.append(listener)

    # ---- 注册 / 查询 -------------------------------------------------

    def register(self, device_id: str, version: str, now: float) -> Device:
        # 未知版本拒绝注册，避免后续版本基线不可知
        self.versions.require_known(version)
        with self.store.lock:
            if device_id in self.store.devices():
                raise DeviceAlreadyRegisteredError(f"设备已注册: {device_id}")
            device = Device(
                device_id=device_id,
                current_version=version,
                last_known_good_version=version,
                status=DeviceStatus.ONLINE,
                last_heartbeat=now,
                registered_at=now,
            )
            self.store.devices()[device_id] = device
            return device

    def get(self, device_id: str) -> Device:
        with self.store.lock:
            device = self.store.devices().get(device_id)
            if device is None:
                raise DeviceNotFoundError(f"设备不存在: {device_id}")
            return device

    def list_devices(self) -> list[Device]:
        with self.store.lock:
            return list(self.store.devices().values())

    # ---- 心跳 / 在线状态 ---------------------------------------------

    def heartbeat(self, device_id: str, now: float) -> Device:
        with self.store.lock:
            device = self._get_locked(device_id)
            device.last_heartbeat = now
            # 心跳只能把“离线”设备拉回在线；更新中/更新失败是发布流程状态，
            # 不得被普通心跳覆盖（状态不回退）。
            if device.status == DeviceStatus.OFFLINE:
                device.status = DeviceStatus.ONLINE
            return device

    def refresh_status(self, device_id: str, now: float) -> DeviceStatus:
        """心跳超时则在线 -> 离线；更新中/失败状态不因超时被改写。"""
        with self.store.lock:
            device = self._get_locked(device_id)
            if (
                device.status == DeviceStatus.ONLINE
                and now - device.last_heartbeat > self.settings.offline_timeout_s
            ):
                device.status = DeviceStatus.OFFLINE
            return device.status

    def refresh_all(self, now: float) -> None:
        with self.store.lock:
            for device_id in list(self.store.devices()):
                self.refresh_status(device_id, now)

    # ---- 版本 / 结果上报 ---------------------------------------------

    def report_version(
        self,
        device_id: str,
        version: str,
        release_id: str | None,
        outcome: str,
        now: float,
        seq: int = 0,
        dedup_key: str | None = None,
    ) -> Device:
        if outcome not in ("success", "failure", "installed"):
            raise ValueError(f"未知上报结果类型: {outcome}")
        # 未知版本显式失败；低于最低支持版本同样显式失败，不静默忽略。
        self.versions.require_known(version)
        if is_older(version, self.settings.min_supported_version):
            raise UnsupportedVersionError(
                f"设备 {device_id} 上报过旧版本 {version}，"
                f"低于最低支持版本 {self.settings.min_supported_version}"
            )

        with self.store.lock:
            device = self._get_locked(device_id)

            # 1) 幂等：完全相同的报文（含成功与失败）只生效一次。
            if dedup_key is not None and dedup_key in device.seen_report_keys:
                return device
            # 2) 防乱序：seq 更小的旧报文拒绝处理。
            if seq < device.last_report_seq:
                raise StaleReportError(
                    f"过期上报被拒绝: seq={seq} < 已处理 seq={device.last_report_seq}"
                )

            # 3) 发布侧前置校验（如：该发布已回滚，则迟到的目标版本上报必须拒绝），
            #    必须发生在设备版本/状态迁移之前。
            for listener in self._report_listeners:
                listener(device, release_id, outcome, version, now, "validate")

            # 首次见到该报文：记录幂等键与序号，再做状态迁移。
            if dedup_key is not None:
                device.seen_report_keys.add(dedup_key)
            device.last_report_seq = max(device.last_report_seq, seq)

            if outcome == "failure":
                # 更新失败：停留在目标版本（刷写失败），标记失败状态。
                device.status = DeviceStatus.UPDATE_FAILED
            else:
                # success / installed：版本推进到上报版本，恢复在线。
                device.current_version = version
                device.status = DeviceStatus.ONLINE
                device.last_heartbeat = now
                if release_id is not None and device.active_release_id == release_id:
                    # 仅当属于当前发布的成功结果时，才更新“稳定版本”基线；
                    # 回滚产生的旧版本上报不应把稳定基线改回旧版。
                    device.last_known_good_version = version
                elif device.active_release_id is None:
                    # 游离的 installed 上报（如人工安装）也更新稳定基线。
                    device.last_known_good_version = version

            # 监听器在锁内、在设备状态迁移之后更新批次计数与触发安全阀。
            for listener in self._report_listeners:
                listener(device, release_id, outcome, version, now, "apply")

            if outcome == "success" and device.active_release_id == release_id:
                device.active_release_id = None
            return device

    # ---- 内部辅助 ----------------------------------------------------

    def _get_locked(self, device_id: str) -> Device:
        device = self.store.devices().get(device_id)
        if device is None:
            raise DeviceNotFoundError(f"设备不存在: {device_id}")
        return device
