"""灰度发布编排：建批、下发、批次推进、健康/失败判定、回滚安全阀。

关键不变量（均在 store.lock 内维护）：
- 同一设备同时只能占用一个进行中的发布（device.active_release_id）。
- 批次严格按序推进；只有当前批次“全部回报且健康比例达标”才放行。
- 失败率超过阈值立即触发安全阀，回滚到发布基线版本。
"""
from __future__ import annotations


from .config import Settings
from .errors import (
    BatchNotReadyError,
    DeviceBusyError,
    DeviceNotFoundError,
    ReleaseNotFoundError,
    ReleaseStateError,
    StaleReportError,
)
from .models import (
    Batch,
    BatchStatus,
    Device,
    DeviceStatus,
    FirmwareRelease,
    ReleaseStatus,
)
from .store import InMemoryStore
from .versions import VersionRegistry


class ReleaseService:
    def __init__(
        self,
        store: InMemoryStore,
        versions: VersionRegistry,
        settings: Settings,
    ) -> None:
        self.store = store
        self.versions = versions
        self.settings = settings

    # ---- 创建 / 查询 -------------------------------------------------

    def create_release(
        self,
        release_id: str,
        target_version: str,
        baseline_version: str,
        device_ids: list[str],
        now: float,
        batch_size: int | None = None,
        healthy_ratio: float | None = None,
        failure_ratio: float | None = None,
    ) -> FirmwareRelease:
        self.versions.require_known(target_version)
        self.versions.require_known(baseline_version)
        if not device_ids:
            raise ReleaseStateError("发布设备列表不能为空")
        batch_size = batch_size or self.settings.default_batch_size
        healthy_ratio = (
            healthy_ratio if healthy_ratio is not None else self.settings.default_healthy_ratio
        )
        failure_ratio = (
            failure_ratio if failure_ratio is not None else self.settings.default_failure_ratio
        )
        if batch_size <= 0:
            raise ReleaseStateError("批次大小必须为正整数")
        if not 0.0 < healthy_ratio <= 1.0:
            raise ReleaseStateError("健康比例阈值必须在 (0, 1] 区间")
        if not 0.0 < failure_ratio <= 1.0:
            raise ReleaseStateError("失败比例阈值必须在 (0, 1] 区间")

        with self.store.lock:
            if release_id in self.store.releases():
                raise ReleaseStateError(f"发布已存在: {release_id}")
            missing = [d for d in device_ids if d not in self.store.devices()]
            if missing:
                raise DeviceNotFoundError(f"设备不存在: {missing}")

            n = len(device_ids)
            chunks = [
                device_ids[i : i + batch_size] for i in range(0, n, batch_size)
            ]
            release = FirmwareRelease(
                release_id=release_id,
                target_version=target_version,
                baseline_version=baseline_version,
                batch_size=batch_size,
                healthy_ratio_threshold=healthy_ratio,
                failure_ratio_threshold=failure_ratio,
                batches=[Batch(index=i, device_ids=list(chunk)) for i, chunk in enumerate(chunks)],
                created_at=now,
                device_ids=list(device_ids),
                history=[f"t={now} 创建发布 {release_id}: {baseline_version} -> {target_version}, "
                         f"{len(chunks)} 批, 健康阈值>={healthy_ratio}, 失败阈值>{failure_ratio}"],
            )
            self.store.releases()[release_id] = release
            return release

    def get_release(self, release_id: str) -> FirmwareRelease:
        with self.store.lock:
            return self._get_locked(release_id)

    def list_releases(self) -> list[FirmwareRelease]:
        with self.store.lock:
            return list(self.store.releases().values())

    # ---- 开始发布 ----------------------------------------------------

    def start(self, release_id: str, now: float) -> FirmwareRelease:
        with self.store.lock:
            release = self._get_locked(release_id)
            if release.status != ReleaseStatus.PENDING:
                raise ReleaseStateError(
                    f"发布 {release_id} 当前状态 {release.status.value}，无法开始"
                )
            # 原子占用：任一设备已在其他进行中的发布里则整体拒绝（全有或全无）。
            for device_id in release.device_ids:
                device = self._device_locked(device_id)
                if device.active_release_id is not None:
                    raise DeviceBusyError(
                        f"设备 {device_id} 已参与进行中的发布 {device.active_release_id}"
                    )
                if device.current_version != release.baseline_version:
                    raise ReleaseStateError(
                        f"设备 {device_id} 当前版本 {device.current_version} 与基线 "
                        f"{release.baseline_version} 不一致，禁止纳入发布"
                    )
            for device_id in release.device_ids:
                self._device_locked(device_id).active_release_id = release_id

            release.status = ReleaseStatus.IN_PROGRESS
            release.active_batch_index = 0
            release.batches[0].status = BatchStatus.ACTIVE
            self._dispatch_batch_locked(release, 0, now)
            release.history.append(f"t={now} 发布开始，激活批次 0（{len(release.batches[0].device_ids)} 台）")
            return release

    # ---- 批次推进 ----------------------------------------------------

    def advance(self, release_id: str, now: float) -> FirmwareRelease:
        with self.store.lock:
            release = self._get_locked(release_id)
            if release.status == ReleaseStatus.ROLLED_BACK:
                raise ReleaseStateError("发布已回滚，不能继续推进")
            if release.status != ReleaseStatus.IN_PROGRESS:
                raise ReleaseStateError(
                    f"发布 {release_id} 当前状态 {release.status.value}，无法推进"
                )
            idx = release.active_batch_index
            if idx is None:
                raise ReleaseStateError("没有进行中的批次")
            batch = release.batches[idx]
            verdict, reason = self._judge_batch_locked(release, batch)
            if verdict == "failed":
                # 安全阀：判定失败直接回滚，绝不推进。
                self._rollback_locked(release, now, f"批次 {idx} 触发安全阀: {reason}", failed_batch_index=idx)
                raise BatchNotReadyError(f"批次 {idx} 失败率超阈值，已自动回滚（{reason}）")
            if verdict != "healthy":
                raise BatchNotReadyError(f"批次 {idx} 尚不能推进: {reason}")

            batch.status = BatchStatus.HEALTHY
            if idx + 1 >= len(release.batches):
                release.status = ReleaseStatus.COMPLETED
                release.active_batch_index = None
                release.history.append(f"t={now} 最后一批（{idx}）健康，发布完成")
                return release

            next_idx = idx + 1
            release.active_batch_index = next_idx
            release.batches[next_idx].status = BatchStatus.ACTIVE
            self._dispatch_batch_locked(release, next_idx, now)
            release.history.append(
                f"t={now} 批次 {idx} 健康（{batch.successes} 成功/{batch.failures} 失败），"
                f"推进并激活批次 {next_idx}"
            )
            return release

    # ---- 回滚 --------------------------------------------------------

    def rollback(self, release_id: str, now: float, reason: str = "manual") -> FirmwareRelease:
        with self.store.lock:
            release = self._get_locked(release_id)
            if release.status not in (ReleaseStatus.IN_PROGRESS, ReleaseStatus.PAUSED):
                raise ReleaseStateError(
                    f"发布 {release_id} 当前状态 {release.status.value}，无需/无法回滚"
                )
            return self._rollback_locked(release, now, f"手动回滚: {reason}")

    # ---- 上报回调（由 DeviceService 在锁内调用，两阶段） -------------

    def handle_device_report(
        self,
        device: Device,
        release_id: str | None,
        outcome: str,
        version: str,
        now: float,
        phase: str,
    ) -> None:
        if release_id is None:
            return
        release = self.store.releases().get(release_id)
        if release is None:
            # 引用未知名发布的上报不能静默忽略，显式报错。
            raise ReleaseNotFoundError(f"上报引用了不存在的发布 {release_id}")
        if release.status in (ReleaseStatus.ROLLED_BACK, ReleaseStatus.ROLLING_BACK):
            # 回滚后迟到的目标版本上报必须被拒绝，保证历史自洽。
            if phase == "validate":
                raise StaleReportError(
                    f"发布 {release_id} 已回滚，迟到的 {outcome} 上报（版本 {version}）被拒绝"
                )
            return
        if phase == "validate":
            return

        idx = release.active_batch_index
        if idx is None:
            return
        batch = release.batches[idx]
        if device.device_id not in batch.device_ids:
            return
        if device.device_id in batch.reported:
            # 幂等保护第二道：DeviceService 已去重，这里再防一次。
            return

        batch.reported.add(device.device_id)
        if outcome == "failure":
            batch.failures += 1
            # 失败设备退出占用（保留 UPDATE_FAILED 状态），不再参与本发布后续批次。
            device.active_release_id = None
            release.history.append(
                f"t={now} 批次 {idx} 设备 {device.device_id} 更新失败"
            )
        else:
            batch.successes += 1
            release.history.append(
                f"t={now} 批次 {idx} 设备 {device.device_id} 成功更新到 {version}"
            )

        total = len(batch.device_ids)
        reported = batch.successes + batch.failures
        if reported < total:
            return  # 批次未收齐，不做结论

        verdict, reason = self._judge_batch_locked(release, batch)
        if verdict == "failed":
            self._rollback_locked(release, now, f"批次 {idx} 触发安全阀: {reason}", failed_batch_index=idx)

    # ---- 判定辅助 ----------------------------------------------------

    def _judge_batch_locked(
        self, release: FirmwareRelease, batch: Batch
    ) -> tuple[str, str]:
        """返回 (healthy | failed | waiting, 判定依据)。"""
        total = len(batch.device_ids)
        reported = batch.successes + batch.failures
        if reported < total:
            return "waiting", f"仅 {reported}/{total} 台回报，等待全部回报后再判定"
        healthy_ratio = batch.successes / total
        failure_ratio = batch.failures / total
        if failure_ratio > release.failure_ratio_threshold:
            return "failed", (
                f"失败率 {failure_ratio:.0%} > 阈值 {release.failure_ratio_threshold:.0%}"
                f"（{batch.failures}/{total} 失败）"
            )
        if healthy_ratio < release.healthy_ratio_threshold:
            return "waiting", (
                f"健康率 {healthy_ratio:.0%} < 阈值 {release.healthy_ratio_threshold:.0%}"
                f"（{batch.successes}/{total} 成功）"
            )
        return "healthy", (
            f"健康率 {healthy_ratio:.0%} >= 阈值 {release.healthy_ratio_threshold:.0%}"
            f"，失败率 {failure_ratio:.0%} <= 阈值 {release.failure_ratio_threshold:.0%}"
        )

    # ---- 内部状态变更 ------------------------------------------------

    def _dispatch_batch_locked(
        self, release: FirmwareRelease, index: int, now: float
    ) -> None:
        """把某一批设备置为更新中（模拟下发）。重复下发由批次状态/占用锁防住。"""
        batch = release.batches[index]
        for device_id in batch.device_ids:
            device = self._device_locked(device_id)
            device.status = DeviceStatus.UPDATING
            device.active_release_id = release.release_id
        release.history.append(
            f"t={now} 向批次 {index} 下发 {release.target_version}（{len(batch.device_ids)} 台）"
        )

    def _rollback_locked(
        self,
        release: FirmwareRelease,
        now: float,
        reason: str,
        failed_batch_index: int | None = None,
    ) -> FirmwareRelease:
        release.status = ReleaseStatus.ROLLING_BACK
        release.rollback_reason = reason
        release.history.append(f"t={now} 进入回滚流程：{reason}")

        for i, batch in enumerate(release.batches):
            if i == failed_batch_index:
                # 仅被安全阀判定失败的批次标记 failed；
                # 手动回滚时在途批次没有失败结论，标记 skipped。
                batch.status = BatchStatus.FAILED
            elif batch.status in (BatchStatus.ACTIVE, BatchStatus.PENDING):
                batch.status = BatchStatus.SKIPPED
            # HEALTHY 批次保持已健康（它们将一并回退版本，但批次结论保留为历史）

        for device_id in release.device_ids:
            device = self._device_locked(device_id)
            device.current_version = release.baseline_version
            device.last_known_good_version = release.baseline_version
            device.active_release_id = None
            device.status = DeviceStatus.ONLINE
        release.status = ReleaseStatus.ROLLED_BACK
        release.active_batch_index = None
        release.history.append(
            f"t={now} 回滚完成：{len(release.device_ids)} 台设备全部恢复到 "
            f"{release.baseline_version}"
        )
        return release

    def _get_locked(self, release_id: str) -> FirmwareRelease:
        release = self.store.releases().get(release_id)
        if release is None:
            raise ReleaseNotFoundError(f"发布不存在: {release_id}")
        return release

    def _device_locked(self, device_id: str) -> Device:
        device = self.store.devices().get(device_id)
        if device is None:
            raise DeviceNotFoundError(f"设备不存在: {device_id}")
        return device

