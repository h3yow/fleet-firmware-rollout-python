"""可区分的业务错误类型，便于上层断言失败原因。"""
from __future__ import annotations


class RolloutError(Exception):
    """所有业务错误的基类。"""

    code = "rollout_error"


class DeviceNotFoundError(RolloutError):
    code = "device_not_found"


class DeviceAlreadyRegisteredError(RolloutError):
    code = "device_already_registered"


class ReleaseNotFoundError(RolloutError):
    code = "release_not_found"


class DeviceBusyError(RolloutError):
    """设备已参与另一个进行中的发布。"""

    code = "device_busy"


class ReleaseStateError(RolloutError):
    """发布状态机不允许该操作。"""

    code = "release_state_error"


class BatchNotReadyError(RolloutError):
    """当前批次健康比例尚未达到推进阈值。"""

    code = "batch_not_ready"


class UnknownVersionError(RolloutError):
    """上报了系统未知的固件版本。"""

    code = "unknown_version"


class StaleReportError(RolloutError):
    """过期/乱序上报：序号落后或为已处理的重复报文。"""

    code = "stale_report"


class UnsupportedVersionError(RolloutError):
    """上报版本低于系统最低支持版本，不得静默忽略。"""

    code = "unsupported_version"
