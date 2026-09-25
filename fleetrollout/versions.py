"""固件版本注册与版本比较。"""
from __future__ import annotations

from .errors import UnknownVersionError


class VersionRegistry:
    """已知版本白名单：未知版本上报必须显式报错，不能静默忽略。"""

    def __init__(self, known_versions: list[str] | None = None) -> None:
        self._known: set[str] = set(known_versions or [])

    def register(self, version: str) -> None:
        self._known.add(version)

    def is_known(self, version: str) -> bool:
        return version in self._known

    def require_known(self, version: str) -> None:
        if not self.is_known(version):
            raise UnknownVersionError(f"未知固件版本: {version!r}，不在已注册版本清单中")


def parse_version(version: str) -> tuple[int, ...]:
    parts = version.strip().split(".")
    try:
        return tuple(int(p) for p in parts)
    except ValueError as exc:  # 非数字版本段同样视为未知/非法版本
        raise UnknownVersionError(f"非法版本字符串: {version!r}") from exc


def is_older(a: str, b: str) -> bool:
    """若 a 比 b 旧则返回 True；版本段数不同时按 0 补齐比较。"""
    pa, pb = parse_version(a), parse_version(b)
    width = max(len(pa), len(pb))
    pa += (0,) * (width - len(pa))
    pb += (0,) * (width - len(pb))
    return pa < pb
