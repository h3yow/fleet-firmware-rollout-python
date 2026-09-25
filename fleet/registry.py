"""Catalog of firmware versions known to the platform.

A report carrying a version that is malformed or absent from this catalog is
rejected with :class:`~fleet.models.RejectReason.UNKNOWN_VERSION` instead of
being silently ignored.
"""

from __future__ import annotations

from .versioning import parse_version


class FirmwareRegistry:
    def __init__(self, known_versions: set[str] | None = None) -> None:
        self._versions: set[str] = set()
        for version in known_versions or ():
            self.add(version)

    def add(self, version: str) -> None:
        if parse_version(version) is None:
            raise ValueError(f"malformed version: {version!r}")
        self._versions.add(version)

    def is_known(self, version: str) -> bool:
        return parse_version(version) is not None and version in self._versions

    def all(self) -> set[str]:
        return set(self._versions)
