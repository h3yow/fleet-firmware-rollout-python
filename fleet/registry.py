"""Catalog of firmware versions known to the platform."""

from __future__ import annotations


class FirmwareRegistry:
    def __init__(self, known_versions: set[str] | None = None) -> None:
        raise NotImplementedError

    def is_known(self, version: str) -> bool:
        raise NotImplementedError

    def add(self, version: str) -> None:
        raise NotImplementedError
