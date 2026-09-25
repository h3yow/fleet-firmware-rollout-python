"""Application facade wiring store, registry and the simulated device clients."""

from __future__ import annotations

from .models import RolloutConfig
from .registry import FirmwareRegistry
from .store import FleetStore


class RolloutService:
    def __init__(self, config: RolloutConfig | None = None) -> None:
        raise NotImplementedError

    @property
    def store(self) -> FleetStore:
        raise NotImplementedError

    @property
    def registry(self) -> FirmwareRegistry:
        raise NotImplementedError
