"""Application facade wiring store, firmware registry and device clients."""

from __future__ import annotations

import threading

from .client import DeviceClient
from .models import RolloutConfig
from .registry import FirmwareRegistry
from .store import FleetStore


class RolloutService:
    def __init__(
        self,
        config: RolloutConfig | None = None,
        known_versions: tuple[str, ...] = ("0.9.0", "1.0.0", "1.1.0", "1.2.0"),
    ) -> None:
        self._config = config or RolloutConfig()
        self._registry = FirmwareRegistry(set(known_versions))
        self._store = FleetStore(self._registry, self._config)
        self._clients: dict[str, DeviceClient] = {}
        self._gate = threading.Lock()

    @property
    def store(self) -> FleetStore:
        return self._store

    @property
    def registry(self) -> FirmwareRegistry:
        return self._registry

    def provision_device(
        self, device_id: str, version: str = "1.0.0", fail_rate: float = 0.0,
        seed: int | None = None,
    ) -> DeviceClient:
        """Create a device + its local simulated client and register it."""
        with self._gate:
            self._store.register_device(device_id, version)
            client = DeviceClient(
                device_id, version, self._store,
                fail_rate=fail_rate, seed=seed,
            )
            client.register()
            self._clients[device_id] = client
            return client

    def client(self, device_id: str) -> DeviceClient:
        return self._clients[device_id]

    def tick_clients(self) -> None:
        """Let every simulated device execute queued commands once."""
        for client in list(self._clients.values()):
            client.run_commands()
