"""Shared fixtures and a small helper that prints inputs + verdicts.

Every test uses :func:`show_case` to print its input and the decision basis,
so a test run doubles as a readable scenario log.
"""

from __future__ import annotations

import pytest

from fleet.models import RolloutConfig
from fleet.registry import FirmwareRegistry
from fleet.service import RolloutService
from fleet.store import FleetStore

KNOWN = ("0.9.0", "1.0.0", "1.0.5", "1.1.0", "1.2.0")


@pytest.fixture
def config():
    return RolloutConfig(
        batch_percentages=(50, 100),
        health_threshold=0.8,
        failure_threshold=0.3,
        offline_timeout_s=30.0,
        min_known_version="1.0.0",
    )


@pytest.fixture
def registry():
    return FirmwareRegistry(set(KNOWN))


@pytest.fixture
def store(registry, config):
    return FleetStore(registry, config)


@pytest.fixture
def service(config):
    return RolloutService(config=config, known_versions=KNOWN)


_cases = {"n": 0}


def show_case(title: str, given: str, verdict: str) -> None:
    _cases["n"] += 1
    print(f"\n[用例 {_cases['n']}] {title}")
    print(f"  输入: {given}")
    print(f"  判定依据: {verdict}")
