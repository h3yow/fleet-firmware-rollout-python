"""Semantic-version style parsing and comparison helpers."""

from __future__ import annotations

from typing import Optional


def parse_version(text: str) -> Optional[tuple[int, ...]]:
    raise NotImplementedError


def compare_version(a: str, b: str) -> int:
    raise NotImplementedError
