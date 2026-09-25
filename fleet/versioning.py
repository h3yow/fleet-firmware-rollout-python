"""Semantic-version style parsing and comparison helpers.

Versions look like ``1.2.3`` (numeric dot-separated components). Missing
components are treated as zero, so ``1.2`` == ``1.2.0``. Anything that cannot
be parsed returns ``None`` / raises :class:`ValueError` so callers can treat
the version as *unknown* rather than silently accepting it.
"""

from __future__ import annotations

from typing import Optional


def parse_version(text: str) -> Optional[tuple[int, ...]]:
    """Parse ``text`` into a tuple of ints, or ``None`` if malformed."""
    if not isinstance(text, str) or not text:
        return None
    parts = text.strip().split(".")
    nums: list[int] = []
    for part in parts:
        if not part.isdigit():
            return None
        nums.append(int(part))
    return tuple(nums) if nums else None


def compare_version(a: str, b: str) -> int:
    """Return -1/0/1 for a<b / a==b / a>b. Raises on malformed versions."""
    pa, pb = parse_version(a), parse_version(b)
    if pa is None:
        raise ValueError(f"malformed version: {a!r}")
    if pb is None:
        raise ValueError(f"malformed version: {b!r}")
    width = max(len(pa), len(pb))
    pa += (0,) * (width - len(pa))
    pb += (0,) * (width - len(pb))
    if pa < pb:
        return -1
    if pa > pb:
        return 1
    return 0
