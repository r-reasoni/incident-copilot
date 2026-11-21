"""Stub processing pipeline; parsing and detection are added in later steps."""
from __future__ import annotations

from collections import Counter
from threading import Lock
from typing import Any


class Pipeline:
    """Count ingested events without parsing or detecting anything."""

    def __init__(self) -> None:
        self._counts: Counter[str] = Counter()
        self._lock = Lock()

    def process(self, event: dict[str, Any]) -> None:
        with self._lock:
            self._counts[event["service"]] += 1

    @property
    def event_count(self) -> int:
        with self._lock:
            return sum(self._counts.values())