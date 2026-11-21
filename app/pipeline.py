"""Stub processing pipeline; parsing and detection are added in later steps."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from threading import Lock
from typing import Any


class Pipeline:
    """Count ingested events without parsing or detecting anything."""

    def __init__(self) -> None:
        self._counts: Counter[str] = Counter()
        self._lock = Lock()
        self._last_tick: datetime | None = None
        self._tick_count: int = 0
        self._flushed: bool = False

    def process(self, event: dict[str, Any]) -> None:
        with self._lock:
            self._counts[event["service"]] += 1

    def tick(self, now: datetime) -> None:
        """Wall-clock or event-time heartbeat to close windows and incidents."""
        with self._lock:
            self._last_tick = now
            self._tick_count += 1

    def flush(self) -> None:
        """Close everything (end of replay / shutdown)."""
        with self._lock:
            self._flushed = True

    @property
    def event_count(self) -> int:
        with self._lock:
            return sum(self._counts.values())

    @property
    def last_tick(self) -> datetime | None:
        with self._lock:
            return self._last_tick

    @property
    def tick_count(self) -> int:
        with self._lock:
            return self._tick_count

    @property
    def is_flushed(self) -> bool:
        with self._lock:
            return self._flushed