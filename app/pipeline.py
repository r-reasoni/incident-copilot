"""
app/pipeline.py
===============
Processing pipeline linking Drain3 parsed logs, window aggregation, and online anomaly detection.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from threading import Lock
from typing import TYPE_CHECKING, Any, Callable

from app.detection.detector import AnomalyDetector, WindowResult
from app.detection.windowing import WindowAggregator

if TYPE_CHECKING:
    from app.parsing.parser import LogParser, ParsedLog


class Pipeline:
    """Ingestion pipeline orchestrating parsing, window feature extraction, and anomaly scoring."""

    def __init__(
        self,
        parser: LogParser | None = None,
        aggregator: WindowAggregator | None = None,
        detector: AnomalyDetector | None = None,
        on_window: list[Callable[[WindowResult], None]] | None = None,
    ) -> None:
        self.parser = parser
        self.aggregator = aggregator or WindowAggregator()
        self.detector = detector or AnomalyDetector()
        self.on_window: list[Callable[[WindowResult], None]] = (
            on_window if on_window is not None else []
        )
        self._counts: Counter[str] = Counter()
        self._lock = Lock()
        self._last_tick: datetime | None = None
        self._tick_count: int = 0
        self._flushed: bool = False

    def process(self, event: dict[str, Any], parsed: ParsedLog | None = None) -> None:
        """Process an ingested log event through aggregation and scoring."""
        with self._lock:
            self._counts[event.get("service", "unknown")] += 1
            closed_windows = self.aggregator.add(event, parsed)
            for window in closed_windows:
                result = self.detector.score_window(window)
                for callback in self.on_window:
                    try:
                        callback(result)
                    except Exception:
                        pass

    def tick(self, now: datetime) -> None:
        """Wall-clock or event-time heartbeat to close windows and incidents."""
        with self._lock:
            self._last_tick = now
            self._tick_count += 1
            closed_windows = self.aggregator.tick(now)
            for window in closed_windows:
                result = self.detector.score_window(window)
                for callback in self.on_window:
                    try:
                        callback(result)
                    except Exception:
                        pass

    def flush(self) -> None:
        """Close everything (end of replay / shutdown)."""
        with self._lock:
            self._flushed = True
            closed_windows = self.aggregator.flush()
            for window in closed_windows:
                result = self.detector.score_window(window)
                for callback in self.on_window:
                    try:
                        callback(result)
                    except Exception:
                        pass
            if self.parser is not None:
                self.parser.save()

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

    @property
    def windows_scored(self) -> int:
        with self._lock:
            return self.detector.windows_scored
