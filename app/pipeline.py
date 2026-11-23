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
    from app.incidents.manager import IncidentManager
    from app.parsing.parser import LogParser, ParsedLog


class Pipeline:
    """Ingestion pipeline orchestrating parsing, window feature extraction, anomaly scoring, and incident grouping."""

    def __init__(
        self,
        parser: LogParser | None = None,
        aggregator: WindowAggregator | None = None,
        detector: AnomalyDetector | None = None,
        incident_manager: IncidentManager | None = None,
        on_window: list[Callable[[WindowResult], None]] | None = None,
    ) -> None:
        self.parser = parser
        self.aggregator = aggregator or WindowAggregator()
        self.detector = detector or AnomalyDetector()
        self.incident_manager = incident_manager
        self.on_window: list[Callable[[WindowResult], None]] = (
            list(on_window) if on_window is not None else []
        )
        if self.incident_manager is not None and self.incident_manager.on_window not in self.on_window:
            self.on_window.append(self.incident_manager.on_window)

        self._counts: Counter[str] = Counter()
        self._lock = Lock()
        self._last_tick: datetime | None = None
        self._tick_count: int = 0
        self._flushed: bool = False

    def process(self, event: dict[str, Any], parsed: ParsedLog | None = None) -> None:
        """Process an ingested log event through aggregation, scoring, and incident tracking."""
        with self._lock:
            self._counts[event.get("service", "unknown")] += 1
            if self.incident_manager is not None:
                self.incident_manager.record_log(event, parsed)
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
            if self.incident_manager is not None:
                self.incident_manager.tick(now)

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
            if self.incident_manager is not None:
                self.incident_manager.flush()
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
