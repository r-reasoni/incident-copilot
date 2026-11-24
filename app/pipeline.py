"""
app/pipeline.py
===============
Processing pipeline linking Drain3 parsed logs, window aggregation, and online anomaly detection.
"""
from __future__ import annotations

import json
import logging
from collections import Counter
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Any, Callable

from app.config import settings
from app.detection.detector import AnomalyDetector, WindowResult
from app.detection.windowing import WindowAggregator

if TYPE_CHECKING:
    from app.incidents.manager import IncidentManager
    from app.parsing.parser import LogParser, ParsedLog

logger = logging.getLogger(__name__)


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
        self._errors: int = 0

    def _dispatch_window_result(self, result: WindowResult) -> None:
        record_path = getattr(settings, "record_windows_path", "")
        if record_path:
            try:
                p = Path(record_path)
                p.parent.mkdir(parents=True, exist_ok=True)
                with open(p, "a", encoding="utf-8") as f:
                    entry = {
                        "window_start": result.window_start.isoformat(),
                        "score": result.score,
                        "threshold": result.threshold,
                        "is_anomalous": result.is_anomalous,
                        "confirmed_at": result.confirmed_at.isoformat() if result.confirmed_at else None,
                    }
                    f.write(json.dumps(entry) + "\n")
            except Exception:
                self._errors += 1
                logger.exception("Failed to write window result to record_windows_path")

        for callback in self.on_window:
            try:
                callback(result)
            except Exception:
                self._errors += 1
                logger.exception("Error executing on_window callback")

    def process(self, event: dict[str, Any], parsed: ParsedLog | None = None) -> None:
        """Process an ingested log event through aggregation, scoring, and incident tracking."""
        with self._lock:
            self._counts[event.get("service", "unknown")] += 1
            if self.incident_manager is not None:
                try:
                    self.incident_manager.record_log(event, parsed)
                except Exception:
                    self._errors += 1
                    logger.exception("Error recording log in incident manager")
            try:
                closed_windows = self.aggregator.add(event, parsed)
            except Exception:
                self._errors += 1
                logger.exception("Error adding event to aggregator")
                closed_windows = []

            for window in closed_windows:
                try:
                    result = self.detector.score_window(window)
                except Exception:
                    self._errors += 1
                    logger.exception("Error scoring window in detector")
                    continue
                self._dispatch_window_result(result)

    def tick(self, now: datetime) -> None:
        """Wall-clock or event-time heartbeat to close windows and incidents."""
        with self._lock:
            self._last_tick = now
            self._tick_count += 1
            try:
                closed_windows = self.aggregator.tick(now)
            except Exception:
                self._errors += 1
                logger.exception("Error ticking aggregator")
                closed_windows = []

            for window in closed_windows:
                try:
                    result = self.detector.score_window(window)
                except Exception:
                    self._errors += 1
                    logger.exception("Error scoring window during tick")
                    continue
                self._dispatch_window_result(result)
            if self.incident_manager is not None:
                try:
                    self.incident_manager.tick(now)
                except Exception:
                    self._errors += 1
                    logger.exception("Error ticking incident manager")

    def flush(self) -> None:
        """Close everything (end of replay / shutdown)."""
        with self._lock:
            self._flushed = True
            try:
                closed_windows = self.aggregator.flush()
            except Exception:
                self._errors += 1
                logger.exception("Error flushing aggregator")
                closed_windows = []

            for window in closed_windows:
                try:
                    result = self.detector.score_window(window)
                except Exception:
                    self._errors += 1
                    logger.exception("Error scoring window during flush")
                    continue
                self._dispatch_window_result(result)
            if self.incident_manager is not None:
                try:
                    self.incident_manager.flush()
                except Exception:
                    self._errors += 1
                    logger.exception("Error flushing incident manager")
            if self.parser is not None:
                try:
                    self.parser.save()
                except Exception:
                    self._errors += 1
                    logger.exception("Error saving parser during flush")

    @property
    def errors(self) -> int:
        with self._lock:
            return self._errors

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
