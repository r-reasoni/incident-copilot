"""Processing pipeline connecting parser, detector, and incident manager."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from threading import Lock
from typing import Any

from app.detection import AnomalyDetector
from app.incidents import IncidentManager


class Pipeline:
    """Processes ingested logs and coordinates with detector and IncidentManager."""

    def __init__(
        self,
        parser: Any | None = None,
        incident_manager: IncidentManager | None = None,
        detector: AnomalyDetector | None = None,
    ) -> None:
        self.parser = parser
        self.incident_manager = incident_manager or IncidentManager()
        self.detector = detector or AnomalyDetector(on_window=self.incident_manager.on_window)
        self._counts: Counter[str] = Counter()
        self._lock = Lock()
        self._last_tick: datetime | None = None
        self._tick_count: int = 0
        self._flushed: bool = False

    def process(self, event: dict[str, Any], parsed: Any | None = None) -> None:
        with self._lock:
            self._counts[event["service"]] += 1
            self.incident_manager.total_log_lines += 1

        self.detector.process(event, parsed)

    def tick(self, now: datetime) -> None:
        """Wall-clock or event-time heartbeat to close windows and incidents."""
        with self._lock:
            self._last_tick = now
            self._tick_count += 1

        self.detector.tick(now)
        self.incident_manager.tick(now)

    def flush(self) -> None:
        """Close everything (end of replay / shutdown)."""
        with self._lock:
            self._flushed = True
            if self.parser is not None and hasattr(self.parser, "save"):
                self.parser.save()

        self.detector.flush()
        self.incident_manager.flush()

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
