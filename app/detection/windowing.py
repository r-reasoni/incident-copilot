"""
app/detection/windowing.py
==========================
Fixed-size time-window feature extractor and aggregator for online anomaly detection.
Groups events into aligned epoch windows based on event timestamps and watermarks.
"""
from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import numpy as np

from app.config import settings


def _parse_event_ts(ts: Any) -> datetime:
    """Parse various timestamp representations into a timezone-aware UTC datetime."""
    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    if isinstance(ts, str):
        cleaned = ts.replace(",", ".")
        try:
            dt = datetime.fromisoformat(cleaned)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            dt = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S,%f")
            dt = dt.replace(tzinfo=timezone.utc)
            return dt
    raise ValueError(f"Cannot parse timestamp from {type(ts)}: {ts}")


@dataclass
class ClosedWindow:
    """Representation of a closed time window with its extracted feature dict."""

    window_start: datetime
    window_end: datetime
    features: dict[str, float]
    event_count: int


class _WindowBuffer:
    """In-flight mutable accumulator for events falling within an open window."""

    def __init__(self) -> None:
        self.count: int = 0
        self.error_count: int = 0
        self.warn_count: int = 0
        self.new_template_count: int = 0
        self.distinct_templates: set[int] = set()
        self.service_error_warn: Counter[str] = Counter()
        self.durations: list[float] = []
        self.template_counts: Counter[int] = Counter()

    def add(
        self,
        service: str,
        level: str,
        template_id: int | None,
        duration_ms: float | None,
        is_new_template: bool = False,
    ) -> None:
        self.count += 1
        level_upper = level.upper()
        if level_upper in ("ERROR", "CRITICAL"):
            self.error_count += 1
            self.service_error_warn[service] += 1
        elif level_upper in ("WARN", "WARNING"):
            self.warn_count += 1
            self.service_error_warn[service] += 1

        if is_new_template:
            self.new_template_count += 1

        if template_id is not None:
            self.distinct_templates.add(template_id)
            self.template_counts[template_id] += 1

        if duration_ms is not None:
            self.durations.append(float(duration_ms))


class WindowAggregator:
    """Aggregates log events into epoch-aligned fixed windows with watermark closing.

    Maintains epoch multiples of `window_seconds`. Closes windows when watermark
    (max_event_ts - grace_seconds) advances past window end. Gaps in event timestamps
    generate empty windows to ensure silence/starvation is scored as a rate drop.
    """

    def __init__(
        self,
        window_seconds: int | None = None,
        window_grace_seconds: int | None = None,
        warmup_windows: int | None = None,
        top_k_templates: int | None = None,
        services: list[str] | None = None,
    ) -> None:
        self.window_seconds: int = window_seconds or settings.window_seconds
        self.window_grace_seconds: int = (
            window_grace_seconds
            if window_grace_seconds is not None
            else settings.window_grace_seconds
        )
        self.warmup_windows: int = warmup_windows or settings.warmup_windows
        self.top_k_templates: int = top_k_templates or settings.top_k_templates
        self.services: list[str] = (
            services
            if services is not None
            else getattr(settings, "services", ["api-service", "worker-service"])
        )

        self.late_dropped: int = 0
        self.total_closed_windows: int = 0

        # Windows mapped by epoch integer: epoch_start -> _WindowBuffer
        self._buffers: dict[int, _WindowBuffer] = {}
        self._max_event_ts: float | None = None
        self._last_closed_epoch: int | None = None

        # Template tracking for warm-up freezing
        self._warmup_template_counts: Counter[int] = Counter()
        self._frozen_top_k: list[int] | None = None
        self._seen_templates_history: set[int] = set()

    @property
    def frozen_top_k(self) -> list[int] | None:
        return self._frozen_top_k

    def _epoch_of(self, ts_sec: float) -> int:
        """Align timestamp to the start epoch multiple of the window."""
        return int(math.floor(ts_sec / self.window_seconds)) * self.window_seconds

    def add(
        self,
        event: dict[str, Any],
        parsed: Any | None = None,
    ) -> list[ClosedWindow]:
        """Ingest a parsed log event and return any windows that close as a result."""
        raw_ts = event.get("ts")
        if raw_ts is None:
            dt = datetime.now(timezone.utc)
        else:
            dt = _parse_event_ts(raw_ts)

        ts_sec = dt.timestamp()
        window_epoch = self._epoch_of(ts_sec)

        # Check for late events
        if self._last_closed_epoch is not None and window_epoch <= self._last_closed_epoch:
            # Event is older than an already-closed window
            self.late_dropped += 1
            return []

        # Advance max event timestamp seen
        if self._max_event_ts is None or ts_sec > self._max_event_ts:
            self._max_event_ts = ts_sec

        # Extract event metadata
        service = str(event.get("service", "unknown"))
        level = str(event.get("level", "INFO"))

        template_id: int | None = None
        duration_ms: float | None = None
        is_new_template = False

        if parsed is not None:
            template_id = getattr(parsed, "template_id", None)
            duration_ms = getattr(parsed, "duration_ms", None)
            if template_id is not None:
                if template_id not in self._seen_templates_history:
                    is_new_template = True
                    self._seen_templates_history.add(template_id)

        # Ensure buffer exists for this epoch
        if window_epoch not in self._buffers:
            self._buffers[window_epoch] = _WindowBuffer()

        self._buffers[window_epoch].add(
            service=service,
            level=level,
            template_id=template_id,
            duration_ms=duration_ms,
            is_new_template=is_new_template,
        )

        watermark = self._max_event_ts - self.window_grace_seconds
        return self._close_windows_up_to(watermark)

    def tick(self, now: datetime | None = None) -> list[ClosedWindow]:
        """Close windows based on wall-clock progression or current watermark."""
        if now is None:
            now_sec = datetime.now(timezone.utc).timestamp()
        else:
            now_sec = _parse_event_ts(now).timestamp()

        # Update watermark from wall clock if it advances past event clock
        wall_watermark = now_sec - self.window_grace_seconds
        event_watermark = (
            (self._max_event_ts - self.window_grace_seconds)
            if self._max_event_ts is not None
            else wall_watermark
        )
        effective_watermark = max(wall_watermark, event_watermark)
        return self._close_windows_up_to(effective_watermark)

    def flush(self) -> list[ClosedWindow]:
        """Close and return all open windows up to the latest known epoch."""
        if not self._buffers and self._last_closed_epoch is None:
            return []

        max_epoch = (
            max(self._buffers.keys())
            if self._buffers
            else self._last_closed_epoch
        )
        if max_epoch is None:
            return []

        # Close all windows through the end of the highest epoch buffer
        return self._close_windows_up_to(float(max_epoch + self.window_seconds))

    def _close_windows_up_to(self, watermark: float) -> list[ClosedWindow]:
        """Emit closed windows whose window_end <= watermark, filling time gaps."""
        closed: list[ClosedWindow] = []

        if self._last_closed_epoch is None:
            if not self._buffers:
                return []
            current_epoch = min(self._buffers.keys())
        else:
            current_epoch = self._last_closed_epoch + self.window_seconds

        while (current_epoch + self.window_seconds) <= watermark:
            buf = self._buffers.pop(current_epoch, None) or _WindowBuffer()
            cw = self._finalize_window(current_epoch, buf)
            closed.append(cw)
            self._last_closed_epoch = current_epoch
            current_epoch += self.window_seconds

        return closed

    def _finalize_window(self, epoch_start: int, buf: _WindowBuffer) -> ClosedWindow:
        """Extract numeric features from a buffer and build a ClosedWindow."""
        self.total_closed_windows += 1
        w_start = datetime.fromtimestamp(epoch_start, tz=timezone.utc)
        w_end = datetime.fromtimestamp(epoch_start + self.window_seconds, tz=timezone.utc)

        # Track template frequencies during warmup
        if self._frozen_top_k is None:
            self._warmup_template_counts.update(buf.template_counts)
            if self.total_closed_windows >= self.warmup_windows:
                # The top-K set is chosen from the warm-up windows and then FROZEN
                # (stable feature keys). Templates outside the set count into tpl_other.
                # Stable feature keys ensure streaming models (MinMaxScaler, HalfSpaceTrees)
                # do not experience dimension expansion or shape drift.
                most_common = self._warmup_template_counts.most_common(self.top_k_templates)
                self._frozen_top_k = sorted([tid for tid, _ in most_common])

        # Base rate and count features
        count = buf.count
        log_rate = float(count) / float(self.window_seconds)
        error_rate = float(buf.error_count) / float(count) if count > 0 else 0.0
        warn_rate = float(buf.warn_count) / float(count) if count > 0 else 0.0
        new_template_count = float(buf.new_template_count)
        distinct_template_count = float(len(buf.distinct_templates))

        # Per-service error + warn counts
        service_counts: dict[str, float] = {}
        for svc in self.services:
            key = f"err_{svc}"
            service_counts[key] = float(buf.service_error_warn[svc])

        # Durations
        if buf.durations:
            dur_mean = float(np.mean(buf.durations))
            dur_p95 = float(np.percentile(buf.durations, 95))
        else:
            dur_mean = 0.0
            dur_p95 = 0.0

        features: dict[str, float] = {
            "log_rate": log_rate,
            "error_rate": error_rate,
            "warn_rate": warn_rate,
            "new_template_count": new_template_count,
            "distinct_template_count": distinct_template_count,
            **service_counts,
            "duration_mean_ms": dur_mean,
            "duration_p95_ms": dur_p95,
        }

        # Template features: top-K templates + tpl_other
        if self._frozen_top_k is not None:
            other_count = 0
            for tid, t_cnt in buf.template_counts.items():
                if tid in self._frozen_top_k:
                    features[f"tpl_{tid}"] = float(t_cnt)
                else:
                    other_count += t_cnt
            for tid in self._frozen_top_k:
                if f"tpl_{tid}" not in features:
                    features[f"tpl_{tid}"] = 0.0
            features["tpl_other"] = float(other_count)
        else:
            # During warmup: emit counts for each template seen so far so the
            # online estimators observe their normal baseline distributions before
            # freezing the top-K set.
            for tid, t_cnt in buf.template_counts.items():
                features[f"tpl_{tid}"] = float(t_cnt)
            features["tpl_other"] = 0.0

        return ClosedWindow(
            window_start=w_start,
            window_end=w_end,
            features=features,
            event_count=count,
        )
