"""
app/incidents/manager.py
========================
Incident grouping, life-cycle tracking, and baseline comparison.
Fed by the detector's on_window callback and by pipeline tick/flush.
All timing is handled in event time.
"""
from __future__ import annotations

import asyncio
import inspect
import logging
import threading
from collections import Counter, defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from sqlalchemy import and_, func, or_, select, text

from app.config import settings
from app.db import repository
from app.db.models import Incident, LogEvent, Template, WindowScore
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)


def _parse_ts(ts: Any) -> datetime:
    """Normalize timestamps to timezone-aware UTC datetime."""
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
            return dt.replace(tzinfo=timezone.utc)
    raise ValueError(f"Cannot parse timestamp: {ts}")


def _get_window_field(window: Any, field_name: str, default: Any = None) -> Any:
    """Safely extract field from WindowResult dataclass or dict."""
    if isinstance(window, dict):
        return window.get(field_name, default)
    return getattr(window, field_name, default)


def _filter_clean_features(features: dict[str, Any]) -> dict[str, Any]:
    """Filter out underscore-prefixed keys from window features."""
    return {k: v for k, v in features.items() if not k.startswith("_")}


@dataclass
class IncidentRecord:
    """In-memory representation of an incident."""

    id: int
    started_at: datetime
    detected_at: datetime | None
    ended_at: datetime | None
    last_anomalous_end: datetime
    status: str
    peak_score: float
    peak_features: dict[str, float]
    category: str | None = None
    affected_services: list[str] = field(default_factory=list)
    affected_services_rates: list[dict[str, Any]] = field(default_factory=list)
    top_templates: list[dict[str, Any]] = field(default_factory=list)
    comparison: dict[str, Any] = field(default_factory=dict)
    alternatives: Any = None
    baseline_source: str = "recent"
    baseline_window_count: int = 0
    windows: list[Any] = field(default_factory=list)
    anomalous_windows: list[Any] = field(default_factory=list)
    baseline_windows: list[Any] = field(default_factory=list)
    llm_calls: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    analyzed_at: datetime | None = None
    root_cause: str | None = None
    confidence: float | None = None
    remediation: Any = None
    agent_trace: Any = None


async def default_agent_hook(incident_id: int, manager: IncidentManager) -> None:
    """Default stub agent hook: simply sets status to 'closed'."""
    manager.update_status(incident_id, "closed")


class IncidentManager:
    """Manages grouping anomalous windows into incidents with SQL baseline comparison."""

    def __init__(
        self,
        anomaly_gap_seconds: int | None = None,
        incident_cooldown_seconds: int | None = None,
        persist: bool = True,
        agent_hook: Callable[[int, IncidentManager], Any] | None = None,
        broadcast_callback: Callable[[dict[str, Any]], Any] | None = None,
        session_factory: Any = SessionLocal,
        services: list[str] | None = None,
    ) -> None:
        self.anomaly_gap_seconds = (
            anomaly_gap_seconds
            if anomaly_gap_seconds is not None
            else getattr(settings, "anomaly_gap_seconds", 30)
        )
        self.incident_cooldown_seconds = (
            incident_cooldown_seconds
            if incident_cooldown_seconds is not None
            else getattr(settings, "incident_cooldown_seconds", 60)
        )
        self.evidence_max_seconds = getattr(settings, "evidence_max_seconds", 300)
        self.suspect_windows = getattr(settings, "suspect_windows", 3)
        self.warmup_windows = getattr(settings, "warmup_windows", 30)
        self.persist = persist
        self.agent_hook = agent_hook or default_agent_hook
        self.broadcast_callback = broadcast_callback
        self.session_factory = session_factory
        self.services = services or getattr(settings, "services", ["api-service", "worker-service"])

        self._lock = threading.RLock()
        self._active_incident: IncidentRecord | None = None
        self._last_closed_incident: IncidentRecord | None = None
        self._incidents: dict[int, IncidentRecord] = {}
        self._next_id: int = 1

        self._watermark: datetime | None = None

        # Template metadata and fallback caches
        self._template_texts: dict[int, str] = {}
        self._template_examples: dict[int, deque[str]] = {}

        # In-memory log buffer for fallback offline aggregation when persist=False
        self._in_memory_logs: list[dict[str, Any]] = []
        self._in_memory_windows: list[Any] = []
        self._all_windows: list[Any] = []

        # Stats counters
        self.total_log_lines: int = 0
        self.total_windows: int = 0

    @property
    def incidents(self) -> int:
        with self._lock:
            return len(self._incidents)

    @property
    def llm_calls(self) -> int:
        with self._lock:
            return sum(inc.llm_calls for inc in self._incidents.values())

    @property
    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "total_log_lines": self.total_log_lines,
                "total_windows": self.total_windows,
                "incidents": self.incidents,
                "llm_calls": self.llm_calls,
                # Backward-compatibility keys
                "logs_ingested": self.total_log_lines,
                "windows_scored": self.total_windows,
            }

    def record_log(self, event: dict[str, Any], parsed: Any | None = None) -> None:
        """Record log event metadata for template text, examples, and line counts."""
        with self._lock:
            self.total_log_lines += 1

            raw_ts = event.get("ts")
            dt = _parse_ts(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
            self._update_watermark(dt)

            tid = getattr(parsed, "template_id", None) if parsed else event.get("template_id")
            raw_msg = event.get("message") or event.get("raw_message", "")
            svc = event.get("service", "unknown")
            lvl = str(event.get("level", "INFO")).upper()

            if parsed is not None:
                t_text = getattr(parsed, "template_text", None)
                if t_text:
                    self._template_texts[tid] = t_text

            if raw_msg and tid is not None:
                if tid not in self._template_examples:
                    self._template_examples[tid] = deque(maxlen=3)
                self._template_examples[tid].append(str(raw_msg)[:300])

            # In-memory buffer for offline testing without DB
            if not self.persist:
                self._in_memory_logs.append({
                    "ts": dt,
                    "service": svc,
                    "level": lvl,
                    "raw_message": raw_msg,
                    "template_id": tid,
                })
                # Cap in-memory logs to recent window
                if len(self._in_memory_logs) > 50_000:
                    self._in_memory_logs = self._in_memory_logs[-30_000:]

    def on_window(self, window: Any) -> None:
        """Callback from detector when a scored window is available."""
        with self._lock:
            self.total_windows += 1

            w_start = _parse_ts(_get_window_field(window, "window_start"))
            w_end = _parse_ts(_get_window_field(window, "window_end"))
            is_anomalous = bool(_get_window_field(window, "is_anomalous", False))
            score = float(_get_window_field(window, "score", 0.0))
            raw_features = dict(_get_window_field(window, "features", {}))
            features = _filter_clean_features(raw_features)

            run_start = _get_window_field(window, "run_start")
            run_start_dt = _parse_ts(run_start) if run_start else w_start

            confirmed_at = _get_window_field(window, "confirmed_at")
            confirmed_at_dt = _parse_ts(confirmed_at) if confirmed_at else w_end

            self._update_watermark(w_end)
            self._all_windows.append(window)
            if not self.persist:
                self._in_memory_windows.append(window)

            if is_anomalous:
                self._handle_anomalous_window(
                    window=window,
                    w_start=w_start,
                    w_end=w_end,
                    score=score,
                    features=features,
                    run_start=run_start_dt,
                    detected_at=confirmed_at_dt,
                )
            else:
                self._handle_normal_window(window=window, w_end=w_end)

    def tick(self, now: datetime) -> None:
        """Event-time heartbeat to close expired incidents."""
        with self._lock:
            now_dt = _parse_ts(now)
            self._update_watermark(now_dt)
            self._check_incident_timeout()

    def flush(self) -> None:
        """Flush and immediately close any active incident."""
        with self._lock:
            if self._active_incident is not None:
                self._close_incident_locked(self._active_incident)

    def get_incident(self, incident_id: int) -> IncidentRecord | None:
        with self._lock:
            record = self._incidents.get(incident_id)
            if record is not None:
                return record

        if self.persist:
            try:
                with self.session_factory() as session:
                    db_inc = repository.get_incident(session, incident_id)
                    if db_inc is not None:
                        return self._db_to_record(db_inc)
            except Exception:
                logger.exception("Failed to query incident %s from database", incident_id)
        return None

    def list_incidents(self, status: str | None = None) -> list[IncidentRecord]:
        with self._lock:
            records = list(self._incidents.values())

        if status:
            records = [r for r in records if r.status == status]

        if not records and self.persist:
            try:
                with self.session_factory() as session:
                    db_list = repository.list_incidents(session, status=status)
                    return [self._db_to_record(item) for item in db_list]
            except Exception:
                logger.exception("Failed to list incidents from database")

        return sorted(records, key=lambda r: r.id)

    def update_status(self, incident_id: int, status: str) -> IncidentRecord | None:
        """Update incident status and broadcast alert on analyzed/closed."""
        with self._lock:
            record = self._incidents.get(incident_id)
            if record is not None:
                record.status = status

            if self.persist:
                try:
                    with self.session_factory() as session, session.begin():
                        repository.update_incident(session, incident_id, status=status)
                except Exception:
                    logger.exception("Failed to update status for incident %s", incident_id)

            if record is not None:
                if status in ("closed", "analyzed"):
                    self._emit_alert({
                        "type": f"incident_{status}",
                        "incident_id": record.id,
                        "status": status,
                        "peak_score": record.peak_score,
                        "affected_services": record.affected_services,
                    })
            return record

    def _update_watermark(self, ts: datetime) -> None:
        if self._watermark is None or ts > self._watermark:
            self._watermark = ts

    def _handle_normal_window(self, window: Any, w_end: datetime) -> None:
        if self._active_incident is not None:
            self._active_incident.windows.append(window)
            self._check_incident_timeout()

    def _handle_anomalous_window(
        self,
        window: Any,
        w_start: datetime,
        w_end: datetime,
        score: float,
        features: dict[str, float],
        run_start: datetime,
        detected_at: datetime,
    ) -> None:
        if self._active_incident is not None:
            gap = (w_start - self._active_incident.last_anomalous_end).total_seconds()
            if gap <= self.anomaly_gap_seconds:
                # Extend open incident
                self._extend_incident_locked(self._active_incident, window, w_end, score, features)
                return
            else:
                # Gap exceeded: close previous, then check cooldown
                self._close_incident_locked(self._active_incident)

        # Check cooldown against last closed incident
        if self._last_closed_incident is not None and self._last_closed_incident.ended_at is not None:
            cooldown_gap = (w_start - self._last_closed_incident.ended_at).total_seconds()
            if cooldown_gap <= self.incident_cooldown_seconds:
                # Reopen / merge into last closed incident
                self._reopen_incident_locked(self._last_closed_incident, window, w_end, score, features)
                return

        # Open brand-new incident
        self._open_new_incident_locked(window, run_start, detected_at, w_end, score, features)

    def _open_new_incident_locked(
        self,
        window: Any,
        run_start: datetime,
        detected_at: datetime,
        w_end: datetime,
        score: float,
        features: dict[str, float],
    ) -> None:
        inc_id = self._next_id
        self._next_id += 1

        record = IncidentRecord(
            id=inc_id,
            started_at=run_start,
            detected_at=detected_at,
            ended_at=w_end,
            last_anomalous_end=w_end,
            status="open",
            peak_score=score,
            peak_features=dict(features),
            windows=[window],
            anomalous_windows=[window],
        )

        self._compute_metrics(record)

        if self.persist:
            try:
                with self.session_factory() as session, session.begin():
                    db_inc = repository.create_incident(
                        session,
                        started_at=record.started_at,
                        detected_at=record.detected_at,
                        ended_at=record.ended_at,
                        status=record.status,
                        peak_score=record.peak_score,
                        affected_services=record.affected_services,
                        top_templates=self._pack_top_templates_payload(record),
                    )
                    record.id = db_inc.id
            except Exception:
                logger.exception("Failed to persist new incident to database")

        self._incidents[record.id] = record
        self._active_incident = record

        self._emit_alert({
            "type": "incident_opened",
            "incident_id": record.id,
            "status": "open",
            "started_at": record.started_at.isoformat(),
            "detected_at": record.detected_at.isoformat() if record.detected_at else None,
            "peak_score": record.peak_score,
            "affected_services": record.affected_services,
        })

    def _extend_incident_locked(
        self,
        incident: IncidentRecord,
        window: Any,
        w_end: datetime,
        score: float,
        features: dict[str, float],
    ) -> None:
        incident.ended_at = w_end
        incident.last_anomalous_end = w_end
        incident.windows.append(window)
        incident.anomalous_windows.append(window)
        if score > incident.peak_score:
            incident.peak_score = score
            incident.peak_features = dict(features)

        self._compute_metrics(incident)
        self._persist_update_locked(incident)

    def _reopen_incident_locked(
        self,
        incident: IncidentRecord,
        window: Any,
        w_end: datetime,
        score: float,
        features: dict[str, float],
    ) -> None:
        incident.status = "open"
        incident.ended_at = w_end
        incident.last_anomalous_end = w_end
        incident.windows.append(window)
        incident.anomalous_windows.append(window)
        if score > incident.peak_score:
            incident.peak_score = score
            incident.peak_features = dict(features)

        self._compute_metrics(incident)
        self._persist_update_locked(incident)
        self._active_incident = incident

        self._emit_alert({
            "type": "incident_reopened",
            "incident_id": incident.id,
            "status": "open",
            "ended_at": incident.ended_at.isoformat() if incident.ended_at else None,
            "peak_score": incident.peak_score,
            "affected_services": incident.affected_services,
        })

    def _check_incident_timeout(self) -> None:
        if self._active_incident is None or self._watermark is None:
            return
        gap = (self._watermark - self._active_incident.last_anomalous_end).total_seconds()
        if gap > self.anomaly_gap_seconds:
            self._close_incident_locked(self._active_incident)

    def _close_incident_locked(self, incident: IncidentRecord) -> None:
        incident.ended_at = incident.last_anomalous_end
        incident.status = "closed"
        if incident.agent_trace is None:
            incident.agent_trace = {
                "agent_status": "not_run",
                "reason": "Incident closed, waiting for agent dispatch",
            }
        self._compute_metrics(incident)

        self._persist_update_locked(incident)

        self._active_incident = None
        self._last_closed_incident = incident

        self._emit_alert({
            "type": "incident_closed",
            "incident_id": incident.id,
            "status": "closed",
            "ended_at": incident.ended_at.isoformat() if incident.ended_at else None,
            "peak_score": incident.peak_score,
            "affected_services": incident.affected_services,
        })

        self._dispatch_agent_hook(incident.id)

    def _persist_update_locked(self, incident: IncidentRecord) -> None:
        if not self.persist:
            return
        try:
            with self.session_factory() as session, session.begin():
                repository.update_incident(
                    session,
                    incident.id,
                    detected_at=incident.detected_at,
                    ended_at=incident.ended_at,
                    status=incident.status,
                    peak_score=incident.peak_score,
                    affected_services=incident.affected_services,
                    top_templates=self._pack_top_templates_payload(incident),
                    agent_trace=incident.agent_trace,
                )
        except Exception:
            logger.exception("Failed to update incident %s in database", incident.id)

    def _pack_top_templates_payload(self, incident: IncidentRecord) -> dict[str, Any]:
        return {
            "top_templates": incident.top_templates,
            "templates": incident.top_templates,
            "comparison": incident.comparison,
            "peak_features": incident.peak_features,
            "affected_services_rates": incident.affected_services_rates,
            "baseline_source": incident.baseline_source,
            "baseline_window_count": incident.baseline_window_count,
        }

    def _get_quiet_windows(self, incident: IncidentRecord) -> tuple[list[dict[str, Any]], str, int]:
        """Identify quiet windows before incident.started_at.

        A window is quiet if:
        - not flagged (is_anomalous == False)
        - not in suspect-window quarantine (neither high score nor within suspect_windows after high score)
        - not within 60 seconds after the end of any earlier incident (and not during any earlier incident)
        - window_end <= incident.started_at
        """
        all_windows = list(self._all_windows)
        if self.persist and len(all_windows) < 10:
            try:
                with self.session_factory() as session:
                    db_wins = session.scalars(
                        select(WindowScore).order_by(WindowScore.window_start.asc())
                    ).all()
                    if db_wins:
                        all_windows = list(db_wins)
            except Exception:
                pass

        # Identify intervals of earlier incidents (plus 60s cooldown)
        earlier_exclusions: list[tuple[datetime, datetime]] = []
        with self._lock:
            all_incidents = list(self._incidents.values())
        if self.persist:
            try:
                with self.session_factory() as session:
                    db_incs = repository.list_incidents(session)
                    for dbi in db_incs:
                        if dbi.id != incident.id:
                            s = dbi.started_at
                            e = (dbi.ended_at or s) + timedelta(seconds=60)
                            earlier_exclusions.append((s, e))
            except Exception:
                pass
        for inc in all_incidents:
            if inc.id != incident.id:
                s = inc.started_at
                e = (inc.ended_at or inc.last_anomalous_end or s) + timedelta(seconds=60)
                earlier_exclusions.append((s, e))

        quarantine_remaining = 0
        annotated: list[dict[str, Any]] = []
        for idx, w in enumerate(all_windows):
            w_start = _parse_ts(_get_window_field(w, "window_start"))
            w_end = _parse_ts(_get_window_field(w, "window_end"))
            is_anom = bool(_get_window_field(w, "is_anomalous", False))
            score = float(_get_window_field(w, "score", 0.0))
            raw_feats = dict(_get_window_field(w, "features", {}))
            raw_thresh = raw_feats.get("_threshold")
            if raw_thresh is None:
                raw_thresh = _get_window_field(w, "threshold")
            thresh = float(raw_thresh) if raw_thresh is not None else 1.0

            in_quarantine = (quarantine_remaining > 0)
            is_high = (score >= thresh) or is_anom

            if is_high:
                quarantine_remaining = self.suspect_windows
            elif quarantine_remaining > 0:
                quarantine_remaining -= 1

            in_earlier = any(
                (w_start < excl_end and w_end > excl_start)
                for excl_start, excl_end in earlier_exclusions
            )

            is_quiet = (not is_anom) and (not is_high) and (not in_quarantine) and (not in_earlier)

            annotated.append({
                "index": idx,
                "w_start": w_start,
                "w_end": w_end,
                "score": score,
                "threshold": thresh,
                "is_anomalous": is_anom,
                "is_high": is_high,
                "in_quarantine": in_quarantine,
                "in_earlier": in_earlier,
                "is_quiet": is_quiet,
                "features": raw_feats,
            })

        candidate_quiet = [
            aw for aw in annotated
            if aw["w_end"] <= incident.started_at and aw["is_quiet"]
        ]

        recent_quiet = candidate_quiet[-12:]
        if len(recent_quiet) >= 4:
            return recent_quiet, "recent", len(recent_quiet)

        post_warmup_quiet = [
            aw for aw in annotated
            if aw["index"] >= self.warmup_windows and aw["is_quiet"]
        ]
        ref_before = [aw for aw in post_warmup_quiet if aw["w_end"] <= incident.started_at]
        if len(ref_before) >= 4:
            ref_selected = ref_before[:12]
        elif post_warmup_quiet:
            ref_selected = post_warmup_quiet[:12]
        else:
            ref_selected = recent_quiet if recent_quiet else candidate_quiet

        return ref_selected, "reference", len(ref_selected)

    def _compute_metrics(self, incident: IncidentRecord) -> None:
        """Compute affected_services and top_templates via SQL aggregation (or in-memory fallback)."""
        started_at = incident.started_at
        ended_at = incident.ended_at or incident.last_anomalous_end

        # 1. Cap evidence interval at EVIDENCE_MAX_SECONDS (default 300s)
        evidence_end = min(ended_at, started_at + timedelta(seconds=self.evidence_max_seconds))
        incident_dur = max((evidence_end - started_at).total_seconds(), 1.0)

        # 2. Quiet-window baseline
        quiet_windows, baseline_source, baseline_count = self._get_quiet_windows(incident)
        incident.baseline_source = baseline_source
        incident.baseline_window_count = baseline_count

        if quiet_windows:
            baseline_intervals = [(w["w_start"], w["w_end"]) for w in quiet_windows]
            baseline_dur = max(sum((we - ws).total_seconds() for ws, we in baseline_intervals), 1.0)
        else:
            baseline_intervals = [(started_at - timedelta(seconds=120), started_at)]
            baseline_dur = 120.0

        if self.persist:
            try:
                self._compute_metrics_sql(
                    incident,
                    started_at,
                    ended_at,
                    evidence_end,
                    baseline_intervals,
                    baseline_dur,
                    incident_dur,
                    quiet_windows,
                )
                return
            except Exception:
                logger.exception("SQL metric computation failed, falling back to in-memory")

        self._compute_metrics_in_memory(
            incident,
            started_at,
            ended_at,
            evidence_end,
            baseline_intervals,
            baseline_dur,
            incident_dur,
            quiet_windows,
        )

    def _compute_metrics_sql(
        self,
        incident: IncidentRecord,
        started_at: datetime,
        ended_at: datetime,
        evidence_end: datetime,
        baseline_intervals: list[tuple[datetime, datetime]],
        baseline_dur: float,
        incident_dur: float,
        quiet_windows: list[dict[str, Any]],
    ) -> None:
        with self.session_factory() as session:
            # Baseline time filter across quiet window intervals
            base_time_filter = or_(*[
                and_(LogEvent.ts >= ws, LogEvent.ts < we)
                for ws, we in baseline_intervals
            ])

            # 1. Affected services: WARN+ERROR counts in evidence interval vs quiet baseline
            inc_err_stmt = (
                select(LogEvent.service, func.count().label("cnt"))
                .where(
                    LogEvent.ts >= started_at,
                    LogEvent.ts <= evidence_end,
                    LogEvent.level.in_(["WARN", "WARNING", "ERROR", "CRITICAL"]),
                )
                .group_by(LogEvent.service)
            )
            base_err_stmt = (
                select(LogEvent.service, func.count().label("cnt"))
                .where(
                    base_time_filter,
                    LogEvent.level.in_(["WARN", "WARNING", "ERROR", "CRITICAL"]),
                )
                .group_by(LogEvent.service)
            )
            inc_err_counts = dict(session.execute(inc_err_stmt).all())
            base_err_counts = dict(session.execute(base_err_stmt).all())

            candidate_services = sorted(set(list(inc_err_counts.keys()) + list(base_err_counts.keys()) + self.services))
            affected = []
            affected_rates = []
            for svc in candidate_services:
                b_cnt = base_err_counts.get(svc, 0)
                i_cnt = inc_err_counts.get(svc, 0)
                b_rate = b_cnt / baseline_dur
                i_rate = i_cnt / incident_dur
                if i_cnt > 0 and (b_rate == 0 or i_rate >= 2.0 * b_rate):
                    affected.append(svc)
                affected_rates.append({
                    "service": svc,
                    "baseline_rate": round(b_rate, 4),
                    "incident_rate": round(i_rate, 4),
                })
            incident.affected_services = affected
            incident.affected_services_rates = affected_rates
            incident.service_error_rates = affected_rates

            # 2. Top templates (max 8) via SQL aggregation
            # Ratio floor: when baseline rate is 0, use 1 / baseline_dur as the denominator
            ratio_floor = round(1.0 / baseline_dur, 6) if baseline_dur > 0 else 0.001
            incident.ratio_floor = ratio_floor
            tpl_inc_stmt = (
                select(LogEvent.template_id, LogEvent.level, func.count().label("cnt"))
                .where(
                    LogEvent.ts >= started_at,
                    LogEvent.ts <= evidence_end,
                    LogEvent.template_id.isnot(None),
                )
                .group_by(LogEvent.template_id, LogEvent.level)
            )
            tpl_inc_rows = session.execute(tpl_inc_stmt).all()
            inc_counts: dict[int, int] = defaultdict(int)
            level_mix: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
            for tid, lvl, cnt in tpl_inc_rows:
                inc_counts[tid] += cnt
                level_mix[tid][lvl] = cnt

            # Baseline counts
            tpl_base_stmt = (
                select(LogEvent.template_id, func.count().label("cnt"))
                .where(
                    base_time_filter,
                    LogEvent.template_id.isnot(None),
                )
                .group_by(LogEvent.template_id)
            )
            base_counts = dict(session.execute(tpl_base_stmt).all())

            # Check is_new: zero events in log_events before started_at
            all_candidate_tids = list(inc_counts.keys())
            prior_counts = {}
            if all_candidate_tids:
                prior_stmt = (
                    select(LogEvent.template_id, func.count().label("cnt"))
                    .where(
                        LogEvent.ts < started_at,
                        LogEvent.template_id.in_(all_candidate_tids),
                    )
                    .group_by(LogEvent.template_id)
                )
                prior_counts = dict(session.execute(prior_stmt).all())

            # Up to 3 example raw lines (capped at evidence_end)
            example_lines: dict[int, list[str]] = defaultdict(list)
            if all_candidate_tids:
                sample_stmt = (
                    select(LogEvent.template_id, LogEvent.raw_message)
                    .where(
                        LogEvent.ts >= started_at,
                        LogEvent.ts <= evidence_end,
                        LogEvent.template_id.in_(all_candidate_tids),
                    )
                    .order_by(LogEvent.id.desc())
                    .limit(100)
                )
                for tid, raw_msg in session.execute(sample_stmt).all():
                    if len(example_lines[tid]) < 3 and raw_msg not in example_lines[tid]:
                        example_lines[tid].append(str(raw_msg)[:300])

            # Template texts
            tpl_texts: dict[int, str] = {}
            if all_candidate_tids:
                txt_stmt = select(Template.template_id, Template.template_text).where(
                    Template.template_id.in_(all_candidate_tids)
                )
                tpl_texts = dict(session.execute(txt_stmt).all())

            top_items = []
            for tid in all_candidate_tids:
                i_cnt = inc_counts[tid]
                b_cnt = base_counts.get(tid, 0)
                b_rate = b_cnt / baseline_dur
                i_rate = i_cnt / incident_dur
                if b_rate > 0:
                    ratio = round(i_rate / b_rate, 2)
                else:
                    # Documented ratio floor: 1 / baseline_seconds prevents division by zero
                    ratio = round(i_rate / ratio_floor, 2) if i_rate > 0 else 0.0

                is_new = prior_counts.get(tid, 0) == 0
                absent_in_baseline = (b_cnt == 0)
                t_text = tpl_texts.get(tid) or self._template_texts.get(tid) or f"Template {tid}"
                exs = example_lines.get(tid) or list(self._template_examples.get(tid, []))[:3]

                top_items.append({
                    "template_id": tid,
                    "text": t_text,
                    "baseline_rate": round(b_rate, 4),
                    "incident_rate": round(i_rate, 4),
                    "ratio": ratio,
                    "level_mix": dict(level_mix[tid]),
                    "examples": exs,
                    "is_new": is_new,
                    "absent_in_baseline": absent_in_baseline,
                    "_sort_key": (i_rate - b_rate, ratio),
                })
            top_items.sort(key=lambda t: t["_sort_key"], reverse=True)
            incident.top_templates = [{k: v for k, v in t.items() if k != "_sort_key"} for t in top_items[:8]]

            # 3. Baseline vs incident summary from window_scores
            base_win_feats = [w.get("features", {}) for w in quiet_windows if isinstance(w, dict)]
            if not base_win_feats and baseline_intervals:
                base_win_stmt = select(WindowScore.features).where(base_time_filter)
                base_win_feats = session.scalars(base_win_stmt).all()

            inc_win_stmt = select(WindowScore.features).where(
                WindowScore.window_start >= started_at,
                WindowScore.window_end <= evidence_end + timedelta(seconds=15),
            )
            inc_win_feats = session.scalars(inc_win_stmt).all()

            comparison = {}
            for metric in ("log_rate", "error_rate", "warn_rate", "duration_mean_ms", "duration_p95_ms"):
                b_vals = [f.get(metric, 0.0) for f in base_win_feats if isinstance(f, dict)]
                i_vals = [f.get(metric, 0.0) for f in inc_win_feats if isinstance(f, dict)]
                b_m = sum(b_vals) / len(b_vals) if b_vals else 0.0
                i_m = sum(i_vals) / len(i_vals) if i_vals else 0.0
                comparison[metric] = {
                    "baseline": round(b_m, 3),
                    "incident": round(i_m, 3),
                }
            incident.comparison = comparison

    def _compute_metrics_in_memory(
        self,
        incident: IncidentRecord,
        started_at: datetime,
        ended_at: datetime,
        evidence_end: datetime,
        baseline_intervals: list[tuple[datetime, datetime]],
        baseline_dur: float,
        incident_dur: float,
        quiet_windows: list[dict[str, Any]],
    ) -> None:
        """In-memory aggregation fallback for unit tests and offline environments."""
        def is_in_quiet(ts: datetime) -> bool:
            return any(ws <= ts < we for ws, we in baseline_intervals)

        # 1. Affected services
        base_errs: Counter[str] = Counter()
        inc_errs: Counter[str] = Counter()
        for e in self._in_memory_logs:
            ts = e["ts"]
            lvl = e["level"]
            svc = e["service"]
            if lvl in ("WARN", "WARNING", "ERROR", "CRITICAL"):
                if is_in_quiet(ts):
                    base_errs[svc] += 1
                elif started_at <= ts <= evidence_end:
                    inc_errs[svc] += 1

        candidate_services = sorted(set(list(inc_errs.keys()) + list(base_errs.keys()) + self.services))
        affected = []
        affected_rates = []
        for svc in candidate_services:
            b_cnt = base_errs[svc]
            i_cnt = inc_errs[svc]
            b_rate = b_cnt / baseline_dur
            i_rate = i_cnt / incident_dur
            if i_cnt > 0 and (b_rate == 0 or i_rate >= 2.0 * b_rate):
                affected.append(svc)
            affected_rates.append({
                "service": svc,
                "baseline_rate": round(b_rate, 4),
                "incident_rate": round(i_rate, 4),
            })
        incident.affected_services = affected
        incident.affected_services_rates = affected_rates
        incident.service_error_rates = affected_rates

        # Ratio floor: when baseline rate is 0, floor baseline rate at 1 / baseline_dur
        ratio_floor = round(1.0 / baseline_dur, 6) if baseline_dur > 0 else 0.001
        incident.ratio_floor = ratio_floor

        # 2. Top templates
        inc_tpl_counts: Counter[int] = Counter()
        base_tpl_counts: Counter[int] = Counter()
        prior_tpl_counts: Counter[int] = Counter()
        level_mix: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        example_lines: dict[int, list[str]] = defaultdict(list)

        for e in self._in_memory_logs:
            ts = e["ts"]
            tid = e["template_id"]
            lvl = e["level"]
            msg = e["raw_message"]
            if tid is None:
                continue

            if ts < started_at:
                prior_tpl_counts[tid] += 1
                if is_in_quiet(ts):
                    base_tpl_counts[tid] += 1
            elif started_at <= ts <= evidence_end:
                inc_tpl_counts[tid] += 1
                level_mix[tid][lvl] += 1
                if len(example_lines[tid]) < 3 and msg not in example_lines[tid]:
                    example_lines[tid].append(str(msg)[:300])

        top_items = []
        for tid in inc_tpl_counts:
            i_cnt = inc_tpl_counts[tid]
            b_cnt = base_tpl_counts[tid]
            b_rate = b_cnt / baseline_dur
            i_rate = i_cnt / incident_dur
            ratio = round(i_rate / b_rate, 2) if b_rate > 0 else (round(i_rate / ratio_floor, 2) if i_rate > 0 else 0.0)
            is_new = prior_tpl_counts[tid] == 0
            absent_in_baseline = (b_cnt == 0)
            t_text = self._template_texts.get(tid) or f"Template {tid}"
            exs = example_lines[tid] or list(self._template_examples.get(tid, []))[:3]

            top_items.append({
                "template_id": tid,
                "text": t_text,
                "baseline_rate": round(b_rate, 4),
                "incident_rate": round(i_rate, 4),
                "ratio": ratio,
                "level_mix": dict(level_mix[tid]),
                "examples": exs,
                "is_new": is_new,
                "absent_in_baseline": absent_in_baseline,
                "_sort_key": (i_rate - b_rate, ratio),
            })
        top_items.sort(key=lambda t: t["_sort_key"], reverse=True)
        incident.top_templates = [{k: v for k, v in t.items() if k != "_sort_key"} for t in top_items[:8]]

        # 3. Window score summary
        base_win_feats = [w.get("features", {}) for w in quiet_windows if isinstance(w, dict)]
        inc_wins = [
            w for w in (self._in_memory_windows or self._all_windows)
            if started_at <= _parse_ts(_get_window_field(w, "window_start")) <= evidence_end
        ]

        comparison = {}
        for metric in ("log_rate", "error_rate", "warn_rate", "duration_mean_ms", "duration_p95_ms"):
            b_vals = [f.get(metric, 0.0) for f in base_win_feats if isinstance(f, dict)]
            i_vals = [_get_window_field(w, "features", {}).get(metric, 0.0) for w in inc_wins]
            b_m = sum(b_vals) / len(b_vals) if b_vals else 0.0
            i_m = sum(i_vals) / len(i_vals) if i_vals else 0.0
            comparison[metric] = {
                "baseline": round(b_m, 3),
                "incident": round(i_m, 3),
            }
        incident.comparison = comparison

    def _dispatch_agent_hook(self, incident_id: int) -> None:
        """Call agent hook asynchronously without blocking."""
        hook = self.agent_hook
        if hook is None:
            return

        def _runner():
            try:
                if inspect.iscoroutinefunction(hook):
                    asyncio.run(hook(incident_id, self))
                else:
                    hook(incident_id, self)
            except Exception:
                logger.exception("Error executing agent hook for incident %s", incident_id)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running() and inspect.iscoroutinefunction(hook):
            asyncio.create_task(hook(incident_id, self))
        else:
            thread = threading.Thread(target=_runner, daemon=True)
            thread.start()

    def _emit_alert(self, payload: dict[str, Any]) -> None:
        """Bridge sync alert emission into asyncio event loop or thread."""
        callback = self.broadcast_callback
        if callback is None:
            return

        if inspect.iscoroutinefunction(callback):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop is not None and loop.is_running():
                asyncio.create_task(callback(payload))
            else:
                threading.Thread(
                    target=lambda: asyncio.run(callback(payload)),
                    daemon=True,
                ).start()
        else:
            try:
                callback(payload)
            except Exception:
                logger.exception("Error calling sync broadcast callback")

    def _db_to_record(self, db_inc: Any) -> IncidentRecord:
        top_data = db_inc.top_templates or {}
        if isinstance(top_data, list):
            top_list = top_data
            comp = {}
            peak_f = {}
            rates = []
        else:
            top_list = top_data.get("top_templates") or top_data.get("templates") or []
            comp = top_data.get("comparison") or {}
            peak_f = top_data.get("peak_features") or {}
            rates = top_data.get("affected_services_rates") or []

        return IncidentRecord(
            id=db_inc.id,
            started_at=db_inc.started_at,
            detected_at=db_inc.detected_at,
            ended_at=db_inc.ended_at,
            last_anomalous_end=db_inc.ended_at or db_inc.started_at,
            status=db_inc.status,
            category=db_inc.category,
            peak_score=db_inc.peak_score,
            peak_features=peak_f,
            affected_services=db_inc.affected_services or [],
            affected_services_rates=rates,
            top_templates=top_list,
            comparison=comp,
            alternatives=db_inc.alternatives,
            baseline_source=top_data.get("baseline_source", "recent") if isinstance(top_data, dict) else "recent",
            baseline_window_count=top_data.get("baseline_window_count", 0) if isinstance(top_data, dict) else 0,
            llm_calls=db_inc.llm_calls or 0,
            created_at=db_inc.created_at,
            analyzed_at=db_inc.analyzed_at,
            root_cause=db_inc.root_cause,
            confidence=db_inc.confidence,
            remediation=db_inc.remediation,
            agent_trace=db_inc.agent_trace,
        )
