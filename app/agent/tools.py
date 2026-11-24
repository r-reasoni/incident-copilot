"""Agent investigation tools for querying logs, metrics, network stats, and deployments."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import psutil
from sqlalchemy.orm import Session

from app.db import repository
from app.db.models import MetricsSample
from app.db.session import SessionLocal

REPO_ROOT = Path(__file__).resolve().parents[2]
CHANGES_PATH = REPO_ROOT / "changes.json"


def _parse_ts(value: Any) -> datetime:
    """Parse string or datetime to timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str):
        cleaned = value.strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(cleaned)
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    raise ValueError(f"Invalid timestamp format: {value!r}")


def query_logs(
    service: str,
    start: str | datetime,
    end: str | datetime,
    level: str | None = None,
    template_id: int | None = None,
    limit: int = 50,
    session: Session | None = None,
) -> dict[str, Any]:
    """Query service logs in [start, end) interval with optional level and template filtering.

    Truncates each log message to 200 characters and caps limit at 100.
    """
    try:
        if not service or not isinstance(service, str):
            return {"error": "service must be a non-empty string"}

        start_dt = _parse_ts(start)
        end_dt = _parse_ts(end)
        if start_dt > end_dt:
            return {"error": "start must be earlier than or equal to end"}
        if (end_dt - start_dt).total_seconds() > 600:
            return {"error": "time range exceeds maximum allowed window of 10 minutes"}

        capped_limit = max(1, min(100, int(limit)))
        norm_level = level.upper() if isinstance(level, str) and level else None
        norm_template_id = int(template_id) if template_id is not None else None

        def _do_query(s: Session) -> list[dict[str, Any]]:
            records = repository.get_logs(
                session=s,
                service=service,
                start=start_dt,
                end=end_dt,
                level=norm_level,
                template_id=norm_template_id,
                limit=capped_limit,
            )
            return [
                {
                    "ts": log.ts.isoformat(),
                    "level": log.level,
                    "template_id": log.template_id,
                    "message": (log.raw_message or "")[:200],
                }
                for log in records
            ]

        if session is not None:
            rows = _do_query(session)
        else:
            with SessionLocal() as s:
                rows = _do_query(s)

        return {
            "service": service,
            "count": len(rows),
            "rows": rows,
        }
    except Exception as exc:
        return {"error": f"query_logs failed: {type(exc).__name__}: {exc}"}


def _summarize_metric_samples(samples: list[MetricsSample]) -> dict[str, Any]:
    """Calculate min, mean, max, peak time, and mem_percent slope per minute."""
    if not samples:
        return {
            "sample_count": 0,
            "cpu_percent": {"min": 0.0, "mean": 0.0, "max": 0.0, "peak_time": None},
            "mem_percent": {
                "min": 0.0,
                "mean": 0.0,
                "max": 0.0,
                "peak_time": None,
                "slope_per_minute": 0.0,
            },
            "p95_ms": {"min": 0.0, "mean": 0.0, "max": 0.0, "peak_time": None},
            "error_count": {"min": 0, "mean": 0.0, "max": 0, "peak_time": None},
            "in_flight_requests": {"min": 0, "mean": 0.0, "max": 0, "peak_time": None},
            "request_count_30s": {"min": 0, "mean": 0.0, "max": 0, "peak_time": None},
        }

    n = len(samples)

    def _stats(values: list[float], timestamps: list[datetime]) -> dict[str, Any]:
        min_v = min(values)
        max_v = max(values)
        mean_v = sum(values) / len(values)
        peak_idx = values.index(max_v)
        peak_time = timestamps[peak_idx].isoformat()
        return {
            "min": round(min_v, 2),
            "mean": round(mean_v, 2),
            "max": round(max_v, 2),
            "peak_time": peak_time,
        }

    ts_list = [s.ts for s in samples]
    cpu_stats = _stats([float(s.cpu_percent or 0.0) for s in samples], ts_list)
    p95_stats = _stats([float(s.p95_ms or 0.0) for s in samples], ts_list)
    err_stats = _stats([float(s.error_count or 0) for s in samples], ts_list)
    err_stats["min"] = int(err_stats["min"])
    err_stats["max"] = int(err_stats["max"])

    in_flight_stats = _stats([float(getattr(s, "in_flight_requests", 0) or 0) for s in samples], ts_list)
    in_flight_stats["min"] = int(in_flight_stats["min"])
    in_flight_stats["max"] = int(in_flight_stats["max"])

    req_count_stats = _stats([float(getattr(s, "request_count_30s", 0) or 0) for s in samples], ts_list)
    req_count_stats["min"] = int(req_count_stats["min"])
    req_count_stats["max"] = int(req_count_stats["max"])

    mem_vals = [float(s.mem_percent or 0.0) for s in samples]
    mem_stats = _stats(mem_vals, ts_list)

    slope = 0.0
    if n >= 2:
        t0 = samples[0].ts
        minutes = [(s.ts - t0).total_seconds() / 60.0 for s in samples]
        total_span = minutes[-1]
        if total_span > 0:
            mean_t = sum(minutes) / n
            mean_m = sum(mem_vals) / n
            denom = sum((t - mean_t) ** 2 for t in minutes)
            if denom > 0:
                slope = sum((t - mean_t) * (m - mean_m) for t, m in zip(minutes, mem_vals)) / denom
            else:
                slope = (mem_vals[-1] - mem_vals[0]) / total_span
    mem_stats["slope_per_minute"] = round(slope, 4)

    return {
        "sample_count": n,
        "cpu_percent": cpu_stats,
        "mem_percent": mem_stats,
        "p95_ms": p95_stats,
        "error_count": err_stats,
        "in_flight_requests": in_flight_stats,
        "request_count_30s": req_count_stats,
    }


def get_host_metrics(
    service: str,
    start: str | datetime,
    end: str | datetime,
    session: Session | None = None,
) -> dict[str, Any]:
    """Retrieve host metric summaries for the incident window AND the 5-minute pre-incident baseline.

    Summarizes cpu_percent, mem_percent, p95_ms, error_count, and mem_percent slope.
    """
    try:
        if not service or not isinstance(service, str):
            return {"error": "service must be a non-empty string"}

        start_dt = _parse_ts(start)
        end_dt = _parse_ts(end)
        if start_dt > end_dt:
            return {"error": "start must be earlier than or equal to end"}
        if (end_dt - start_dt).total_seconds() > 600:
            return {"error": "time range exceeds maximum allowed window of 10 minutes"}

        baseline_start = start_dt - timedelta(minutes=5)

        def _fetch(s: Session) -> tuple[list[MetricsSample], list[MetricsSample]]:
            baseline = repository.get_metrics(s, service, baseline_start, start_dt)
            incident = repository.get_metrics(s, service, start_dt, end_dt)
            return baseline, incident

        if session is not None:
            baseline_samples, incident_samples = _fetch(session)
        else:
            with SessionLocal() as s:
                baseline_samples, incident_samples = _fetch(s)

        return {
            "service": service,
            "incident_window": {
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat(),
                **_summarize_metric_samples(incident_samples),
            },
            "baseline_window": {
                "start": baseline_start.isoformat(),
                "end": start_dt.isoformat(),
                **_summarize_metric_samples(baseline_samples),
            },
        }
    except Exception as exc:
        return {"error": f"get_host_metrics failed: {type(exc).__name__}: {exc}"}


def _calc_probe_stats(samples: list[MetricsSample]) -> dict[str, Any]:
    """Calculate probe failure rate, mean latency, and p95 latency."""
    if not samples:
        return {"failure_rate": 0.0, "mean_probe_ms": None, "p95_probe_ms": None}
    failures = sum(1 for s in samples if not s.probe_ok)
    failure_rate = failures / len(samples)
    latencies = sorted(s.probe_ms for s in samples if s.probe_ms is not None)
    if not latencies:
        return {
            "failure_rate": round(failure_rate, 4),
            "mean_probe_ms": None,
            "p95_probe_ms": None,
        }
    mean_lat = sum(latencies) / len(latencies)
    p95_idx = min(len(latencies) - 1, int((len(latencies) - 1) * 0.95))
    return {
        "failure_rate": round(failure_rate, 4),
        "mean_probe_ms": round(mean_lat, 2),
        "p95_probe_ms": round(latencies[p95_idx], 2),
    }


def get_network_stats(
    service: str,
    start: str | datetime,
    end: str | datetime,
    session: Session | None = None,
) -> dict[str, Any]:
    """Return network probe statistics (before vs during incident) and a live psutil network snapshot.

    Note: Packet loss in the testbed is simulated at the application level.
    Uses cross-platform APIs; never calls ss or /proc.
    """
    try:
        if not service or not isinstance(service, str):
            return {"error": "service must be a non-empty string"}

        start_dt = _parse_ts(start)
        end_dt = _parse_ts(end)
        if start_dt > end_dt:
            return {"error": "start must be earlier than or equal to end"}
        if (end_dt - start_dt).total_seconds() > 600:
            return {"error": "time range exceeds maximum allowed window of 10 minutes"}

        baseline_start = start_dt - timedelta(minutes=5)

        def _fetch(s: Session) -> tuple[list[MetricsSample], list[MetricsSample]]:
            baseline = repository.get_metrics(s, service, baseline_start, start_dt)
            incident = repository.get_metrics(s, service, start_dt, end_dt)
            return baseline, incident

        if session is not None:
            baseline_samples, incident_samples = _fetch(session)
        else:
            with SessionLocal() as s:
                baseline_samples, incident_samples = _fetch(s)

        before_stats = _calc_probe_stats(baseline_samples)
        during_stats = _calc_probe_stats(incident_samples)

        # Cross-platform live psutil snapshot
        net_io = psutil.net_io_counters()
        net_io_dict = {
            "errin": getattr(net_io, "errin", 0) if net_io else 0,
            "errout": getattr(net_io, "errout", 0) if net_io else 0,
            "dropin": getattr(net_io, "dropin", 0) if net_io else 0,
            "dropout": getattr(net_io, "dropout", 0) if net_io else 0,
        }

        conn_counts: dict[str, int] = {}
        try:
            for conn in psutil.net_connections(kind="inet"):
                status = conn.status or "UNKNOWN"
                conn_counts[status] = conn_counts.get(status, 0) + 1
        except Exception:
            try:
                for conn in psutil.Process().net_connections(kind="inet"):
                    status = conn.status or "UNKNOWN"
                    conn_counts[status] = conn_counts.get(status, 0) + 1
            except Exception:
                conn_counts = {}

        return {
            "service": service,
            "probes": {
                "before": before_stats,
                "during": during_stats,
            },
            "live_snapshot": {
                "net_io_counters": net_io_dict,
                "connections_by_status": conn_counts,
            },
            "note": "Packet loss in the testbed is simulated at the application level.",
        }
    except Exception as exc:
        return {"error": f"get_network_stats failed: {type(exc).__name__}: {exc}"}


def recent_changes(
    start: str | datetime,
    end: str | datetime,
) -> dict[str, Any]:
    """Read changes.json at the repo root and return entries within the given time window."""
    try:
        start_dt = _parse_ts(start)
        end_dt = _parse_ts(end)
        if start_dt > end_dt:
            return {"error": "start must be earlier than or equal to end"}
        if (end_dt - start_dt).total_seconds() > 600:
            return {"error": "time range exceeds maximum allowed window of 10 minutes"}

        if not CHANGES_PATH.exists():
            CHANGES_PATH.write_text("[]\n", encoding="utf-8")
            return {"changes": [], "count": 0}

        try:
            content = CHANGES_PATH.read_text(encoding="utf-8").strip()
            data = json.loads(content) if content else []
        except Exception:
            data = []

        if not isinstance(data, list):
            data = []

        matching: list[dict[str, Any]] = []
        for item in data:
            if not isinstance(item, dict):
                continue
            ts_val = item.get("ts") or item.get("timestamp")
            if not ts_val:
                continue
            try:
                item_ts = _parse_ts(ts_val)
                if start_dt <= item_ts <= end_dt:
                    matching.append(item)
            except Exception:
                continue

        return {"changes": matching, "count": len(matching)}
    except Exception as exc:
        return {"error": f"recent_changes failed: {type(exc).__name__}: {exc}"}


TOOL_FUNCTIONS = {
    "query_logs": query_logs,
    "get_host_metrics": get_host_metrics,
    "get_network_stats": get_network_stats,
    "recent_changes": recent_changes,
}

TOOL_DECLARATIONS = [
    {
        "name": "query_logs",
        "description": "Query service logs in [start, end) interval with optional level and template filtering.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "service": {"type": "STRING", "description": "Target service name (e.g. api-service)"},
                "start": {"type": "STRING", "description": "ISO 8601 start timestamp"},
                "end": {"type": "STRING", "description": "ISO 8601 end timestamp"},
                "level": {"type": "STRING", "description": "Log level (INFO, WARN, ERROR, CRITICAL)"},
                "template_id": {"type": "INTEGER", "description": "Drain3 template ID"},
                "limit": {"type": "INTEGER", "description": "Max rows to return (1-100, default 50)"},
            },
            "required": ["service", "start", "end"],
        },
    },
    {
        "name": "get_host_metrics",
        "description": "Get host metrics summary (CPU, memory, p95 latency, error count, mem slope) comparing incident window vs 5-minute pre-incident baseline.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "service": {"type": "STRING", "description": "Target service name"},
                "start": {"type": "STRING", "description": "ISO 8601 incident start timestamp"},
                "end": {"type": "STRING", "description": "ISO 8601 incident end timestamp"},
            },
            "required": ["service", "start", "end"],
        },
    },
    {
        "name": "get_network_stats",
        "description": "Get probe failure rates and latency (before vs during) plus live network counters.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "service": {"type": "STRING", "description": "Target service name"},
                "start": {"type": "STRING", "description": "ISO 8601 incident start timestamp"},
                "end": {"type": "STRING", "description": "ISO 8601 incident end timestamp"},
            },
            "required": ["service", "start", "end"],
        },
    },
    {
        "name": "recent_changes",
        "description": "Read deployed code and configuration changes occurring within the time window.",
        "parameters": {
            "type": "OBJECT",
            "properties": {
                "start": {"type": "STRING", "description": "ISO 8601 start timestamp"},
                "end": {"type": "STRING", "description": "ISO 8601 end timestamp"},
            },
            "required": ["start", "end"],
        },
    },
]
