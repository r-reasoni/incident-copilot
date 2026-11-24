"""Evidence pack generation for incidents."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from app.db.models import Incident, Template
from app.db.session import SessionLocal


def build_evidence_pack(
    incident_or_id: int | Incident | Any,
    session: Session | None = None,
    manager: Any | None = None,
) -> dict[str, Any]:
    """Build a structured evidence pack from an Incident record or incident ID.

    Contains incident window, affected services, baseline vs incident metrics,
    top templates, and peak window features.
    Stays compact (< 8000 chars / ~2000 tokens) and never exposes ground-truth fault_events.
    """
    incident: Any = None
    if isinstance(incident_or_id, int):
        if manager is not None and hasattr(manager, "get_incident"):
            incident = manager.get_incident(incident_or_id)
        if incident is None:
            if session is not None:
                incident = session.get(Incident, incident_or_id)
            else:
                try:
                    with SessionLocal() as s:
                        incident = s.get(Incident, incident_or_id)
                except Exception:
                    incident = None
    else:
        incident = incident_or_id

    if incident is None:
        raise ValueError(f"Incident {incident_or_id} could not be resolved")

    started_at = getattr(incident, "started_at", None)
    ended_at = getattr(incident, "ended_at", None)

    duration = 0.0
    if started_at and ended_at:
        duration = max(0.0, (ended_at - started_at).total_seconds())

    # Comparison
    comparison = getattr(incident, "comparison", None)
    if not isinstance(comparison, dict):
        comparison = None
    if not comparison and hasattr(incident, "agent_trace") and isinstance(incident.agent_trace, dict):
        comparison = incident.agent_trace.get("comparison")
    if not isinstance(comparison, dict):
        comparison = {
            "log_rate": {"baseline": 0.0, "incident": 0.0},
            "error_rate": {"baseline": 0.0, "incident": 0.0},
            "duration_mean_ms": {"baseline": 0.0, "incident": 0.0},
            "duration_p95_ms": {"baseline": 0.0, "incident": 0.0},
        }

    # Peak features
    peak_features = getattr(incident, "peak_features", None)
    if not isinstance(peak_features, dict):
        peak_features = None
    if not peak_features and hasattr(incident, "agent_trace") and isinstance(incident.agent_trace, dict):
        peak_features = incident.agent_trace.get("peak_features", {})
    if not isinstance(peak_features, dict):
        peak_features = {}

    # Top templates
    raw_templates = getattr(incident, "top_templates", None) or []
    top_templates: list[dict[str, Any]] = []

    if isinstance(raw_templates, list):
        for item in raw_templates[:8]:
            if isinstance(item, dict):
                cleaned = dict(item)
                if "examples" in cleaned and isinstance(cleaned["examples"], list):
                    cleaned["examples"] = [str(ex)[:90] for ex in cleaned["examples"][:2]]
                top_templates.append(cleaned)
            else:
                top_templates.append({"template": item})
    elif isinstance(raw_templates, dict):
        for tid_str, count in list(raw_templates.items())[:8]:
            top_templates.append(
                {
                    "template_id": int(tid_str) if str(tid_str).isdigit() else tid_str,
                    "count": count,
                }
            )

    # New templates
    raw_new = getattr(incident, "new_templates", None) or []
    new_templates: list[Any] = []
    if isinstance(raw_new, list):
        for item in raw_new[:8]:
            if isinstance(item, dict):
                cleaned = dict(item)
                if "examples" in cleaned and isinstance(cleaned["examples"], list):
                    cleaned["examples"] = [str(ex)[:90] for ex in cleaned["examples"][:1]]
                new_templates.append(cleaned)
            else:
                new_templates.append(item)
    elif isinstance(raw_new, (set, tuple)):
        new_templates = list(raw_new)[:8]

    time_range = {
        "start": started_at.isoformat() if started_at else None,
        "end": ended_at.isoformat() if ended_at else None,
    }

    service_error_rates = (
        getattr(incident, "service_error_rates", None)
        or getattr(incident, "affected_services_rates", None)
        or []
    )
    # Ratio floor: when baseline count is 0, ratio uses 1 / baseline_dur as the rate floor
    ratio_floor = getattr(incident, "ratio_floor", None)
    if ratio_floor is None:
        ratio_floor = round(1.0 / max(1.0, duration), 6) if duration > 0 else 0.001

    return {
        "incident_id": getattr(incident, "id", None),
        "time_range": time_range,
        "started_at": started_at.isoformat() if started_at else None,
        "ended_at": ended_at.isoformat() if ended_at else None,
        "duration_seconds": duration,
        "baseline_source": getattr(incident, "baseline_source", "pre_incident_quiet_windows"),
        "baseline_window_count": getattr(incident, "baseline_window_count", 10),
        "status": getattr(incident, "status", "open"),
        "peak_score": getattr(incident, "peak_score", 0.0),
        "affected_services": list(getattr(incident, "affected_services", []) or []),
        "service_error_rates": service_error_rates,
        "ratio_floor": ratio_floor,
        "baseline_vs_incident": comparison,
        "top_templates": top_templates,
        "new_templates": new_templates,
        "peak_window_features": peak_features,
    }
