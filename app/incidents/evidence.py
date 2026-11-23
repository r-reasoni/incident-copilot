"""
app/incidents/evidence.py
=========================
Builds compact JSON evidence packs for root-cause analysis and LLM investigation.
Guarantees size <= 2000 tokens (~8000 characters) and enforces strict privacy
by never exposing fault_events data or evaluation ground truth.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from app.db import repository
from app.db.session import SessionLocal
from app.incidents.manager import IncidentManager, IncidentRecord

MAX_TOP_TEMPLATES = 8
MAX_EXAMPLE_LINES = 3
MAX_EXAMPLE_CHARS = 160
MAX_PACK_CHARS = 7800  # Strictly under ~2000 tokens (approx 4 chars per token)

FORBIDDEN_KEYS = {
    "fault_events",
    "fault_event",
    "fault_type",
    "target_service",
    "ground_truth",
}


def _clean_value(val: Any) -> Any:
    """Recursively strip any forbidden evaluation or fault_events keys."""
    if isinstance(val, dict):
        return {
            k: _clean_value(v)
            for k, v in val.items()
            if k.lower() not in FORBIDDEN_KEYS
        }
    if isinstance(val, list):
        return [_clean_value(item) for item in val]
    return val


def build_evidence_pack(
    incident_id: int | IncidentRecord | Any,
    session: Any = None,
    manager: IncidentManager | None = None,
) -> dict[str, Any]:
    """Build a compact evidence pack JSON dict for an incident under 2000 tokens (~8000 chars)."""
    incident: Any = None

    if isinstance(incident_id, (IncidentRecord, dict)):
        incident = incident_id
    elif manager is not None:
        incident = manager.get_incident(int(incident_id))

    if incident is None:
        if session is not None:
            incident = repository.get_incident(session, int(incident_id))
        else:
            try:
                with SessionLocal() as db_session:
                    incident = repository.get_incident(db_session, int(incident_id))
            except Exception:
                pass

    if incident is None:
        raise ValueError(f"Incident not found: {incident_id}")

    # Extract fields cleanly from Incident OR IncidentRecord OR dict
    inc_id = getattr(incident, "id", None) or (incident.get("id") if isinstance(incident, dict) else incident_id)
    started_at = getattr(incident, "started_at", None) or (incident.get("started_at") if isinstance(incident, dict) else None)
    detected_at = getattr(incident, "detected_at", None) or (incident.get("detected_at") if isinstance(incident, dict) else None)
    ended_at = getattr(incident, "ended_at", None) or (incident.get("ended_at") if isinstance(incident, dict) else None)
    peak_score = getattr(incident, "peak_score", 0.0) or (incident.get("peak_score", 0.0) if isinstance(incident, dict) else 0.0)

    top_raw = getattr(incident, "top_templates", {}) or (incident.get("top_templates", {}) if isinstance(incident, dict) else {})
    if isinstance(top_raw, dict):
        top_templates = top_raw.get("top_templates") or top_raw.get("templates") or []
        comparison = top_raw.get("comparison") or {}
        peak_features = top_raw.get("peak_features") or {}
        affected_rates = top_raw.get("affected_services_rates") or []
    else:
        top_templates = list(top_raw)
        comparison = getattr(incident, "comparison", {}) or {}
        peak_features = getattr(incident, "peak_features", {}) or {}
        affected_rates = getattr(incident, "affected_services_rates", []) or []

    # Clean peak features: remove underscore keys
    clean_peak_features = {k: v for k, v in peak_features.items() if not k.startswith("_")}

    # Calculate duration
    duration_sec: float | None = None
    if isinstance(started_at, datetime) and isinstance(ended_at, datetime):
        duration_sec = round((ended_at - started_at).total_seconds(), 2)

    # Format time strings
    started_str = started_at.isoformat() if isinstance(started_at, datetime) else str(started_at)
    detected_str = detected_at.isoformat() if isinstance(detected_at, datetime) else (str(detected_at) if detected_at else None)
    ended_str = ended_at.isoformat() if isinstance(ended_at, datetime) else (str(ended_at) if ended_at else None)

    # Clean and cap top_templates
    capped_templates: list[dict[str, Any]] = []
    for tpl in top_templates[:MAX_TOP_TEMPLATES]:
        examples = tpl.get("examples", [])
        capped_examples = [
            str(ex)[:MAX_EXAMPLE_CHARS] for ex in examples[:MAX_EXAMPLE_LINES]
        ]
        capped_templates.append({
            "template_id": tpl.get("template_id"),
            "text": str(tpl.get("text", ""))[:120],
            "baseline_rate": tpl.get("baseline_rate", 0.0),
            "incident_rate": tpl.get("incident_rate", 0.0),
            "ratio": tpl.get("ratio", 0.0),
            "level_mix": tpl.get("level_mix", {}),
            "examples": capped_examples,
            "is_new": bool(tpl.get("is_new", False)),
        })

    # Build evidence pack
    pack: dict[str, Any] = {
        "incident_id": inc_id,
        "time_range": {
            "started_at": started_str,
            "detected_at": detected_str,
            "ended_at": ended_str,
        },
        "duration_seconds": duration_sec,
        "peak_score": round(float(peak_score), 4),
        "affected_services": affected_rates or getattr(incident, "affected_services", []),
        "baseline_vs_incident": comparison,
        "top_templates": capped_templates,
        "peak_window_features": clean_peak_features,
    }

    # Clean out any potential evaluation / fault_events references
    pack = _clean_value(pack)

    # Enforce strictly <= MAX_PACK_CHARS (~2000 tokens)
    serialized = json.dumps(pack)
    if len(serialized) > MAX_PACK_CHARS:
        for item in pack["top_templates"]:
            item["examples"] = item.get("examples", [])[:1]
        serialized = json.dumps(pack)

    if len(serialized) > MAX_PACK_CHARS:
        pack["top_templates"] = pack["top_templates"][:5]
        pack["peak_window_features"] = {
            k: v for i, (k, v) in enumerate(pack["peak_window_features"].items()) if i < 15
        }

    return pack
