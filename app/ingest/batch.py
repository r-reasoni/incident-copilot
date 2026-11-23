"""
app/ingest/batch.py
===================
Shared batch ingestion path used by both live API consumer and offline replay.
Flow: parse -> insert_logs -> batch upsert templates -> pipeline.process(event, parsed).
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.db.repository import insert_logs, upsert_templates
from app.db.session import SessionLocal
from app.detection.windowing import _parse_event_ts
from app.parsing.parser import LogParser, ParsedLog


def ingest_batch(
    events: list[Any],
    parser: LogParser,
    pipeline: Any,
    session_factory: Any = SessionLocal,
    session: Any = None,
) -> None:
    """Ingest a batch of events: parse, persist logs & templates, and feed to pipeline."""
    if not events:
        return

    parsed_batch: list[tuple[dict[str, Any], ParsedLog]] = []
    log_records: list[dict[str, Any]] = []
    template_aggregates: dict[int, dict[str, Any]] = {}

    for item in events:
        if hasattr(item, "model_dump"):
            # Pydantic model (e.g. LogEventInput)
            e_dict = item.model_dump()
            ts = item.ts
            service = item.service
            host = item.host
            level = item.level
            message = item.message
        elif isinstance(item, dict):
            e_dict = dict(item)
            raw_ts = e_dict.get("ts")
            ts = _parse_event_ts(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
            e_dict["ts"] = ts
            service = e_dict.get("service", "unknown")
            host = e_dict.get("host", "localhost")
            level = str(e_dict.get("level", "INFO")).upper()
            message = e_dict.get("message") or e_dict.get("raw_message", "")
        else:
            raw_ts = getattr(item, "ts", None)
            ts = _parse_event_ts(raw_ts) if raw_ts is not None else datetime.now(timezone.utc)
            service = getattr(item, "service", "unknown")
            host = getattr(item, "host", "localhost")
            level = str(getattr(item, "level", "INFO")).upper()
            message = getattr(item, "message", "") or getattr(item, "raw_message", "")
            e_dict = {"ts": ts, "service": service, "host": host, "level": level, "message": message}

        parsed = parser.parse(message)
        parsed_batch.append((e_dict, parsed))

        log_records.append({
            "ts": ts,
            "service": service,
            "host": host,
            "level": level,
            "raw_message": message,
            "template_id": parsed.template_id,
            "params": {
                "values": parsed.params,
                "duration_ms": parsed.duration_ms,
            },
        })

        tid = parsed.template_id
        if tid not in template_aggregates:
            template_aggregates[tid] = {
                "template_id": tid,
                "template_text": parsed.template_text,
                "first_seen": ts,
                "count": 0,
            }
        template_aggregates[tid]["count"] += 1
        template_aggregates[tid]["template_text"] = parsed.template_text
        if ts < template_aggregates[tid]["first_seen"]:
            template_aggregates[tid]["first_seen"] = ts

    # Persist to DB if session or session_factory is available
    if session is not None:
        insert_logs(session, log_records)
        upsert_templates(session, list(template_aggregates.values()))
    elif session_factory is not None:
        try:
            with session_factory() as s, s.begin():
                insert_logs(s, log_records)
                upsert_templates(s, list(template_aggregates.values()))
        except Exception:
            pass

    # Feed each event to pipeline
    for e_dict, parsed in parsed_batch:
        pipeline.process(e_dict, parsed)
