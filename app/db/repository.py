"""Persistence operations for the five application tables."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import FaultEvent, Incident, LogEvent, Template, WindowScore


def insert_logs(session: Session, logs: list[dict[str, Any]]) -> list[int]:
    """Bulk insert log events and return their generated IDs."""
    if not logs:
        return []
    result = session.execute(insert(LogEvent).returning(LogEvent.id), logs)
    session.flush()
    return list(result.scalars())


def upsert_template(
    session: Session,
    template_id: int,
    template_text: str,
    first_seen: datetime,
    count: int = 1,
) -> Template:
    """Insert a template or accumulate its count while preserving first_seen."""
    statement = insert(Template).values(
        template_id=template_id,
        template_text=template_text,
        first_seen=first_seen,
        count=count,
    )
    statement = statement.on_conflict_do_update(
        index_elements=[Template.template_id],
        set_={
            "template_text": statement.excluded.template_text,
            "first_seen": func.least(Template.first_seen, statement.excluded.first_seen),
            "count": Template.count + statement.excluded.count,
        },
    ).returning(Template)
    template = session.scalars(statement).one()
    session.flush()
    return template


def upsert_templates(session: Session, templates: list[dict[str, Any]]) -> None:
    """Batch insert or update templates, accumulating counts and updating latest template_text."""
    if not templates:
        return
    statement = insert(Template).values(templates)
    statement = statement.on_conflict_do_update(
        index_elements=[Template.template_id],
        set_={
            "template_text": statement.excluded.template_text,
            "first_seen": func.least(Template.first_seen, statement.excluded.first_seen),
            "count": Template.count + statement.excluded.count,
        },
    )
    session.execute(statement)
    session.flush()


def save_window_score(session: Session, **values: Any) -> WindowScore:
    """Persist one scored window and return it."""
    window_score = WindowScore(**values)
    session.add(window_score)
    session.flush()
    return window_score


def create_incident(session: Session, **values: Any) -> Incident:
    """Create and return an incident."""
    incident = Incident(**values)
    session.add(incident)
    session.flush()
    return incident


def update_incident(session: Session, incident_id: int, **values: Any) -> Incident | None:
    """Update an incident's supplied fields, returning None when not found."""
    incident = session.get(Incident, incident_id)
    if incident is None:
        return None
    for field, value in values.items():
        if field in {"id", "created_at"} or not hasattr(Incident, field):
            raise ValueError(f"Incident field cannot be updated: {field}")
        setattr(incident, field, value)
    session.flush()
    return incident


def get_incident(session: Session, incident_id: int) -> Incident | None:
    """Return one incident by its primary key."""
    return session.get(Incident, incident_id)


def list_incidents(session: Session, status: str | None = None) -> list[Incident]:
    """List incidents in newest-first order, optionally filtered by status."""
    statement = select(Incident).order_by(Incident.started_at.desc(), Incident.id.desc())
    if status is not None:
        statement = statement.where(Incident.status == status)
    return list(session.scalars(statement))


def get_logs(
    session: Session,
    service: str,
    start: datetime,
    end: datetime,
    level: str | None = None,
    template_id: int | None = None,
    limit: int = 50,
) -> list[LogEvent]:
    """Fetch service logs in the half-open interval [start, end)."""
    if limit < 1:
        raise ValueError("limit must be at least 1")
    statement = select(LogEvent).where(
        LogEvent.service == service,
        LogEvent.ts >= start,
        LogEvent.ts < end,
    )
    if level is not None:
        statement = statement.where(LogEvent.level == level)
    if template_id is not None:
        statement = statement.where(LogEvent.template_id == template_id)
    statement = statement.order_by(LogEvent.ts.desc(), LogEvent.id.desc()).limit(limit)
    return list(session.scalars(statement))


def insert_fault_event(session: Session, **values: Any) -> FaultEvent:
    """Record evaluation-only ground-truth fault metadata."""
    fault_event = FaultEvent(**values)
    session.add(fault_event)
    session.flush()
    return fault_event


def list_fault_events(session: Session) -> list[FaultEvent]:
    """List evaluation ground-truth events in start-time order."""
    return list(session.scalars(select(FaultEvent).order_by(FaultEvent.started_at, FaultEvent.id)))

