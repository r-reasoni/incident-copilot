"""PostgreSQL repository tests isolated to a disposable per-run schema."""
from __future__ import annotations

import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterator

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db import repository
from app.db.models import Base


@pytest.fixture(scope="session")
def isolated_engine() -> Iterator:
    database_url = os.getenv("TEST_DATABASE_URL") or get_settings().database_url
    url = make_url(database_url)
    if url.get_backend_name() != "postgresql":
        pytest.fail("Repository integration tests require a PostgreSQL TEST_DATABASE_URL")

    schema = f"incident_copilot_test_{uuid.uuid4().hex}"
    admin_engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 2},
    )
    try:
        with admin_engine.begin() as connection:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
    except OperationalError as error:
        admin_engine.dispose()
        pytest.skip(f"PostgreSQL is unavailable for repository integration tests: {type(error).__name__}")

    test_engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"options": f"-csearch_path={schema}", "connect_timeout": 2},
    )
    try:
        Base.metadata.create_all(test_engine)
        yield test_engine
    finally:
        test_engine.dispose()
        with admin_engine.begin() as connection:
            connection.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin_engine.dispose()


@pytest.fixture
def db_session(isolated_engine) -> Iterator[Session]:
    with Session(isolated_engine) as session:
        with session.begin():
            yield session


def test_schema_has_exactly_five_tables(isolated_engine) -> None:
    assert set(inspect(isolated_engine).get_table_names()) == {
        "log_events",
        "templates",
        "window_scores",
        "incidents",
        "fault_events",
    }


def test_insert_logs_and_get_logs(db_session: Session) -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    log_ids = repository.insert_logs(
        db_session,
        [
            {
                "ts": start,
                "service": "payments",
                "host": "host-a",
                "level": "ERROR",
                "raw_message": "request failed",
                "template_id": 4,
                "params": {"code": 503},
            },
            {
                "ts": start + timedelta(seconds=1),
                "service": "payments",
                "host": "host-a",
                "level": "INFO",
                "raw_message": "request completed",
                "template_id": 5,
                "params": None,
            },
            {
                "ts": start + timedelta(seconds=2),
                "service": "inventory",
                "host": "host-b",
                "level": "ERROR",
                "raw_message": "stock lookup failed",
                "template_id": 4,
                "params": {},
            },
        ],
    )

    assert len(log_ids) == 3
    results = repository.get_logs(
        db_session,
        "payments",
        start,
        start + timedelta(seconds=3),
        level="ERROR",
        template_id=4,
    )
    assert len(results) == 1
    assert results[0].raw_message == "request failed"
    assert repository.insert_logs(db_session, []) == []


def test_upsert_template_accumulates_count(db_session: Session) -> None:
    first = datetime(2026, 1, 1, tzinfo=timezone.utc)
    later = first + timedelta(hours=1)

    repository.upsert_template(db_session, 12, "first text", later, count=2)
    template = repository.upsert_template(db_session, 12, "updated text", first, count=3)

    assert template.template_text == "updated text"
    assert template.first_seen == first
    assert template.count == 5


def test_save_window_score(db_session: Session) -> None:
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    score = repository.save_window_score(
        db_session,
        window_start=start,
        window_end=start + timedelta(seconds=10),
        score=0.92,
        is_anomalous=True,
        features={"error_rate": 0.8},
    )
    assert score.id is not None
    assert score.features == {"error_rate": 0.8}


def test_incident_create_update_get_and_list(db_session: Session) -> None:
    started = datetime(2026, 1, 1, tzinfo=timezone.utc)
    incident = repository.create_incident(
        db_session,
        started_at=started,
        peak_score=0.9,
        affected_services=["payments"],
        top_templates={"4": 3},
    )
    assert incident.id is not None
    assert incident.status == "open"
    assert incident.llm_calls == 0

    updated = repository.update_incident(
        db_session,
        incident.id,
        status="analyzed",
        root_cause="database timeout",
        confidence=0.8,
        remediation=[{"action": "inspect database"}],
        agent_trace={"steps": 2},
        llm_calls=1,
    )
    assert updated is not None
    assert repository.get_incident(db_session, incident.id).root_cause == "database timeout"
    assert [item.id for item in repository.list_incidents(db_session, status="analyzed")] == [
        incident.id
    ]
    assert repository.list_incidents(db_session, status="closed") == []
    assert repository.get_incident(db_session, -1) is None
    assert repository.update_incident(db_session, -1, status="closed") is None


def test_fault_events_are_listed_for_evaluation(db_session: Session) -> None:
    started = datetime(2026, 1, 1, tzinfo=timezone.utc)
    fault_event = repository.insert_fault_event(
        db_session,
        fault_type="latency",
        target_service="payments",
        started_at=started,
        ended_at=None,
    )
    events = repository.list_fault_events(db_session)
    assert len(events) == 1
    assert events[0].id == fault_event.id
    assert events[0].target_service == "payments"