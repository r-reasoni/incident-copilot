"""Tests for the Typer + Rich copilot CLI."""
from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from typer.testing import CliRunner

from app.config import get_settings
from app.db import repository
from app.db.models import Base
from cli.main import app, compute_llm_savings

runner = CliRunner()


@pytest.fixture(scope="module")
def cli_test_engine() -> Iterator:
    database_url = os.getenv("TEST_DATABASE_URL") or get_settings().database_url
    url = make_url(database_url)
    if url.get_backend_name() != "postgresql":
        pytest.skip("PostgreSQL TEST_DATABASE_URL required for CLI schema tests")

    schema = f"incident_copilot_cli_test_{uuid.uuid4().hex}"
    admin_engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 2},
    )
    try:
        with admin_engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    except OperationalError as exc:
        admin_engine.dispose()
        pytest.skip(f"PostgreSQL unavailable for CLI schema tests: {type(exc).__name__}")

    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={"options": f"-csearch_path={schema}", "connect_timeout": 2},
    )
    try:
        Base.metadata.create_all(engine)
        yield engine
    finally:
        engine.dispose()
        with admin_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin_engine.dispose()


def test_help_output_and_subcommands() -> None:
    res = runner.invoke(app, ["--help"])
    assert res.exit_code == 0
    commands = ("init-db", "serve", "replay", "incidents", "show", "stats", "watch", "demo", "eval")
    for cmd in commands:
        assert cmd in res.output
        sub = runner.invoke(app, [cmd, "--help"])
        assert sub.exit_code == 0, f"{cmd} --help failed: {sub.output}"


def test_eval_stub() -> None:
    res = runner.invoke(app, ["eval", "--runs", "2", "--mock"])
    assert res.exit_code == 0
    assert "implemented in Step 9" in res.output


def test_stats_math() -> None:
    assert compute_llm_savings(1000, 100, 2) == pytest.approx((99.8, 98.0))
    assert compute_llm_savings(0, 0, 0) == (0.0, 0.0)
    assert compute_llm_savings(500, 50, 0) == pytest.approx((100.0, 100.0))


def test_stats_cli_output(monkeypatch) -> None:
    class _FakeSession:
        def __init__(self) -> None:
            self._vals = iter([1000, 25, 100, 2, 2])

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def scalar(self, _stmt):
            return next(self._vals)

    monkeypatch.setattr("app.db.session.SessionLocal", lambda: _FakeSession())
    res = runner.invoke(app, ["stats"])
    assert res.exit_code == 0
    assert "1,000" in res.output
    assert "99.80%" in res.output
    assert "98.00%" in res.output


def test_incidents_and_show_on_test_schema(cli_test_engine, monkeypatch) -> None:
    TestSessionLocal = sessionmaker(bind=cli_test_engine, autoflush=False, expire_on_commit=False)
    monkeypatch.setattr("app.db.session.SessionLocal", TestSessionLocal)

    now = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)
    with TestSessionLocal() as session, session.begin():
        repository.insert_logs(
            session,
            [
                {
                    "ts": now,
                    "service": "api-service",
                    "host": "localhost",
                    "level": "ERROR",
                    "raw_message": "Worker thread saturated at 99% CPU",
                    "template_id": 1,
                    "params": {},
                }
            ],
        )
        repository.upsert_template(session, 1, "Worker thread saturated at <*>% CPU", now, count=1)
        repository.save_window_score(
            session,
            window_start=now,
            window_end=now + timedelta(seconds=10),
            score=0.95,
            is_anomalous=True,
            features={"log_rate": 10.0},
        )
        inc = repository.create_incident(
            session,
            started_at=now,
            ended_at=now + timedelta(seconds=40),
            status="analyzed",
            category="cpu",
            peak_score=0.95,
            affected_services=["api-service"],
            root_cause="Tight CPU loop in request handler",
            confidence=0.91,
            alternatives=[{"category": "db_lock", "reason": "No lock wait spikes observed"}],
            remediation={
                "steps": [
                    {"step": "Capture CPU profile on api-service", "risk": "low"},
                    {"step": "Restart saturated worker pool", "risk": "med"},
                ]
            },
            agent_trace={
                "final_json": {
                    "evidence": ["cpu_percent peaked at 99.2%", "Template #1 occurred 42 times"]
                }
            },
            llm_calls=2,
        )
        inc_id = inc.id

    res_list = runner.invoke(app, ["incidents"], env={"COLUMNS": "140"})
    assert res_list.exit_code == 0
    assert str(inc_id) in res_list.output
    assert "api-service" in res_list.output
    assert "0.950" in res_list.output
    assert "cpu" in res_list.output
    assert "0.91" in res_list.output
    assert "analyzed" in res_list.output

    res_filtered = runner.invoke(app, ["incidents", "--status", "analyzed"], env={"COLUMNS": "140"})
    assert res_filtered.exit_code == 0
    assert str(inc_id) in res_filtered.output

    res_bad_status = runner.invoke(app, ["incidents", "--status", "invalid"])
    assert res_bad_status.exit_code != 0

    res_show = runner.invoke(app, ["show", str(inc_id)])
    assert res_show.exit_code == 0
    assert "Tight CPU loop in request handler" in res_show.output
    assert "cpu_percent peaked at 99.2%" in res_show.output
    assert "db_lock" in res_show.output
    assert "Capture CPU profile on api-service" in res_show.output
    assert "LOW" in res_show.output
    assert "suggestions for a human operator" in res_show.output

    res_missing = runner.invoke(app, ["show", "999999"])
    assert res_missing.exit_code == 1

    res_stats = runner.invoke(app, ["stats"])
    assert res_stats.exit_code == 0
    assert "LLM calls saved" in res_stats.output
