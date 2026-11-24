"""SQLAlchemy models for the incident-copilot PostgreSQL schema."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Float,
    Index,
    Integer,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import DateTime


class Base(DeclarativeBase):
    """Base metadata for the five application tables."""


class LogEvent(Base):
    __tablename__ = "log_events"
    # This high-volume table could be time-partitioned in production; it is not partitioned here.
    __table_args__ = (
        Index("ix_log_events_ts", "ts"),
        Index("ix_log_events_service_ts", "service", "ts"),
        Index("ix_log_events_template_id", "template_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    service: Mapped[str] = mapped_column(Text, nullable=False)
    host: Mapped[str] = mapped_column(Text, nullable=False)
    level: Mapped[str] = mapped_column(Text, nullable=False)
    raw_message: Mapped[str] = mapped_column(Text, nullable=False)
    template_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    params: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


class Template(Base):
    __tablename__ = "templates"

    template_id: Mapped[int] = mapped_column(Integer, primary_key=True)
    template_text: Mapped[str] = mapped_column(Text, nullable=False)
    first_seen: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    count: Mapped[int] = mapped_column(BigInteger, nullable=False)


class WindowScore(Base):
    __tablename__ = "window_scores"
    __table_args__ = (Index("ix_window_scores_window_start", "window_start"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    window_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    window_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    is_anomalous: Mapped[bool] = mapped_column(nullable=False)
    features: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class Incident(Base):
    __tablename__ = "incidents"
    __table_args__ = (
        CheckConstraint("status IN ('open', 'analyzed', 'closed')", name="ck_incidents_status"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    analyzed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    status: Mapped[str] = mapped_column(Text, nullable=False, server_default="open")
    category: Mapped[str | None] = mapped_column(Text, nullable=True)
    peak_score: Mapped[float] = mapped_column(Float, nullable=False)
    affected_services: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    top_templates: Mapped[dict[str, Any] | list[Any]] = mapped_column(
        JSONB, nullable=False, server_default=text("'{}'::jsonb")
    )
    alternatives: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(JSONB, nullable=True)
    root_cause: Mapped[str | None] = mapped_column(Text, nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    remediation: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(JSONB, nullable=True)
    agent_trace: Mapped[dict[str, Any] | list[Any] | None] = mapped_column(JSONB, nullable=True)
    llm_calls: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text("now()")
    )


class FaultEvent(Base):
    __tablename__ = "fault_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    fault_type: Mapped[str] = mapped_column(Text, nullable=False)
    target_service: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

class MetricsSample(Base):
    __tablename__ = "metrics_samples"
    __table_args__ = (
        Index("ix_metrics_samples_service_ts", "service", "ts"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    service: Mapped[str] = mapped_column(Text, nullable=False)
    cpu_percent: Mapped[float] = mapped_column(Float, nullable=False)
    mem_percent: Mapped[float] = mapped_column(Float, nullable=False)
    open_handles: Mapped[int] = mapped_column(Integer, nullable=False)
    threads: Mapped[int] = mapped_column(Integer, nullable=False)
    p50_ms: Mapped[float] = mapped_column(Float, nullable=False)
    p95_ms: Mapped[float] = mapped_column(Float, nullable=False)
    error_count: Mapped[int] = mapped_column(Integer, nullable=False)
    in_flight_requests: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    request_count_30s: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    probe_ms: Mapped[float | None] = mapped_column(Float, nullable=True)
    probe_ok: Mapped[bool] = mapped_column(Boolean, nullable=False)
