"""
tests/test_pipeline.py
======================
Unit and integration tests for the Pipeline interface (counting stub)
and its background ticking and flush behaviour within the FastAPI lifespan.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from app.api.main import app, lifespan
from app.pipeline import Pipeline


def test_pipeline_process_counts_services() -> None:
    """process(event) should increment service counts and total event_count."""
    pipeline = Pipeline()
    assert pipeline.event_count == 0

    pipeline.process({"service": "order-service", "message": "created order"})
    pipeline.process({"service": "order-service", "message": "updated order"})
    pipeline.process({"service": "payment-service", "message": "payment processed"})

    assert pipeline.event_count == 3


def test_pipeline_tick_records_heartbeat() -> None:
    """tick(now) should update last_tick timestamp and tick_count."""
    pipeline = Pipeline()
    assert pipeline.last_tick is None
    assert pipeline.tick_count == 0

    t1 = datetime(2026, 11, 21, 10, 0, 0, tzinfo=UTC)
    pipeline.tick(t1)
    assert pipeline.last_tick == t1
    assert pipeline.tick_count == 1

    t2 = datetime(2026, 11, 21, 10, 0, 1, tzinfo=UTC)
    pipeline.tick(t2)
    assert pipeline.last_tick == t2
    assert pipeline.tick_count == 2


def test_pipeline_flush_marks_flushed() -> None:
    """flush() should mark the pipeline as flushed."""
    pipeline = Pipeline()
    assert not pipeline.is_flushed

    pipeline.flush()
    assert pipeline.is_flushed


@pytest.mark.asyncio
async def test_fastapi_lifespan_ticks_and_flushes() -> None:
    """FastAPI lifespan should run background tick every 1s and flush on shutdown."""
    async with lifespan(app):
        pipeline: Pipeline = app.state.pipeline
        assert isinstance(pipeline, Pipeline)
        assert not pipeline.is_flushed

        # Sleep slightly over 1s so the background _tick_worker has executed at least once
        await asyncio.sleep(1.1)
        assert pipeline.tick_count >= 1
        assert pipeline.last_tick is not None

    # After lifespan exits, flush() must have been called
    assert pipeline.is_flushed
