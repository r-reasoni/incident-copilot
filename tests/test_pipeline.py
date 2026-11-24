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


def test_pipeline_catches_callback_exceptions_and_increments_errors() -> None:
    """Pipeline must catch callback and tick/flush exceptions, log them, increment errors, and not crash."""
    def buggy_callback(res):
        raise RuntimeError("Callback crash simulation")

    pipeline = Pipeline(on_window=[buggy_callback])
    # Process enough events to close a window or call tick
    for i in range(50):
        pipeline.process({
            "ts": datetime(2026, 11, 21, 10, 0, i, tzinfo=UTC),
            "service": "api-service",
            "host": "localhost",
            "level": "INFO",
            "message": f"msg {i}",
        })
    pipeline.tick(datetime(2026, 11, 21, 10, 1, 0, tzinfo=UTC))
    pipeline.flush()

    assert pipeline.errors > 0, "Pipeline must catch exceptions and increment errors counter"


def test_threshold_fallback_low_history_does_not_crash() -> None:
    """Threshold computation with few or identical values does not crash and respects bounds."""
    from app.detection.detector import AnomalyDetector
    detector = AnomalyDetector(warmup_windows=5, save_to_db=False)
    # Empty history
    assert detector._compute_threshold() is None
    # Add identical scores
    detector._score_history.extend([0.05, 0.05, 0.05])
    t = detector._compute_threshold()
    assert t is not None
    assert t >= 0.01
    assert t <= detector.threshold_ceiling
