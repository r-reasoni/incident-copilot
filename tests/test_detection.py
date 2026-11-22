"""
tests/test_detection.py
=======================
Unit tests for window aggregation and online anomaly detection with synthetic events.
No database connection required.
"""
from __future__ import annotations

import random
from datetime import datetime, timezone

from app.detection.detector import AnomalyDetector, WindowResult
from app.detection.windowing import ClosedWindow, WindowAggregator
from app.parsing.parser import LogParser


def _make_event(
    ts_epoch: float,
    service: str = "api-service",
    level: str = "INFO",
    message: str = "Request GET /users/1 completed in 35ms",
) -> dict:
    dt = datetime.fromtimestamp(ts_epoch, tz=timezone.utc)
    return {
        "ts": dt,
        "service": service,
        "level": level,
        "message": message,
    }


def test_steady_poisson_traffic_yields_zero_flags_after_warmup() -> None:
    """Steady Poisson-like traffic for 60 windows yields zero flags after warm-up."""
    rng = random.Random(42)
    parser = LogParser(state_path="")
    aggregator = WindowAggregator(window_seconds=10, warmup_windows=30)
    detector = AnomalyDetector(warmup_windows=30, seed=42, save_to_db=False)

    scored_windows: list[WindowResult] = []
    base_epoch = 1_700_000_000.0  # Aligned to 10s

    # 60 windows = 600 seconds
    for w_idx in range(60):
        w_start = base_epoch + w_idx * 10
        for _ in range(50):
            offset = rng.uniform(0.0, 9.9)
            user_id = rng.choice([1, 2, 3])
            dur = max(10, int(rng.gauss(35, 5)))
            event = _make_event(
                ts_epoch=w_start + offset,
                service="api-service",
                level="INFO",
                message=f"Request GET /users/{user_id} completed in {dur}ms",
            )
            parsed = parser.parse(event["message"])
            for cw in aggregator.add(event, parsed):
                scored_windows.append(detector.score_window(cw))

    for cw in aggregator.flush():
        scored_windows.append(detector.score_window(cw))

    assert len(scored_windows) >= 60

    # First 30 windows are warm-up: must have zero flags
    for res in scored_windows[:30]:
        assert not res.is_anomalous, "Warm-up window should not be anomalous"

    # Post warm-up windows (30..60): steady traffic should produce zero flags
    post_warmup_anomalies = [res for res in scored_windows[30:] if res.is_anomalous]
    assert len(post_warmup_anomalies) == 0, f"Expected 0 flags on steady traffic, got {len(post_warmup_anomalies)}"


def test_burst_error_templates_flagged_within_3_windows() -> None:
    """Steady traffic then a burst of new ERROR templates with 5x log rate is flagged within 3 windows."""
    rng = random.Random(42)
    parser = LogParser(state_path="")
    aggregator = WindowAggregator(window_seconds=10, warmup_windows=30)
    detector = AnomalyDetector(warmup_windows=30, seed=42, save_to_db=False)

    scored_windows: list[WindowResult] = []
    base_epoch = 1_700_000_000.0

    # 30 warm-up windows of normal traffic (~30 events per window)
    for w_idx in range(30):
        w_start = base_epoch + w_idx * 10
        for _ in range(30):
            offset = rng.uniform(0.0, 9.9)
            event = _make_event(
                ts_epoch=w_start + offset,
                service="api-service",
                level="INFO",
                message=f"Request GET /items/{rng.choice([1, 2, 3])} completed in 20ms",
            )
            parsed = parser.parse(event["message"])
            for cw in aggregator.add(event, parsed):
                scored_windows.append(detector.score_window(cw))

    # Advance time to cleanly close window 29 before fault
    for cw in aggregator.tick(datetime.fromtimestamp(base_epoch + 300 + 10, tz=timezone.utc)):
        scored_windows.append(detector.score_window(cw))

    assert len(scored_windows) == 30

    # Fault injection starting at window index 30: 5x log rate (150 events) with new ERROR templates
    fault_start_window = len(scored_windows)
    for f_idx in range(5):
        w_start = base_epoch + (30 + f_idx) * 10
        for i in range(150):
            offset = rng.uniform(0.0, 9.9)
            event = _make_event(
                ts_epoch=w_start + offset,
                service="api-service",
                level="ERROR",
                message=f"DatabaseConnectionPoolExhaustedException: connection {i % 10} failed on cluster-alpha",
            )
            parsed = parser.parse(event["message"])
            for cw in aggregator.add(event, parsed):
                scored_windows.append(detector.score_window(cw))

    for cw in aggregator.flush():
        scored_windows.append(detector.score_window(cw))

    # Check that anomaly is flagged within 3 windows from fault start (windows 30, 31, 32)
    fault_windows = scored_windows[fault_start_window : fault_start_window + 3]
    flagged = [res for res in fault_windows if res.is_anomalous]
    assert len(flagged) > 0, "Burst error template fault must be flagged within 3 windows"


def test_latency_only_shift_flagged() -> None:
    """A latency-only shift (same templates, duration_ms 35 -> 800) is flagged."""
    rng = random.Random(42)
    parser = LogParser(state_path="")
    aggregator = WindowAggregator(window_seconds=10, warmup_windows=30)
    detector = AnomalyDetector(warmup_windows=30, seed=42, save_to_db=False)

    scored_windows: list[WindowResult] = []
    base_epoch = 1_700_000_000.0

    # 30 warm-up windows with low latency (35ms mean)
    for w_idx in range(30):
        w_start = base_epoch + w_idx * 10
        for _ in range(40):
            offset = rng.uniform(0.0, 9.9)
            dur = max(5, int(rng.gauss(35, 5)))
            event = _make_event(
                ts_epoch=w_start + offset,
                service="api-service",
                level="INFO",
                message=f"Request GET /users/{rng.choice([1, 2])} completed in {dur}ms",
            )
            parsed = parser.parse(event["message"])
            for cw in aggregator.add(event, parsed):
                scored_windows.append(detector.score_window(cw))

    for cw in aggregator.tick(datetime.fromtimestamp(base_epoch + 300 + 10, tz=timezone.utc)):
        scored_windows.append(detector.score_window(cw))

    # 5 windows with high latency (800ms) with identical templates and rate
    fault_start_window = len(scored_windows)
    for f_idx in range(5):
        w_start = base_epoch + (30 + f_idx) * 10
        for _ in range(40):
            offset = rng.uniform(0.0, 9.9)
            dur = max(500, int(rng.gauss(800, 50)))
            event = _make_event(
                ts_epoch=w_start + offset,
                service="api-service",
                level="INFO",
                message=f"Request GET /users/{rng.choice([1, 2])} completed in {dur}ms",
            )
            parsed = parser.parse(event["message"])
            for cw in aggregator.add(event, parsed):
                scored_windows.append(detector.score_window(cw))

    for cw in aggregator.flush():
        scored_windows.append(detector.score_window(cw))

    fault_windows = scored_windows[fault_start_window:]
    flagged = [res for res in fault_windows if res.is_anomalous]
    assert len(flagged) > 0, "Latency-only shift (35ms -> 800ms) must be flagged as anomalous"


def test_windows_emitted_for_gaps_and_silent_gap_visible() -> None:
    """A silent gap produces empty windows with a log_rate drop."""
    aggregator = WindowAggregator(window_seconds=10, warmup_windows=5)
    base_epoch = 1_700_000_000.0

    # 1 event at epoch 0
    e1 = _make_event(ts_epoch=base_epoch + 2)
    aggregator.add(e1)

    # Big gap: next event is 50 seconds later (at epoch 50)
    e2 = _make_event(ts_epoch=base_epoch + 52)
    closed = aggregator.add(e2)

    # Windows for 0, 10, 20, 30, 40 should be closed and emitted
    assert len(closed) >= 4, f"Expected at least 4 windows emitted for gap, got {len(closed)}"

    # Check that intermediate gap windows have log_rate == 0
    gap_windows = [w for w in closed if w.window_start.timestamp() in (base_epoch + 10, base_epoch + 20, base_epoch + 30)]
    for gw in gap_windows:
        assert gw.event_count == 0
        assert gw.features["log_rate"] == 0.0


def test_late_events_dropped() -> None:
    """Events later than the grace period for an already-closed window are dropped and counted."""
    aggregator = WindowAggregator(window_seconds=10, window_grace_seconds=2, warmup_windows=5)
    base_epoch = 1_700_000_000.0

    # Event at t=5
    aggregator.add(_make_event(ts_epoch=base_epoch + 5))

    # Event at t=25 (advances watermark to 23, closing window [0, 10))
    closed = aggregator.add(_make_event(ts_epoch=base_epoch + 25))
    assert any(cw.window_start.timestamp() == base_epoch for cw in closed)

    # Now an event arrives for t=4 (belongs to window [0, 10) which is already closed)
    late_event = _make_event(ts_epoch=base_epoch + 4)
    res = aggregator.add(late_event)
    assert res == []
    assert aggregator.late_dropped == 1


def test_deterministic_scores_with_same_seed() -> None:
    """The same seed gives identical scores."""
    def run_simulation(seed: int) -> list[float]:
        rng = random.Random(123)
        parser = LogParser(state_path="")
        aggregator = WindowAggregator(window_seconds=10, warmup_windows=10)
        detector = AnomalyDetector(warmup_windows=10, seed=seed, save_to_db=False)
        scores: list[float] = []

        base_epoch = 1_700_000_000.0
        for w_idx in range(25):
            w_start = base_epoch + w_idx * 10
            for _ in range(20):
                offset = rng.uniform(0.0, 9.9)
                event = _make_event(
                    ts_epoch=w_start + offset,
                    message=f"Request {rng.choice(['GET', 'POST'])} /endpoint in {int(rng.uniform(10, 50))}ms",
                )
                parsed = parser.parse(event["message"])
                for cw in aggregator.add(event, parsed):
                    scores.append(detector.score_window(cw).score)

        for cw in aggregator.flush():
            scores.append(detector.score_window(cw).score)
        return scores

    run1 = run_simulation(42)
    run2 = run_simulation(42)
    assert len(run1) > 0
    assert run1 == run2, "Runs with identical seed must yield identical scores"


def test_frozen_top_k_keys_stay_stable_after_warmup() -> None:
    """Frozen top-K keys stay stable after warm-up."""
    rng = random.Random(42)
    parser = LogParser(state_path="")
    aggregator = WindowAggregator(window_seconds=10, warmup_windows=5, top_k_templates=3)

    closed_windows: list[ClosedWindow] = []
    base_epoch = 1_700_000_000.0

    # 5 warm-up windows with 5 different templates
    for w_idx in range(5):
        w_start = base_epoch + w_idx * 10
        for i in range(20):
            event = _make_event(
                ts_epoch=w_start + (i * 0.4),
                message=f"Template type {i % 5} action performed",
            )
            parsed = parser.parse(event["message"])
            closed_windows.extend(aggregator.add(event, parsed))

    for cw in aggregator.tick(datetime.fromtimestamp(base_epoch + 50 + 10, tz=timezone.utc)):
        closed_windows.append(cw)

    assert aggregator.frozen_top_k is not None
    assert len(aggregator.frozen_top_k) <= 3
    frozen_keys = set(closed_windows[-1].features.keys())

    # Introduce brand new templates in subsequent windows
    post_windows: list[ClosedWindow] = []
    for w_idx in range(5, 10):
        w_start = base_epoch + w_idx * 10
        for i in range(20):
            event = _make_event(
                ts_epoch=w_start + (i * 0.4),
                message=f"Completely different template {i * 100} arrived",
            )
            parsed = parser.parse(event["message"])
            post_windows.extend(aggregator.add(event, parsed))

    for cw in aggregator.flush():
        post_windows.append(cw)

    for pw in post_windows:
        assert set(pw.features.keys()) == frozen_keys, "Feature keys must remain identical to frozen keys"
