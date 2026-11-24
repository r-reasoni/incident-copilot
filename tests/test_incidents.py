"""
tests/test_incidents.py
=======================
Tests for incident grouping, cooldown merges, life-cycle transitions,
evidence pack construction, and template tracking outside top-K features.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from app.detection.detector import WindowResult
from app.incidents.evidence import build_evidence_pack
from app.incidents.manager import IncidentManager


def _make_window(
    start: datetime,
    duration_s: int = 10,
    is_anomalous: bool = False,
    score: float = 0.1,
    features: dict[str, float] | None = None,
    run_start: datetime | None = None,
    confirmed_at: datetime | None = None,
) -> WindowResult:
    end = start + timedelta(seconds=duration_s)
    feats = features or {
        "log_rate": 10.0,
        "error_rate": 0.05 if is_anomalous else 0.0,
        "warn_rate": 0.02 if is_anomalous else 0.0,
        "err_api-service": 2.0 if is_anomalous else 0.0,
        "err_worker-service": 0.0,
        "duration_mean_ms": 150.0 if is_anomalous else 45.0,
        "duration_p95_ms": 400.0 if is_anomalous else 90.0,
        "tpl_1": 15.0 if is_anomalous else 2.0,
        "tpl_2": 8.0 if is_anomalous else 1.0,
    }
    return WindowResult(
        window_start=start,
        window_end=end,
        score=score,
        threshold=0.5,
        is_anomalous=is_anomalous,
        features=feats,
        run_start=run_start or (start if is_anomalous else None),
        confirmed_at=confirmed_at or (end if is_anomalous else None),
    )


def test_one_contiguous_anomaly_gives_one_incident() -> None:
    """A contiguous sequence of anomalous windows produces exactly one incident."""
    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)

    # 12 normal baseline windows
    for i in range(12):
        w = _make_window(base_t + timedelta(seconds=10 * i), is_anomalous=False)
        manager.on_window(w)

    t_start = base_t + timedelta(seconds=120)
    t_confirm = t_start + timedelta(seconds=20)
    # 5 contiguous anomalous windows
    for i in range(5):
        w = _make_window(
            t_start + timedelta(seconds=10 * i),
            is_anomalous=True,
            score=0.85 + i * 0.02,
            run_start=t_start,
            confirmed_at=t_confirm,
        )
        manager.on_window(w)

    incidents = manager.list_incidents()
    assert len(incidents) == 1
    inc = incidents[0]
    assert inc.status == "open"
    assert inc.started_at == t_start
    assert inc.detected_at == t_confirm
    assert inc.ended_at == t_start + timedelta(seconds=50)
    assert pytest.approx(inc.peak_score, 0.01) == 0.93

    # Close with tick past anomaly_gap_seconds (30s)
    manager.tick(t_start + timedelta(seconds=50 + 35))
    inc = manager.get_incident(inc.id)
    assert inc is not None
    assert inc.status == "closed"
    assert len(manager.list_incidents()) == 1


def test_two_anomalies_20s_apart_give_one_incident() -> None:
    """Two anomalies separated by 20s (<= ANOMALY_GAP_SECONDS=30) merge into one incident."""
    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)

    # First anomaly window: [10:00:00, 10:00:10]
    w1 = _make_window(base_t, duration_s=10, is_anomalous=True, score=0.8)
    manager.on_window(w1)
    assert len(manager.list_incidents()) == 1

    # 20s gap: next anomaly at +30s (20s gap from end of w1 at +10s)
    w_norm1 = _make_window(base_t + timedelta(seconds=10), duration_s=10, is_anomalous=False)
    w_norm2 = _make_window(base_t + timedelta(seconds=20), duration_s=10, is_anomalous=False)
    manager.on_window(w_norm1)
    manager.on_window(w_norm2)

    w2 = _make_window(base_t + timedelta(seconds=30), duration_s=10, is_anomalous=True, score=0.9)
    manager.on_window(w2)

    incidents = manager.list_incidents()
    assert len(incidents) == 1
    assert incidents[0].peak_score == 0.9
    assert incidents[0].ended_at == base_t + timedelta(seconds=40)

    # Advance watermark past gap to close
    manager.tick(base_t + timedelta(seconds=75))
    assert incidents[0].status == "closed"
    assert len(manager.list_incidents()) == 1


def test_two_anomalies_120s_apart_give_two_incidents() -> None:
    """Two anomalies separated by 120s (> 30s gap and > 60s cooldown) give two separate incidents."""
    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)

    # First anomaly window: [10:00:00, 10:00:10]
    w1 = _make_window(base_t, duration_s=10, is_anomalous=True, score=0.8)
    manager.on_window(w1)
    assert len(manager.list_incidents()) == 1

    # Advance tick to 10:00:45 -> closes incident 1 at ended_at = 10:00:10
    manager.tick(base_t + timedelta(seconds=45))
    inc1 = manager.get_incident(1)
    assert inc1 is not None and inc1.status == "closed"

    # Second anomaly at 10:02:10 (120s after w1 ended at 10:00:10)
    w2 = _make_window(base_t + timedelta(seconds=130), duration_s=10, is_anomalous=True, score=0.85)
    manager.on_window(w2)

    incidents = manager.list_incidents()
    assert len(incidents) == 2
    assert incidents[0].id == 1
    assert incidents[1].id == 2


def test_flapping_inside_cooldown_reopens_incident() -> None:
    """An anomaly occurring within INCIDENT_COOLDOWN_SECONDS of closure merges/reopens that incident."""
    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)

    # Anomaly 1: [10:00:00, 10:00:10]
    w1 = _make_window(base_t, duration_s=10, is_anomalous=True, score=0.8)
    manager.on_window(w1)

    # Tick at 10:00:45: gap is 35s > 30s -> incident closes (ended_at = 10:00:10)
    manager.tick(base_t + timedelta(seconds=45))
    inc = manager.get_incident(1)
    assert inc is not None
    assert inc.status == "closed"

    # Flapping anomaly arrives at 10:00:55 (45s after ended_at, within 60s cooldown)
    w2 = _make_window(base_t + timedelta(seconds=55), duration_s=10, is_anomalous=True, score=0.95)
    manager.on_window(w2)

    # Must be merged into incident 1 and reopened
    assert len(manager.list_incidents()) == 1
    inc = manager.get_incident(1)
    assert inc is not None
    assert inc.status == "open"
    assert inc.peak_score == 0.95
    assert inc.ended_at == base_t + timedelta(seconds=65)

    # Finally close after cooldown
    manager.tick(base_t + timedelta(seconds=100))
    assert inc.status == "closed"
    assert len(manager.list_incidents()) == 1


def test_flush_closes_an_open_incident() -> None:
    """Calling flush() immediately closes any active open incident."""
    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)

    w = _make_window(base_t, duration_s=10, is_anomalous=True, score=0.88)
    manager.on_window(w)

    inc = manager.get_incident(1)
    assert inc is not None
    assert inc.status == "open"

    manager.flush()
    inc = manager.get_incident(1)
    assert inc is not None
    assert inc.status == "closed"


def test_evidence_pack_size_and_privacy() -> None:
    """Evidence pack must be under 2000 tokens (<=8000 chars) and contain zero fault_events data."""
    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)

    # Populate baseline log events (within 120s baseline before incident)
    for tid in range(1, 15):
        for line_num in range(2):
            manager.record_log(
                {
                    "ts": base_t + timedelta(seconds=10 * line_num),
                    "service": "api-service",
                    "level": "INFO",
                    "message": f"Service normal message {tid}:{line_num}",
                    "template_id": tid,
                },
                type("Parsed", (), {"template_id": tid, "template_text": f"Service normal message <*>", "duration_ms": 50.0})(),
            )

    # Anomalous windows starting at base_t + 120s
    t_start = base_t + timedelta(seconds=120)
    for tid in range(1, 15):
        for line_num in range(10):
            manager.record_log(
                {
                    "ts": t_start + timedelta(seconds=line_num),
                    "service": "api-service",
                    "level": "ERROR",
                    "message": f"Service failure code {tid}:{line_num} error",
                    "template_id": tid,
                },
                type("Parsed", (), {"template_id": tid, "template_text": f"Service failure code <*>", "duration_ms": 300.0})(),
            )

    w = _make_window(t_start, duration_s=20, is_anomalous=True, score=0.94)
    manager.on_window(w)
    manager.flush()

    pack = build_evidence_pack(1, manager=manager)

    # 1. Structure checks
    assert pack["incident_id"] == 1
    assert "time_range" in pack
    assert "duration_seconds" in pack
    assert "affected_services" in pack
    assert "service_error_rates" in pack
    assert "ratio_floor" in pack
    assert "baseline_vs_incident" in pack
    assert "top_templates" in pack
    assert "peak_window_features" in pack

    for t in pack["top_templates"]:
        if isinstance(t, dict) and "template_id" in t:
            assert "is_new" in t
            assert "absent_in_baseline" in t

    # 2. Max 8 top templates
    assert len(pack["top_templates"]) <= 8

    # 3. Compact JSON size (under ~2000 tokens => characters / 4 < 2000 => len < 8000)
    pack_json = json.dumps(pack)
    char_count = len(pack_json)
    estimated_tokens = char_count / 4.0
    assert char_count <= 8000, f"Evidence pack characters {char_count} exceeded 8000 limit"
    assert estimated_tokens < 2000, f"Estimated tokens {estimated_tokens} exceeded 2000 limit"

    # 4. Strict privacy: ZERO fault_events data
    forbidden = ["fault_events", "fault_event", "fault_type", "target_service", "ground_truth"]
    pack_str_lower = pack_json.lower()
    for item in forbidden:
        assert item not in pack_str_lower, f"Forbidden evaluation field '{item}' leaked into evidence pack!"


def test_template_outside_top_k_is_reported_in_top_templates() -> None:
    """A template not tracked in the detector top-K features (in tpl_other) is still identified from log_events."""
    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)

    # Template 999 is an obscure template outside top-K features
    t_start = base_t + timedelta(seconds=120)

    # 1 baseline event for template 999
    manager.record_log(
        {
            "ts": base_t + timedelta(seconds=50),
            "service": "api-service",
            "level": "INFO",
            "message": "Rare warning on cluster node 42",
            "template_id": 999,
        },
        type("Parsed", (), {"template_id": 999, "template_text": "Rare warning on cluster node <*>", "duration_ms": 30.0})(),
    )

    # 40 error events for template 999 during the incident
    for i in range(40):
        manager.record_log(
            {
                "ts": t_start + timedelta(seconds=i),
                "service": "api-service",
                "level": "ERROR",
                "message": f"Rare warning on cluster node {i}",
                "template_id": 999,
            },
            type("Parsed", (), {"template_id": 999, "template_text": "Rare warning on cluster node <*>", "duration_ms": 350.0})(),
        )

    # Window features only contain tpl_1, tpl_2 and tpl_other; template 999 is NOT in window features
    feats = {"tpl_1": 5.0, "tpl_2": 2.0, "tpl_other": 40.0, "log_rate": 20.0, "error_rate": 0.5}
    w = _make_window(t_start, duration_s=40, is_anomalous=True, score=0.96, features=feats)
    manager.on_window(w)
    manager.flush()

    inc = manager.get_incident(1)
    assert inc is not None
    # Template 999 must appear in top_templates because computation aggregates from log_events
    reported_tids = [t["template_id"] for t in inc.top_templates]
    assert 999 in reported_tids
    t999 = next(t for t in inc.top_templates if t["template_id"] == 999)
    assert t999["incident_rate"] > t999["baseline_rate"]
    assert len(t999["examples"]) > 0
    assert "Rare warning on cluster node" in t999["text"]


def test_api_incidents_endpoints() -> None:
    """GET /incidents and GET /incidents/{id} return real incident data including detected_at and category."""
    from fastapi.testclient import TestClient
    from app.api.main import app

    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)
    t_confirm = base_t + timedelta(seconds=10)

    w = _make_window(base_t, duration_s=10, is_anomalous=True, score=0.91, confirmed_at=t_confirm)
    manager.on_window(w)
    manager.flush()

    app.state.incident_manager = manager

    client = TestClient(app)
    # List incidents
    res = client.get("/incidents")
    assert res.status_code == 200
    incidents = res.json()
    assert len(incidents) >= 1
    assert incidents[0]["id"] == 1
    assert incidents[0]["status"] == "closed"
    assert incidents[0]["peak_score"] == 0.91
    assert "detected_at" in incidents[0]
    assert "category" in incidents[0]

    # Get single incident
    res_one = client.get("/incidents/1")
    assert res_one.status_code == 200
    data = res_one.json()
    assert data["id"] == 1
    assert "detected_at" in data
    assert "category" in data
    assert "top_templates" in data

    # 404 for unknown incident
    res_404 = client.get("/incidents/999")
    assert res_404.status_code == 404

    # Stats endpoint
    res_stats = client.get("/stats")
    assert res_stats.status_code == 200
    stats_data = res_stats.json()
    assert "incidents" in stats_data
    assert "total_windows" in stats_data
    assert stats_data["incidents"] >= 1


def test_quiet_window_baseline_excludes_earlier_incident_and_quarantine() -> None:
    """Quiet-window baseline must exclude earlier incidents, their 60s cooldown, and suspect quarantine."""
    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)

    # 1. 10 initial quiet windows (10:00:00 to 10:01:40)
    for i in range(10):
        w = _make_window(base_t + timedelta(seconds=10 * i), is_anomalous=False, score=0.1)
        manager.on_window(w)

    # 2. First incident: 10:01:40 to 10:02:10 (3 anomalous windows)
    t1_start = base_t + timedelta(seconds=100)
    for i in range(3):
        w = _make_window(
            t1_start + timedelta(seconds=10 * i),
            is_anomalous=True,
            score=0.9,
            run_start=t1_start,
        )
        manager.on_window(w)

    # Tick to close incident 1 at 10:02:10 + 35s = 10:02:45
    manager.tick(t1_start + timedelta(seconds=30 + 35))
    inc1 = manager.get_incident(1)
    assert inc1 is not None
    assert inc1.status == "closed"
    assert inc1.ended_at == t1_start + timedelta(seconds=30)  # 10:02:10

    # 3. Windows in cooldown period (10:02:10 to 10:03:10, within 60s of inc1 end)
    # These must NOT be used as quiet baseline windows for inc2!
    for i in range(6):
        w = _make_window(inc1.ended_at + timedelta(seconds=10 * i), is_anomalous=False, score=0.1)
        manager.on_window(w)

    # 4. Clean quiet windows post-cooldown (10:03:10 to 10:05:00, 11 windows)
    post_cool = inc1.ended_at + timedelta(seconds=60)
    for i in range(11):
        w = _make_window(post_cool + timedelta(seconds=10 * i), is_anomalous=False, score=0.1)
        manager.on_window(w)

    # 5. Second incident starts at 10:05:00
    t2_start = post_cool + timedelta(seconds=110)
    for i in range(3):
        w = _make_window(
            t2_start + timedelta(seconds=10 * i),
            is_anomalous=True,
            score=0.92,
            run_start=t2_start,
        )
        manager.on_window(w)

    manager.flush()
    inc2 = manager.get_incident(2)
    assert inc2 is not None
    assert inc2.status == "closed"

    pack = build_evidence_pack(inc2)
    assert "baseline_source" in pack
    assert "baseline_window_count" in pack
    assert pack["baseline_window_count"] >= 4
    assert pack["baseline_source"] == "recent"

    # Verify that quiet windows for incident 2 do not overlap incident 1 or its 60s cooldown
    quiet_wins, src, count = manager._get_quiet_windows(inc2)
    inc1_excl_end = inc1.ended_at + timedelta(seconds=60)
    for qw in quiet_wins:
        assert qw["w_start"] >= inc1_excl_end or qw["w_end"] <= inc1.started_at


def test_evidence_interval_cap() -> None:
    """Incident metrics and top_templates aggregation cap evidence at EVIDENCE_MAX_SECONDS (300s)."""
    manager = IncidentManager(persist=False, anomaly_gap_seconds=30, incident_cooldown_seconds=60)
    manager.evidence_max_seconds = 300
    base_t = datetime(2026, 11, 23, 10, 0, 0, tzinfo=UTC)

    # Baseline quiet windows
    for i in range(12):
        w = _make_window(base_t + timedelta(seconds=10 * i), is_anomalous=False, score=0.1)
        manager.on_window(w)

    t_start = base_t + timedelta(seconds=120)

    # Incident logs:
    # Template 101 emits 10 errors inside first 300s (e.g. at t_start + 50s)
    for _ in range(10):
        manager.record_log({
            "ts": (t_start + timedelta(seconds=50)).isoformat(),
            "service": "api-service",
            "level": "ERROR",
            "message": "Error inside evidence interval cap",
        }, parsed=type("Parsed", (), {"template_id": 101, "template": "Error inside evidence interval cap"})())

    # Template 102 emits 100 errors AFTER 300s (at t_start + 450s)
    for _ in range(100):
        manager.record_log({
            "ts": (t_start + timedelta(seconds=450)).isoformat(),
            "service": "api-service",
            "level": "ERROR",
            "message": "Late error after evidence interval cap",
        }, parsed=type("Parsed", (), {"template_id": 102, "template": "Late error after evidence interval cap"})())

    # Incident spans 60 windows = 600 seconds
    for i in range(60):
        w = _make_window(
            t_start + timedelta(seconds=10 * i),
            is_anomalous=True,
            score=0.95,
            run_start=t_start,
        )
        manager.on_window(w)

    manager.flush()
    inc = manager.get_incident(1)
    assert inc is not None
    # Incident true duration is 600s
    true_dur = (inc.ended_at - inc.started_at).total_seconds()
    assert true_dur == 600.0

    # But template 102 (which only occurred at +450s) must NOT dominate or appear above template 101
    # Because evidence aggregation was capped at 300s!
    tpl_ids = [t["template_id"] for t in inc.top_templates]
    assert 101 in tpl_ids
    assert 102 not in tpl_ids

    pack = build_evidence_pack(inc)
    assert pack["duration_seconds"] == 600.0
    assert "baseline_source" in pack
    assert "baseline_window_count" in pack
