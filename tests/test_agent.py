"""Unit and integration tests for Step 7: Root-cause agent."""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from google.genai import types

from app.agent.agent import (
    AgentFinalResponse,
    run_agent,
)
from app.agent.llm import MockLLM
from app.agent.tools import (
    get_host_metrics,
    get_network_stats,
    query_logs,
    recent_changes,
)
from app.config import get_settings
from app.db.models import Incident, LogEvent, MetricsSample


def test_ground_truth_isolation_and_closed_incident_enforcement():
    """Verify that app/agent has zero reference to fault_events, and agent rejects open incidents."""
    agent_dir = Path(__file__).resolve().parent.parent / "app" / "agent"
    assert agent_dir.exists()

    for py_file in agent_dir.rglob("*.py"):
        file_content = py_file.read_text(encoding="utf-8")
        assert "fault_events" not in file_content, f"Ground-truth violation: {py_file} references fault_events"

    open_incident = Incident(
        id=999,
        started_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ended_at=None,
        status="open",
        peak_score=5.5,
        affected_services=["api-service"],
    )
    mock_session = MagicMock()
    mock_session.get.return_value = open_incident

    with pytest.raises(ValueError, match="Agent runs ONLY for closed incidents"):
        run_agent(999, session=mock_session)


def test_tools_return_correct_data_and_schema(tmp_path, monkeypatch):
    """Verify all agent tools validate arguments, query data, and never raise."""
    now = datetime.now(timezone.utc)
    t_start = (now - timedelta(minutes=5)).isoformat()
    t_end = now.isoformat()

    # 1. recent_changes
    dummy_changes = [
        {"ts": (now - timedelta(minutes=2)).isoformat(), "description": "Deployed config update"},
        {"ts": (now - timedelta(hours=2)).isoformat(), "description": "Old change outside window"},
    ]
    changes_file = tmp_path / "changes.json"
    changes_file.write_text(json.dumps(dummy_changes), encoding="utf-8")

    monkeypatch.setattr("app.agent.tools.CHANGES_PATH", changes_file)

    rc = recent_changes(t_start, t_end)
    assert "changes" in rc
    assert len(rc["changes"]) == 1
    assert rc["changes"][0]["description"] == "Deployed config update"

    # 2. query_logs
    log_row = LogEvent(
        id=1,
        ts=now - timedelta(minutes=1),
        service="api-service",
        host="host1",
        level="ERROR",
        raw_message="A" * 300,
        template_id=42,
    )
    monkeypatch.setattr("app.agent.tools.repository.get_logs", lambda **kw: [log_row])

    logs_res = query_logs("api-service", t_start, t_end, limit=150)
    assert "rows" in logs_res
    assert len(logs_res["rows"]) == 1
    assert len(logs_res["rows"][0]["message"]) == 200

    # 3. get_host_metrics
    mock_metrics = [
        MetricsSample(
            id=1,
            ts=now - timedelta(minutes=2),
            service="api-service",
            cpu_percent=85.0,
            mem_percent=55.0,
            open_handles=10,
            threads=5,
            p50_ms=20.0,
            p95_ms=90.0,
            error_count=3,
            in_flight_requests=1,
            request_count_30s=10,
            probe_ms=15.0,
            probe_ok=True,
        )
    ]
    monkeypatch.setattr("app.agent.tools.repository.get_metrics", lambda s, svc, st, et: mock_metrics)

    metrics_res = get_host_metrics("api-service", t_start, t_end)
    assert "incident_window" in metrics_res
    assert "cpu_percent" in metrics_res["incident_window"]
    assert metrics_res["incident_window"]["cpu_percent"]["max"] == 85.0

    # 4. get_network_stats
    net_res = get_network_stats("api-service", t_start, t_end)
    assert "probes" in net_res
    assert "live_snapshot" in net_res


def test_mock_llm_categories_on_hand_built_evidence():
    """Verify MockLLM correctly categorizes hand-built evidence for all 5 faults."""
    mock_llm = MockLLM()

    # 1. CPU
    ev_cpu = {
        "affected_services": ["api-service"],
        "host_metrics": {
            "incident_window": {"cpu_percent": {"mean": 92.0, "max": 98.0}, "mem_percent": {"slope_per_minute": 0.0, "max": 40.0}},
            "baseline_window": {"cpu_percent": {"mean": 15.0}},
        },
        "network_stats": {"probes": {"during": {"failure_rate": 0.0}}},
    }
    res_cpu, _ = mock_llm.investigate(ev_cpu)
    assert res_cpu["category"] == "cpu"
    assert res_cpu["confidence"] >= 0.85

    # 2. Memory
    ev_mem = {
        "affected_services": ["api-service"],
        "host_metrics": {
            "incident_window": {"cpu_percent": {"mean": 20.0, "max": 25.0}, "mem_percent": {"slope_per_minute": 0.15, "max": 88.0}},
            "baseline_window": {"cpu_percent": {"mean": 15.0}},
        },
        "network_stats": {"probes": {"during": {"failure_rate": 0.0}}},
    }
    res_mem, _ = mock_llm.investigate(ev_mem)
    assert res_mem["category"] == "memory"
    assert res_mem["confidence"] >= 0.85

    # 3. Packet loss
    ev_pkt = {
        "affected_services": ["api-service"],
        "host_metrics": {
            "incident_window": {"cpu_percent": {"mean": 20.0, "max": 25.0}, "mem_percent": {"slope_per_minute": 0.0, "max": 40.0}},
            "baseline_window": {"cpu_percent": {"mean": 15.0}},
        },
        "network_stats": {"probes": {"during": {"failure_rate": 0.35, "mean_probe_ms": 1200.0}}},
    }
    res_pkt, _ = mock_llm.investigate(ev_pkt)
    assert res_pkt["category"] == "packet_loss"
    assert res_pkt["confidence"] >= 0.85

    # 4. DB lock
    ev_db = {
        "affected_services": ["api-service"],
        "host_metrics": {
            "incident_window": {"cpu_percent": {"mean": 15.0, "max": 20.0}, "mem_percent": {"slope_per_minute": 0.0, "max": 40.0}},
            "baseline_window": {"cpu_percent": {"mean": 15.0}},
        },
        "network_stats": {"probes": {"during": {"failure_rate": 0.0}}},
        "top_templates_detail": [
            {"template_text": "Database query lock acquisition timeout on table accounts"},
            {"template_text": "Slow query detected: waited 15000ms for lock"},
        ],
    }
    res_db, _ = mock_llm.investigate(ev_db)
    assert res_db["category"] == "db_lock"
    assert res_db["confidence"] >= 0.85

    # 5. Network latency
    ev_lat = {
        "affected_services": ["api-service"],
        "host_metrics": {
            "incident_window": {
                "cpu_percent": {"mean": 15.0, "max": 20.0},
                "mem_percent": {"slope_per_minute": 0.0, "max": 40.0},
                "p95_ms": {"mean": 450.0, "max": 500.0},
                "error_count": {"mean": 0.0},
            },
            "baseline_window": {"cpu_percent": {"mean": 15.0}, "p95_ms": {"mean": 25.0}},
        },
        "network_stats": {"probes": {"during": {"failure_rate": 0.0, "mean_probe_ms": 350.0}}},
    }
    res_lat, _ = mock_llm.investigate(ev_lat)
    assert res_lat["category"] == "network_latency"
    assert res_lat["confidence"] >= 0.80


def test_invalid_json_retry_path_success(monkeypatch):
    """Verify agent retries on invalid JSON from LLM and succeeds on second attempt."""
    incident = Incident(
        id=101,
        started_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ended_at=datetime.now(timezone.utc),
        status="closed",
        peak_score=4.0,
        affected_services=["api-service"],
        top_templates=[1],
    )
    mock_session = MagicMock()
    mock_session.get.return_value = incident
    monkeypatch.setattr("app.agent.agent.SessionLocal", lambda: MagicMock(__enter__=lambda s: mock_session, __exit__=lambda s, *a: None))
    monkeypatch.setattr("app.agent.agent.build_evidence_pack", lambda inc, s: {"incident_id": inc.id, "text": "evidence"})
    monkeypatch.setattr("app.agent.agent._broadcast_alert_safely", lambda p: None)

    class FakeRetryLLM:
        def __init__(self):
            self.call_count = 0

        def call_raw(self, contents, config=None, **kwargs):
            self.call_count += 1
            if kwargs.get("tools"):
                resp = MagicMock()
                resp.function_calls = []
                resp.candidates = [MagicMock(content=types.Content(parts=[types.Part.from_text(text="no tools needed")]))]
                resp.text = "no tools needed"
                return resp
            if self.call_count == 2:
                resp = MagicMock()
                resp.function_calls = []
                resp.candidates = [MagicMock(content=types.Content(parts=[types.Part.from_text(text="NOT VALID JSON {foo: bar")]))]
                resp.text = "NOT VALID JSON {foo: bar"
                return resp
            else:
                valid_resp = {
                    "root_cause": "CPU saturation from unbounded loop",
                    "category": "cpu",
                    "confidence": 0.92,
                    "evidence": ["CPU spike to 99%"],
                    "alternatives": [],
                    "remediation": [{"step": "Scale out pods", "risk": "low"}],
                }
                resp = MagicMock()
                resp.function_calls = []
                resp.candidates = [MagicMock(content=types.Content(parts=[types.Part.from_text(text=json.dumps(valid_resp))]))]
                resp.text = json.dumps(valid_resp)
                return resp

    fake_llm = FakeRetryLLM()
    monkeypatch.setattr("app.agent.agent.get_llm", lambda: fake_llm)

    res = run_agent(101, session=mock_session)
    assert res["category"] == "cpu"
    assert res["confidence"] == 0.92
    assert fake_llm.call_count == 3


def test_invalid_json_retry_path_fallback_to_unknown(monkeypatch):
    """Verify that on double failure, agent stores unknown with confidence 0."""
    incident = Incident(
        id=103,
        started_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ended_at=datetime.now(timezone.utc),
        status="closed",
        peak_score=4.0,
        affected_services=["api-service"],
        top_templates=[1],
    )
    mock_session = MagicMock()
    mock_session.get.return_value = incident
    monkeypatch.setattr("app.agent.agent.SessionLocal", lambda: MagicMock(__enter__=lambda s: mock_session, __exit__=lambda s, *a: None))
    monkeypatch.setattr("app.agent.agent.build_evidence_pack", lambda inc, s: {"incident_id": inc.id, "text": "evidence"})
    monkeypatch.setattr("app.agent.agent._broadcast_alert_safely", lambda p: None)

    class DoubleFailLLM:
        def __init__(self):
            self.call_count = 0

        def call_raw(self, contents, config=None, **kwargs):
            self.call_count += 1
            if kwargs.get("tools"):
                resp = MagicMock()
                resp.function_calls = []
                resp.candidates = [MagicMock(content=types.Content(parts=[types.Part.from_text(text="no tools needed")]))]
                resp.text = "no tools needed"
                return resp
            resp = MagicMock()
            resp.function_calls = []
            resp.candidates = [MagicMock(content=types.Content(parts=[types.Part.from_text(text="MALFORMED JSON")]))]
            resp.text = "MALFORMED JSON"
            return resp

    double_fail_llm = DoubleFailLLM()
    monkeypatch.setattr("app.agent.agent.get_llm", lambda: double_fail_llm)

    res = run_agent(103, session=mock_session)
    assert res["category"] == "unknown"
    assert res["confidence"] == 0.0


def test_tool_call_cap_is_enforced(monkeypatch):
    """Verify tool calls are strictly capped at max_agent_tool_calls."""
    incident = Incident(
        id=102,
        started_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ended_at=datetime.now(timezone.utc),
        status="closed",
        peak_score=4.0,
        affected_services=["api-service"],
        top_templates=[1],
    )
    mock_session = MagicMock()
    mock_session.get.return_value = incident
    monkeypatch.setattr("app.agent.agent.SessionLocal", lambda: MagicMock(__enter__=lambda s: mock_session, __exit__=lambda s, *a: None))
    monkeypatch.setattr("app.agent.agent.build_evidence_pack", lambda inc, s: {"incident_id": inc.id, "text": "evidence"})
    monkeypatch.setattr("app.agent.agent._broadcast_alert_safely", lambda p: None)

    settings = get_settings()
    monkeypatch.setattr(settings, "max_agent_tool_calls", 3)

    class LoopToolLLM:
        def __init__(self):
            self.tool_calls_made = 0

        def call_raw(self, contents, config=None, **kwargs):
            if kwargs.get("tools"):
                self.tool_calls_made += 1
                fc = types.FunctionCall(name="recent_changes", args={"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:05:00Z"})
                resp = MagicMock()
                resp.function_calls = [fc]
                resp.candidates = [MagicMock(content=types.Content(parts=[types.Part(function_call=fc)]))]
                resp.text = None
                return resp
            else:
                valid_resp = {
                    "root_cause": "Unknown failure",
                    "category": "unknown",
                    "confidence": 0.2,
                    "evidence": ["Inspected recent changes"],
                    "alternatives": [],
                    "remediation": [],
                }
                resp = MagicMock()
                resp.function_calls = []
                resp.candidates = [MagicMock(content=types.Content(parts=[types.Part.from_text(text=json.dumps(valid_resp))]))]
                resp.text = json.dumps(valid_resp)
                return resp

    loop_llm = LoopToolLLM()
    monkeypatch.setattr("app.agent.agent.get_llm", lambda: loop_llm)

    res = run_agent(102, session=mock_session)
    assert loop_llm.tool_calls_made == 3
    assert res["category"] == "unknown"


def test_tools_validate_ten_minute_range():
    """Tools must reject time ranges greater than 10 minutes and return error dict."""
    now = datetime.now(timezone.utc)
    t_start = (now - timedelta(minutes=15)).isoformat()
    t_end = now.isoformat()

    assert "error" in query_logs("api-service", t_start, t_end)
    assert "error" in get_host_metrics("api-service", t_start, t_end)
    assert "error" in get_network_stats("api-service", t_start, t_end)
    assert "error" in recent_changes(t_start, t_end)


def test_mock_llm_db_lock_on_in_flight_high():
    """MockLLM classifies high in_flight requests with low CPU as db_lock."""
    mock_llm = MockLLM()
    ev_lock = {
        "affected_services": ["api-service"],
        "host_metrics": {
            "incident_window": {
                "cpu_percent": {"mean": 8.0, "max": 12.0},
                "mem_percent": {"slope_per_minute": 0.0, "max": 40.0},
                "in_flight_requests": {"max": 15},
                "request_count_30s": {"mean": 2.0},
            },
            "baseline_window": {"cpu_percent": {"mean": 10.0}},
        },
        "network_stats": {"probes": {"during": {"failure_rate": 0.0}}},
    }
    res, _ = mock_llm.investigate(ev_lock)
    assert res["category"] == "db_lock"
    assert res["confidence"] >= 0.85


@pytest.mark.asyncio
async def test_sampler_failure_handling():
    """Metrics sampler returns probe_ok=False and zeroes when target is unreachable, without raising."""
    import httpx
    from app.agent.metrics_sampler import sample_target
    async with httpx.AsyncClient() as client:
        # Port 59999 is unlikely to be listening
        sample = await sample_target(client, "dummy-service", "http://127.0.0.1:59999")
        assert sample["service"] == "dummy-service"
        assert sample["probe_ok"] is False
        assert sample["probe_ms"] is None
        assert sample["cpu_percent"] == 0.0


def test_gemini_client_503_retry_and_backoff(monkeypatch):
    """GeminiClient retries 503 errors up to 6 times with exponential backoff and jitter."""
    import time
    from unittest.mock import MagicMock
    from app.agent.llm import GeminiClient

    client = GeminiClient(api_key="fake-key", model="gemini-2.5-flash")
    mock_genai_client = MagicMock()
    client._client = mock_genai_client

    call_count = 0
    mock_success_resp = MagicMock(text="success")

    def mock_generate_content(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise Exception("503 Service Unavailable: overloaded")
        return mock_success_resp

    mock_genai_client.models.generate_content.side_effect = mock_generate_content

    slept_delays = []
    monkeypatch.setattr(time, "sleep", lambda delay: slept_delays.append(delay))

    resp = client.call_raw([{"role": "user", "parts": ["test prompt"]}])
    assert resp == mock_success_resp
    assert call_count == 3
    assert len(slept_delays) == 2
    assert slept_delays[0] >= 2.0  # initial backoff >= 2s
    assert slept_delays[1] >= 4.0  # second backoff >= 4s
    assert client.last_model_used == "gemini-2.5-flash"


def test_gemini_client_fallback_model(monkeypatch):
    """GeminiClient falls back to GEMINI_FALLBACK_MODELS when primary model exhausts retries."""
    import time
    from unittest.mock import MagicMock
    from app.agent.llm import GeminiClient

    monkeypatch.setattr("app.config.settings.gemini_fallback_models", "fallback-model-1")

    client = GeminiClient(api_key="fake-key", model="primary-model")
    mock_genai_client = MagicMock()
    client._client = mock_genai_client

    called_models = []
    mock_success_resp = MagicMock(text="success from fallback")

    def mock_generate_content(*args, **kwargs):
        model = kwargs.get("model")
        called_models.append(model)
        if model == "primary-model":
            raise Exception("503 Service Unavailable")
        return mock_success_resp

    mock_genai_client.models.generate_content.side_effect = mock_generate_content
    monkeypatch.setattr(time, "sleep", lambda delay: None)

    resp = client.call_raw([{"role": "user", "parts": ["test prompt"]}])
    assert resp == mock_success_resp
    assert called_models.count("primary-model") == 6
    assert called_models[-1] == "fallback-model-1"
    assert client.last_model_used == "fallback-model-1"


def test_reanalyze_cli(monkeypatch):
    """reanalyze CLI re-runs closed incidents with force=True."""
    from app.agent.reanalyze import reanalyze_all_closed, reanalyze_incident

    closed_inc = Incident(
        id=201,
        started_at=datetime.now(timezone.utc) - timedelta(minutes=5),
        ended_at=datetime.now(timezone.utc),
        status="closed",
        peak_score=4.0,
        affected_services=["api-service"],
    )

    mock_session = MagicMock()
    mock_session.get.return_value = closed_inc

    executed_ids = []

    def mock_run_agent(inc_id, force=False):
        executed_ids.append((inc_id, force))

    monkeypatch.setattr("app.agent.reanalyze.get_session", lambda: MagicMock(__enter__=lambda s: mock_session, __exit__=lambda s, *a: None))
    monkeypatch.setattr("app.agent.reanalyze.run_agent", mock_run_agent)
    monkeypatch.setattr("app.agent.reanalyze.list_incidents", lambda session, status: [closed_inc])

    ok = reanalyze_incident(201, force=True)
    assert ok is True
    assert executed_ids == [("201", True)]

    executed_ids.clear()
    count = reanalyze_all_closed(force=True)
    assert count == 1
    assert executed_ids == [("201", True)]
