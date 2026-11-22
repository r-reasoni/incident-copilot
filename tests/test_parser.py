"""
tests/test_parser.py
====================
Unit tests for Drain3 log parser, custom token masking, duration extraction,
HTTP path normalization, template ID stability, and offline parse isolation.
"""
from __future__ import annotations

from pathlib import Path

from app.config import settings
from app.parsing.offline_parse import parse_log_file
from app.parsing.parser import LogParser, extract_duration_ms


def test_extract_duration_ms() -> None:
    """extract_duration_ms extracts first duration in ms or s converted to milliseconds."""
    assert extract_duration_ms("completed in 35ms") == 35.0
    assert extract_duration_ms("after 1.5s") == 1500.0
    assert extract_duration_ms("Request GET /health completed in 2ms") == 2.0
    assert extract_duration_ms("Order 1 created for user 2 in 7ms") == 7.0
    assert extract_duration_ms("latency 0.25s recorded") == 250.0
    assert extract_duration_ms("no duration in this log line") is None
    # Extracts the FIRST duration when multiple are present
    assert extract_duration_ms("step 1 in 50ms, step 2 in 1.2s") == 50.0


def test_masking_tokens(tmp_path: Path) -> None:
    """Masking instructions mask UUIDs, IPs, hex IDs, durations, HTTP paths, and numbers."""
    parser = LogParser(state_path=tmp_path / "drain.bin")

    # UUID masking
    res_uuid = parser.parse("Transaction 12345678-abcd-ef01-2345-6789abcdef01 confirmed")
    assert "<UUID>" in res_uuid.template_text
    assert "12345678-abcd-ef01-2345-6789abcdef01" in res_uuid.params

    # IP masking
    res_ip = parser.parse("Connection established from 192.168.1.100 port 5000")
    assert "<IP>" in res_ip.template_text
    assert "192.168.1.100" in res_ip.params

    # Hex ID masking
    res_hex = parser.parse("Pointer dereference at 0xdeadbeef failed")
    assert "<HEX>" in res_hex.template_text
    assert "0xdeadbeef" in res_hex.params

    # Duration masking
    res_dur = parser.parse("Request completed in 35ms")
    assert "<DUR>" in res_dur.template_text
    assert "35ms" in res_dur.params
    assert res_dur.duration_ms == 35.0

    res_dur_s = parser.parse("Operation timed out after 1.5s")
    assert "<DUR>" in res_dur_s.template_text
    assert "1.5s" in res_dur_s.params
    assert res_dur_s.duration_ms == 1500.0

    # Plain number masking
    res_num = parser.parse("Worker processed 42 jobs")
    assert "<NUM>" in res_num.template_text
    assert "42" in res_num.params


def test_http_path_masking_and_distinct_template_ids() -> None:
    """HTTP paths with numeric segments are normalized while method and endpoint stay literal.

    /users and /orders produce different template IDs.
    """
    parser = LogParser(persistence=False)

    res_users = parser.parse("Request GET /users/42 completed in 35ms")
    assert res_users.template_text == "Request GET /users/<NUM> completed in <DUR>"
    assert res_users.duration_ms == 35.0

    res_orders = parser.parse("Request POST /orders completed in 20ms")
    assert res_orders.template_text == "Request POST /orders completed in <DUR>"
    assert res_orders.duration_ms == 20.0

    # /users and /orders must produce different template IDs
    assert res_users.template_id != res_orders.template_id

    # Subsequent /users requests with different IDs match the same /users template ID
    res_users_2 = parser.parse("Request GET /users/99 completed in 12ms")
    assert res_users_2.template_id == res_users.template_id
    assert res_users_2.template_text == "Request GET /users/<NUM> completed in <DUR>"
    assert res_users_2.is_new_template is False


def test_same_line_shape_same_template_id(tmp_path: Path) -> None:
    """The same line shape always gets the same template_id; is_new_template is True only once."""
    parser = LogParser(state_path=tmp_path / "drain.bin")

    res1 = parser.parse("Order 1 created for user 2 in 7ms")
    assert res1.is_new_template is True
    assert res1.duration_ms == 7.0

    res2 = parser.parse("Order 2 created for user 3 in 9ms")
    assert res2.template_id == res1.template_id
    assert res2.is_new_template is False
    assert res2.duration_ms == 9.0

    res3 = parser.parse("Order 999 created for user 88 in 12ms")
    assert res3.template_id == res1.template_id
    assert res3.is_new_template is False


def test_different_message_creates_new_template(tmp_path: Path) -> None:
    """A clearly different message creates a new template with is_new_template=True."""
    parser = LogParser(state_path=tmp_path / "drain.bin")

    res1 = parser.parse("Order 1 created for user 2 in 7ms")
    res2 = parser.parse("Memory allocation slowed as process footprint reached 500MB")

    assert res2.template_id != res1.template_id
    assert res2.is_new_template is True
    assert res2.duration_ms is None


def test_realistic_testbed_lines() -> None:
    """Realistic testbed lines produce expected templates and durations."""
    test_lines = [
        ("API service ready on port 8101", "API service ready on port <NUM>", None),
        ("Request GET /health completed in 2ms", "Request GET /health completed in <DUR>", 2.0),
        (
            "Request GET /users/1 completed in 17ms",
            "Request GET /users/<NUM> completed in <DUR>",
            17.0,
        ),
        (
            "Order 1 created for user 2 in 7ms",
            "Order <NUM> created for user <NUM> in <DUR>",
            7.0,
        ),
        ("Request POST /orders completed in 9ms", "Request POST /orders completed in <DUR>", 9.0),
        (
            "Slow request GET /users/2 completed in 850ms",
            "Slow request GET /users/<NUM> completed in <DUR>",
            850.0,
        ),
        (
            "Order persistence resumed after delay of 850ms",
            "Order persistence resumed after delay of <DUR>",
            850.0,
        ),
        (
            "Memory allocation slowed as process footprint reached 500MB",
            "Memory allocation slowed as process footprint reached 500MB",
            None,
        ),
        (
            "Request timed out waiting for an upstream response",
            "Request timed out waiting for an upstream response",
            None,
        ),
        ("Worker service ready on port 8102", "Worker service ready on port <NUM>", None),
        (
            "Upstream request timed out; retrying job",
            "Upstream request timed out; retrying job",
            None,
        ),
        ("Job 1 failed after retry", "Job <NUM> failed after retry", None),
        ("Job 2 completed for user 2", "Job <NUM> completed for user <NUM>", None),
    ]

    for line, expected_text, expected_dur in test_lines:
        parser = LogParser(persistence=False)
        parsed = parser.parse(line)
        assert parsed.template_id > 0
        assert parsed.template_text == expected_text
        assert parsed.duration_ms == expected_dur


def test_state_persists_across_instances(tmp_path: Path) -> None:
    """State persists across a new LogParser instance."""
    state_file = tmp_path / "drain_state.bin"

    parser1 = LogParser(state_path=state_file)
    res1 = parser1.parse("Request GET /users/1 completed in 10ms")
    assert res1.is_new_template is True
    initial_id = res1.template_id
    assert state_file.exists()

    # Create a brand new parser instance pointing to the same state file
    parser2 = LogParser(state_path=state_file)
    res2 = parser2.parse("Request GET /users/2 completed in 15ms")
    assert res2.template_id == initial_id
    assert res2.is_new_template is False


def test_reset_deletes_state(tmp_path: Path) -> None:
    """reset() deletes the persisted state and resets the miner."""
    state_file = tmp_path / "drain_state.bin"

    parser = LogParser(state_path=state_file)
    parser.parse("Job 1 failed after retry")
    assert state_file.exists()
    assert parser.cluster_count >= 1

    parser.reset()
    assert not state_file.exists()
    assert parser.cluster_count == 0

    # Next parse creates cluster 1 again
    res_after = parser.parse("Job 1 failed after retry")
    assert res_after.template_id == 1
    assert res_after.is_new_template is True


def test_offline_parse_does_not_touch_state_file(tmp_path: Path, monkeypatch) -> None:
    """offline_parse uses an in-memory miner and never reads or writes the live DRAIN_STATE_PATH."""
    live_state = tmp_path / "live_drain_state.bin"
    monkeypatch.setattr(settings, "drain_state_path", str(live_state))

    log_file = tmp_path / "sample.log"
    log_file.write_text(
        "2026-11-22 10:00:00,123 [api-service] INFO Request GET /users/1 completed in 10ms\n"
        "2026-11-22 10:00:01,123 [api-service] INFO Request POST /orders completed in 20ms\n",
        encoding="utf-8",
    )

    parse_log_file(log_file)

    # State file must not have been created or modified
    assert not live_state.exists()
