"""Fast unit tests for testbed fault flags and plain log formatting."""
from __future__ import annotations

from testbed.common import log_shipper
from testbed.common.faults import FaultController
from testbed.common.log_shipper import LogShipper
from testbed.smoke import _message_pattern


def test_fault_flags_start_and_stop_without_activating_resources() -> None:
    controller = FaultController()
    for kind in ("latency", "packet_loss"):
        controller.start(kind, {"delay_ms": 123})
        actual_kind, params = controller.snapshot()
        assert actual_kind == kind
        assert params == {"delay_ms": 123}
        controller.stop()
        assert controller.snapshot() == (None, {})


def test_log_format_is_plain_and_contains_expected_fields(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(log_shipper, "LOG_DIR", tmp_path)
    shipper = LogShipper("api-service")
    shipper.log("INFO", "Order 4821 created for user 77 in 35ms")
    shipper.close()

    line = (tmp_path / "api-service.log").read_text(encoding="utf-8").strip()
    assert line.startswith("20")
    assert " [api-service] INFO Order 4821 created for user 77 in 35ms" in line
    assert "{" not in line


def test_smoke_message_patterns_normalize_measurements() -> None:
    pattern = _message_pattern(
        "2026-10-01 14:32:01,123 [api-service] WARN Slow request POST /orders completed in 823ms"
    )
    assert pattern == "Slow request POST /orders completed in N"