"""Fast unit tests for testbed fault flags, plain log formatting, and evaluation leak prevention."""
from __future__ import annotations

import ast
from pathlib import Path

from testbed.common import log_shipper
from testbed.common.faults import FaultController
from testbed.common.log_shipper import LogShipper
from testbed.smoke import _message_pattern

FORBIDDEN_PHRASES = [
    "conditions changed",
    "returned to normal",
    "lock is delaying",
    "sustained compute load",
    "allocation rate increased",
    "cpu_hog",
    "memory_leak",
    "latency",
    "packet_loss",
    "db_lock",
    "_fault",
    "_internal",
    "/health",
    "/metrics",
]


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


def test_no_evaluation_leaks_in_source_or_logs(monkeypatch) -> None:
    """Scan testbed source code and log output of a short run for forbidden phrases."""
    testbed_dir = Path(__file__).resolve().parents[1] / "testbed"

    # 1. Scan source code log calls via AST
    source_files = [
        testbed_dir / "api_service.py",
        testbed_dir / "worker_service.py",
        testbed_dir / "common" / "faults.py",
        testbed_dir / "common" / "log_shipper.py",
    ]

    for file_path in source_files:
        if not file_path.exists():
            continue
        tree = ast.parse(file_path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                is_log = (
                    isinstance(node.func, ast.Attribute) and node.func.attr == "log"
                ) or (
                    isinstance(node.func, ast.Name) and node.func.id == "_log"
                )

                if is_log and len(node.args) >= 2:
                    msg_node = node.args[1]
                    msg_texts: list[str] = []
                    if isinstance(msg_node, ast.Constant) and isinstance(msg_node.value, str):
                        msg_texts.append(msg_node.value)
                    elif isinstance(msg_node, ast.JoinedStr):
                        msg_texts.extend(
                            [
                                p.value
                                for p in msg_node.values
                                if isinstance(p, ast.Constant) and isinstance(p.value, str)
                            ]
                        )
                    full_msg = " ".join(msg_texts).lower()
                    for phrase in FORBIDDEN_PHRASES:
                        assert phrase.lower() not in full_msg, (
                            f"Forbidden phrase '{phrase}' found in {file_path.name}: {full_msg}"
                        )

    # 2. Simulate short run with FaultController and capture logs
    emitted_logs: list[str] = []

    def capture_log(level: str, msg: str) -> None:
        emitted_logs.append(msg)

    controller = FaultController()
    monkeypatch.setattr(controller, "_hold_database_lock", lambda: None)
    for fault in ("latency", "packet_loss", "cpu_hog", "memory_leak", "db_lock"):
        controller.start(fault, {"delay_ms": 10, "cap_mb": 50, "rate_mb": 10}, log=capture_log)
        controller.stop()

    for msg in emitted_logs:
        msg_lower = msg.lower()
        for phrase in FORBIDDEN_PHRASES:
            assert phrase.lower() not in msg_lower, (
                f"Forbidden phrase '{phrase}' emitted during fault run: {msg}"
            )

    # 3. Real test run with API service handling control, health, and fault requests
    from fastapi.testclient import TestClient
    import testbed.api_service as api_mod

    service_logs: list[str] = []
    monkeypatch.setattr(api_mod.shipper, "log", lambda level, msg: service_logs.append(msg))
    monkeypatch.setattr(api_mod.faults, "_hold_database_lock", lambda: None)

    with TestClient(api_mod.app) as client:
        # Request to control and meta endpoints
        client.get("/health")
        client.get("/metrics")
        client.post("/_fault/start", json={"type": "latency", "params": {"delay_ms": 10}})
        client.post("/_fault/stop")
        client.post("/_internal/stop")

    for msg in service_logs:
        msg_lower = msg.lower()
        for phrase in FORBIDDEN_PHRASES:
            assert phrase.lower() not in msg_lower, (
                f"Forbidden phrase '{phrase}' emitted in testbed service log: {msg}"
            )
