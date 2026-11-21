"""Run brief local traffic and all fault types, reporting observed symptoms."""
from __future__ import annotations

import multiprocessing
import re
import subprocess
import sys
import threading
import time
from collections import Counter
from pathlib import Path

import httpx
from sqlalchemy import select

from app.db.models import FaultEvent
from app.db.session import SessionLocal
from testbed.fault_injector import run_fault
from testbed.common.log_shipper import LOG_DIR

FAULT_PLAN = ["cpu_hog", "memory_leak", "latency", "packet_loss", "db_lock"]
LABEL_PATTERN = re.compile(r"\b(cpu_hog|memory_leak|packet_loss|db_lock|latency)\b", re.I)


def _send_traffic(stop: threading.Event) -> None:
    index = 0
    with httpx.Client(timeout=2.5) as client:
        while not stop.is_set():
            index += 1
            try:
                if index % 2:
                    client.get(f"http://127.0.0.1:8101/users/1")
                else:
                    client.post("http://127.0.0.1:8101/orders", json={"user_id": 2, "amount": 18.75})
            except httpx.HTTPError:
                pass
            stop.wait(0.15)


def _message_pattern(line: str) -> str:
    message = line.strip().split("] ", 1)[-1].split(" ", 1)[-1]
    message = re.sub(r"\d+(?:\.\d+)?(?:ms|MB|%)?", "N", message)
    return message[:100]


def _metrics() -> dict[str, float]:
    try:
        response = httpx.get("http://127.0.0.1:8101/metrics", timeout=2)
        response.raise_for_status()
        data = response.json()
        return {key: float(data.get(key, 0)) for key in (
            "cpu_percent", "memory_percent", "request_latency_p95_ms"
        )}
    except httpx.HTTPError:
        return {"cpu_percent": 0.0, "memory_percent": 0.0, "request_latency_p95_ms": 0.0}


def main() -> int:
    log_paths = [LOG_DIR / "api-service.log", LOG_DIR / "worker-service.log"]
    starting_sizes = {path: path.stat().st_size if path.exists() else 0 for path in log_paths}
    command = [sys.executable, "-m", "testbed.run_all"]
    services = subprocess.Popen(command, cwd=Path(__file__).resolve().parents[1])
    event_ids: list[int] = []
    traffic_stop = threading.Event()
    traffic = threading.Thread(target=_send_traffic, args=(traffic_stop,), daemon=True)
    try:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                if httpx.get("http://127.0.0.1:8101/health", timeout=0.4).status_code < 500:
                    break
            except httpx.HTTPError:
                pass
            if services.poll() is not None:
                raise RuntimeError("testbed services exited during startup")
            time.sleep(0.25)
        else:
            raise RuntimeError("API service did not become ready")

        traffic.start()
        print("Baseline traffic: 20 seconds")
        time.sleep(20)
        for fault in FAULT_PLAN:
            before = {path: path.stat().st_size if path.exists() else 0 for path in log_paths}
            peaks = {"cpu_percent": 0.0, "memory_percent": 0.0, "request_latency_p95_ms": 0.0}
            metric_stop = threading.Event()

            def sample_metrics() -> None:
                while not metric_stop.is_set():
                    current = _metrics()
                    for metric, value in current.items():
                        peaks[metric] = max(peaks[metric], value)
                    metric_stop.wait(0.25)

            sampler = threading.Thread(target=sample_metrics, daemon=True)
            sampler.start()
            print(f"Injecting {fault} for 8 seconds")
            event_ids.append(run_fault(fault, "api", 8, announce=False))
            metric_stop.set()
            sampler.join(timeout=2)
            time.sleep(1.0)
            lines: list[str] = []
            for path in log_paths:
                if path.exists():
                    with path.open("r", encoding="utf-8") as file:
                        file.seek(before[path])
                        lines.extend(file.readlines())
            warnings = sum(" WARN " in line for line in lines)
            errors = sum(" ERROR " in line for line in lines)
            symptom_lines = [line for line in lines if " WARN " in line or " ERROR " in line]
            patterns = Counter(_message_pattern(line) for line in symptom_lines)
            print(
                f"{fault}: WARN={warnings} ERROR={errors} "
                f"top_patterns={patterns.most_common(3)} "
                f"peak_cpu={peaks['cpu_percent']:.1f}% "
                f"peak_memory={peaks['memory_percent']:.2f}% "
                f"peak_p95={peaks['request_latency_p95_ms']:.1f}ms"
            )

        traffic_stop.set()
        traffic.join(timeout=3)
        with SessionLocal() as session:
            recorded = list(session.scalars(select(FaultEvent).where(FaultEvent.id.in_(event_ids))))
        print(f"fault_events recorded by this smoke run: {len(recorded)} / 5")

        new_lines: list[str] = []
        for path in log_paths:
            if path.exists():
                with path.open("r", encoding="utf-8") as file:
                    file.seek(starting_sizes[path])
                    new_lines.extend(file.readlines())
        leaked = [line.strip() for line in new_lines if LABEL_PATTERN.search(line)]
        print(f"Fault labels in smoke log lines: {len(leaked)}")
        if leaked or len(recorded) != 5:
            if leaked:
                print("A forbidden fault label was found in a log line.")
            return 1
        return 0
    finally:
        traffic_stop.set()
        if traffic.is_alive():
            traffic.join(timeout=3)
        for port in (8101, 8102):
            try:
                httpx.post(f"http://127.0.0.1:{port}/_fault/stop", timeout=1)
                httpx.post(f"http://127.0.0.1:{port}/_internal/stop", timeout=1)
            except httpx.HTTPError:
                pass
        try:
            services.wait(timeout=12)
        except subprocess.TimeoutExpired:
            services.terminate()
            services.wait(timeout=5)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    sys.exit(main())