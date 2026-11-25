"""eval/run_step7.py
===================
Automated verification script for Step 7:
1. Truncates DB tables (log_events, templates, window_scores, incidents, metrics_samples, fault_events).
2. Deletes Drain3 state file, creates eval/runs/step7 directory.
3. Launches Ingestion API (port 8000) with RECORD_EVENTS_PATH, RECORD_METRICS_PATH, LLM_PROVIDER=gemini.
4. Launches Testbed services (API on 8101, worker on 8102).
5. Starts load generator (5 RPS) for steady-state baseline.
6. Runs 8 minutes steady-state (warmup + steady state), confirms 0 window flags.
7. Injects 5 faults in order (memory_leak, db_lock, cpu_hog, packet_loss, latency),
   60s duration each, with 120s recovery between each.
8. Stops loadgen, flushes pipeline via POST /flush, waits for background agent analysis.
9. Dumps fault events to eval/runs/step7/faults.json.
10. Formats and prints detailed table of incidents and matching faults.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from sqlalchemy import select, text

from app.db.models import FaultEvent, Incident, WindowScore
from app.db.session import SessionLocal
from eval.dump_faults import dump_faults
from eval.replay import print_incident_table
from testbed.fault_injector import run_fault

ROOT_DIR = Path(__file__).resolve().parents[1]
RUN_NAME = os.getenv("STEP7_RUN_NAME", "step7b")
RUN_DIR = ROOT_DIR / "eval" / "runs" / RUN_NAME
EVENTS_PATH = RUN_DIR / "events.jsonl"
METRICS_PATH = RUN_DIR / "metrics.jsonl"
WINDOWS_PATH = RUN_DIR / "windows.jsonl"
FAULTS_PATH = RUN_DIR / "faults.json"
DRAIN_STATE_PATH = ROOT_DIR / "data" / "drain_state.bin"


def clean_state() -> None:
    print("--- 1. Cleaning DB tables and local state ---")
    with SessionLocal() as session, session.begin():
        session.execute(
            text(
                "TRUNCATE TABLE log_events, templates, window_scores, incidents, "
                "metrics_samples, fault_events RESTART IDENTITY CASCADE;"
            )
        )
    print("Truncated log_events, templates, window_scores, incidents, metrics_samples, fault_events.")

    if DRAIN_STATE_PATH.exists():
        DRAIN_STATE_PATH.unlink()
        print(f"Removed {DRAIN_STATE_PATH}.")

    RUN_DIR.mkdir(parents=True, exist_ok=True)
    for p in (EVENTS_PATH, METRICS_PATH, WINDOWS_PATH, FAULTS_PATH):
        if p.exists():
            p.unlink()
            print(f"Removed old {p}.")


def wait_healthy(url: str, timeout: float = 20.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(url, timeout=1.0)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def main() -> None:
    clean_state()

    env_api = os.environ.copy()
    env_api["RECORD_EVENTS_PATH"] = str(EVENTS_PATH)
    env_api["RECORD_METRICS_PATH"] = str(METRICS_PATH)
    env_api["RECORD_WINDOWS_PATH"] = str(WINDOWS_PATH)
    env_api["LLM_PROVIDER"] = "gemini"

    print("\n--- 2. Starting Ingestion API and Testbed services ---")
    proc_api = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.api.main:app", "--host", "127.0.0.1", "--port", "8000"],
        cwd=ROOT_DIR,
        env=env_api,
    )
    proc_testbed = subprocess.Popen(
        [sys.executable, "-m", "testbed.run_all"],
        cwd=ROOT_DIR,
    )

    proc_loadgen = None
    try:
        if not wait_healthy("http://127.0.0.1:8000/health"):
            raise RuntimeError("Ingestion API failed to become healthy on port 8000")
        if not wait_healthy("http://127.0.0.1:8101/health"):
            raise RuntimeError("Testbed API failed to become healthy on port 8101")
        if not wait_healthy("http://127.0.0.1:8102/health"):
            raise RuntimeError("Testbed Worker failed to become healthy on port 8102")
        print("All services are healthy.")

        # Start loadgen for 26 minutes (5.0 RPS)
        print("\n--- 3. Starting baseline traffic generator (5 RPS) ---")
        proc_loadgen = subprocess.Popen(
            [sys.executable, "-m", "testbed.loadgen", "--minutes", "26", "--rps", "5.0"],
            cwd=ROOT_DIR,
        )

        # 8 minutes warmup + steady state (480s)
        print("\n--- 4. Running 8 minutes of steady-state (warm-up + steady) ---")
        steady_start = datetime.now(timezone.utc)
        for minute in range(1, 9):
            time.sleep(60)
            print(f"  Steady-state minute {minute}/8 elapsed...")

        # Query database window scores in steady state
        steady_end = datetime.now(timezone.utc)
        with SessionLocal() as session:
            steady_scores = session.scalars(
                select(WindowScore).where(
                    WindowScore.window_end >= steady_start,
                    WindowScore.window_end <= steady_end,
                )
            ).all()

        steady_total = len(steady_scores)
        steady_flags = sum(1 for w in steady_scores if w.is_anomalous)
        print(f"Steady-state check: {steady_flags} flags out of {steady_total} windows.")
        if steady_flags > 0:
            print(f"WARNING: Expected 0 flags in steady state, found {steady_flags}!")
        else:
            print("Confirmed zero flags in steady state.")

        # Injects 5 faults in order (memory_leak, db_lock, cpu_hog, packet_loss, latency)
        # 60s duration, 120s recovery between faults, 150s after the last fault
        faults_order = ["memory_leak", "db_lock", "cpu_hog", "packet_loss", "latency"]
        print("\n--- 5. Injecting 5 faults in order (60s duration, 120s recovery between faults) ---")
        for idx, fault_name in enumerate(faults_order, 1):
            print(f"\n[{idx}/5] Injecting fault: {fault_name} (duration 60s)...")
            run_fault(fault_name, "api", duration=60.0, announce=True)
            if idx < len(faults_order):
                print(f"[{idx}/5] Fault {fault_name} finished. Waiting 120s recovery...")
                for s in range(2):
                    time.sleep(60)
                    print(f"  Recovery elapsed {(s + 1) * 60}s / 120s...")
            else:
                print(f"[{idx}/5] Last fault {fault_name} finished. Keeping loadgen active for 150s observation...")
                for s in range(2):
                    time.sleep(60)
                    print(f"  Observation elapsed {(s + 1) * 60}s / 150s...")
                time.sleep(30)
                print("  Observation elapsed 150s / 150s.")

        print("\nAll 5 faults and observation window completed.")

        # Stop loadgen so loadgen and recording end at the exact same moment
        print("Stopping loadgen concurrently with end of recording window...")
        if proc_loadgen and proc_loadgen.poll() is None:
            proc_loadgen.terminate()
            try:
                proc_loadgen.wait(timeout=5)
            except Exception:
                pass
            proc_loadgen = None

        # Flush pipeline and wait for background agents
        print("\n--- 6. Flushing pipeline and waiting for agent analysis ---")
        try:
            r = httpx.post("http://127.0.0.1:8000/flush", timeout=10.0)
            print(f"Flush response: {r.status_code} {r.json()}")
        except Exception as e:
            print(f"Flush call error: {e}")

        # Wait for all background agents to complete before taking down the API
        print("Waiting for all pending agent analyses to finish (via /agents/wait)...")
        try:
            r = httpx.post("http://127.0.0.1:8000/agents/wait?timeout=60", timeout=70.0)
            print(f"Agents wait response: {r.status_code} {r.json()}")
        except Exception as e:
            print(f"Agents wait call error: {e}")

    finally:
        print("\n--- 7. Stopping background processes ---")
        if proc_loadgen and proc_loadgen.poll() is None:
            proc_loadgen.terminate()
            proc_loadgen.wait(timeout=5)
        for port in (8101, 8102):
            try:
                httpx.post(f"http://127.0.0.1:{port}/_internal/stop", timeout=1.0)
            except Exception:
                pass
        if proc_testbed and proc_testbed.poll() is None:
            proc_testbed.terminate()
            proc_testbed.wait(timeout=5)
        if proc_api and proc_api.poll() is None:
            proc_api.terminate()
            proc_api.wait(timeout=5)
        print("Background processes stopped.")

    # Dump faults to JSON
    print("\n--- 8. Dumping faults to JSON ---")
    dump_faults(FAULTS_PATH)

    # Print incident summary table
    print("\n--- 9. Incident Summary & Root-Cause Agent Results (Gemini) ---")
    with SessionLocal() as session:
        incidents = session.scalars(select(Incident).order_by(Incident.started_at)).all()
        fault_records = session.scalars(select(FaultEvent).order_by(FaultEvent.started_at)).all()
        faults_data = [
            {
                "id": r.id,
                "fault_type": r.fault_type,
                "target_service": r.target_service,
                "started_at": r.started_at.isoformat(),
                "ended_at": r.ended_at.isoformat() if r.ended_at else None,
            }
            for r in fault_records
        ]

    print_incident_table(incidents, faults_data)


if __name__ == "__main__":
    main()
