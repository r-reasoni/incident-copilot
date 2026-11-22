"""
eval/run_verification.py
========================
Automated verification script for Step 5.5:
1. Truncates DB tables (log_events, templates, window_scores, fault_events).
2. Deletes Drain3 state file, creates eval/runs/step5 directory.
3. Launches Ingestion API (port 8000) with RECORD_EVENTS_PATH set.
4. Launches Testbed services (API on 8101, worker on 8102).
5. Starts load generator (5 RPS) for steady-state baseline.
6. Waits 8 minutes (warmup + steady state), confirms 0 window flags.
7. Injects 5 faults in order (memory_leak, db_lock, cpu_hog, packet_loss, latency),
   60s duration each, with 120s recovery between each.
8. Dumps fault events via eval.dump_faults.
9. Queries DB window_scores and reports steady-state flags, out-of-interval flags,
   and per-fault detection metrics.
10. Runs offline replay via eval.offline_detect across seeds 42, 1, 2.
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

from app.db.models import FaultEvent, WindowScore
from app.db.session import SessionLocal
from testbed.fault_injector import run_fault

ROOT_DIR = Path(__file__).resolve().parents[1]
EVENTS_PATH = ROOT_DIR / "eval" / "runs" / "step5" / "events.jsonl"
FAULTS_PATH = ROOT_DIR / "eval" / "runs" / "step5" / "faults.json"
DRAIN_STATE_PATH = ROOT_DIR / "data" / "drain_state.bin"


def clean_state() -> None:
    print("--- 1. Cleaning DB tables and local state ---")
    with SessionLocal() as session, session.begin():
        session.execute(
            text("TRUNCATE TABLE log_events, templates, window_scores, fault_events RESTART IDENTITY CASCADE;")
        )
    print("Truncated log_events, templates, window_scores, fault_events.")

    if DRAIN_STATE_PATH.exists():
        DRAIN_STATE_PATH.unlink()
        print(f"Removed {DRAIN_STATE_PATH}.")

    EVENTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    if EVENTS_PATH.exists():
        EVENTS_PATH.unlink()
        print(f"Removed old {EVENTS_PATH}.")
    if FAULTS_PATH.exists():
        FAULTS_PATH.unlink()
        print(f"Removed old {FAULTS_PATH}.")


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

    print("--- 2. Starting Ingestion API and Testbed services ---")
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
        print("--- 3. Starting baseline traffic generator (5 RPS) ---")
        proc_loadgen = subprocess.Popen(
            [sys.executable, "-m", "testbed.loadgen", "--minutes", "26", "--rps", "5.0"],
            cwd=ROOT_DIR,
        )

        # 8 minutes warmup + steady state (480s)
        print("--- 4. Running 8 minutes of steady-state (warm-up + steady) ---")
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
        # 60s duration, 120s recovery
        faults_order = ["memory_leak", "db_lock", "cpu_hog", "packet_loss", "latency"]
        print("--- 5. Injecting 5 faults in order (60s duration, 120s recovery) ---")
        for idx, fault_name in enumerate(faults_order, 1):
            print(f"\n[{idx}/5] Injecting fault: {fault_name} (duration 60s)...")
            run_fault(fault_name, "api", duration=60.0, announce=True)
            print(f"[{idx}/5] Fault {fault_name} finished. Waiting 120s recovery...")
            for s in range(2):
                time.sleep(60)
                print(f"  Recovery elapsed {(s + 1) * 60}s / 120s...")

        print("\nAll 5 faults completed.")
        # Give 10s for final windows to be processed
        time.sleep(10)

    finally:
        print("\n--- 6. Stopping background processes ---")
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
    print("\n--- 7. Dumping faults to JSON ---")
    from eval.dump_faults import dump_faults
    dump_faults(FAULTS_PATH)

    # DB Results Reporting
    print("\n--- 8. Database Verification Report ---")
    with SessionLocal() as session:
        all_windows = session.scalars(select(WindowScore).order_by(WindowScore.window_start)).all()
        fault_rows = session.scalars(select(FaultEvent).order_by(FaultEvent.started_at)).all()

    # Total steady state flags
    # Faults began after steady_end
    steady_windows = [w for w in all_windows if w.window_end <= steady_end]
    steady_flag_count = sum(1 for w in steady_windows if w.is_anomalous)
    print(f"Flags in steady state: {steady_flag_count} / {len(steady_windows)}")

    # Flags outside fault intervals +/-30s
    out_of_interval_flags: list[WindowScore] = []
    for w in all_windows:
        if not w.is_anomalous:
            continue
        in_fault = any(
            w.window_end > (f.started_at - timedelta(seconds=30))
            and w.window_start < ((f.ended_at or f.started_at + timedelta(seconds=60)) + timedelta(seconds=30))
            for f in fault_rows
        )
        if not in_fault:
            out_of_interval_flags.append(w)

    print(f"Flags outside fault intervals (±30s): {len(out_of_interval_flags)}")
    for w in out_of_interval_flags:
        print(f"  [FP] {w.window_start:%H:%M:%S} - {w.window_end:%H:%M:%S} | score={w.score:.4f}")

    # Per-fault table from DB
    print("\n" + "=" * 80)
    print(f"{'Fault':<15} | {'Service':<16} | {'Flagged':<7} | {'TTD (conf - start)':<18} | {'Peak Score':<10}")
    print("-" * 80)
    for f in fault_rows:
        f_end = f.ended_at or f.started_at + timedelta(seconds=60)
        overlap = [
            w for w in all_windows
            if w.window_end > f.started_at and w.window_start < f_end + timedelta(seconds=30)
        ]
        peak = max((w.score for w in overlap), default=0.0)
        flagged = any(w.is_anomalous for w in overlap)

        # Look for confirmed_at in features JSONB
        ttd_str = "N/A"
        for w in overlap:
            conf_str = w.features.get("_confirmed_at")
            if conf_str:
                conf_dt = datetime.fromisoformat(conf_str).astimezone(timezone.utc)
                if f.started_at <= conf_dt <= f_end + timedelta(seconds=30):
                    flagged = True
                    ttd_sec = (conf_dt - f.started_at).total_seconds()
                    ttd_str = f"+{ttd_sec:.1f}s"
                    break

        print(
            f"{f.fault_type:<15} | {f.target_service:<16} | {'YES' if flagged else 'NO':<7} | {ttd_str:<18} | {peak:.4f}"
        )

    # 10. Replay offline with seeds 42, 1, 2
    print("\n--- 9. Offline Replay Across Seeds (42, 1, 2) ---")
    from eval.offline_detect import run_offline_detect
    for seed in [42, 1, 2]:
        print(f"\n>>> OFFLINE REPLAY WITH SEED {seed} <<<")
        run_offline_detect(EVENTS_PATH, faults_path=FAULTS_PATH, hst_seed=seed)


if __name__ == "__main__":
    main()
