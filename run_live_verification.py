"""Live testbed verification script for Step 7 with Gemini LLM."""
from __future__ import annotations

import json
import logging
import os
import random
import subprocess
import threading
import time
from datetime import datetime, timezone

import httpx

from app.agent.agent import run_agent
from app.db.models import Incident, LogEvent, Template
from app.db.repository import get_incident
from app.db.session import SessionLocal
from testbed.fault_injector import run_fault

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("verification")


def _run_traffic(stop_event: threading.Event) -> None:
    users = [1, 2, 3]
    with httpx.Client(timeout=2.0) as client:
        while not stop_event.is_set():
            u = random.choice(users)
            try:
                if random.random() < 0.6:
                    client.get(f"http://127.0.0.1:8101/users/{u}")
                else:
                    client.post(
                        "http://127.0.0.1:8101/orders",
                        json={"user_id": u, "amount": round(random.uniform(5.0, 150.0), 2)},
                    )
            except Exception:
                pass
            stop_event.wait(0.2)


def main():
    os.environ["LLM_PROVIDER"] = "gemini"
    from app.config import get_settings
    get_settings.cache_clear()

    logger.info("Starting local testbed services and Ingestion API...")
    sub_env = dict(os.environ, LLM_PROVIDER="gemini")
    api_proc = subprocess.Popen([".venv/bin/uvicorn", "testbed.api_service:app", "--port", "8101", "--log-level", "warning"], env=sub_env)
    worker_proc = subprocess.Popen([".venv/bin/uvicorn", "testbed.worker_service:app", "--port", "8102", "--log-level", "warning"], env=sub_env)
    main_proc = subprocess.Popen([".venv/bin/uvicorn", "app.api.main:app", "--port", "8000", "--log-level", "warning"], env=sub_env)

    time.sleep(4)
    # Check health
    with httpx.Client() as client:
        for port in (8101, 8102, 8000):
            r = client.get(f"http://127.0.0.1:{port}/health", timeout=3.0)
            logger.info("Port %s health check: %s", port, r.status_code)

    stop_traffic = threading.Event()
    traffic_thread = threading.Thread(target=_run_traffic, args=(stop_traffic,), daemon=True)
    traffic_thread.start()
    logger.info("Traffic generation started.")

    logger.info("Collecting initial 30s baseline metrics...")
    time.sleep(30)

    faults_part_b = ["cpu_hog", "memory_leak", "db_lock"]
    faults_part_c = ["latency", "packet_loss"]
    all_results = []

    def run_fault_cycle(fault_name: str, group_label: str):
        logger.info("=== Starting [%s] fault: %s ===", group_label, fault_name)
        fault_start = datetime.now(timezone.utc)
        
        # Inject fault for 90s
        logger.info("Injecting %s on 'api' for 90s...", fault_name)
        run_fault(fault_name, "api", 90.0, announce=False)
        fault_end = datetime.now(timezone.utc)
        logger.info("Completed %s injection. Entering 2-minute (120s) recovery period...", fault_name)
        time.sleep(120.0)
        logger.info("Recovery period complete for %s.", fault_name)

        # Create closed incident record covering the fault duration
        with SessionLocal() as session, session.begin():
            # Find top templates from log events in this window
            logs = session.query(LogEvent.template_id).filter(
                LogEvent.ts >= fault_start, LogEvent.ts <= fault_end, LogEvent.template_id.isnot(None)
            ).all()
            from collections import Counter
            counts = Counter(l[0] for l in logs)
            top_tpls = [tpl_id for tpl_id, _ in counts.most_common(5)]
            
            incident = Incident(
                started_at=fault_start,
                ended_at=fault_end,
                status="closed",
                peak_score=4.5,
                affected_services=["api-service"],
                top_templates=top_tpls or [1],
            )
            session.add(incident)
            session.flush()
            incident_id = incident.id

        logger.info("Created closed incident ID %s for %s. Running investigation agent with Gemini...", incident_id, fault_name)
        agent_out = run_agent(incident_id)
        
        with SessionLocal() as session:
            inc_row = get_incident(session, incident_id)
            tool_calls = []
            rate_limit_error = None
            if inc_row and inc_row.agent_trace:
                trace = inc_row.agent_trace
                tool_calls = trace.get("tool_calls", [])
                if "error" in trace:
                    rate_limit_error = trace["error"]
            
            result_entry = {
                "fault": fault_name,
                "group": group_label,
                "incident_id": incident_id,
                "category": agent_out.get("category", "unknown"),
                "confidence": agent_out.get("confidence", 0.0),
                "root_cause": agent_out.get("root_cause", ""),
                "llm_calls": inc_row.llm_calls if inc_row else 0,
                "tool_calls": [tc.get("tool", tc.get("name")) for tc in tool_calls],
                "rate_limit_error": rate_limit_error,
            }
            all_results.append(result_entry)
            logger.info("Incident %s Result: category=%s, confidence=%.2f, llm_calls=%s, tool_calls=%s",
                        incident_id, result_entry["category"], result_entry["confidence"],
                        result_entry["llm_calls"], result_entry["tool_calls"])
            if rate_limit_error:
                logger.warning("Rate limit / provider error noted: %s", rate_limit_error)

    try:
        # Part B: cpu_hog, memory_leak, db_lock
        for f in faults_part_b:
            run_fault_cycle(f, "Part B")

        # Part C: latency, packet_loss
        for f in faults_part_c:
            run_fault_cycle(f, "Part C")

    finally:
        logger.info("Stopping traffic and shutting down services...")
        stop_traffic.set()
        traffic_thread.join(timeout=3)
        for p in (api_proc, worker_proc, main_proc):
            p.terminate()
            try:
                p.wait(timeout=3)
            except Exception:
                p.kill()

    print("\n" + "="*80)
    print("FINAL VERIFICATION SUMMARY (Step 7)")
    print("="*80)
    print(f"{'Group':<8} {'Fault':<14} {'Category':<16} {'Conf':<6} {'LLM Calls':<10} {'Tools Used'}")
    print("-" * 80)
    for r in all_results:
        tools_str = ", ".join(r["tool_calls"]) if r["tool_calls"] else "None"
        print(f"{r['group']:<8} {r['fault']:<14} {r['category']:<16} {r['confidence']:<6.2f} {r['llm_calls']:<10} {tools_str}")
        print(f"  Root Cause: {r['root_cause']}")
        if r.get('rate_limit_error'):
            print(f"  Provider Error / Rate Limit: {r['rate_limit_error']}")
        print()
    print("="*80)

if __name__ == "__main__":
    main()
