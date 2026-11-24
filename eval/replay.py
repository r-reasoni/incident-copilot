"""
eval/replay.py
==============
Replay recorded log events in event time through the shared ingest_batch pipeline.
Usage:
    python -m eval.replay <events.jsonl> [--faults faults.json] --yes
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from app.config import settings
from app.db.models import Incident
from app.db.session import SessionLocal
from app.detection.windowing import _parse_event_ts
from app.incidents.manager import IncidentManager
from app.ingest.batch import ingest_batch
from app.parsing.parser import LogParser
from app.pipeline import Pipeline


def truncate_replay_tables() -> None:
    """Truncate log_events, templates, window_scores, incidents, and metrics_samples."""
    with SessionLocal() as session, session.begin():
        session.execute(
            text("TRUNCATE TABLE log_events, templates, window_scores, incidents, metrics_samples RESTART IDENTITY CASCADE;")
        )


def reset_drain_state() -> None:
    """Delete the Drain3 state file if present."""
    drain_path = Path(settings.drain_state_path)
    if drain_path.exists():
        drain_path.unlink()
        print(f"Removed Drain state at {drain_path}")


def load_metrics_samples(metrics_path: Path) -> int:
    """Load pre-recorded metrics samples into metrics_samples table."""
    from app.db.repository import insert_metrics_sample
    count = 0
    with metrics_path.open("r", encoding="utf-8") as f, SessionLocal() as session, session.begin():
        for line in f:
            line = line.strip()
            if not line:
                continue
            data = json.loads(line)
            if "ts" in data and isinstance(data["ts"], str):
                data["ts"] = _parse_event_ts(data["ts"])
            insert_metrics_sample(session, **data)
            count += 1
    print(f"Loaded {count} metrics samples from {metrics_path}.")
    return count


def replay(
    events_path: Path,
    faults_path: Path | None = None,
    metrics_path: Path | None = None,
    batch_size: int = 100,
    truncate_after_faults: int = 150,
) -> list[Any]:
    """Execute event-time replay of recorded logs through ingest_batch."""
    truncate_replay_tables()
    reset_drain_state()

    if metrics_path and metrics_path.exists():
        load_metrics_samples(metrics_path)

    # Pre-parse faults if provided to determine truncation cutoff
    faults: list[dict[str, Any]] = []
    cutoff_ts: datetime | None = None
    if faults_path and faults_path.exists():
        with faults_path.open("r", encoding="utf-8") as f:
            faults = json.load(f)
        if faults:
            max_fault_end = max(
                _parse_event_ts(flt["ended_at"])
                if flt.get("ended_at")
                else _parse_event_ts(flt["started_at"]) + timedelta(seconds=60)
                for flt in faults
            )
            cutoff_ts = max_fault_end + timedelta(seconds=truncate_after_faults)
            print(f"Faults loaded ({len(faults)}). Evaluation cutoff at {cutoff_ts.isoformat()} (+{truncate_after_faults}s).")

    parser = LogParser()
    incident_manager = IncidentManager()
    pipeline = Pipeline(parser=parser, incident_manager=incident_manager)

    print(f"Replaying events from {events_path} (batch_size={batch_size})...")

    batch: list[dict[str, Any]] = []
    last_tick_ts: datetime | None = None
    total_events = 0
    dropped_events = 0

    with events_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event = json.loads(line)
            ts = _parse_event_ts(event["ts"])

            if cutoff_ts is not None and ts > cutoff_ts:
                dropped_events += 1
                continue

            total_events += 1

            if last_tick_ts is None or (ts - last_tick_ts).total_seconds() >= 1.0:
                pipeline.tick(ts)
                last_tick_ts = ts

            batch.append(event)
            if len(batch) >= batch_size:
                ingest_batch(batch, parser=parser, pipeline=pipeline)
                batch = []

    if batch:
        ingest_batch(batch, parser=parser, pipeline=pipeline)

    if dropped_events > 0:
        print(f"Truncated after faults: dropped {dropped_events} trailing events after {cutoff_ts.isoformat()}.")
        print("Note: Trailing silence after an experiment is not part of the evaluation.")

    print(f"Replayed {total_events} events. Flushing pipeline...")
    pipeline.flush()

    incidents = incident_manager.list_incidents()
    print(f"\nReplay complete. Found {len(incidents)} incidents.")

    from app.agent.agent import run_agent
    from app.db import repository
    print(f"Executing root-cause agent on {len(incidents)} incidents sequentially...")
    for inc in incidents:
        try:
            run_agent(inc.id)
        except Exception as exc:
            print(f"Agent analysis failed for incident {inc.id}: {exc}")

    with SessionLocal() as session:
        db_incidents = repository.list_incidents(session)
    if db_incidents:
        incidents = db_incidents

    print_incident_table(incidents, faults)
    return incidents


def print_incident_table(incidents: list[Any], faults: list[dict[str, Any]]) -> None:
    """Format and print detailed table of incidents and matching faults."""
    parsed_faults: list[dict[str, Any]] = []
    for f in faults:
        f_start = _parse_event_ts(f["started_at"])
        f_end = _parse_event_ts(f["ended_at"]) if f.get("ended_at") else f_start + timedelta(seconds=60)
        parsed_faults.append({
            "id": f.get("id"),
            "fault_type": f.get("fault_type"),
            "target_service": f.get("target_service"),
            "started_at": f_start,
            "ended_at": f_end,
        })

    matched_fault_ids: set[int] = set()
    unmatched_incidents: list[int] = []

    print("\n" + "=" * 120)
    print(f"{'INCIDENT SUMMARY TABLE':^120}")
    print("=" * 120)

    for inc in incidents:
        inc_id = inc.id
        started = inc.started_at
        detected = inc.detected_at
        ended = inc.ended_at or inc.last_anomalous_end
        peak_score = inc.peak_score
        affected = inc.affected_services
        top_tpls = getattr(inc, "top_templates", [])
        if isinstance(top_tpls, dict):
            top_tpls = top_tpls.get("top_templates", top_tpls.get("templates", []))
        if not isinstance(top_tpls, list):
            top_tpls = []

        # Match against faults (overlap with fault interval +/- 20s)
        matching_fault = None
        delay_s = None
        for pf in parsed_faults:
            window_start_padded = pf["started_at"] - timedelta(seconds=20)
            window_end_padded = pf["ended_at"] + timedelta(seconds=20)
            if max(started, window_start_padded) <= min(ended, window_end_padded):
                matching_fault = pf
                matched_fault_ids.add(pf["id"])
                if detected:
                    delay_s = round((detected - pf["started_at"]).total_seconds(), 1)
                break

        if matching_fault is None:
            unmatched_incidents.append(inc_id)

        print(f"\n[Incident #{inc_id}] Status: {inc.status} | Peak Score: {peak_score:.4f}")
        print(f"  Time Window : Started:  {started.isoformat()}")
        print(f"                Detected: {detected.isoformat() if detected else 'N/A'}")
        print(f"                Ended:    {ended.isoformat() if ended else 'N/A'}")
        print(f"  Affected Svc: {affected}")

        trace = getattr(inc, "agent_trace", {}) or {}
        tool_calls = [tc.get("tool", tc.get("name")) for tc in trace.get("tool_calls", [])]
        print(f"  Agent Diag  : Category:   {inc.category}")
        print(f"                Confidence: {inc.confidence}")
        print(f"                LLM Calls:  {inc.llm_calls}")
        print(f"                Tools Used: {', '.join(tool_calls) if tool_calls else 'None'}")
        print(f"                Root Cause: {inc.root_cause}")
        if "error" in trace:
            print(f"                Error/RateLimit: {trace['error']}")

        fault_to_cat = {
            "cpu_hog": "cpu",
            "memory_leak": "memory",
            "db_lock": "db_lock",
            "packet_loss": "packet_loss",
            "latency": "network_latency",
        }
        if matching_fault:
            exp_cat = fault_to_cat.get(matching_fault["fault_type"])
            matches = (inc.category == exp_cat)
            print(f"  Matching Flt: {matching_fault['fault_type']} (expected: {exp_cat}) | Category Match: {matches}")
        else:
            print("  Matching Flt: NONE (Unmatched incident)")

        print("  Top Templates (up to 3):")
        for i, t in enumerate(top_tpls[:3], start=1):
            t_text = str(t.get("text", ""))[:65]
            ratio = t.get("ratio", 0.0)
            is_new = t.get("is_new", False)
            print(f"    {i}. [ratio={ratio}x, is_new={is_new}] {t_text}")

    print("\n" + "-" * 120)
    print(f"Summary: {len(incidents)} incidents created versus {len(parsed_faults)} injected faults.")
    if len(matched_fault_ids) == len(parsed_faults) and not unmatched_incidents:
        print("Result: PERFECT MATCH - All faults detected with 0 false positive incidents!")
    else:
        unmatched_faults = [pf["fault_type"] for pf in parsed_faults if pf["id"] not in matched_fault_ids]
        if unmatched_faults:
            print(f"Missed Faults: {unmatched_faults}")
        if unmatched_incidents:
            print(f"Unmatched Incidents: {unmatched_incidents}")
    print("=" * 120 + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay recorded events through shared ingest_batch.")
    parser.add_argument("events", type=Path, help="Path to events.jsonl")
    parser.add_argument("--faults", type=Path, default=None, help="Optional path to faults.json")
    parser.add_argument("--metrics", type=Path, default=None, help="Optional path to metrics.jsonl")
    parser.add_argument("--yes", action="store_true", help="Confirm table truncation")
    parser.add_argument("--batch-size", type=int, default=100, help="Batch size for ingestion")
    parser.add_argument(
        "--truncate-after-faults",
        type=int,
        default=150,
        help="Stop feeding events SECONDS after last fault ended (default: 150)",
    )

    args = parser.parse_args()
    if not args.yes:
        print("ERROR: Pass --yes to confirm truncating log_events, templates, window_scores, and incidents.", file=sys.stderr)
        sys.exit(1)

    if not args.events.exists():
        print(f"ERROR: Events file not found at {args.events}", file=sys.stderr)
        sys.exit(1)

    replay(
        args.events,
        faults_path=args.faults,
        metrics_path=args.metrics,
        batch_size=args.batch_size,
        truncate_after_faults=args.truncate_after_faults,
    )


if __name__ == "__main__":
    main()
