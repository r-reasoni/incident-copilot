"""
app/agent/reanalyze.py
======================
CLI tool to re-run the root-cause agent on closed incidents.
Allows re-analyzing incidents after a provider outage without repeating full runs.
"""
from __future__ import annotations

import argparse
import logging
import sys

from app.agent.agent import run_agent
from app.db.repository import get_incident, list_incidents
from app.db.session import get_session

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("reanalyze")


def reanalyze_incident(incident_id: int | str, force: bool = True) -> bool:
    """Re-run the agent on a single closed incident."""
    inc_id_int = int(incident_id)
    with get_session() as session:
        incident = get_incident(session, inc_id_int)
        if incident is None:
            logger.error("Incident #%s not found", incident_id)
            return False
        if incident.status != "closed":
            logger.warning("Incident #%s is not closed (status=%s), skipping", incident_id, incident.status)
            return False

    logger.info("Re-running agent for incident #%s...", incident_id)
    try:
        run_agent(str(incident_id), force=force)
    except Exception as exc:
        logger.exception("Failed to re-run agent for incident #%s: %s", incident_id, exc)
        return False

    with get_session() as session:
        updated = get_incident(session, inc_id_int)
        cat = updated.category if updated else None
        conf = updated.confidence if updated else None
        trace = updated.agent_trace if updated else {}
        status = trace.get("agent_status", "unknown") if isinstance(trace, dict) else "unknown"
        logger.info(
            "Incident #%s reanalyzed: status=%s, category=%s, confidence=%s",
            incident_id,
            status,
            cat,
            conf,
        )
    return True


def reanalyze_all_closed(force: bool = True) -> int:
    """Re-run the agent on all closed incidents."""
    with get_session() as session:
        closed = list_incidents(session, status="closed")

    # Sort ascending by ID
    closed_sorted = sorted(closed, key=lambda inc: inc.id)
    if not closed_sorted:
        logger.info("No closed incidents found.")
        return 0

    logger.info("Found %d closed incident(s) to reanalyze.", len(closed_sorted))
    success_count = 0
    for inc in closed_sorted:
        if reanalyze_incident(inc.id, force=force):
            success_count += 1
    logger.info("Reanalysis complete: %d/%d succeeded.", success_count, len(closed_sorted))
    return success_count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Re-run root-cause agent on closed incidents.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--incident", type=int, help="Incident ID to reanalyze")
    group.add_argument("--all-closed", action="store_true", help="Reanalyze all closed incidents")
    parser.add_argument("--no-force", action="store_true", help="Do not overwrite existing root-cause if already present")

    args = parser.parse_args(argv)
    force = not args.no_force

    if args.incident is not None:
        ok = reanalyze_incident(args.incident, force=force)
        return 0 if ok else 1
    elif args.all_closed:
        reanalyze_all_closed(force=force)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
