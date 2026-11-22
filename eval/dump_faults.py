"""
eval/dump_faults.py
===================
CLI tool to dump all fault_events from the database to a JSON file.
Usage:
    python -m eval.dump_faults <out.json>
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from sqlalchemy import select

from app.db.models import FaultEvent
from app.db.session import SessionLocal


def dump_faults(out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with SessionLocal() as session:
        records = session.scalars(select(FaultEvent).order_by(FaultEvent.started_at)).all()
        data = [
            {
                "id": r.id,
                "fault_type": r.fault_type,
                "target_service": r.target_service,
                "started_at": r.started_at.isoformat(),
                "ended_at": r.ended_at.isoformat() if r.ended_at else None,
            }
            for r in records
        ]

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Dumped {len(data)} fault records to {out_path}")


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: python -m eval.dump_faults <out.json>", file=sys.stderr)
        sys.exit(1)
    dump_faults(Path(sys.argv[1]))


if __name__ == "__main__":
    main()
