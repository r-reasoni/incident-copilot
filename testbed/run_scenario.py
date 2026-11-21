"""Run seeded baseline/fault/recovery scenarios and save their manifest."""
from __future__ import annotations

import json
import random
import threading
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import typer

from testbed.fault_injector import FAULTS, run_fault

app = typer.Typer(help="Run a reproducible local baseline/fault/recovery scenario.")
MANIFEST_PATH = Path(__file__).resolve().parents[1] / "eval" / "scenarios.json"


def _traffic_loop(stop_event: threading.Event) -> None:
    index = 0
    with httpx.Client(timeout=2.5) as client:
        while not stop_event.is_set():
            index += 1
            try:
                if index % 2:
                    client.get(f"http://127.0.0.1:8101/users/{(index % 3) + 1}")
                else:
                    client.post(
                        "http://127.0.0.1:8101/orders",
                        json={"user_id": (index % 3) + 1, "amount": 26.5},
                    )
            except httpx.HTTPError:
                pass
            stop_event.wait(0.2)


def run_scenario(
    faults: list[str], repeats: int, seed: int, baseline_seconds: float,
    fault_seconds: float, recovery_seconds: float,
) -> dict:
    rng = random.Random(seed)
    sequence = faults * repeats
    rng.shuffle(sequence)
    scenario_id = str(uuid.uuid4())
    planned_start = datetime.now(timezone.utc) + timedelta(seconds=baseline_seconds)
    entries = []
    for offset, fault in enumerate(sequence):
        start = planned_start + timedelta(seconds=offset * (fault_seconds + recovery_seconds))
        service = "api" if offset % 2 == 0 else "worker"
        entries.append({
            "scenario_id": scenario_id,
            "fault_type": fault,
            "target_service": service,
            "planned_start": start.isoformat(),
            "planned_end": (start + timedelta(seconds=fault_seconds)).isoformat(),
        })

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    if MANIFEST_PATH.exists():
        existing = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        if not isinstance(existing, list):
            existing = []
    else:
        existing = []
    existing.extend(entries)
    MANIFEST_PATH.write_text(json.dumps(existing, indent=2) + "\n", encoding="utf-8")

    stop_event = threading.Event()
    traffic = threading.Thread(target=_traffic_loop, args=(stop_event,), daemon=True)
    traffic.start()
    try:
        time.sleep(baseline_seconds)
        for item in entries:
            run_fault(item["fault_type"], item["target_service"], fault_seconds, announce=False)
            time.sleep(recovery_seconds)
    finally:
        stop_event.set()
        traffic.join(timeout=3)
    return {"scenario_id": scenario_id, "events": entries}


@app.command()
def main(
    faults_option: str = typer.Option("all", "--faults"),
    repeats: int = typer.Option(1, min=1),
    seed: int = typer.Option(7),
    baseline_seconds: float = typer.Option(180.0, min=0),
    fault_seconds: float = typer.Option(120.0, min=0.1),
    recovery_seconds: float = typer.Option(120.0, min=0),
) -> None:
    """Run baseline, shuffled fault periods, and recovery intervals."""
    selected = sorted(FAULTS) if faults_option == "all" else [
        value.strip() for value in faults_option.split(",") if value.strip()
    ]
    unknown = set(selected) - FAULTS
    if unknown:
        raise typer.BadParameter("unknown fault type")
    result = run_scenario(
        selected, repeats, seed, baseline_seconds, fault_seconds, recovery_seconds
    )
    typer.echo(f"Scenario {result['scenario_id']} recorded in {MANIFEST_PATH}")


if __name__ == "__main__":
    app()