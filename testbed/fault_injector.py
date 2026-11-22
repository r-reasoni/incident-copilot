"""Apply local faults and record evaluation-only ground truth."""
from __future__ import annotations

import time
from datetime import datetime, timezone

import httpx
import typer
from sqlalchemy import text

from app.db.repository import insert_fault_event
from app.db.session import SessionLocal

app = typer.Typer(help="Inject a controlled fault into a local testbed process.")
SERVICES = {"api": "http://127.0.0.1:8101", "worker": "http://127.0.0.1:8102"}
FAULTS = {"cpu_hog", "memory_leak", "latency", "packet_loss", "db_lock"}


def run_fault(
    fault_type: str,
    service: str,
    duration: float,
    params: dict | None = None,
    *,
    announce: bool = True,
) -> int:
    if fault_type not in FAULTS:
        raise ValueError("unsupported fault type")
    if service not in SERVICES:
        raise ValueError("service must be api or worker")
    if duration <= 0:
        raise ValueError("duration must be positive")

    endpoint = SERVICES[service]
    started_at = datetime.now(timezone.utc)
    with SessionLocal() as session:
        with session.begin():
            record = insert_fault_event(
                session,
                fault_type=fault_type,
                target_service=service,
                started_at=started_at,
                ended_at=None,
            )
            event_id = record.id

    started = False
    try:
        with httpx.Client(timeout=5.0) as client:
            response = client.post(
                f"{endpoint}/_fault/start",
                json={"type": fault_type, "params": params or {}},
            )
            response.raise_for_status()
            started = True
            if announce:
                typer.echo(f"Started {fault_type} on {service}; duration {duration:g}s")
            deadline = time.monotonic() + duration
            while time.monotonic() < deadline:
                time.sleep(min(0.2, deadline - time.monotonic()))
    finally:
        try:
            with httpx.Client(timeout=5.0) as client:
                client.post(f"{endpoint}/_fault/stop")
        except httpx.HTTPError:
            pass
        ended_at = datetime.now(timezone.utc)
        with SessionLocal() as session, session.begin():
            session.execute(
                text("UPDATE fault_events SET ended_at = :ended_at WHERE id = :event_id"),
                {"ended_at": ended_at, "event_id": event_id},
            )
        if announce:
            typer.echo("Fault stopped; cleanup completed.")
    return event_id


@app.command()
def inject(
    fault_type: str = typer.Argument(...),
    service: str = typer.Option("api", help="Target service: api or worker"),
    duration: float = typer.Option(120.0, min=0.1),
) -> None:
    """Inject a fault and always stop it, including on Ctrl+C."""
    try:
        run_fault(fault_type, service, duration)
    except KeyboardInterrupt:
        typer.echo("Interrupted; cleanup was requested.")
        raise typer.Exit(130)


@app.command()
def stop(
    service: str = typer.Option("api", help="Target service: api or worker"),
) -> None:
    """Stop any active fault on the target service."""
    endpoint = SERVICES.get(service)
    if not endpoint:
        raise ValueError("service must be api or worker")
    try:
        with httpx.Client(timeout=5.0) as client:
            client.post(f"{endpoint}/_fault/stop")
        typer.echo(f"Sent stop signal to {service}")
    except httpx.HTTPError as exc:
        typer.echo(f"Failed to contact {service}: {exc}")


if __name__ == "__main__":
    app()