"""
cli/main.py
===========
Typer + Rich command-line interface for Incident Copilot.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import httpx
import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from sqlalchemy import func, select, text

from app.config import get_settings, settings

app = typer.Typer(
    name="copilot",
    help="Incident Copilot: cheap log anomaly detection with LLM root-cause analysis.",
    add_completion=False,
    no_args_is_help=True,
)
console = Console()

VALID_INCIDENT_STATUSES = {"open", "analyzed", "closed"}
LOG_LINE_PATTERN = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}[,\.]\d{3})\s+"
    r"\[(?P<service>[^]]+)\]\s+(?P<level>[A-Z]+)\s+(?P<message>.*)$"
)


def _parse_plain_log_line(line: str, default_host: str) -> dict[str, Any] | None:
    """Parse one plain-text log line into an event dict."""
    match = LOG_LINE_PATTERN.match(line.rstrip("\r\n"))
    if match is None:
        return None
    message = match.group("message").strip()
    if not message:
        return None
    ts_raw = match.group("ts").replace(".", ",")
    dt = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S,%f").replace(tzinfo=UTC)
    return {
        "ts": dt,
        "service": match.group("service").strip(),
        "host": default_host,
        "level": match.group("level").upper(),
        "message": message,
    }


def _format_duration(started_at: datetime | None, ended_at: datetime | None) -> str:
    if started_at is None:
        return "-"
    if ended_at is None:
        return "ongoing"
    secs = max(0.0, (ended_at - started_at).total_seconds())
    return f"{secs:.0f}s"


def compute_llm_savings(log_lines: int, windows: int, llm_calls: int) -> tuple[float, float]:
    """Return (% LLM calls saved vs 1 call/log line, % saved vs 1 call/window)."""
    saved_vs_logs = ((log_lines - llm_calls) / log_lines * 100.0) if log_lines > 0 else 0.0
    saved_vs_windows = ((windows - llm_calls) / windows * 100.0) if windows > 0 else 0.0
    return saved_vs_logs, saved_vs_windows


def render_incidents_table(incidents: list[Any], out_console: Console | None = None) -> Table:
    """Build and optionally print a Rich table of incidents."""
    table = Table(title="Incidents", show_lines=False)
    table.add_column("ID", justify="right", style="cyan", no_wrap=True)
    table.add_column("Started", style="white", no_wrap=True)
    table.add_column("Duration", justify="right")
    table.add_column("Services", style="magenta", no_wrap=True)
    table.add_column("Peak Score", justify="right")
    table.add_column("Category", style="yellow")
    table.add_column("Confidence", justify="right")
    table.add_column("Status", style="green", no_wrap=True)

    for inc in incidents:
        started = inc.started_at.strftime("%Y-%m-%d %H:%M:%S") if inc.started_at else "-"
        duration = _format_duration(inc.started_at, inc.ended_at)
        services = ", ".join(inc.affected_services or []) or "-"
        peak = f"{inc.peak_score:.3f}" if inc.peak_score is not None else "-"
        category = inc.category or "-"
        confidence = f"{inc.confidence:.2f}" if inc.confidence is not None else "-"
        table.add_row(
            str(inc.id),
            started,
            duration,
            services,
            peak,
            category,
            confidence,
            inc.status,
        )

    if out_console is not None:
        out_console.print(table)
    return table


def _extract_evidence_bullets(inc: Any) -> list[str]:
    trace = getattr(inc, "agent_trace", None)
    if isinstance(trace, dict):
        final_json = trace.get("final_json")
        if isinstance(final_json, dict) and isinstance(final_json.get("evidence"), list):
            return [str(x) for x in final_json["evidence"]]
    return []


def _extract_alternatives(inc: Any) -> list[dict[str, Any]]:
    alts = getattr(inc, "alternatives", None)
    if isinstance(alts, list):
        return [a for a in alts if isinstance(a, dict)]
    rem = getattr(inc, "remediation", None)
    if isinstance(rem, dict) and isinstance(rem.get("alternatives"), list):
        return [a for a in rem["alternatives"] if isinstance(a, dict)]
    return []


def _extract_remediation_steps(inc: Any) -> list[dict[str, Any]]:
    rem = getattr(inc, "remediation", None)
    if isinstance(rem, list):
        return [s for s in rem if isinstance(s, dict)]
    if isinstance(rem, dict):
        steps = rem.get("steps") or rem.get("remediation") or []
        if isinstance(steps, list):
            return [s for s in steps if isinstance(s, dict)]
    return []


def render_incident_detail(inc: Any, out_console: Console | None = None) -> Panel:
    """Build and optionally print a Rich panel with full incident analysis."""
    target = out_console or console
    lines: list[str] = []

    confidence_str = f"{inc.confidence:.2f}" if inc.confidence is not None else "-"
    services_str = ", ".join(inc.affected_services or []) or "-"
    lines.append(
        f"[bold]Status:[/bold] {inc.status}   "
        f"[bold]Category:[/bold] {inc.category or '-'}   "
        f"[bold]Confidence:[/bold] {confidence_str}"
    )
    lines.append(
        f"[bold]Services:[/bold] {services_str}   "
        f"[bold]Peak Score:[/bold] {inc.peak_score:.3f}   "
        f"[bold]LLM Calls Used:[/bold] {inc.llm_calls or 0}"
    )
    lines.append("")
    lines.append(f"[bold cyan]Root Cause:[/bold cyan]\n{inc.root_cause or 'Not yet analyzed.'}")

    evidence = _extract_evidence_bullets(inc)
    lines.append("\n[bold cyan]Evidence:[/bold cyan]")
    if evidence:
        for bullet in evidence:
            lines.append(f"  • {bullet}")
    else:
        lines.append("  • No evidence bullets recorded.")

    alternatives = _extract_alternatives(inc)
    lines.append("\n[bold cyan]Alternatives Considered:[/bold cyan]")
    if alternatives:
        for alt in alternatives:
            cat = alt.get("category", "unknown")
            reason = alt.get("reason", "")
            lines.append(f"  • [yellow]{cat}[/yellow]: {reason}")
    else:
        lines.append("  • None recorded.")

    steps = _extract_remediation_steps(inc)
    lines.append("\n[bold cyan]Ranked Remediation:[/bold cyan]")
    if steps:
        for idx, step in enumerate(steps, start=1):
            risk = str(step.get("risk", "unknown")).upper()
            desc = step.get("step", "")
            lines.append(f"  {idx}. [[bold]{risk}[/bold]] {desc}")
    else:
        lines.append("  No remediation steps recorded.")

    panel = Panel(
        "\n".join(lines),
        title=f"Incident #{inc.id}",
        subtitle=_format_duration(inc.started_at, inc.ended_at),
        border_style="blue",
    )
    target.print(panel)
    target.print(
        "[dim]Note: All remediation actions above are suggestions for a human operator "
        "— never auto-executed.[/dim]"
    )
    return panel


@app.command("init-db")
def init_db() -> None:
    """Initialize the PostgreSQL database schema."""
    from app.db.init import main as db_init_main

    db_init_main()


@app.command()
def serve(
    port: int = typer.Option(
        8000, "--port", "-p", min=1, max=65535, help="Port to bind the API server."
    ),
    host: str = typer.Option("127.0.0.1", "--host", help="Host interface to bind."),
) -> None:
    """Start the FastAPI ingestion and incident API server."""
    import uvicorn

    uvicorn.run("app.api.main:app", host=host, port=port)


@app.command()
def replay(
    logfile: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="Path to a plain log file (YYYY-MM-DD HH:MM:SS,mmm [service] LEVEL message).",
        ),
    ],
    speed: float = typer.Option(
        0.0,
        "--speed",
        min=0.0,
        help="Replay speed multiplier in event time (0 = no sleeping, as fast as possible).",
    ),
    mock: bool = typer.Option(
        False,
        "--mock",
        help="Force MockLLM instead of the configured LLM_PROVIDER.",
    ),
) -> None:
    """Replay a plain log file in-process through the detection and analysis pipeline."""
    if mock:
        os.environ["LLM_PROVIDER"] = "mock"
        get_settings.cache_clear()
        settings.llm_provider = "mock"

    from app.agent.agent import incident_agent_hook, wait_for_agents
    from app.incidents.manager import IncidentManager
    from app.ingest.batch import ingest_batch
    from app.parsing.parser import LogParser
    from app.pipeline import Pipeline

    parser = LogParser()
    incident_manager = IncidentManager(agent_hook=incident_agent_hook)
    pipeline = Pipeline(parser=parser, incident_manager=incident_manager)

    default_host = socket.gethostname()
    batch: list[dict[str, Any]] = []
    batch_size = 100
    prev_ts: datetime | None = None
    last_tick_ts: datetime | None = None
    lines_read = 0
    events_parsed = 0
    start_wall = time.monotonic()

    with logfile.open("r", encoding="utf-8", errors="replace") as f:
        for raw_line in f:
            if not raw_line.strip():
                continue
            lines_read += 1
            event = _parse_plain_log_line(raw_line, default_host)
            if event is None:
                continue
            events_parsed += 1
            ts: datetime = event["ts"]

            if speed > 0.0 and prev_ts is not None:
                delta = (ts - prev_ts).total_seconds()
                if delta > 0:
                    time.sleep(delta / speed)
            prev_ts = ts

            if last_tick_ts is None or (ts - last_tick_ts).total_seconds() >= 1.0:
                if batch:
                    ingest_batch(batch, parser=parser, pipeline=pipeline)
                    batch.clear()
                pipeline.tick(ts)
                last_tick_ts = ts

            batch.append(event)
            if len(batch) >= batch_size:
                ingest_batch(batch, parser=parser, pipeline=pipeline)
                batch.clear()

    if batch:
        ingest_batch(batch, parser=parser, pipeline=pipeline)
        batch.clear()

    pipeline.flush()
    wait_for_agents(timeout_seconds=120.0)
    elapsed = time.monotonic() - start_wall

    incidents = incident_manager.list_incidents()
    analyzed = sum(1 for inc in incidents if inc.status == "analyzed")
    provider_name = "mock" if mock else get_settings().llm_provider

    summary = Table(title="Replay Summary", show_header=False)
    summary.add_column("Metric", style="bold cyan")
    summary.add_column("Value", style="white")
    summary.add_row("Log file", str(logfile))
    summary.add_row("Lines read", str(lines_read))
    summary.add_row("Events ingested", str(events_parsed))
    summary.add_row("Windows scored", str(incident_manager.total_windows))
    summary.add_row("Incidents", f"{len(incidents)} ({analyzed} analyzed)")
    summary.add_row("LLM provider", provider_name)
    summary.add_row("Elapsed time", f"{elapsed:.2f}s")
    console.print(summary)


@app.command()
def incidents(
    status: str | None = typer.Option(
        None,
        "--status",
        help="Filter incidents by status: open, analyzed, or closed.",
    ),
) -> None:
    """List recorded incidents in a Rich table."""
    if status is not None and status not in VALID_INCIDENT_STATUSES:
        raise typer.BadParameter("status must be one of: open, analyzed, closed")

    from app.db.repository import list_incidents
    from app.db.session import SessionLocal

    with SessionLocal() as session:
        rows = list_incidents(session, status=status)

    if not rows:
        console.print("[yellow]No incidents found.[/yellow]")
        return
    render_incidents_table(rows, console)


@app.command()
def show(
    incident_id: int = typer.Argument(..., help="Numeric ID of the incident to display."),
) -> None:
    """Show detailed root-cause analysis and remediation for a single incident."""
    from app.db.repository import get_incident
    from app.db.session import SessionLocal

    with SessionLocal() as session:
        inc = get_incident(session, incident_id)

    if inc is None:
        console.print(f"[red]Incident {incident_id} not found.[/red]")
        raise typer.Exit(code=1)

    render_incident_detail(inc, console)


@app.command()
def stats() -> None:
    """Show pipeline counts and percentage of LLM calls saved."""
    from app.db.models import Incident, LogEvent, Template, WindowScore
    from app.db.session import SessionLocal

    with SessionLocal() as session:
        log_lines = session.scalar(select(func.count()).select_from(LogEvent)) or 0
        templates = session.scalar(select(func.count()).select_from(Template)) or 0
        windows = session.scalar(select(func.count()).select_from(WindowScore)) or 0
        inc_count = session.scalar(select(func.count()).select_from(Incident)) or 0
        llm_calls = session.scalar(select(func.coalesce(func.sum(Incident.llm_calls), 0))) or 0

    saved_vs_logs, saved_vs_windows = compute_llm_savings(log_lines, windows, llm_calls)

    table = Table(title="Pipeline Statistics", show_header=False)
    table.add_column("Metric", style="bold cyan")
    table.add_column("Value", justify="right", style="white")
    table.add_row("Log lines", f"{log_lines:,}")
    table.add_row("Templates", f"{templates:,}")
    table.add_row("Windows", f"{windows:,}")
    table.add_row("Incidents", f"{inc_count:,}")
    table.add_row("LLM calls", f"{llm_calls:,}")
    table.add_row("LLM calls saved vs 1/log line", f"{saved_vs_logs:.2f}%")
    table.add_row("LLM calls saved vs 1/window", f"{saved_vs_windows:.2f}%")
    console.print(table)


async def _watch_loop(url: str) -> None:
    import websockets

    backoff = 2.0
    while True:
        try:
            async with websockets.connect(url) as ws:
                console.print(f"[green]Connected to {url}[/green]")
                backoff = 2.0
                async for raw in ws:
                    try:
                        payload = json.loads(raw)
                        status = payload.get("status", "alert")
                        inc_id = payload.get("incident_id", "?")
                        cat = payload.get("category", "-")
                        rc = payload.get("root_cause", "")
                        msg = Text()
                        msg.append(f"[{status.upper()}] ", style="bold yellow")
                        msg.append(f"Incident #{inc_id} ")
                        if cat and cat != "-":
                            msg.append(f"({cat}) ", style="cyan")
                        if rc:
                            msg.append(str(rc))
                        console.print(msg)
                    except json.JSONDecodeError:
                        console.print(raw)
        except (OSError, Exception) as exc:
            console.print(
                f"[yellow]Disconnected ({exc}); reconnecting in {backoff:.0f}s...[/yellow]"
            )
            await asyncio.sleep(backoff)
            backoff = min(10.0, backoff * 1.5)


@app.command()
def watch(
    url: str = typer.Option(
        "ws://localhost:8000/ws/alerts",
        "--url",
        help="WebSocket URL for live incident alerts.",
    ),
) -> None:
    """Stream live incident alerts from the API WebSocket, reconnecting on failure."""
    try:
        asyncio.run(_watch_loop(url))
    except KeyboardInterrupt:
        console.print("\n[dim]Stopped watching alerts.[/dim]")


def _wait_healthy(url: str, timeout: float = 25.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = httpx.get(url, timeout=1.0)
            if r.status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    return False


def _stop_process(proc: subprocess.Popen[Any] | None, name: str) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=2)
    console.print(f"[dim]Stopped {name}.[/dim]")


@app.command()
def demo(
    fault: str = typer.Option(
        "cpu_hog",
        "--fault",
        help="Fault to inject: cpu_hog, memory_leak, latency, packet_loss, db_lock.",
    ),
    fast: bool = typer.Option(
        False,
        "--fast",
        help="Use fast demo timings (WINDOW_SECONDS=5, WARMUP_WINDOWS=24, ~4-5 min total).",
    ),
) -> None:
    """Run a full end-to-end live demo with testbed, fault injection, and incident analysis."""
    from app.db.init import main as db_init_main
    from app.db.repository import list_incidents
    from app.db.session import SessionLocal
    from testbed.fault_injector import FAULTS, run_fault

    if fault not in FAULTS:
        raise typer.BadParameter(f"fault must be one of: {', '.join(sorted(FAULTS))}")

    env = os.environ.copy()
    if fast:
        env["WINDOW_SECONDS"] = "5"
        env["WARMUP_WINDOWS"] = "24"

    window_seconds = int(env.get("WINDOW_SECONDS", str(settings.window_seconds)))
    warmup_windows = int(env.get("WARMUP_WINDOWS", str(settings.warmup_windows)))
    anomaly_gap = int(env.get("ANOMALY_GAP_SECONDS", str(settings.anomaly_gap_seconds)))

    console.print("[bold cyan]1. Resetting demo database and Drain3 state...[/bold cyan]")
    db_init_main()
    with SessionLocal() as session, session.begin():
        session.execute(
            text(
                "TRUNCATE TABLE log_events, templates, window_scores, incidents, "
                "metrics_samples, fault_events RESTART IDENTITY CASCADE;"
            )
        )
    drain_path = Path(settings.drain_state_path)
    if drain_path.exists():
        drain_path.unlink()

    proc_api: subprocess.Popen[Any] | None = None
    proc_testbed: subprocess.Popen[Any] | None = None
    proc_loadgen: subprocess.Popen[Any] | None = None

    try:
        console.print("[bold cyan]2. Starting API server and testbed services...[/bold cyan]")
        proc_api = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "uvicorn",
                "app.api.main:app",
                "--host",
                "127.0.0.1",
                "--port",
                "8000",
                "--log-level",
                "warning",
            ],
            env=env,
        )
        proc_testbed = subprocess.Popen(
            [sys.executable, "-m", "testbed.run_all"],
            env=env,
        )

        if not _wait_healthy("http://127.0.0.1:8000/health"):
            raise RuntimeError("API server failed to become healthy on port 8000")
        if not _wait_healthy("http://127.0.0.1:8101/health"):
            raise RuntimeError("Testbed API failed to become healthy on port 8101")
        if not _wait_healthy("http://127.0.0.1:8102/health"):
            raise RuntimeError("Testbed Worker failed to become healthy on port 8102")

        console.print(
            "[bold cyan]3. Starting load generator and waiting for warm-up...[/bold cyan]"
        )
        proc_loadgen = subprocess.Popen(
            [sys.executable, "-m", "testbed.loadgen", "--minutes", "15", "--rps", "5.0"],
            env=env,
        )

        warmup_timeout = time.monotonic() + (warmup_windows * window_seconds) + 45.0
        while time.monotonic() < warmup_timeout:
            try:
                r = httpx.get("http://127.0.0.1:8000/stats", timeout=2.0)
                if r.status_code == 200 and r.json().get("total_windows", 0) >= warmup_windows:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(2.0)

        fault_duration = 45.0 if fast else 60.0
        console.print(
            f"[bold yellow]4. Injecting fault '{fault}' for {fault_duration:.0f}s...[/bold yellow]"
        )
        run_fault(fault, "api", duration=fault_duration)

        console.print("[bold cyan]5. Waiting for incident to close and be analyzed...[/bold cyan]")
        wait_deadline = time.monotonic() + anomaly_gap + 90.0
        analyzed_ids: list[int] = []
        while time.monotonic() < wait_deadline:
            try:
                r = httpx.get("http://127.0.0.1:8000/incidents", timeout=2.0)
                if r.status_code == 200:
                    data = r.json()
                    analyzed_ids = [item["id"] for item in data if item.get("status") == "analyzed"]
                    if analyzed_ids:
                        break
            except httpx.HTTPError:
                pass
            time.sleep(2.0)

        if not analyzed_ids:
            with contextlib.suppress(httpx.HTTPError):
                httpx.post("http://127.0.0.1:8000/flush", timeout=5.0)
                httpx.post(
                    "http://127.0.0.1:8000/agents/wait", params={"timeout": 60.0}, timeout=65.0
                )

        with SessionLocal() as session:
            all_incidents = list_incidents(session)
        if not all_incidents:
            console.print("[yellow]No incidents were recorded during the demo.[/yellow]")
        else:
            for inc in all_incidents:
                render_incident_detail(inc, console)
    except KeyboardInterrupt:
        console.print("\n[yellow]Demo interrupted; cleaning up...[/yellow]")
        raise typer.Exit(code=130) from None
    finally:
        _stop_process(proc_loadgen, "loadgen")
        for port in (8101, 8102):
            with contextlib.suppress(httpx.HTTPError):
                httpx.post(f"http://127.0.0.1:{port}/_internal/stop", timeout=1.0)
        _stop_process(proc_testbed, "testbed")
        _stop_process(proc_api, "API server")


@app.command("eval")
def eval_cmd(
    runs: int = typer.Option(3, "--runs", min=1, help="Number of evaluation runs."),
    mock: bool = typer.Option(False, "--mock", help="Use MockLLM for evaluation."),
) -> None:
    """Run the evaluation harness."""
    import eval as eval_pkg

    eval_pkg.run_eval(runs=runs, mock=mock)


@app.command()
def version() -> None:
    """Print the current version."""
    typer.echo("incident-copilot 0.1.0")


if __name__ == "__main__":
    app()
