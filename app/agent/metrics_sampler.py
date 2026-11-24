"""Background metrics sampler polling target services and recording host metrics."""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

from app.config import get_settings
from app.db.repository import insert_metrics_sample
from app.db.session import SessionLocal

logger = logging.getLogger(__name__)


async def sample_target(
    client: httpx.AsyncClient,
    service_name: str,
    base_url: str,
) -> dict[str, Any]:
    """Poll a single service's health and metrics endpoints."""
    t0 = time.perf_counter()
    probe_ok = False
    probe_ms: float | None = None
    try:
        health_resp = await client.get(f"{base_url}/health", timeout=2.0)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        if health_resp.status_code < 400:
            probe_ok = True
            probe_ms = round(elapsed_ms, 2)
    except Exception:
        probe_ok = False
        probe_ms = None

    cpu_percent = 0.0
    mem_percent = 0.0
    open_handles = 0
    threads = 0
    p50_ms = 0.0
    p95_ms = 0.0
    error_count = 0
    in_flight_requests = 0
    request_count_30s = 0

    try:
        metrics_resp = await client.get(f"{base_url}/metrics", timeout=2.0)
        if metrics_resp.status_code == 200:
            data = metrics_resp.json()
            cpu_percent = float(data.get("cpu_percent", 0.0))
            mem_percent = float(data.get("memory_percent", 0.0))
            open_handles = int(data.get("open_handles", 0))
            threads = int(data.get("thread_count", 0))
            p50_ms = float(data.get("request_latency_p50_ms", 0.0))
            p95_ms = float(data.get("request_latency_p95_ms", 0.0))
            error_count = int(data.get("error_count_30s", 0))
            in_flight_requests = int(data.get("in_flight_requests", 0))
            request_count_30s = int(data.get("request_count_30s", 0))
        else:
            probe_ok = False
    except Exception:
        probe_ok = False

    return {
        "ts": datetime.now(timezone.utc),
        "service": service_name,
        "cpu_percent": cpu_percent,
        "mem_percent": mem_percent,
        "open_handles": open_handles,
        "threads": threads,
        "p50_ms": p50_ms,
        "p95_ms": p95_ms,
        "error_count": error_count,
        "in_flight_requests": in_flight_requests,
        "request_count_30s": request_count_30s,
        "probe_ms": probe_ms,
        "probe_ok": probe_ok,
    }


async def run_metrics_sampler() -> None:
    """Continuous polling loop started during API lifespan."""
    settings = get_settings()
    sample_interval = max(1, settings.metrics_sample_seconds)
    targets = settings.get_metrics_targets()
    logger.info("Starting metrics sampler for targets: %s", targets)

    async with httpx.AsyncClient() as client:
        while True:
            try:
                samples = await asyncio.gather(
                    *(
                        sample_target(client, name, url)
                        for name, url in targets.items()
                    ),
                    return_exceptions=True,
                )
                valid_samples = [s for s in samples if isinstance(s, dict)]
                if valid_samples:
                    record_path = getattr(settings, "record_metrics_path", "")
                    if record_path:
                        try:
                            p = Path(record_path)
                            p.parent.mkdir(parents=True, exist_ok=True)
                            with p.open("a", encoding="utf-8") as rf:
                                for s in valid_samples:
                                    row = dict(s)
                                    if isinstance(row.get("ts"), datetime):
                                        row["ts"] = row["ts"].isoformat()
                                    rf.write(json.dumps(row) + "\n")
                        except Exception:
                            logger.exception("Failed to write metrics samples to record_metrics_path")

                    try:
                        with SessionLocal() as session, session.begin():
                            for sample in valid_samples:
                                insert_metrics_sample(session, **sample)
                    except Exception as db_exc:
                        logger.warning("Failed to persist metrics samples: %s", db_exc)
            except asyncio.CancelledError:
                logger.info("Metrics sampler task cancelled.")
                break
            except Exception as loop_exc:
                logger.error("Unexpected error in metrics sampler: %s", loop_exc)

            try:
                await asyncio.sleep(sample_interval)
            except asyncio.CancelledError:
                break
