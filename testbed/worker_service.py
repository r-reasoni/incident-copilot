"""Background worker workload service for the local testbed."""
from __future__ import annotations

import asyncio
import random
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
import psutil
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from testbed.common.faults import FaultController
from testbed.common.log_shipper import configure_file_logger

SERVICE = "worker-service"
API_URL = "http://127.0.0.1:8101"
shipper = configure_file_logger(SERVICE)
faults = FaultController()
request_samples: deque[tuple[float, float, int]] = deque()
sample_lock = threading.Lock()
process = psutil.Process()
process.cpu_percent(None)
worker_stop = asyncio.Event()
worker_task: asyncio.Task | None = None
randomizer = random.Random()


def _record_request(elapsed: float, status: int) -> None:
    now = time.monotonic()
    with sample_lock:
        request_samples.append((now, elapsed, status))
        while request_samples and now - request_samples[0][0] > 30:
            request_samples.popleft()


async def _work_loop() -> None:
    job_id = 0
    async with httpx.AsyncClient(timeout=2.0) as client:
        while not worker_stop.is_set():
            job_id += 1
            user_id = randomizer.randint(1, 3)
            success = False
            for attempt in range(2):
                try:
                    fault_kind, fault_params = faults.snapshot()
                    if fault_kind == "latency":
                        delay = float(fault_params.get("delay_ms", 800))
                        jitter = float(fault_params.get("jitter_ms", 200))
                        await asyncio.sleep(
                            max(0, randomizer.uniform(delay - jitter, delay + jitter)) / 1000
                        )
                    elif fault_kind == "cpu_hog":
                        cycle_start = time.perf_counter()
                        val = 1
                        while (time.perf_counter() - cycle_start) < 0.2:
                            for n in range(1, 2000):
                                val = (val * n) % 104729
                    elif fault_kind == "packet_loss" and randomizer.random() * 100 < float(
                        fault_params.get("percent", 20)
                    ):
                        timeout_seconds = max(
                            2.2, float(fault_params.get("timeout_seconds", 2.2))
                        )
                        await asyncio.sleep(timeout_seconds)
                        raise httpx.ReadTimeout("upstream response timed out")
                    user = await client.get(f"{API_URL}/users/{user_id}")
                    user.raise_for_status()
                    result = await client.post(
                        f"{API_URL}/orders",
                        json={"user_id": user_id, "amount": round(randomizer.uniform(4, 250), 2)},
                    )
                    result.raise_for_status()
                    shipper.log("INFO", f"Job {job_id} completed for user {user_id}")
                    success = True
                    break
                except (httpx.TimeoutException, httpx.TransportError):
                    shipper.log("WARN", "Upstream request timed out; retrying job")
                except httpx.HTTPStatusError as error:
                    shipper.log("ERROR", f"Upstream returned HTTP {error.response.status_code}")
            if not success:
                shipper.log("ERROR", f"Job {job_id} failed after retry")
            try:
                await asyncio.wait_for(worker_stop.wait(), timeout=0.5)
            except TimeoutError:
                pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    global worker_task
    worker_stop.clear()
    worker_task = asyncio.create_task(_work_loop())
    shipper.log("INFO", "Worker service ready on port 8102")
    yield
    worker_stop.set()
    if worker_task:
        await worker_task
    faults.stop()
    shipper.close()


app = FastAPI(title="Testbed Worker Service", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}



SKIP_LOG_PREFIXES = ("/_fault", "/_internal")
SKIP_LOG_EXACT = {"/health", "/metrics"}


def _should_skip_logging(path: str) -> bool:
    return path.startswith(SKIP_LOG_PREFIXES) or path in SKIP_LOG_EXACT


@app.middleware("http")
async def observe_requests(request: Request, call_next):
    started = time.perf_counter()
    path = request.url.path
    try:
        kind, params = faults.snapshot()
        if not path.startswith("/_fault/"):
            if kind == "latency":
                base = float(params.get("delay_ms", 800))
                spread = float(params.get("jitter_ms", 200))
                await asyncio.sleep(max(0, randomizer.uniform(base - spread, base + spread)) / 1000)
            elif kind == "cpu_hog":
                delay_ms = randomizer.uniform(100, 600)
                cycle_start = time.perf_counter()
                val = 1
                while (time.perf_counter() - cycle_start) * 1000 < delay_ms:
                    for n in range(1, 2000):
                        val = (val * n) % 104729
            elif kind == "packet_loss":
                drop_percent = max(0, min(100, float(params.get("percent", 20))))
                if randomizer.random() * 100 < drop_percent:
                    await asyncio.sleep(max(2.2, float(params.get("timeout_seconds", 2.2))))
                    elapsed = (time.perf_counter() - started) * 1000
                    _record_request(elapsed, 504)
                    if not _should_skip_logging(path):
                        shipper.log("ERROR", "Connection reset while waiting for worker response")
                    return JSONResponse(status_code=504, content={"detail": "request timed out"})
        response = await call_next(request)
        elapsed = (time.perf_counter() - started) * 1000
        _record_request(elapsed, response.status_code)
        if not _should_skip_logging(path):
            if elapsed > 500:
                shipper.log("WARN", f"Slow request {request.method} {path} completed in {elapsed:.0f}ms")
            else:
                shipper.log("INFO", f"Request {request.method} {path} completed in {elapsed:.0f}ms")
        return response
    except Exception as error:
        elapsed = (time.perf_counter() - started) * 1000
        _record_request(elapsed, 500)
        if not _should_skip_logging(path):
            shipper.log("ERROR", f"Request handler failed with {type(error).__name__}")
        raise


@app.get("/metrics")
def metrics() -> dict[str, Any]:
    now = time.monotonic()
    with sample_lock:
        while request_samples and now - request_samples[0][0] > 30:
            request_samples.popleft()
        latencies = sorted(sample[1] for sample in request_samples)
        errors = sum(1 for sample in request_samples if sample[2] >= 400)
    handles = process.num_handles() if hasattr(process, "num_handles") else process.num_fds()
    return {
        "cpu_percent": process.cpu_percent(None),
        "memory_percent": process.memory_percent(),
        "open_handles": handles,
        "thread_count": process.num_threads(),
        "request_latency_p50_ms": _percentile(latencies, 0.50),
        "request_latency_p95_ms": _percentile(latencies, 0.95),
        "error_count_30s": errors,
    }


def _percentile(values: list[float], percentile: float) -> float:
    return round(values[min(len(values) - 1, int((len(values) - 1) * percentile))], 2) if values else 0.0


@app.post("/_fault/start")
def start_fault(payload: dict[str, Any]) -> dict[str, str]:
    kind = str(payload.get("type", ""))
    if kind not in {"cpu_hog", "memory_leak", "latency", "packet_loss", "db_lock"}:
        return JSONResponse(status_code=400, content={"detail": "unsupported fault type"})
    try:
        faults.start(kind, payload.get("params") or {}, shipper.log)
    except Exception:
        return JSONResponse(status_code=503, content={"detail": "fault could not be started"})
    return {"status": "started"}


@app.post("/_fault/stop")
def stop_fault() -> dict[str, str]:
    faults.stop()
    return {"status": "stopped"}


@app.post("/_internal/stop", include_in_schema=False)
def internal_stop(request: Request) -> dict[str, str]:
    server = getattr(request.app.state, "uvicorn_server", None)
    if server is not None:
        server.should_exit = True
    return {"status": "stopping"}