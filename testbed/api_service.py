"""Local API workload service for the incident-copilot testbed."""
from __future__ import annotations

import asyncio
import random
import threading
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import psutil
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select

from testbed.common.database import Order, TestbedSession, User, initialize_schema
from testbed.common.faults import FaultController
from testbed.common.log_shipper import configure_file_logger

SERVICE = "api-service"
shipper = configure_file_logger(SERVICE)
faults = FaultController()
request_samples: deque[tuple[float, float, int]] = deque()
sample_lock = threading.Lock()
process = psutil.Process()
process.cpu_percent(None)
RANDOM = random.Random()


class OrderRequest(BaseModel):
    user_id: int
    amount: float


def _record_request(elapsed: float, status: int) -> None:
    now = time.monotonic()
    with sample_lock:
        request_samples.append((now, elapsed, status))
        while request_samples and now - request_samples[0][0] > 30:
            request_samples.popleft()


@asynccontextmanager
async def lifespan(_: FastAPI):
    initialize_schema()
    shipper.log("INFO", "API service ready on port 8101")
    yield
    faults.stop()
    shipper.close()


app = FastAPI(title="Testbed API Service", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
                await asyncio.sleep(max(0, RANDOM.uniform(base - spread, base + spread)) / 1000)
            elif kind == "packet_loss":
                drop_percent = max(0, min(100, float(params.get("percent", 20))))
                if RANDOM.random() * 100 < drop_percent:
                    await asyncio.sleep(max(2.2, float(params.get("timeout_seconds", 2.2))))
                    elapsed = (time.perf_counter() - started) * 1000
                    _record_request(elapsed, 504)
                    shipper.log("ERROR", "Request timed out waiting for an upstream response")
                    return _json_error(504, "upstream request timed out")
        response = await call_next(request)
        elapsed = (time.perf_counter() - started) * 1000
        _record_request(elapsed, response.status_code)
        if elapsed > 500:
            shipper.log("WARN", f"Slow request {request.method} {path} completed in {elapsed:.0f}ms")
        elif response.status_code >= 500:
            shipper.log("ERROR", f"Request {request.method} {path} returned {response.status_code}")
        else:
            shipper.log("INFO", f"Request {request.method} {path} completed in {elapsed:.0f}ms")
        return response
    except Exception as error:
        elapsed = (time.perf_counter() - started) * 1000
        _record_request(elapsed, 500)
        shipper.log("ERROR", f"Request handler failed with {type(error).__name__}")
        raise


def _json_error(status: int, detail: str):
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=status, content={"detail": detail})


@app.get("/users/{user_id}")
def get_user(user_id: int) -> dict[str, Any]:
    with TestbedSession() as session:
        user = session.get(User, user_id)
        if user is None:
            raise HTTPException(status_code=404, detail="user not found")
        return {"id": user.id, "name": user.name}


@app.post("/orders", status_code=201)
def create_order(order: OrderRequest) -> dict[str, Any]:
    started = time.perf_counter()
    with TestbedSession() as session, session.begin():
        if session.get(User, order.user_id) is None:
            raise HTTPException(status_code=404, detail="user not found")
        item = Order(
            user_id=order.user_id,
            amount=order.amount,
            created_at=datetime.now(timezone.utc),
        )
        session.add(item)
        session.flush()
        result = {"id": item.id, "user_id": item.user_id, "amount": item.amount}
    elapsed_ms = (time.perf_counter() - started) * 1000
    if elapsed_ms > 500:
        shipper.log("WARN", f"Order persistence resumed after delay of {elapsed_ms:.0f}ms")
    else:
        shipper.log(
            "INFO",
            f"Order {result['id']} created for user {result['user_id']} in {elapsed_ms:.0f}ms",
        )
    return result


@app.get("/orders/{order_id}")
def get_order(order_id: int) -> dict[str, Any]:
    with TestbedSession() as session:
        order = session.get(Order, order_id)
        if order is None:
            raise HTTPException(status_code=404, detail="order not found")
        return {
            "id": order.id,
            "user_id": order.user_id,
            "amount": order.amount,
            "created_at": order.created_at.isoformat(),
        }


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
    if not values:
        return 0.0
    return round(values[min(len(values) - 1, int((len(values) - 1) * percentile))], 2)


@app.post("/_internal/stop", include_in_schema=False)
def internal_stop(request: Request) -> dict[str, str]:
    server = getattr(request.app.state, "uvicorn_server", None)
    if server is not None:
        server.should_exit = True
    return {"status": "stopping"}


@app.post("/_fault/start")
def start_fault(payload: dict[str, Any]) -> dict[str, str]:
    kind = str(payload.get("type", ""))
    params = payload.get("params") or {}
    if kind not in {"cpu_hog", "memory_leak", "latency", "packet_loss", "db_lock"}:
        raise HTTPException(status_code=400, detail="unsupported fault type")
    try:
        faults.start(kind, params, shipper.log)
    except Exception as error:
        raise HTTPException(status_code=503, detail="fault could not be started") from error
    return {"status": "started"}


@app.post("/_fault/stop")
def stop_fault() -> dict[str, str]:
    faults.stop()
    return {"status": "stopped"}