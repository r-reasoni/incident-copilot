"""FastAPI ingestion and read-only incident endpoints."""
from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import func, select

from app.config import settings
from app.db.models import Incident, LogEvent, Template, WindowScore
from app.db.repository import insert_logs, list_incidents, upsert_templates
from app.db.session import SessionLocal
from app.parsing import LogParser, ParsedLog
from app.pipeline import Pipeline

logger = logging.getLogger(__name__)
QUEUE_MAXSIZE = 10_000
INGEST_BATCH_SIZE = 200
INGEST_FLUSH_SECONDS = 1.0
VALID_LEVELS = {"DEBUG", "INFO", "WARN", "WARNING", "ERROR", "CRITICAL"}


class LogEventInput(BaseModel):
  model_config = ConfigDict(extra="forbid")

  ts: datetime
  service: str
  host: str
  level: str
  message: str

  @field_validator("level")
  @classmethod
  def validate_level(cls, value: str) -> str:
    value = value.upper()
    if value not in VALID_LEVELS:
      raise ValueError("unsupported log level")
    return value

  @field_validator("message")
  @classmethod
  def validate_message(cls, value: str) -> str:
    if not value.strip():
      raise ValueError("message must not be empty")
    return value

  @field_validator("service", "host")
  @classmethod
  def validate_nonempty(cls, value: str) -> str:
    if not value.strip():
      raise ValueError("value must not be empty")
    return value


async def _ingest_consumer(application: FastAPI) -> None:
  queue: asyncio.Queue[LogEventInput] = application.state.ingest_queue
  pipeline: Pipeline = application.state.pipeline
  parser: LogParser = application.state.parser
  while True:
    batch: list[LogEventInput] = []
    try:
      first = await asyncio.wait_for(queue.get(), timeout=INGEST_FLUSH_SECONDS)
      batch.append(first)
    except TimeoutError:
      continue
    while len(batch) < INGEST_BATCH_SIZE:
      try:
        batch.append(queue.get_nowait())
      except asyncio.QueueEmpty:
        break

    try:
      parsed_batch: list[tuple[LogEventInput, ParsedLog]] = []
      for event in batch:
        parsed = parser.parse(event.message)
        parsed_batch.append((event, parsed))

      log_records = [
        {
          "ts": event.ts,
          "service": event.service,
          "host": event.host,
          "level": event.level,
          "raw_message": event.message,
          "template_id": parsed.template_id,
          "params": {
            "values": parsed.params,
            "duration_ms": parsed.duration_ms,
          },
        }
        for event, parsed in parsed_batch
      ]

      template_aggregates: dict[int, dict[str, Any]] = {}
      for event, parsed in parsed_batch:
        tid = parsed.template_id
        if tid not in template_aggregates:
          template_aggregates[tid] = {
            "template_id": tid,
            "template_text": parsed.template_text,
            "first_seen": event.ts,
            "count": 0,
          }
        template_aggregates[tid]["count"] += 1
        template_aggregates[tid]["template_text"] = parsed.template_text
        if event.ts < template_aggregates[tid]["first_seen"]:
          template_aggregates[tid]["first_seen"] = event.ts

      with SessionLocal() as session, session.begin():
        insert_logs(session, log_records)
        upsert_templates(session, list(template_aggregates.values()))

      for event, parsed in parsed_batch:
        pipeline.process(event.model_dump(), parsed)
    except Exception:
      logger.exception("Failed to persist an ingestion batch")
    finally:
      for _ in batch:
        queue.task_done()


async def _tick_worker(application: FastAPI) -> None:
  pipeline: Pipeline = application.state.pipeline
  while True:
    try:
      await asyncio.sleep(1.0)
      pipeline.tick(datetime.now(UTC))
    except asyncio.CancelledError:
      break
    except Exception:
      logger.exception("Error in pipeline tick worker")


@asynccontextmanager
async def lifespan(application: FastAPI):
  application.state.ingest_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
  parser = LogParser()
  application.state.parser = parser
  application.state.pipeline = Pipeline(parser=parser)
  consumer = asyncio.create_task(_ingest_consumer(application))
  ticker = asyncio.create_task(_tick_worker(application))
  try:
    yield
  finally:
    ticker.cancel()
    with suppress(asyncio.CancelledError):
      await ticker
    await application.state.ingest_queue.join()
    consumer.cancel()
    with suppress(asyncio.CancelledError):
      await consumer
    application.state.pipeline.flush()

app = FastAPI(
    title="Incident Copilot",
    description=(
        "Real-time anomaly detection on application logs "
        "with LLM-powered root-cause analysis."
    ),
    version="0.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


@app.get("/health", tags=["meta"], summary="Liveness probe")
async def health() -> dict[str, str]:
    """Return a simple liveness confirmation."""
    return {"status": "ok", "llm_provider": settings.llm_provider}


@app.post("/ingest/logs", status_code=202, tags=["ingestion"])
async def ingest_logs(events: list[LogEventInput], request: Request) -> dict[str, int]:
    queue: asyncio.Queue[LogEventInput] = request.app.state.ingest_queue
    if queue.maxsize and queue.qsize() + len(events) > queue.maxsize:
        raise HTTPException(status_code=503, detail="ingestion queue is full")
    try:
        for event in events:
            queue.put_nowait(event)
    except asyncio.QueueFull as error:
        raise HTTPException(status_code=503, detail="ingestion queue is full") from error
    return {"accepted": len(events)}


@app.get("/incidents", tags=["incidents"])
def get_incidents(status: str | None = None) -> list[dict[str, Any]]:
    with SessionLocal() as session:
        return [
            {
                "id": item.id,
                "started_at": item.started_at,
                "ended_at": item.ended_at,
                "status": item.status,
                "peak_score": item.peak_score,
                "affected_services": item.affected_services,
                "root_cause": item.root_cause,
                "confidence": item.confidence,
                "remediation": item.remediation,
                "llm_calls": item.llm_calls,
                "created_at": item.created_at,
            }
            for item in list_incidents(session, status)
        ]


@app.get("/incidents/{incident_id}", tags=["incidents"])
def get_incident(incident_id: int) -> dict[str, Any]:
    with SessionLocal() as session:
        item = session.get(Incident, incident_id)
        if item is None:
            raise HTTPException(status_code=404, detail="incident not found")
        return {
            "id": item.id,
            "started_at": item.started_at,
            "ended_at": item.ended_at,
            "status": item.status,
            "peak_score": item.peak_score,
            "affected_services": item.affected_services,
            "top_templates": item.top_templates,
            "root_cause": item.root_cause,
            "confidence": item.confidence,
            "remediation": item.remediation,
            "agent_trace": item.agent_trace,
            "llm_calls": item.llm_calls,
            "created_at": item.created_at,
        }


@app.get("/stats", tags=["meta"])
def stats() -> dict[str, int]:
    with SessionLocal() as session:
        return {
            "logs_ingested": session.scalar(select(func.count()).select_from(LogEvent)) or 0,
            "templates": session.scalar(select(func.count()).select_from(Template)) or 0,
            "windows_scored": session.scalar(select(func.count()).select_from(WindowScore)) or 0,
            "incidents": session.scalar(select(func.count()).select_from(Incident)) or 0,
            "llm_calls": session.scalar(
                select(func.coalesce(func.sum(Incident.llm_calls), 0))
            )
            or 0,
        }


alert_sockets: set[WebSocket] = set()


async def broadcast_alert(payload: dict[str, Any]) -> None:
    """Send an alert payload to connected clients, dropping failed sockets."""
    failed: list[WebSocket] = []
    for socket in tuple(alert_sockets):
        try:
            await socket.send_json(payload)
        except Exception:
            failed.append(socket)
    for socket in failed:
        alert_sockets.discard(socket)


@app.websocket("/ws/alerts")
async def alerts(websocket: WebSocket) -> None:
    await websocket.accept()
    alert_sockets.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        alert_sockets.discard(websocket)
