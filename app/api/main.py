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
from app.incidents import IncidentManager
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
  loop = asyncio.get_running_loop()
  application.state.ingest_queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
  parser = LogParser()
  incident_manager = IncidentManager(loop=loop, broadcast_fn=broadcast_alert)
  from app.detection import AnomalyDetector
  detector = AnomalyDetector(on_window=incident_manager.on_window)
  pipeline = Pipeline(parser=parser, incident_manager=incident_manager, detector=detector)
  application.state.parser = parser
  application.state.incident_manager = incident_manager
  application.state.detector = detector
  application.state.pipeline = pipeline
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
def get_incidents(status: str | None = None, request: Request = None) -> list[dict[str, Any]]:
    items = []
    try:
        with SessionLocal() as session:
            items = [
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
    except Exception:
        pass

    if items:
        return items

    manager: IncidentManager | None = (
        getattr(request.app.state, "incident_manager", None) if request and hasattr(request, "app") else None
    )
    if manager is not None:
        return [
            {
                "id": item.id,
                "started_at": item.started_at,
                "ended_at": item.ended_at,
                "status": item.status,
                "peak_score": item.peak_score,
                "affected_services": item.affected_services,
                "root_cause": None,
                "confidence": None,
                "remediation": None,
                "llm_calls": 0,
                "created_at": item.started_at,
            }
            for item in manager.list_incidents(status)
        ]
    return []


@app.get("/incidents/{incident_id}", tags=["incidents"])
def get_incident(incident_id: int, request: Request = None) -> dict[str, Any]:
    try:
        with SessionLocal() as session:
            item = session.get(Incident, incident_id)
            if item is not None:
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
    except Exception:
        pass

    manager: IncidentManager | None = (
        getattr(request.app.state, "incident_manager", None) if request and hasattr(request, "app") else None
    )
    if manager is not None:
        rec = manager.get_incident(incident_id)
        if rec is not None:
            return {
                "id": rec.id,
                "started_at": rec.started_at,
                "ended_at": rec.ended_at,
                "status": rec.status,
                "peak_score": rec.peak_score,
                "affected_services": rec.affected_services,
                "top_templates": rec.top_templates,
                "root_cause": None,
                "confidence": None,
                "remediation": None,
                "agent_trace": {
                    "comparison": rec.comparison,
                    "new_templates": rec.new_templates,
                    "peak_features": rec.peak_features,
                },
                "llm_calls": 0,
                "created_at": rec.started_at,
            }

    raise HTTPException(status_code=404, detail="incident not found")


@app.get("/stats", tags=["meta"])
def stats(request: Request = None) -> dict[str, int]:
    manager: IncidentManager | None = (
        getattr(request.app.state, "incident_manager", None) if request and hasattr(request, "app") else None
    )
    total_logs = manager.total_log_lines if manager else 0
    total_windows = manager.total_windows if manager else 0
    incidents_cnt = manager.incidents_count if manager else 0
    llm_cnt = manager.llm_calls if manager else 0

    try:
        with SessionLocal() as session:
            db_logs = session.scalar(select(func.count()).select_from(LogEvent)) or 0
            db_templates = session.scalar(select(func.count()).select_from(Template)) or 0
            db_windows = session.scalar(select(func.count()).select_from(WindowScore)) or 0
            db_incidents = session.scalar(select(func.count()).select_from(Incident)) or 0
            db_llm = session.scalar(select(func.coalesce(func.sum(Incident.llm_calls), 0))) or 0

            return {
                "logs_ingested": db_logs or total_logs,
                "total_log_lines": db_logs or total_logs,
                "templates": db_templates,
                "windows_scored": db_windows or total_windows,
                "total_windows": db_windows or total_windows,
                "incidents": db_incidents or incidents_cnt,
                "llm_calls": db_llm or llm_cnt,
            }
    except Exception:
        return {
            "logs_ingested": total_logs,
            "total_log_lines": total_logs,
            "templates": 0,
            "windows_scored": total_windows,
            "total_windows": total_windows,
            "incidents": incidents_cnt,
            "llm_calls": llm_cnt,
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
