"""Non-blocking local log writer and bounded best-effort batch shipper."""
from __future__ import annotations

import json
import logging
import os
import socket
import threading
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
SHIP_URL = "http://localhost:8000/ingest/logs"
MAX_BUFFER = 500
MAX_BATCH = 50


class LogShipper:
    def __init__(self, service: str) -> None:
        self.service = service
        self.host = socket.gethostname()
        self._buffer: deque[dict[str, Any]] = deque(maxlen=MAX_BUFFER)
        self.dropped_logs: int = 0
        self._lock = threading.Lock()
        self._file_lock = threading.Lock()
        self._stop = threading.Event()
        self._wake = threading.Event()
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self._file_path = LOG_DIR / f"{service}.log"
        self._thread = threading.Thread(target=self._ship_loop, daemon=True)
        self._thread.start()

    def log(self, level: str, message: str) -> None:
        now = datetime.now(timezone.utc)
        local_now = now.astimezone()
        line = f"{local_now:%Y-%m-%d %H:%M:%S},{local_now.microsecond // 1000:03d} [{self.service}] {level.upper()} {message}"
        with self._file_lock:
            with self._file_path.open("a", encoding="utf-8") as file:
                file.write(line + os.linesep)
        record = {
            "ts": now.isoformat(),
            "service": self.service,
            "host": self.host,
            "level": level.upper(),
            "message": message,
        }
        with self._lock:
            if len(self._buffer) >= MAX_BUFFER:
                self.dropped_logs += 1
                if self.dropped_logs == 1 or self.dropped_logs % 100 == 0:
                    logging.getLogger(self.service).warning(
                        "LogShipper dropped %d logs due to full buffer", self.dropped_logs
                    )
            self._buffer.append(record)
            should_send = len(self._buffer) >= MAX_BATCH
        if should_send:
            self._wake.set()

    def _ship_once(self) -> None:
        with self._lock:
            batch = list(self._buffer)[:MAX_BATCH]
        if not batch:
            return
        try:
            response = httpx.post(SHIP_URL, json=batch, timeout=0.2)
            response.raise_for_status()
        except Exception:
            return
        with self._lock:
            for _ in batch:
                if self._buffer:
                    self._buffer.popleft()

    def _ship_loop(self) -> None:
        while not self._stop.is_set():
            self._wake.wait(1.0)
            self._wake.clear()
            self._ship_once()

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=0.5)


def configure_file_logger(service: str) -> LogShipper:
    logging.getLogger(service).addHandler(logging.NullHandler())
    return LogShipper(service)