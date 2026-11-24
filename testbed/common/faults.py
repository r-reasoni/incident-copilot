"""Process-local fault flags and resources used by testbed services."""
from __future__ import annotations

import multiprocessing
import os
import threading
import time
from collections.abc import Callable
from typing import Any


def _cpu_worker() -> None:
    try:
        os.nice(15)
    except Exception:
        pass
    while True:
        cycle = time.perf_counter()
        while time.perf_counter() - cycle < 0.08:
            value = 1
            for number in range(1, 2000):
                value = (value * number) % 104729
        time.sleep(0.02)


class FaultController:
    """Manage local fault flags and release every resource on stop."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.kind: str | None = None
        self.params: dict[str, Any] = {}
        self.started_at = 0.0
        self._stop_event = threading.Event()
        self._memory_thread: threading.Thread | None = None
        self._memory_chunks: list[bytearray] = []
        self._cpu_workers: list[multiprocessing.Process] = []
        self._db_connection: Any | None = None
        self._db_transaction: Any | None = None
        self._log: Callable[[str, str], None] | None = None

    def start(
        self,
        kind: str,
        params: dict[str, Any] | None = None,
        log: Callable[[str, str], None] | None = None,
    ) -> None:
        self.stop()
        with self.lock:
            self.kind = kind
            self.params = dict(params or {})
            self.started_at = time.monotonic()
            self._log = log
            self._stop_event.clear()

            if kind == "cpu_hog":
                process_count = max(1, round((os.cpu_count() or 1) * 0.5))
                self._cpu_workers = [
                    multiprocessing.Process(target=_cpu_worker, daemon=True)
                    for _ in range(process_count)
                ]
                for worker in self._cpu_workers:
                    worker.start()
            elif kind == "memory_leak":
                cap_mb = max(20, min(int(self.params.get("cap_mb", 800)), 800))
                rate_mb = max(1, min(int(self.params.get("rate_mb", 20)), 20))
                self._memory_thread = threading.Thread(
                    target=self._allocate_memory, args=(cap_mb, rate_mb), daemon=True
                )
                self._memory_thread.start()
            elif kind == "db_lock":
                self._hold_database_lock()

    def _allocate_memory(self, cap_mb: int, rate_mb: int) -> None:
        allocated_mb = 0
        while not self._stop_event.is_set() and allocated_mb < cap_mb:
            chunk_mb = min(rate_mb, cap_mb - allocated_mb)
            try:
                self._memory_chunks.append(bytearray(chunk_mb * 1024 * 1024))
                allocated_mb += chunk_mb
                if self._log:
                    self._log("WARN", f"Memory allocation slowed as process footprint reached {allocated_mb}MB")
            except MemoryError:
                if self._log:
                    self._log("ERROR", "Memory allocation slowed after allocation failure")
                return
            self._stop_event.wait(1.0)

    def _hold_database_lock(self) -> None:
        from sqlalchemy import create_engine, text

        from app.config import get_settings

        engine = create_engine(get_settings().database_url, pool_pre_ping=True)
        self._db_connection = engine.connect()
        self._db_transaction = self._db_connection.begin()
        self._db_connection.execute(text("LOCK TABLE testbed.orders IN ACCESS EXCLUSIVE MODE"))
        self._db_engine = engine

    def stop(self) -> None:
        with self.lock:
            self._stop_event.set()
            for worker in self._cpu_workers:
                try:
                    if worker.is_alive():
                        worker.terminate()
                    worker.join(timeout=1)
                    if worker.is_alive():
                        worker.kill()
                        worker.join(timeout=1)
                except Exception:
                    pass
            self._cpu_workers.clear()
            if self._memory_thread and self._memory_thread.is_alive():
                self._memory_thread.join(timeout=1)
            self._memory_thread = None
            self._memory_chunks.clear()
            if self._db_transaction is not None:
                self._db_transaction.rollback()
                self._db_transaction = None
            if self._db_connection is not None:
                self._db_connection.close()
                self._db_connection = None
            if getattr(self, "_db_engine", None) is not None:
                self._db_engine.dispose()
                self._db_engine = None
            self.kind = None
            self.params = {}
            self.started_at = 0.0
            self._log = None

    def snapshot(self) -> tuple[str | None, dict[str, Any]]:
        with self.lock:
            return self.kind, dict(self.params)