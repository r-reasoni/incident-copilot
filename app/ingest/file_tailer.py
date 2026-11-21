"""Follow a plain testbed log file and ship parsed events to the ingestion API."""
from __future__ import annotations

import argparse
import asyncio
import re
import socket
from collections import deque
from datetime import datetime
from pathlib import Path

import httpx

LOG_PATTERN = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) "
    r"\[(?P<service>[^]]+)\] (?P<level>[A-Z]+) (?P<message>.*)$"
)
SHIP_URL = "http://127.0.0.1:8000/ingest/logs"
MAX_PENDING = 1000


def parse_log_line(line: str, service: str, host: str | None = None) -> dict | None:
    match = LOG_PATTERN.match(line.rstrip("\r\n"))
    if match is None or not match.group("message").strip():
        return None
    timestamp = datetime.strptime(match.group("ts"), "%Y-%m-%d %H:%M:%S,%f").astimezone()
    return {
        "ts": timestamp.isoformat(),
        "service": service,
        "host": host or socket.gethostname(),
        "level": match.group("level"),
        "message": match.group("message"),
    }


async def follow(path: Path, service: str, poll_seconds: float = 0.2) -> None:
    pending: deque[dict] = deque(maxlen=MAX_PENDING)
    current_file = None
    identity: tuple[int, int] | None = None
    position = 0
    last_send = asyncio.get_running_loop().time()
    async with httpx.AsyncClient(timeout=1.0) as client:
        try:
            while True:
                try:
                    stat = path.stat()
                    new_identity = (stat.st_dev, stat.st_ino)
                    rotated = identity != new_identity or stat.st_size < position
                    if current_file is None or rotated:
                        if current_file is not None:
                            current_file.close()
                        current_file = path.open("r", encoding="utf-8", errors="replace")
                        identity = new_identity
                        position = 0
                        current_file.seek(0, 2)
                        position = current_file.tell()
                except FileNotFoundError:
                    if current_file is not None:
                        current_file.close()
                        current_file = None
                        identity = None
                        position = 0

                if current_file is not None:
                    while line := current_file.readline():
                        position = current_file.tell()
                        event = parse_log_line(line, service)
                        if event is not None:
                            pending.append(event)

                now = asyncio.get_running_loop().time()
                if pending and (len(pending) >= 50 or now - last_send >= 1.0):
                    batch = list(pending)[:50]
                    try:
                        response = await client.post(SHIP_URL, json=batch)
                        response.raise_for_status()
                    except httpx.HTTPError:
                        pass
                    else:
                        for _ in batch:
                            pending.popleft()
                    last_send = now
                await asyncio.sleep(poll_seconds)
        finally:
            if current_file is not None:
                current_file.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Follow and ship a plain application log file")
    parser.add_argument("file", type=Path)
    parser.add_argument("--service", required=True)
    args = parser.parse_args()
    try:
        asyncio.run(follow(args.file, args.service))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()