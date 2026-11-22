"""
app/parsing/parser.py
=====================
Drain3 log-template miner wrapping TemplateMiner with programmatic configuration,
regex-based token masking, state persistence, duration extraction, and thread safety.
"""
from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from pathlib import Path

from drain3.file_persistence import FilePersistence
from drain3.masking import MaskingInstruction
from drain3.template_miner import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig

from app.config import settings

# Extract the FIRST "<number>ms" or "<number>s" duration from the raw message BEFORE masking.
_DURATION_REGEX = re.compile(r"\b(\d+(?:\.\d+)?)\s*(ms|s)\b")


def extract_duration_ms(message: str) -> float | None:
    """Extract the first duration in ms or s from a raw message, converted to milliseconds."""
    match = _DURATION_REGEX.search(message)
    if not match:
        return None
    value = float(match.group(1))
    unit = match.group(2)
    return value if unit == "ms" else value * 1000.0


@dataclass
class ParsedLog:
    """Structured representation of a log message extracted by Drain3."""

    template_id: int
    template_text: str
    params: list[str]
    duration_ms: float | None
    is_new_template: bool


class LogParser:
    """Thread-safe Drain3 log parser with masking, persistence, and duration extraction.

    Note: Drain3 can later generalise a template's text while its cluster id stays stable,
    so downstream code must key on template_id, never on text.
    """

    def __init__(
        self, state_path: str | Path | None = None, persistence: bool = True
    ) -> None:
        if not persistence or state_path == "":
            self.state_path: Path | None = None
        elif state_path is None:
            resolved = settings.drain_state_path
            self.state_path = Path(resolved) if resolved else None
        else:
            self.state_path = Path(state_path)

        self._lock = threading.Lock()
        self._persistence: FilePersistence | None = None
        self._miner: TemplateMiner = self._build_miner()

    def _build_miner(self) -> TemplateMiner:
        config = TemplateMinerConfig()
        config.drain_sim_th = 0.4
        config.drain_depth = 6
        config.drain_max_clusters = 1000

        # Masking instructions applied in exact specified order:
        # 1. UUIDs
        # 2. IPv4 addresses
        # 3. hex ids (0x...)
        # 4. durations like "35ms" or "1.2s"
        # 5. HTTP numeric path segments (e.g. /users/42 -> /users/<NUM>)
        # 6. plain numbers
        # Mask names: <UUID>, <IP>, <HEX>, <DUR>, <NUM>
        config.masking_instructions = [
            MaskingInstruction(
                r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b",
                "UUID",
            ),
            MaskingInstruction(r"\b(?:\d{1,3}\.){3}\d{1,3}\b", "IP"),
            MaskingInstruction(r"\b0x[0-9a-fA-F]+\b", "HEX"),
            MaskingInstruction(r"\b\d+(?:\.\d+)?(?:ms|s)\b", "DUR"),
            MaskingInstruction(r"(?<=/)\d+\b", "NUM"),
            MaskingInstruction(r"\b\d+(?:\.\d+)?\b", "NUM"),
        ]

        if self.state_path is not None:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            self._persistence = FilePersistence(str(self.state_path))
        else:
            self._persistence = None

        return TemplateMiner(persistence_handler=self._persistence, config=config)

    def parse(self, message: str) -> ParsedLog:
        """Parse a raw log message into structured template and variable parameters.

        Note: Drain3 can later generalise a template's text while its cluster id stays stable,
        so downstream code must key on template_id, never on text.
        """
        duration_ms = extract_duration_ms(message)

        with self._lock:
            result = self._miner.add_log_message(message)
            template_id = int(result["cluster_id"])
            template_text = str(result["template_mined"])
            is_new = bool(result["change_type"] == "cluster_created")

            extracted = self._miner.extract_parameters(template_text, message)
            if extracted is None:
                extracted = self._miner.extract_parameters(
                    template_text, message, exact_matching=False
                )
            params = [param.value for param in extracted] if extracted else []

        return ParsedLog(
            template_id=template_id,
            template_text=template_text,
            params=params,
            duration_ms=duration_ms,
            is_new_template=is_new,
        )

    def reset(self) -> None:
        """Delete persisted state and reset the template miner to clean state."""
        with self._lock:
            if self.state_path is not None and self.state_path.exists():
                self.state_path.unlink()
            self._miner = self._build_miner()

    def save(self) -> None:
        """Explicitly persist the miner state to disk."""
        with self._lock:
            if self._persistence is not None:
                self._miner.save_state("manual_save")

    @property
    def cluster_count(self) -> int:
        """Return the number of clusters currently tracked."""
        with self._lock:
            return len(self._miner.drain.clusters)
