"""
app/config.py
=============
Central configuration loaded from environment variables / .env file.

All other modules should import `settings` from here rather than
reading os.environ directly.
"""
from __future__ import annotations

import os
from functools import lru_cache
from typing import Any

from dotenv import load_dotenv

# Load .env early so os.getenv calls below see the values.
load_dotenv()


class Settings:
    """Application settings populated from environment variables."""

    def __init__(self) -> None:
        self.llm_provider = os.getenv("LLM_PROVIDER", "gemini")
        self.gemini_api_key = os.getenv("GEMINI_API_KEY", "your_key_here")
        self.gemini_model = os.getenv("GEMINI_MODEL", "gemini-3.8-flash")
        self.database_url = os.getenv(
            "DATABASE_URL",
            "postgresql+psycopg://postgres:postgres@localhost:5432/incident_copilot",
        )
        self.window_seconds = int(os.getenv("WINDOW_SECONDS", "10"))
        self.warmup_windows = int(os.getenv("WARMUP_WINDOWS", "30"))
        self.anomaly_gap_seconds = int(os.getenv("ANOMALY_GAP_SECONDS", "30"))
        self.top_k_templates = int(os.getenv("TOP_K_TEMPLATES", "30"))
        self.window_grace_seconds = int(os.getenv("WINDOW_GRACE_SECONDS", "5"))
        self.hst_n_trees = int(os.getenv("HST_N_TREES", "25"))
        self.hst_height = int(os.getenv("HST_HEIGHT", "15"))
        self.hst_window_size = int(os.getenv("HST_WINDOW_SIZE", "100"))
        self.threshold_lookback = int(os.getenv("THRESHOLD_LOOKBACK", "200"))
        self.threshold_std_mult = float(os.getenv("THRESHOLD_STD_MULT", "3.0"))
        self.threshold_ceiling = float(os.getenv("THRESHOLD_CEILING", "0.98"))
        self.suspect_windows = int(os.getenv("SUSPECT_WINDOWS", "3"))
        self.consecutive_high_windows = int(os.getenv("CONSECUTIVE_HIGH_WINDOWS", "2"))
        self.incident_cooldown_seconds = int(os.getenv("INCIDENT_COOLDOWN_SECONDS", "60"))
        self.max_agent_tool_calls = int(os.getenv("MAX_AGENT_TOOL_CALLS", "5"))
        self.drain_state_path = os.getenv("DRAIN_STATE_PATH", "data/drain_state.bin")
        self.metrics_sample_seconds = int(os.getenv("METRICS_SAMPLE_SECONDS", "5"))
        self.record_events_path = os.getenv("RECORD_EVENTS_PATH", "")
        self.services = [
            s.strip()
            for s in os.getenv("SERVICES", "api-service,worker-service").split(",")
            if s.strip()
        ]

    @property
    def effective_hst_window_size(self) -> int:
        """Effective HST window size.

        River HalfSpaceTrees scores are not meaningful until the first HST_WINDOW_SIZE
        windows have been learned to populate reference tree depths and mass distributions.
        During warm-up (WARMUP_WINDOWS), we cap the effective HST window size so the model
        completes its reference window well before warm-up ends:
            effective_hst_window = min(HST_WINDOW_SIZE, max(5, WARMUP_WINDOWS // 2))
        """
        return min(self.hst_window_size, max(5, self.warmup_windows // 2))

    def __getattr__(self, name: str) -> Any:
        lower = name.lower()
        if lower in self.__dict__:
            return self.__dict__[lower]
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance (cached after first call)."""
    return Settings()


# Convenience singleton imported by other modules.
settings: Settings = get_settings()
