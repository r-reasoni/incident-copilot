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


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton Settings instance (cached after first call)."""
    return Settings()


# Convenience singleton imported by other modules.
settings: Settings = get_settings()
