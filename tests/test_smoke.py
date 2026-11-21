"""
tests/test_smoke.py
===================
Trivial smoke tests that verify the project skeleton is importable and the
Settings object loads expected defaults.  No external services are required.
"""
from __future__ import annotations


def test_package_importable() -> None:
    """The top-level `app` package must be importable without errors."""
    import app  # noqa: F401


def test_config_defaults(monkeypatch) -> None:
    """
    Settings must expose the correct default values when no .env is present.
    These defaults mirror what is documented in .env.example.
    """
    from app.config import Settings

    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("GEMINI_API_KEY", "your_key_here")
    monkeypatch.setenv("GEMINI_MODEL", "gemini-2.5-flash")
    monkeypatch.setenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5432/incident_copilot",
    )
    monkeypatch.setenv("WINDOW_SECONDS", "10")
    monkeypatch.setenv("WARMUP_WINDOWS", "30")
    monkeypatch.setenv("ANOMALY_GAP_SECONDS", "30")

    s = Settings()

    assert s.llm_provider == "gemini"
    assert s.gemini_api_key == "your_key_here"
    assert s.gemini_model == "gemini-2.5-flash"
    assert s.database_url == (
        "postgresql+psycopg://postgres:postgres@localhost:5432/incident_copilot"
    )
    assert s.window_seconds == 10
    assert s.warmup_windows == 30
    assert s.anomaly_gap_seconds == 30


def test_config_loads_environment_values(monkeypatch) -> None:
    from app.config import Settings

    values = {
        "LLM_PROVIDER": "mock",
        "GEMINI_API_KEY": "your_key_here",
        "GEMINI_MODEL": "gemini-2.5-flash",
        "DATABASE_URL": "postgresql+psycopg://postgres:postgres@localhost:5432/incident_copilot",
        "WINDOW_SECONDS": "10",
        "WARMUP_WINDOWS": "30",
        "ANOMALY_GAP_SECONDS": "30",
    }
    for key, value in values.items():
        monkeypatch.setenv(key, value)

    settings = Settings()
    assert settings.llm_provider == "mock"
    assert settings.database_url == values["DATABASE_URL"]
    assert settings.window_seconds == 10
    assert settings.warmup_windows == 30
    assert settings.anomaly_gap_seconds == 30


def test_settings_singleton_is_cached() -> None:
    """get_settings() must return the same object on repeated calls."""
    from app.config import get_settings

    assert get_settings() is get_settings()


def test_missing_gemini_key_fails_only_on_call(monkeypatch) -> None:
    import pytest

    from app.agent.llm import LLMConfigurationError, get_llm
    from app.config import get_settings

    monkeypatch.setenv("LLM_PROVIDER", "gemini")
    monkeypatch.setenv("GEMINI_API_KEY", "your_key_here")
    get_settings.cache_clear()
    try:
        client = get_llm()
        with pytest.raises(LLMConfigurationError, match="GEMINI_API_KEY"):
            client.generate("system", [{"role": "user", "content": "hello"}])
    finally:
        get_settings.cache_clear()


def test_mock_llm_is_rule_based_and_offline() -> None:
    from app.agent.llm import MockLLM

    llm = MockLLM()
    assert llm.generate("", [{"role": "user", "content": "Reply with the single word OK"}]) == "OK"
    assert "no external model" in llm.generate("", [{"role": "user", "content": "hello"}])


def test_subpackages_importable() -> None:
    """Every sub-package __init__ must be importable without errors."""
    import app.agent  # noqa: F401
    import app.api  # noqa: F401
    import app.db  # noqa: F401
    import app.detection  # noqa: F401
    import app.incidents  # noqa: F401
    import app.ingest  # noqa: F401
    import app.parsing  # noqa: F401
    import cli  # noqa: F401
    import eval  # noqa: F401
    import testbed  # noqa: F401
