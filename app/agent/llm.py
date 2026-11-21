"""LLM provider clients for Gemini and deterministic offline runs."""
from __future__ import annotations

import time
from typing import Any

from app.config import get_settings


class LLMConfigurationError(RuntimeError):
    """Raised when a provider call cannot be configured."""


class GeminiClient:
    """Gemini client using the google-genai SDK, loaded only when called."""

    def __init__(self, api_key: str, model: str, retries: int = 3) -> None:
        self._api_key = api_key
        self._model = model
        self._retries = retries
        self._client: Any | None = None

    def _get_client(self) -> Any:
        if not self._api_key or self._api_key == "your_key_here":
            raise LLMConfigurationError(
                "GEMINI_API_KEY is missing or still set to your_key_here; "
                "set it in .env before making a Gemini call."
            )
        if self._client is None:
            try:
                from google import genai
            except ImportError as exc:
                raise LLMConfigurationError(
                    "Gemini support requires the google-genai package."
                ) from exc
            self._client = genai.Client(api_key=self._api_key)
        return self._client

    def generate(
        self,
        system_prompt: str,
        messages: list[dict[str, str]],
        tools: list[Any] | None = None,
    ) -> str:
        client = self._get_client()
        from google.genai import types

        contents = [
            {
                "role": "model" if message.get("role") == "assistant" else message.get("role", "user"),
                "parts": [{"text": message.get("content", "")}],
            }
            for message in messages
        ]
        config: dict[str, Any] = {"system_instruction": system_prompt}
        if tools is not None:
            config["tools"] = tools

        for attempt in range(self._retries + 1):
            try:
                response = client.models.generate_content(
                    model=self._model,
                    contents=contents,
                    config=types.GenerateContentConfig(**config),
                )
                return response.text or ""
            except Exception as exc:
                if self._status_code(exc) == 404:
                    raise RuntimeError(
                        f"Model {self._model} is not available for this key. Run "
                        "python scripts/list_models.py and update GEMINI_MODEL in .env."
                    ) from exc
                if attempt >= self._retries or not self._is_retryable(exc):
                    raise
                time.sleep(2**attempt)
        raise RuntimeError("Gemini generation failed without a response")

    @staticmethod
    def _status_code(error: Exception) -> int | None:
        status = getattr(error, "status_code", None)
        if status is None:
            status = getattr(error, "code", None)
        try:
            return int(status)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _is_retryable(error: Exception) -> bool:
        status_code = GeminiClient._status_code(error)
        if status_code is None:
            return False
        return status_code == 429 or 500 <= status_code <= 599


class MockLLM:
    """Rule-based local response generator that never makes network calls."""

    def generate(
        self,
        system_prompt: str,
        messages: list[dict[str, str]],
        tools: list[Any] | None = None,
    ) -> str:
        del system_prompt, tools
        request = " ".join(message.get("content", "") for message in messages)
        if "single word ok" in request.lower():
            return "OK"
        if "root cause" in request.lower() or "incident" in request.lower():
            return "Mock analysis: review the incident evidence and recent changes."
        return "Mock response: no external model was called."


def get_llm() -> GeminiClient | MockLLM:
    """Build the configured provider without requiring credentials up front."""
    settings = get_settings()
    provider = settings.llm_provider.lower()
    if provider == "gemini":
        return GeminiClient(settings.gemini_api_key, settings.gemini_model)
    if provider == "mock":
        return MockLLM()
    raise LLMConfigurationError(
        f"Unsupported LLM_PROVIDER '{settings.llm_provider}'; choose 'gemini' or 'mock'."
    )


def generate(
    system_prompt: str,
    messages: list[dict[str, str]],
    tools: list[Any] | None = None,
) -> str:
    """Generate a response using the provider selected by LLM_PROVIDER."""
    return get_llm().generate(system_prompt, messages, tools)