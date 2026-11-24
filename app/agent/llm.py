"""LLM provider clients for Gemini and deterministic offline runs."""
from __future__ import annotations

import json
import logging
import random
import time
from typing import Any

from app.config import get_settings
from app.agent.tools import get_host_metrics, get_network_stats

logger = logging.getLogger(__name__)


class LLMConfigurationError(RuntimeError):
    """Raised when a provider call cannot be configured."""


class GeminiClient:
    """Gemini client using the google-genai SDK, loaded only when called."""

    def __init__(self, api_key: str, model: str, retries: int = 6) -> None:
        self._api_key = api_key
        self._model = model
        self._retries = retries
        self._client: Any | None = None
        self.last_model_used: str = model

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

        settings = get_settings()
        fb_raw = getattr(settings, "gemini_fallback_models", "")
        fallback_candidates = [m.strip() for m in fb_raw.split(",") if m.strip()]
        models_to_try = [self._model] + [m for m in fallback_candidates if m != self._model]

        for model_idx, model_name in enumerate(models_to_try):
            for attempt in range(self._retries):
                try:
                    response = client.models.generate_content(
                        model=model_name,
                        contents=contents,
                        config=types.GenerateContentConfig(**config),
                    )
                    self.last_model_used = model_name
                    return response.text or ""
                except Exception as exc:
                    status = self._status_code(exc)
                    if status == 404:
                        raise RuntimeError(
                            f"Model {model_name} is not available for this key. Run "
                            "python scripts/list_models.py and update GEMINI_MODEL in .env."
                        ) from exc
                    if not self._is_retryable(exc):
                        raise
                    if attempt >= self._retries - 1:
                        if model_idx + 1 < len(models_to_try):
                            next_model = models_to_try[model_idx + 1]
                            logger.warning(
                                "Model %s exhausted %d attempts (%s); falling back to %s",
                                model_name, self._retries, exc, next_model,
                            )
                            break
                        raise
                    delay = min(60.0, 2.0 * (2 ** attempt)) + random.uniform(0.1, 1.0)
                    logger.warning(
                        "Transient error (%s) calling %s (attempt %d/%d). Retrying in %.2fs...",
                        exc, model_name, attempt + 1, self._retries, delay,
                    )
                    time.sleep(delay)
        raise RuntimeError("Gemini generation failed without a response")

    def call_raw(
        self,
        contents: list[Any],
        system_prompt: str | None = None,
        tools: list[Any] | None = None,
        disable_automatic_function_calling: bool = False,
        response_mime_type: str | None = None,
        response_schema: Any | None = None,
    ) -> Any:
        """Call Gemini model with explicit config options and retry logic."""
        client = self._get_client()
        from google.genai import types

        config_dict: dict[str, Any] = {}
        if system_prompt:
            config_dict["system_instruction"] = system_prompt
        if tools is not None:
            config_dict["tools"] = tools
        if disable_automatic_function_calling:
            config_dict["automatic_function_calling"] = types.AutomaticFunctionCallingConfig(
                disable=True
            )
        if response_mime_type:
            config_dict["response_mime_type"] = response_mime_type
        if response_schema is not None:
            config_dict["response_schema"] = response_schema

        config = types.GenerateContentConfig(**config_dict)

        settings = get_settings()
        fb_raw = getattr(settings, "gemini_fallback_models", "")
        fallback_candidates = [m.strip() for m in fb_raw.split(",") if m.strip()]
        models_to_try = [self._model] + [m for m in fallback_candidates if m != self._model]

        for model_idx, model_name in enumerate(models_to_try):
            for attempt in range(self._retries):
                try:
                    response = client.models.generate_content(
                        model=model_name,
                        contents=contents,
                        config=config,
                    )
                    self.last_model_used = model_name
                    return response
                except Exception as exc:
                    status = self._status_code(exc)
                    if status == 404:
                        raise RuntimeError(
                            f"Model {model_name} is not available for this key. Run "
                            "python scripts/list_models.py and update GEMINI_MODEL in .env."
                        ) from exc
                    if not self._is_retryable(exc):
                        raise
                    if attempt >= self._retries - 1:
                        if model_idx + 1 < len(models_to_try):
                            next_model = models_to_try[model_idx + 1]
                            logger.warning(
                                "Model %s exhausted %d attempts (%s); falling back to %s",
                                model_name, self._retries, exc, next_model,
                            )
                            break
                        raise
                    delay = min(60.0, 2.0 * (2 ** attempt)) + random.uniform(0.1, 1.0)
                    logger.warning(
                        "Transient error (%s) calling %s (attempt %d/%d). Retrying in %.2fs...",
                        exc, model_name, attempt + 1, self._retries, delay,
                    )
                    time.sleep(delay)
        raise RuntimeError("Gemini API call failed without a response")

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
        if status_code is not None:
            return status_code == 429 or 500 <= status_code <= 599
        err_msg = str(error).lower()
        return "503" in err_msg or "429" in err_msg or "unavailable" in err_msg or "resource_exhausted" in err_msg


class MockLLM:
    """Deterministic rule-based agent used with LLM_PROVIDER=mock.

    Calls get_host_metrics and get_network_stats once each, then decides by rules:
    - high cpu -> cpu
    - mem slope rising or high mem -> memory
    - probe failures with timeouts -> packet_loss
    - blocked-query/slow-query templates with normal cpu -> db_lock
    - uniformly high latency with normal cpu and few errors -> network_latency
    - otherwise -> unknown
    """

    def __init__(self) -> None:
        self.last_model_used = "mock"

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
            return json.dumps(
                {
                    "root_cause": "Mock analysis: review the incident evidence and recent changes.",
                    "category": "unknown",
                    "confidence": 0.0,
                    "evidence": ["Baseline mock test"],
                    "alternatives": [],
                    "remediation": [{"step": "Check service logs", "risk": "low"}],
                }
            )
        return "Mock response: no external model was called."

    def investigate(
        self,
        evidence_pack: dict[str, Any],
        session: Any | None = None,
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Execute deterministic rule-based investigation for the evidence pack."""
        services = evidence_pack.get("affected_services") or ["api-service"]
        service = services[0] if services else "api-service"
        start = evidence_pack.get("started_at") or "2026-01-01T00:00:00+00:00"
        end = evidence_pack.get("ended_at") or start

        # Check if hand-built metrics/stats were passed directly in evidence_pack
        if "host_metrics" in evidence_pack:
            host_metrics = evidence_pack["host_metrics"]
        else:
            host_metrics = get_host_metrics(service, start, end, session=session)

        if "network_stats" in evidence_pack:
            network_stats = evidence_pack["network_stats"]
        else:
            network_stats = get_network_stats(service, start, end, session=session)

        tool_calls_trace = [
            {
                "tool": "get_host_metrics",
                "args": {"service": service, "start": start, "end": end},
                "result": host_metrics,
            },
            {
                "tool": "get_network_stats",
                "args": {"service": service, "start": start, "end": end},
                "result": network_stats,
            },
        ]

        incident_w = host_metrics.get("incident_window", {})
        baseline_w = host_metrics.get("baseline_window", {})

        cpu_stats = incident_w.get("cpu_percent", {})
        baseline_cpu = baseline_w.get("cpu_percent", {})
        mem_stats = incident_w.get("mem_percent", {})
        p95_stats = incident_w.get("p95_ms", {})
        err_stats = incident_w.get("error_count", {})

        probes = network_stats.get("probes", {})
        during_probes = probes.get("during", {})
        probe_failure_rate = float(during_probes.get("failure_rate", 0.0))
        mean_probe_lat = during_probes.get("mean_probe_ms")

        cpu_mean = float(cpu_stats.get("mean", 0.0))
        cpu_max = float(cpu_stats.get("max", 0.0))
        mem_slope = float(mem_stats.get("slope_per_minute", 0.0))
        mem_mean = float(mem_stats.get("mean", 0.0))
        mem_max = float(mem_stats.get("max", 0.0))
        p95_mean = float(p95_stats.get("mean", 0.0))
        p95_max = float(p95_stats.get("max", 0.0))
        err_mean = float(err_stats.get("mean", 0.0))

        in_flight_stats = incident_w.get("in_flight_requests", {})
        in_flight_max = float(in_flight_stats.get("max", 0.0))
        req_count_stats = incident_w.get("request_count_30s", {})
        req_count_mean = float(req_count_stats.get("mean", 0.0))

        # Check template texts for database locks / slow queries
        template_texts: list[str] = []
        detail = evidence_pack.get("top_templates_detail") or []
        for d in detail:
            if isinstance(d, dict) and "template_text" in d:
                template_texts.append(str(d["template_text"]).lower())
        top_tpls = evidence_pack.get("top_templates") or []
        if isinstance(top_tpls, list):
            for item in top_tpls:
                if isinstance(item, dict):
                    if "text" in item:
                        template_texts.append(str(item["text"]).lower())
                    if "template" in item:
                        template_texts.append(str(item["template"]).lower())
        elif isinstance(top_tpls, dict):
            for k in top_tpls.keys():
                template_texts.append(str(k).lower())

        has_db_lock_signal = any(
            any(needle in text for needle in ("lock", "deadlock", "blocked", "slow query", "lock wait", "resumed after database lock wait"))
            for text in template_texts
        ) or bool(evidence_pack.get("has_db_lock"))

        # Deterministic Rules
        # Rule 1: High CPU
        if cpu_mean >= 70.0 or cpu_max >= 80.0 or (cpu_mean >= 50.0 and cpu_mean - float(baseline_cpu.get("mean", 0.0)) >= 30.0):
            return {
                "category": "cpu",
                "root_cause": "CPU starvation caused by computation-heavy workload or busy spin loop",
                "confidence": 0.9,
                "evidence": [
                    f"CPU elevated to mean {cpu_mean:.1f}% / max {cpu_max:.1f}% during incident (baseline {baseline_cpu.get('mean', 0.0):.1f}%)"
                ],
                "alternatives": [
                    {"category": "application_bug", "reason": "Inefficient algorithm or busy-waiting thread loop"}
                ],
                "remediation": [
                    {"step": "Scale out service replicas or increase CPU allocation", "risk": "low"},
                    {"step": "Profile hot methods to identify computational bottleneck", "risk": "med"},
                ],
            }, tool_calls_trace

        # Rule 2: Memory slope rising or high memory
        if mem_slope > 0.05 or mem_mean >= 70.0 or mem_max >= 80.0 or bool(evidence_pack.get("has_memory_leak")):
            return {
                "category": "memory",
                "root_cause": "Memory leak or memory exhaustion due to uncollected object accumulation",
                "confidence": 0.9,
                "evidence": [
                    f"Memory slope increased at {mem_slope:.4f}%/min reaching max {mem_max:.1f}%"
                ],
                "alternatives": [
                    {"category": "application_bug", "reason": "Unbounded cache or object collection retention"}
                ],
                "remediation": [
                    {"step": "Restart service container to immediately reclaim system memory", "risk": "med"},
                    {"step": "Capture and inspect heap dump for retained references", "risk": "low"},
                ],
            }, tool_calls_trace

        # Rule 3: Probe failures with timeouts -> packet_loss
        if probe_failure_rate > 0.10 or bool(evidence_pack.get("has_packet_loss")):
            return {
                "category": "packet_loss",
                "root_cause": "Network packet loss causing dropped connections and request timeouts",
                "confidence": 0.85,
                "evidence": [
                    f"Health check probe failure rate of {probe_failure_rate*100:.1f}% with connection timeouts"
                ],
                "alternatives": [
                    {"category": "network_latency", "reason": "Severe network latency causing client timeouts"}
                ],
                "remediation": [
                    {"step": "Inspect network interfaces, drop counters, and packet filter rules", "risk": "low"},
                    {"step": "Reroute traffic around degraded network segments", "risk": "med"},
                ],
            }, tool_calls_trace

        # Rule 4: in_flight high with low CPU and few completed requests, or lock templates -> db_lock
        if (
            (in_flight_max >= 5 and cpu_mean < 50.0 and req_count_mean < 25)
            or (has_db_lock_signal and cpu_mean < 70.0)
            or bool(evidence_pack.get("has_db_lock"))
        ):
            return {
                "category": "db_lock",
                "root_cause": "Database lock contention blocking transaction execution and request completion",
                "confidence": 0.9,
                "evidence": [
                    f"In-flight requests reached {in_flight_max:.0f} with low CPU ({cpu_mean:.1f}%) and low request completion ({req_count_mean:.1f}/30s)"
                ],
                "alternatives": [
                    {"category": "application_bug", "reason": "Long-running uncommitted transactions holding locks"}
                ],
                "remediation": [
                    {"step": "Identify and terminate blocking queries via pg_stat_activity", "risk": "med"},
                    {"step": "Review transaction boundaries and acquire locks in consistent order", "risk": "low"},
                ],
            }, tool_calls_trace

        # Rule 5: Uniformly high latency with normal CPU and few errors -> network_latency
        if (
            cpu_mean < 70.0
            and probe_failure_rate <= 0.10
            and err_mean < 5.0
            and (p95_max >= 300.0 or (mean_probe_lat is not None and mean_probe_lat >= 300.0) or bool(evidence_pack.get("has_latency")))
        ):
            return {
                "category": "network_latency",
                "root_cause": "Elevated network latency causing high end-to-end request duration",
                "confidence": 0.85,
                "evidence": [
                    f"P95 latency elevated to {p95_max:.1f}ms while CPU and error rates remained normal"
                ],
                "alternatives": [
                    {"category": "application_bug", "reason": "Downstream dependency latency degradation"}
                ],
                "remediation": [
                    {"step": "Check upstream network hops and egress latency across routing paths", "risk": "low"},
                    {"step": "Adjust client request timeouts and connection pooling parameters", "risk": "low"},
                ],
            }, tool_calls_trace

        # Fallback: unknown
        return {
            "category": "unknown",
            "root_cause": "Insufficient evidence to distinguish between fault categories",
            "confidence": 0.0,
            "evidence": ["Metrics and logs did not exhibit distinctive anomalous patterns"],
            "alternatives": [],
            "remediation": [{"step": "Perform manual log inspection and service health audit", "risk": "low"}],
        }, tool_calls_trace


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
