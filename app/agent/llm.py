"""LLM provider clients for Gemini and deterministic offline runs."""
from __future__ import annotations

import json
import logging
import random
import re
import time
from typing import Any, Callable

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
        self.calls_by_model: dict[str, int] = {}

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

    def _execute_with_retry(self, call_fn: Callable[[Any, str], Any]) -> Any:
        """Execute a Gemini API call with daily-quota fast-fallback and retry-delay handling."""
        client = self._get_client()
        settings = get_settings()
        fb_raw = getattr(settings, "gemini_fallback_models", "")
        fallback_candidates = [m.strip() for m in fb_raw.split(",") if m.strip()]
        models_to_try = [self._model] + [m for m in fallback_candidates if m != self._model]

        for model_idx, model_name in enumerate(models_to_try):
            for attempt in range(self._retries):
                self.calls_by_model[model_name] = self.calls_by_model.get(model_name, 0) + 1
                try:
                    response = call_fn(client, model_name)
                    self.last_model_used = model_name
                    logger.info(
                        "Gemini response received from %s (calls_by_model=%s)",
                        model_name,
                        self.calls_by_model,
                    )
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
                    if self._is_daily_quota_exhausted(exc):
                        if model_idx + 1 < len(models_to_try):
                            next_model = models_to_try[model_idx + 1]
                            logger.warning(
                                "Model %s hit daily quota (%s); falling back immediately to %s",
                                model_name,
                                exc,
                                next_model,
                            )
                            break
                        raise
                    if attempt >= self._retries - 1:
                        if model_idx + 1 < len(models_to_try):
                            next_model = models_to_try[model_idx + 1]
                            logger.warning(
                                "Model %s exhausted %d attempts (%s); falling back to %s",
                                model_name,
                                self._retries,
                                exc,
                                next_model,
                            )
                            break
                        raise
                    parsed_delay = self._extract_retry_delay(exc)
                    if parsed_delay is not None:
                        delay = parsed_delay
                    else:
                        delay = min(60.0, 2.0 * (2 ** attempt)) + random.uniform(0.1, 1.0)
                    logger.warning(
                        "Transient error (%s) calling %s (attempt %d/%d). Retrying in %.2fs...",
                        exc,
                        model_name,
                        attempt + 1,
                        self._retries,
                        delay,
                    )
                    time.sleep(delay)
        raise RuntimeError("Gemini API call failed without a response")

    def generate(
        self,
        system_prompt: str,
        messages: list[dict[str, str]],
        tools: list[Any] | None = None,
    ) -> str:
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

        response = self._execute_with_retry(
            lambda client, model_name: client.models.generate_content(
                model=model_name,
                contents=contents,
                config=types.GenerateContentConfig(**config),
            )
        )
        return response.text or ""

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

        return self._execute_with_retry(
            lambda client, model_name: client.models.generate_content(
                model=model_name,
                contents=contents,
                config=config,
            )
        )

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

    @staticmethod
    def _is_daily_quota_exhausted(error: Exception) -> bool:
        status_code = GeminiClient._status_code(error)
        err_msg = str(error).lower()
        is_429 = status_code == 429 or "429" in err_msg or "resource_exhausted" in err_msg
        if not is_429:
            return False
        daily_markers = (
            "per-day",
            "per day",
            "daily limit",
            "daily_limit",
            "requestsperday",
            "perday",
        )
        return any(marker in err_msg for marker in daily_markers)

    @staticmethod
    def _extract_retry_delay(error: Exception) -> float | None:
        attr_delay = getattr(error, "retry_delay", None)
        if isinstance(attr_delay, (int, float)) and attr_delay > 0:
            return min(120.0, max(0.1, float(attr_delay)))
        msg = str(error)
        for pattern in (
            r"retry\s+in\s+(\d+(?:\.\d+)?)\s*s",
            r"retrydelay[\"']?\s*[:=]\s*[\"']?(\d+(?:\.\d+)?)\s*s",
        ):
            match = re.search(pattern, msg, re.IGNORECASE)
            if match:
                return min(120.0, max(0.1, float(match.group(1))))
        return None


class MockLLM:
    """Metrics-only heuristic baseline used with LLM_PROVIDER=mock.

    Calls get_host_metrics and get_network_stats once each (never reads log
    template text), then classifies using scale-free thresholds relative to the
    pre-incident baseline window (calibrated on DEV recordings step7 and step7b):
    - cpu: cpu_mean >= 3.0x baseline (with +25pp delta) or cpu_max >= max(75%, 4x baseline)
    - memory: mem_max increase over baseline mean >= 25% relative AND mem_slope > 0.02%/min
      AND mem_mean > baseline mem_mean
    - packet_loss: probe failure rate delta >= +0.08 (or >0.05 with error_count >= 3x baseline)
    - db_lock: in_flight_requests peak >= 4.0x baseline mean while CPU stays normal
    - network_latency: normal CPU, low probe failures, and probe latency >= 3.0x before
      (or p95_ms >= 3.0x baseline with low error rate)
    - otherwise: unknown
    """

    def __init__(self) -> None:
        self.last_model_used = "mock"
        self.calls_by_model: dict[str, int] = {"mock": 0}

    def generate(
        self,
        system_prompt: str,
        messages: list[dict[str, str]],
        tools: list[Any] | None = None,
    ) -> str:
        del system_prompt, tools
        self.calls_by_model["mock"] = self.calls_by_model.get("mock", 0) + 1
        request = " ".join(message.get("content", "") for message in messages)
        if "single word ok" in request.lower():
            return "OK"
        if "root cause" in request.lower() or "incident" in request.lower():
            return json.dumps(
                {
                    "root_cause": "Metrics-only heuristic baseline: review the incident evidence and recent changes.",
                    "category": "unknown",
                    "confidence": 0.0,
                    "evidence": ["Metrics-only heuristic baseline test"],
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
        """Execute deterministic metrics-only heuristic baseline investigation."""
        self.calls_by_model["mock"] = self.calls_by_model.get("mock", 0) + 1
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
        baseline_mem = baseline_w.get("mem_percent", {})
        p95_stats = incident_w.get("p95_ms", {})
        baseline_p95 = baseline_w.get("p95_ms", {})
        err_stats = incident_w.get("error_count", {})
        baseline_err = baseline_w.get("error_count", {})
        in_flight_stats = incident_w.get("in_flight_requests", {})
        baseline_in_flight = baseline_w.get("in_flight_requests", {})
        req_count_stats = incident_w.get("request_count_30s", {})
        baseline_req = baseline_w.get("request_count_30s", {})

        probes = network_stats.get("probes", {})
        before_probes = probes.get("before", {})
        during_probes = probes.get("during", {})
        before_probe_fail = float(before_probes.get("failure_rate", 0.0))
        probe_failure_rate = float(during_probes.get("failure_rate", 0.0))
        before_probe_lat = before_probes.get("mean_probe_ms")
        mean_probe_lat = during_probes.get("mean_probe_ms")

        cpu_mean = float(cpu_stats.get("mean", 0.0))
        cpu_max = float(cpu_stats.get("max", 0.0))
        base_cpu_mean = float(baseline_cpu.get("mean", 0.0))

        mem_slope = float(mem_stats.get("slope_per_minute", 0.0))
        mem_mean = float(mem_stats.get("mean", 0.0))
        mem_max = float(mem_stats.get("max", 0.0))
        base_mem_mean = float(baseline_mem.get("mean", 0.0))

        p95_mean = float(p95_stats.get("mean", 0.0))
        p95_max = float(p95_stats.get("max", 0.0))
        base_p95_mean = float(baseline_p95.get("mean", 0.0))

        err_mean = float(err_stats.get("mean", 0.0))
        base_err_mean = float(baseline_err.get("mean", 0.0))

        in_flight_max = float(in_flight_stats.get("max", 0.0))
        base_in_flight_mean = float(baseline_in_flight.get("mean", 0.0))
        req_count_mean = float(req_count_stats.get("mean", 0.0))
        base_req_mean = float(baseline_req.get("mean", 0.0))

        # Scale-free relative ratios against baseline_window (DEV-calibrated thresholds)
        cpu_ratio = cpu_mean / max(base_cpu_mean, 1.0)
        cpu_delta = cpu_mean - base_cpu_mean
        cpu_normal = cpu_mean < max(40.0, base_cpu_mean * 3.0)

        # Memory relative increase over baseline mean
        mem_rel_inc = (mem_max - base_mem_mean) / max(base_mem_mean, 0.1)

        # Probe failure delta and error ratio
        probe_fail_delta = probe_failure_rate - before_probe_fail
        err_ratio = err_mean / max(base_err_mean, 0.5)

        # In-flight request peak ratio (DB lock / resource blocking)
        in_flight_ratio = in_flight_max / max(base_in_flight_mean, 1.0)

        # Probe latency ratio and p95 latency ratio
        probe_lat_ratio = (
            float(mean_probe_lat) / max(float(before_probe_lat), 1.0)
            if mean_probe_lat is not None and before_probe_lat is not None
            else 0.0
        )
        p95_ratio = p95_mean / max(base_p95_mean, 1.0)

        # Rule 1: High CPU relative to baseline
        if (cpu_ratio >= 3.0 and cpu_delta >= 25.0) or cpu_max >= max(75.0, base_cpu_mean * 4.0):
            return {
                "category": "cpu",
                "root_cause": (
                    "Metrics-only heuristic baseline: CPU utilization spiked relative to baseline"
                ),
                "confidence": 0.9,
                "evidence": [
                    f"CPU elevated to mean {cpu_mean:.1f}% / max {cpu_max:.1f}% "
                    f"(baseline mean {base_cpu_mean:.1f}%, {cpu_ratio:.1f}x)"
                ],
                "alternatives": [
                    {"category": "application_bug", "reason": "Inefficient algorithm or busy-waiting thread loop"}
                ],
                "remediation": [
                    {"step": "Scale out service replicas or increase CPU allocation", "risk": "low"},
                    {"step": "Profile hot methods to identify computational bottleneck", "risk": "med"},
                ],
            }, tool_calls_trace

        # Rule 2: Sustained memory rise relative to baseline AND positive slope
        if mem_rel_inc >= 0.25 and mem_slope > 0.02 and mem_mean > base_mem_mean:
            return {
                "category": "memory",
                "root_cause": (
                    "Metrics-only heuristic baseline: sustained memory growth relative to baseline"
                ),
                "confidence": 0.9,
                "evidence": [
                    f"Memory rose {mem_rel_inc * 100:.1f}% over baseline mean {base_mem_mean:.2f}% "
                    f"(max {mem_max:.2f}%, slope {mem_slope:.4f}%/min)"
                ],
                "alternatives": [
                    {"category": "application_bug", "reason": "Unbounded cache or object collection retention"}
                ],
                "remediation": [
                    {"step": "Restart service container to immediately reclaim system memory", "risk": "med"},
                    {"step": "Capture and inspect heap dump for retained references", "risk": "low"},
                ],
            }, tool_calls_trace

        # Rule 3: Probe failures / timeouts -> packet_loss
        if probe_fail_delta >= 0.08 or (probe_failure_rate > 0.05 and err_ratio >= 3.0):
            return {
                "category": "packet_loss",
                "root_cause": (
                    "Metrics-only heuristic baseline: elevated health probe failures and errors"
                ),
                "confidence": 0.85,
                "evidence": [
                    f"Probe failure rate {probe_failure_rate * 100:.1f}% "
                    f"(before {before_probe_fail * 100:.1f}%, error_count {err_mean:.1f})"
                ],
                "alternatives": [
                    {"category": "network_latency", "reason": "Severe network latency causing client timeouts"}
                ],
                "remediation": [
                    {"step": "Inspect network interfaces, drop counters, and packet filter rules", "risk": "low"},
                    {"step": "Reroute traffic around degraded network segments", "risk": "med"},
                ],
            }, tool_calls_trace

        # Rule 4: In-flight requests peak several times baseline mean while CPU is normal -> db_lock
        if in_flight_ratio >= 4.0 and cpu_normal:
            return {
                "category": "db_lock",
                "root_cause": (
                    "Metrics-only heuristic baseline: in-flight requests surged relative to baseline with normal CPU"
                ),
                "confidence": 0.9,
                "evidence": [
                    f"In-flight peak {in_flight_max:.0f} ({in_flight_ratio:.1f}x baseline mean {base_in_flight_mean:.2f}), "
                    f"CPU {cpu_mean:.1f}%, requests/30s {req_count_mean:.1f} (baseline {base_req_mean:.1f})"
                ],
                "alternatives": [
                    {"category": "application_bug", "reason": "Long-running uncommitted transactions holding locks"}
                ],
                "remediation": [
                    {"step": "Identify and terminate blocking queries via pg_stat_activity", "risk": "med"},
                    {"step": "Review transaction boundaries and acquire locks in consistent order", "risk": "low"},
                ],
            }, tool_calls_trace

        # Rule 5: Uniformly elevated latency with normal CPU and low probe failures -> network_latency
        if (
            cpu_normal
            and probe_fail_delta < 0.08
            and (
                probe_lat_ratio >= 3.0
                or (p95_ratio >= 3.0 and err_mean <= max(2.0, base_err_mean * 2.0))
            )
        ):
            return {
                "category": "network_latency",
                "root_cause": (
                    "Metrics-only heuristic baseline: request and probe latency elevated with normal CPU"
                ),
                "confidence": 0.85,
                "evidence": [
                    f"P95 latency mean {p95_mean:.1f}ms (max {p95_max:.1f}ms), "
                    f"probe latency {mean_probe_lat}ms vs {before_probe_lat}ms before"
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
            "root_cause": (
                "Metrics-only heuristic baseline: insufficient metric separation between categories"
            ),
            "confidence": 0.0,
            "evidence": ["Metrics did not cross relative baseline thresholds"],
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
