"""Root-cause investigation agent executing tool-use and final synthesis phases."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import logging
import threading
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError
from sqlalchemy.orm import Session

from app.config import get_settings
from app.db.models import Incident
from app.db.repository import get_incident, update_incident
from app.db.session import SessionLocal
from app.incidents.evidence import build_evidence_pack
from app.agent.llm import GeminiClient, MockLLM, get_llm
from app.agent.tools import (
    TOOL_DECLARATIONS,
    TOOL_FUNCTIONS,
    get_host_metrics,
    get_network_stats,
    query_logs,
    recent_changes,
)

logger = logging.getLogger(__name__)

CategoryType = Literal[
    "cpu",
    "memory",
    "network_latency",
    "packet_loss",
    "db_lock",
    "application_bug",
    "unknown",
]

RiskType = Literal["low", "med", "high"]


class Alternative(BaseModel):
    category: CategoryType
    reason: str


class RemediationStep(BaseModel):
    step: str
    risk: RiskType


class AgentFinalResponse(BaseModel):
    root_cause: str
    category: CategoryType
    confidence: float = Field(ge=0.0, le=1.0)
    evidence: list[str] = Field(default_factory=list)
    alternatives: list[Alternative] = Field(default_factory=list, max_length=2)
    remediation: list[RemediationStep] = Field(default_factory=list)


SYSTEM_PROMPT = """You are an expert root-cause analysis agent for distributed systems and cloud services.
Investigate the provided closed incident using ONLY the supplied evidence pack and available tools.
Rules:
1. Compare incident metrics against baseline metrics to identify anomalies.
2. Consider total silence in the evidence as a possible stall or outage, not as a cause by itself.
3. Say "unknown" if the evidence is insufficient to identify the root cause.
4. NEVER recommend automatic execution; all recommendations are for a human operator.
5. Cite specific metrics, values, and Drain3 log templates in your evidence.
6. Explain how the observed symptoms distinguish the chosen category from the other categories.
Available categories: [cpu, memory, network_latency, packet_loss, db_lock, application_bug, unknown].
"""


def _truncate_result(result: Any, max_chars: int = 1500) -> Any:
    """Truncate tool result representations for token efficiency in conversation traces."""
    if isinstance(result, dict):
        truncated = {}
        for k, v in result.items():
            if isinstance(v, list) and len(v) > 10:
                truncated[k] = v[:10] + [{"_truncated": f"... {len(v) - 10} more items"}]
            else:
                truncated[k] = v
        text = json.dumps(truncated)
        if len(text) > max_chars:
            return {"_truncated": text[:max_chars] + "..."}
        return truncated
    if isinstance(result, str) and len(result) > max_chars:
        return result[:max_chars] + "..."
    return result


def _broadcast_alert_safely(payload: dict[str, Any]) -> None:
    """Send websocket alert without raising or blocking synchronous code."""
    try:
        from app.api.main import broadcast_alert

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(broadcast_alert(payload))
        except RuntimeError:
            asyncio.run(broadcast_alert(payload))
    except Exception as exc:
        logger.debug("Failed to broadcast alert: %s", exc)


def run_agent(
    incident_id: int,
    session: Session | None = None,
    custom_llm: Any | None = None,
) -> dict[str, Any]:
    """Execute the root-cause agent for a closed incident.

    Runs ONLY for closed incidents. Never queries ground-truth fault data.
    Phase 1: Tool loop with Gemini function calling (automatic calling disabled) or MockLLM rules.
    Phase 2: Final JSON synthesis validated via Pydantic with single retry on invalid JSON.
    """
    settings = get_settings()
    max_tool_calls = max(1, settings.max_agent_tool_calls)

    def _get_inc(s: Session) -> Incident:
        inc = get_incident(s, incident_id)
        if inc is None:
            raise ValueError(f"Incident {incident_id} not found")
        return inc

    if session is not None:
        incident = _get_inc(session)
    else:
        with SessionLocal() as s:
            incident = _get_inc(s)

    # Strictly enforce that the agent ONLY runs for closed incidents
    if incident.status != "closed":
        raise ValueError(
            f"Agent runs ONLY for closed incidents; incident {incident_id} has status='{incident.status}'"
        )

    evidence_pack = build_evidence_pack(incident, session)
    provider = custom_llm if custom_llm is not None else get_llm()

    trace_messages: list[dict[str, Any]] = []
    trace_tool_calls: list[dict[str, Any]] = []
    total_llm_calls = 0

    # Branch 1: MockLLM deterministic rule-based baseline
    if isinstance(provider, MockLLM) or getattr(provider, "is_mock", False):
        try:
            total_llm_calls += 1
            final_data, tool_trace = provider.investigate(evidence_pack, session=session)
            trace_tool_calls.extend(tool_trace)
            validated = AgentFinalResponse.model_validate(final_data)

            remediation_payload = {
                "category": validated.category,
                "alternatives": [alt.model_dump() for alt in validated.alternatives],
                "steps": [step.model_dump() for step in validated.remediation],
                "remediation": [step.model_dump() for step in validated.remediation],
            }

            full_trace = {
                "evidence_pack": evidence_pack,
                "tool_calls": trace_tool_calls,
                "final_json": validated.model_dump(),
                "llm_calls": total_llm_calls,
            }

            def _save_success(s: Session) -> None:
                update_incident(
                    s,
                    incident_id,
                    category=validated.category,
                    root_cause=validated.root_cause,
                    confidence=validated.confidence,
                    alternatives=[alt.model_dump() for alt in validated.alternatives],
                    remediation=remediation_payload,
                    agent_trace=full_trace,
                    llm_calls=total_llm_calls,
                    analyzed_at=datetime.now(timezone.utc),
                    status="analyzed",
                )

            if session is not None:
                _save_success(session)
            else:
                with SessionLocal() as s, s.begin():
                    _save_success(s)

            _broadcast_alert_safely(
                {
                    "incident_id": incident_id,
                    "status": "analyzed",
                    "category": validated.category,
                    "confidence": validated.confidence,
                    "root_cause": validated.root_cause,
                }
            )
            return validated.model_dump()
        except Exception as exc:
            logger.exception("Error in MockLLM investigation: %s", exc)
            return {"error": str(exc)}

    # Branch 2: Gemini or scripted LLM
    try:
        from google.genai import types

        # Build tools config
        genai_tools = []
        for decl in TOOL_DECLARATIONS:
            props = {}
            for p_name, p_spec in decl["parameters"]["properties"].items():
                props[p_name] = types.Schema(
                    type=p_spec["type"],
                    description=p_spec["description"],
                )
            genai_tools.append(
                types.Tool(
                    function_declarations=[
                        types.FunctionDeclaration(
                            name=decl["name"],
                            description=decl["description"],
                            parameters=types.Schema(
                                type="OBJECT",
                                properties=props,
                                required=decl["parameters"]["required"],
                            ),
                        )
                    ]
                )
            )

        # Initial prompt
        user_initial = (
            f"An incident has occurred and is now closed. Here is the evidence pack:\n"
            f"{json.dumps(evidence_pack, indent=2, default=str)}\n\n"
            f"Investigate using the available tools to determine the root cause."
        )

        contents: list[Any] = [
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=user_initial)],
            )
        ]
        trace_messages.append({"role": "user", "content": user_initial})

        # Phase 1: Tool loop with automatic function calling DISABLED
        tool_call_count = 0
        while tool_call_count < max_tool_calls:
            total_llm_calls += 1
            response = provider.call_raw(
                contents=contents,
                system_prompt=SYSTEM_PROMPT,
                tools=genai_tools,
                disable_automatic_function_calling=True,
            )

            # Check for candidate and function calls
            has_func_calls = False
            func_calls_to_execute = []
            if getattr(response, "function_calls", None):
                func_calls_to_execute = list(response.function_calls)
                has_func_calls = len(func_calls_to_execute) > 0
            elif getattr(response, "candidates", None) and response.candidates:
                candidate = response.candidates[0]
                if getattr(candidate, "content", None) and candidate.content.parts:
                    for part in candidate.content.parts:
                        if getattr(part, "function_call", None):
                            func_calls_to_execute.append(part.function_call)
                            has_func_calls = True

            # Append assistant message to contents
            assistant_parts = []
            if getattr(response, "candidates", None) and response.candidates:
                candidate_content = response.candidates[0].content
                if candidate_content and candidate_content.parts:
                    assistant_parts = candidate_content.parts
            if not assistant_parts and getattr(response, "text", None):
                assistant_parts = [types.Part.from_text(text=response.text)]

            contents.append(types.Content(role="model", parts=assistant_parts))
            trace_messages.append(
                {
                    "role": "model",
                    "content": response.text if hasattr(response, "text") else "",
                    "function_calls": [
                        {"name": fc.name, "args": fc.args} for fc in func_calls_to_execute
                    ],
                }
            )

            if not has_func_calls:
                # Model concluded tool calling on its own
                break

            # Execute tool calls
            response_parts = []
            for fc in func_calls_to_execute:
                tool_call_count += 1
                func_name = fc.name
                func_args = dict(fc.args) if fc.args else {}

                tool_func = TOOL_FUNCTIONS.get(func_name)
                if tool_func is not None:
                    # Inject session if accepted
                    try:
                        result = tool_func(**func_args, session=session)
                    except TypeError:
                        result = tool_func(**func_args)
                else:
                    result = {"error": f"Unknown tool: {func_name}"}

                trace_tool_calls.append(
                    {
                        "tool": func_name,
                        "args": func_args,
                        "result": _truncate_result(result),
                    }
                )

                response_parts.append(
                    types.Part.from_function_response(
                        name=func_name,
                        response={"result": _truncate_result(result)},
                    )
                )
                if tool_call_count >= max_tool_calls:
                    break

            contents.append(types.Content(role="user", parts=response_parts))
            trace_messages.append({"role": "user", "tool_responses": len(response_parts)})

        # Phase 2: Final answer with tools removed and JSON output requested
        synthesis_prompt = (
            "Now synthesize your final root-cause analysis based on all gathered evidence.\n"
            "Return a single JSON object with the following schema:\n"
            "{\n"
            '  "root_cause": "<concise description of root cause>",\n'
            '  "category": "cpu" | "memory" | "network_latency" | "packet_loss" | "db_lock" | "application_bug" | "unknown",\n'
            '  "confidence": <float 0.0 to 1.0>,\n'
            '  "evidence": ["<specific metric or log observation>", ...],\n'
            '  "alternatives": [{"category": "<category>", "reason": "<rationale>"}], (up to 2 candidates)\n'
            '  "remediation": [{"step": "<operator action>", "risk": "low" | "med" | "high"}]\n'
            "}"
        )

        contents.append(
            types.Content(
                role="user",
                parts=[types.Part.from_text(text=synthesis_prompt)],
            )
        )
        trace_messages.append({"role": "user", "content": synthesis_prompt})

        validated_response: AgentFinalResponse | None = None
        raw_final_text = ""

        # First Phase 2 attempt
        total_llm_calls += 1
        resp2 = provider.call_raw(
            contents=contents,
            system_prompt=SYSTEM_PROMPT,
            tools=None,  # tools strictly removed
            response_mime_type="application/json",
            response_schema=AgentFinalResponse,
        )
        raw_final_text = resp2.text or ""

        try:
            validated_response = AgentFinalResponse.model_validate_json(raw_final_text)
        except (ValidationError, json.JSONDecodeError, ValueError) as err1:
            logger.warning("Agent Phase 2 JSON validation failed on first attempt: %s", err1)
            # Retry once with validation error feedback
            retry_prompt = (
                f"Your previous output failed schema validation: {err1}.\n"
                f"Please fix the error and return ONLY valid JSON matching the required schema."
            )
            contents.append(types.Content(role="model", parts=[types.Part.from_text(text=raw_final_text)]))
            contents.append(types.Content(role="user", parts=[types.Part.from_text(text=retry_prompt)]))
            trace_messages.append({"role": "user", "content": retry_prompt})

            total_llm_calls += 1
            resp_retry = provider.call_raw(
                contents=contents,
                system_prompt=SYSTEM_PROMPT,
                tools=None,
                response_mime_type="application/json",
                response_schema=AgentFinalResponse,
            )
            raw_final_text = resp_retry.text or ""
            try:
                validated_response = AgentFinalResponse.model_validate_json(raw_final_text)
            except (ValidationError, json.JSONDecodeError, ValueError) as err2:
                logger.error("Agent Phase 2 JSON validation failed on retry: %s", err2)
                # Second failure: category="unknown", confidence=0, raw text in trace
                validated_response = AgentFinalResponse(
                    root_cause="Analysis output failed schema validation",
                    category="unknown",
                    confidence=0.0,
                    evidence=["Model response did not conform to required JSON schema"],
                    alternatives=[],
                    remediation=[{"step": "Perform manual incident log audit", "risk": "low"}],
                )

        remediation_payload = {
            "category": validated_response.category,
            "alternatives": [alt.model_dump() for alt in validated_response.alternatives],
            "steps": [step.model_dump() for step in validated_response.remediation],
            "remediation": [step.model_dump() for step in validated_response.remediation],
        }

        full_trace = {
            "evidence_pack": evidence_pack,
            "messages": trace_messages,
            "tool_calls": trace_tool_calls,
            "raw_text": raw_final_text,
            "final_json": validated_response.model_dump(),
            "llm_calls": total_llm_calls,
        }

        def _save_result(s: Session) -> None:
            update_incident(
                s,
                incident_id,
                category=validated_response.category,
                root_cause=validated_response.root_cause,
                confidence=validated_response.confidence,
                alternatives=[a.model_dump() for a in validated_response.alternatives],
                remediation=remediation_payload,
                agent_trace=full_trace,
                llm_calls=total_llm_calls,
                analyzed_at=datetime.now(timezone.utc),
                status="analyzed",
            )

        if session is not None:
            _save_result(session)
        else:
            with SessionLocal() as s, s.begin():
                _save_result(s)

        _broadcast_alert_safely(
            {
                "incident_id": incident_id,
                "status": "analyzed",
                "category": validated_response.category,
                "confidence": validated_response.confidence,
                "root_cause": validated_response.root_cause,
            }
        )

        return validated_response.model_dump()

    except Exception as exc:
        # Provider errors (429/5xx after retries) must leave incident as 'closed' with error note in agent_trace
        logger.exception("Provider error during agent execution for incident %s: %s", incident_id, exc)
        error_trace = {
            "error": str(exc),
            "trace_messages": trace_messages,
            "tool_calls": trace_tool_calls,
            "llm_calls": total_llm_calls,
        }
        try:
            def _save_closed_error(s: Session) -> None:
                update_incident(
                    s,
                    incident_id,
                    agent_trace=error_trace,
                    llm_calls=total_llm_calls,
                    status="closed",
                )

            if session is not None:
                _save_closed_error(session)
            else:
                with SessionLocal() as s, s.begin():
                    _save_closed_error(s)
        except Exception as save_err:
            logger.error("Failed to save error trace to incident: %s", save_err)

        return {"error": str(exc), "status": "closed"}


def run_agent_background(incident_id: int) -> threading.Thread:
    """Run the agent in a worker thread so log ingestion is never blocked."""
    thread = threading.Thread(
        target=run_agent,
        args=(incident_id,),
        name=f"agent-investigate-{incident_id}",
        daemon=True,
    )
    thread.start()
    return thread


def incident_agent_hook(incident_id: int, manager: Any) -> None:
    """Close hook called by IncidentManager when an incident is closed.

    Marks status closed and runs root-cause agent in a background worker thread.
    """
    if hasattr(manager, "update_status"):
        manager.update_status(incident_id, "closed")
    run_agent_background(incident_id)
