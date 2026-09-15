"""Safe chat orchestration using a validated semantic plan.

Natural language is interpreted once by :class:`SemanticPlanner`.  All
database access remains a bounded compilation to ``QueryPlan`` and all routes
are derived by the backend from the plan's data/guidance requirements.
"""

from __future__ import annotations

import re
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from self_healthy_kafka.config import AnalyticsChatConfig, RagConfig
from self_healthy_kafka.rag.answer_composer import GroundedAnswerComposer, QwenJsonGenerator
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore
from self_healthy_kafka.rag.retriever import RunbookRetriever
from self_healthy_kafka.rag.shadow import ShadowRetrievalCoordinator
from self_healthy_kafka.rag.workflow import RunbookRagWorkflow
from self_healthy_kafka.redaction import redact_text
from self_healthy_kafka.semantic.catalog import SEMANTIC_CATALOG
from self_healthy_kafka.semantic.evidence import AnalyticsResponseComposer, build_evidence
from self_healthy_kafka.semantic.outcome import (
    cannot_verify,
    classify_execution,
    degraded,
    needs_clarification,
)
from self_healthy_kafka.semantic.planner import (
    SemanticPlan,
    SemanticPlanError,
    SemanticPlanner,
    compile_analytics_request,
)
from self_healthy_kafka.semantic.tsql import ExecutedTsql, compile_incident_query
from self_healthy_kafka.semantic.presentation import (
    SemanticResponseRenderer,
    build_fallback_presentation,
    build_presentation_facts,
)
from self_healthy_kafka.storage.common import json_safe
from self_healthy_kafka.webhook.analytics import MAX_LIMIT, QueryPlan, resolve_time_range

CATALOG = SEMANTIC_CATALOG
_CONVERSATION_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_MAX_ERROR_MESSAGE_CHARS = 2_000
_MAX_ANALYTICS_ROWS = 1_000
_FACT_FETCH_LIMIT = _MAX_ANALYTICS_ROWS + 1
_MAX_EXECUTED_RESULT_ROWS = 500
_TECHNICAL_CODE = re.compile(
    r"\b(?:ORA-\d{5}|SQLSTATE-?[0-9A-Z]{5}|HTTP-?\d{3}|[A-Z][A-Z0-9]+(?:_[A-Z0-9]+){1,6})\b",
    re.IGNORECASE,
)
_EXCEPTION_CLASS = re.compile(r"(?:[A-Za-z_$][\w$]*\.)*([A-Z][A-Za-z0-9_$]*(?:Exception|Error))\b")


class ChatInputError(ValueError):
    """The browser supplied an invalid chat request."""


class ChatPlanningError(RuntimeError):
    """Backward-compatible exception type for callers that explicitly plan."""


@dataclass(frozen=True)
class _ConversationState:
    expires_at: datetime
    semantic_plan: SemanticPlan
    query_plan: QueryPlan | None
    facts: tuple[dict[str, Any], ...]
    evidence_ids: tuple[str, ...]

    def planner_context(self) -> dict[str, Any]:
        return {
            **self.semantic_plan.context(),
            "previous_query_plan": self.query_plan.to_dict() if self.query_plan else None,
            "available_evidence_ids": list(self.evidence_ids[:20]),
            "available_fact_count": len(self.facts),
        }


class AnalyticsChatService:
    """Semantic planner, safe compiler, analytics execution and optional RAG."""

    def __init__(
        self,
        config: AnalyticsChatConfig,
        *,
        incident_facts: Callable[..., list[dict[str, Any]]],
        execute_incident_query: Callable[..., list[dict[str, Any]]] | None = None,
        client: httpx.Client | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        rag_config: RagConfig | None = None,
        rag_workflow: RunbookRagWorkflow | None = None,
        semantic_planner: SemanticPlanner | None = None,
    ):
        self._config = config
        self._incident_facts = incident_facts
        self._execute_incident_query = execute_incident_query
        self._owns_client = client is None
        self._client = client or httpx.Client()
        self._now = now
        self._rag_config = rag_config
        self._qwen = QwenJsonGenerator(config, self._client)
        generator = self._qwen.generate if config.hf_endpoint_url else None
        self._planner = semantic_planner or SemanticPlanner(
            generator, max_tokens=config.hf_planner_max_tokens
        )
        self._response_composer = AnalyticsResponseComposer(
            generator, max_tokens=config.hf_response_max_tokens
        )
        self._rag_workflow = rag_workflow
        self._closed = False
        self._conversation_states: OrderedDict[str, _ConversationState] = OrderedDict()
        self._conversation_lock = RLock()
        if self._rag_workflow is None and rag_config is not None and rag_config.enabled:
            self._rag_workflow = self._build_rag_workflow(rag_config)

    def _build_rag_workflow(self, rag_config: RagConfig) -> RunbookRagWorkflow:
        retrieval_mode = getattr(rag_config, "effective_retrieval_mode", rag_config.search_mode)
        primary_mode = "dense" if retrieval_mode == "shadow" else retrieval_mode
        canonical_mode = bool(
            getattr(rag_config, "retrieval_mode", "") or getattr(rag_config, "hybrid_shadow_enabled", False)
        )
        if canonical_mode:
            primary_collection = (
                getattr(rag_config, "dense_collection", rag_config.collection)
                if primary_mode == "dense"
                else getattr(rag_config, "hybrid_collection", rag_config.collection)
            )
        else:
            primary_collection = rag_config.collection
        primary_config = _retrieval_config(rag_config, mode=primary_mode, collection=primary_collection)
        retriever: RunbookRetriever | ShadowRetrievalCoordinator = RunbookRetriever(
            primary_config, QdrantRunbookStore(primary_config)
        )
        if retrieval_mode == "shadow":
            shadow_config = _retrieval_config(
                rag_config,
                mode="hybrid",
                collection=getattr(rag_config, "hybrid_collection", rag_config.collection),
                timeout_seconds=float(getattr(rag_config, "shadow_timeout_seconds", rag_config.request_timeout_seconds)),
            )
            retriever = ShadowRetrievalCoordinator(
                retriever,
                RunbookRetriever(shadow_config, QdrantRunbookStore(shadow_config)),
                sample_rate=float(getattr(rag_config, "shadow_sample_rate", 0.0)),
                queue_size=int(getattr(rag_config, "shadow_queue_size", 100)),
                shutdown_timeout_seconds=float(getattr(rag_config, "shadow_timeout_seconds", rag_config.request_timeout_seconds)),
            )
        return RunbookRagWorkflow(
            rag_config,
            retriever=retriever,
            composer=GroundedAnswerComposer(
                (
                    lambda messages: self._qwen.generate(
                        messages, max_tokens=self._config.hf_response_max_tokens
                    )
                ) if self._config.hf_endpoint_url else None
            ),
        )

    @property
    def enabled(self) -> bool:
        return self._config.enabled or bool(self._rag_config and self._rag_config.enabled)

    @property
    def path(self) -> str:
        return "/api/v1/chat"

    def validate(self) -> None:
        if not self.enabled:
            return
        try:
            ZoneInfo(self._config.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("CHAT_ANALYTICS_TIMEZONE is invalid") from exc
        if bool(self._config.hf_endpoint_url) != bool(self._config.hf_token):
            raise ValueError("HF_CHAT_ENDPOINT_URL and HF_CHAT_TOKEN must be configured together")
        if self._config.hf_endpoint_url and not self._config.hf_model_id:
            raise ValueError("HF_CHAT_MODEL_ID is required with Hugging Face endpoint")
        if not 1 <= self._config.conversation_ttl_seconds <= 86_400:
            raise ValueError("CHAT_CONVERSATION_TTL_SECONDS must be between 1 and 86400")
        if not 1 <= self._config.conversation_max_entries <= 10_000:
            raise ValueError("CHAT_CONVERSATION_MAX_ENTRIES must be between 1 and 10000")
        if not 128 <= self._config.hf_planner_max_tokens <= 4_096:
            raise ValueError("HF_CHAT_PLANNER_MAX_TOKENS must be between 128 and 4096")
        if not 128 <= self._config.hf_response_max_tokens <= 4_096:
            raise ValueError("HF_CHAT_RESPONSE_MAX_TOKENS must be between 128 and 4096")
        if self._rag_config is not None and self._rag_config.enabled:
            self._rag_config.validate()
            missing = [
                name for name, value in (
                    ("HF_CHAT_ENDPOINT_URL", self._config.hf_endpoint_url),
                    ("HF_CHAT_TOKEN", self._config.hf_token),
                    ("HF_CHAT_MODEL_ID", self._config.hf_model_id),
                ) if not value.strip()
            ]
            if missing:
                raise ValueError("RAG is enabled but Qwen configuration is missing: " + ", ".join(missing))

    def ask(self, question: str, *, conversation_id: str | None = None) -> dict[str, Any]:
        question = question.strip()
        if not question or len(question) > 4_000:
            raise ChatInputError("question must contain between 1 and 4000 characters")
        conversation_id = _validate_conversation_id(conversation_id)
        prior = self._get_conversation(conversation_id) if conversation_id else None
        planning_model_index = self._qwen.call_count
        try:
            semantic_plan, planning_attempts = self._planner.plan(
                question,
                context=prior.planner_context() if prior else None,
            )
        except SemanticPlanError as exc:
            result = _planning_failure(str(exc), timezone_name=self._config.timezone)
            result["model_usage"] = {
                "planning": self._qwen.calls_since(planning_model_index),
                "analytics_response": [],
                "runbook": [],
            }
            return self._with_conversation(result, conversation_id, context_used=prior is not None, action="planning_failed")
        planning_calls = self._qwen.calls_since(planning_model_index)

        if semantic_plan.conversation_action == "clear_context":
            if conversation_id:
                self.clear_conversation(conversation_id)
            result = {
                "answer": "Mình đã xóa ngữ cảnh của cuộc trò chuyện này. Bạn có thể bắt đầu câu hỏi mới.",
                "route": "conversation",
                "source": "deterministic_fallback",
                "status": "ok",
                "reason": None,
                "fallback_reason": None,
                "citations": [],
                "sources": [],
                "evidence": [],
                "query_plan": None,
                "semantic_plan": semantic_plan.to_dict(),
                "row_count": 0,
                "evidence_ids": [],
                "planning_attempts": planning_attempts,
            }
            result["model_usage"] = {"planning": planning_calls, "analytics_response": [], "runbook": []}
            return self._with_conversation(result, conversation_id, context_used=False, action="clear_context")

        if semantic_plan.clarification:
            clarification_outcome = needs_clarification()
            presentation = build_presentation_facts(
                semantic_plan,
                query_plan=None,
                outcome=clarification_outcome,
                source_rows=[],
                facts=[],
                evidence=[],
                from_at=None,
                to_at=None,
                timezone_name=self._config.timezone,
            )
            result = {
                **_planning_failure("material_ambiguity", timezone_name=self._config.timezone),
                "answer": SemanticResponseRenderer().render_outcome(presentation),
                "route": "clarification",
                "status": "needs_clarification",
                "outcome": "needs_clarification",
                "query_executed": False,
                "evidence_complete": False,
                "reason": "material_ambiguity",
                "fallback_reason": "material_ambiguity",
                "semantic_plan": semantic_plan.to_dict(),
                "planning_attempts": planning_attempts,
            }
            result["model_usage"] = {"planning": planning_calls, "analytics_response": [], "runbook": []}
            return self._with_conversation(result, conversation_id, context_used=prior is not None, action="clarification")

        execution_model_index = self._qwen.call_count
        if self._rag_workflow is not None:
            result = self._rag_workflow.ask(
                question,
                plan=semantic_plan,
                analytics_ask=lambda planned: self._execute_analytics(planned, question=question),
            )
        else:
            result = self._without_rag(semantic_plan, question=question)
        result["semantic_plan"] = semantic_plan.to_dict()
        result["planning_attempts"] = planning_attempts
        analytics_calls = result.pop("_analytics_model_calls", [])
        execution_calls = self._qwen.calls_since(execution_model_index)
        result["model_usage"] = {
            "planning": planning_calls,
            "analytics_response": analytics_calls,
            "runbook": execution_calls[len(analytics_calls):],
        }
        internal = result.pop("_conversation_state", None)
        if conversation_id and isinstance(internal, dict):
            self._remember_conversation(conversation_id, semantic_plan, internal)
        return self._with_conversation(
            result,
            conversation_id,
            context_used=prior is not None and bool(semantic_plan.inherited_fields),
            action="semantic_plan",
        )

    def _without_rag(self, plan: SemanticPlan, *, question: str) -> dict[str, Any]:
        route = plan.route
        if route is None:
            return _planning_failure("unsupported_semantic_plan", timezone_name=self._config.timezone)
        if route.value == "analytics":
            return _analytics_envelope(self._execute_analytics(plan, question=question))
        if route.value == "combined":
            result = _analytics_envelope(self._execute_analytics(plan, question=question))
            return {
                **result,
                "route": "combined",
                # Preserve the independently established analytics outcome.
                # A missing supplementary runbook must not relabel verified
                # data as degraded, or turn unverified data into a result.
                "status": result.get("status"),
                "reason": result.get("reason"),
                "fallback_reason": "runbook_unavailable",
            }
        return {
            **_planning_failure("runbook_unavailable", timezone_name=self._config.timezone),
            "route": "runbook",
            "status": "degraded",
            "outcome": "degraded",
            "query_executed": False,
            "evidence_complete": False,
            "reason": "runbook_unavailable",
            "fallback_reason": "runbook_unavailable",
        }

    def _execute_analytics(self, semantic_plan: SemanticPlan, *, question: str = "") -> dict[str, Any]:
        try:
            plan = compile_analytics_request(
                semantic_plan, require_semantic_enforcement=True
            )
        except SemanticPlanError as exc:
            return _planning_failure(
                f"semantic_compiler_failed:{exc}", timezone_name=self._config.timezone
            )
        try:
            from_at, to_at = resolve_time_range(
                plan.time_range, now=self._now(), timezone_name=self._config.timezone
            )
        except (TypeError, ValueError) as exc:
            return _planning_failure(
                f"semantic_time_range_failed:{type(exc).__name__}", timezone_name=self._config.timezone
            )
        # Production wiring supplies this executor.  It runs the exact
        # compiler-produced T-SQL aggregation/ranking query, which prevents a
        # misleading UI query being shown for Python-side aggregation.  The
        # legacy bounded fact callable remains for offline fixtures and for
        # query shapes intentionally not supported by the aggregate compiler.
        if self._execute_incident_query is not None:
            try:
                compiled = compile_incident_query(
                    plan,
                    from_at=from_at,
                    to_at=to_at,
                    row_limit=_MAX_EXECUTED_RESULT_ROWS + 1,
                )
            except ValueError:
                compiled = None
            if compiled is not None:
                return self._execute_compiled_analytics(
                    semantic_plan=semantic_plan,
                    plan=plan,
                    compiled=compiled,
                    from_at=from_at,
                    to_at=to_at,
                )
        try:
            raw_rows = self._incident_facts(
                from_at=from_at,
                to_at=to_at,
                event_type=plan.event_types[0] if plan.event_types else None,
                final_outcome=plan.outcomes[0] if plan.outcomes else None,
                connector_name=plan.connector_name,
                error_code=_database_error_filter(plan.error_code),
                limit=_FACT_FETCH_LIMIT,
            )
        except Exception as exc:
            return _execution_failure(
                "analytics_source_unavailable", exception=exc, timezone_name=self._config.timezone
            )
        truncated = len(raw_rows) > _MAX_ANALYTICS_ROWS
        rows = _prepare_incident_rows(raw_rows[:_MAX_ANALYTICS_ROWS])
        if _database_error_filter(plan.error_code) is None:
            rows = _filter_failure_code(rows, plan.error_code)
        facts = _aggregate(rows, plan)
        comparison_facts: list[dict[str, Any]] = []
        comparison_rows: list[dict[str, Any]] = []
        if plan.comparison and from_at and to_at:
            interval = to_at - from_at
            try:
                previous_raw = self._incident_facts(
                    from_at=from_at - interval,
                    to_at=from_at,
                    event_type=plan.event_types[0] if plan.event_types else None,
                    final_outcome=plan.outcomes[0] if plan.outcomes else None,
                    connector_name=plan.connector_name,
                    error_code=_database_error_filter(plan.error_code),
                    limit=_FACT_FETCH_LIMIT,
                )
            except Exception as exc:
                return _execution_failure(
                    "analytics_comparison_source_unavailable",
                    exception=exc,
                    timezone_name=self._config.timezone,
                )
            truncated = truncated or len(previous_raw) > _MAX_ANALYTICS_ROWS
            comparison_rows = _prepare_incident_rows(previous_raw[:_MAX_ANALYTICS_ROWS])
            if _database_error_filter(plan.error_code) is None:
                comparison_rows = _filter_failure_code(comparison_rows, plan.error_code)
            comparison_facts = _aggregate(comparison_rows, plan)
        outcome = classify_execution(
            row_count=len(rows), fact_count=len(facts), truncated=truncated
        )
        evidence = build_evidence(
            facts if outcome.outcome == "verified_results" else [],
            query_plan=plan,
            semantic_plan=semantic_plan,
            from_at=from_at,
            to_at=to_at,
            truncated=truncated,
        )
        presentation = build_presentation_facts(
            semantic_plan,
            query_plan=plan,
            outcome=outcome,
            source_rows=rows,
            facts=facts,
            evidence=evidence,
            from_at=from_at,
            to_at=to_at,
            timezone_name=self._config.timezone,
        )
        response_model_index = self._qwen.call_count
        answer, response_source, fallback_reason, response_attempts, claims = self._response_composer.compose(
            presentation=presentation,
        )
        # ``_execute_analytics`` is also called by the RAG workflow. Its public
        # result is useful independently; a generic renderer remains grounded
        # even when a response provider is unavailable.
        sources = [{
            "source": "vConnectorIncidentFacts",
            "count": len(rows),
            "items": [json_safe(row) for row in rows[:MAX_LIMIT]],
        }]
        if comparison_rows:
            sources.append({
                "source": "vConnectorIncidentFacts.previous_period",
                "count": len(comparison_rows),
                "items": [json_safe(row) for row in comparison_rows[:MAX_LIMIT]],
            })
        return {
            "answer": answer,
            "source": response_source,
            "sources": sources,
            "query_plan": plan.to_dict(),
            "from_at": from_at.isoformat() if from_at else None,
            "to_at": to_at.isoformat() if to_at else None,
            "time_range_applied": _time_range_applied(from_at, to_at, self._config.timezone),
            **outcome.to_dict(),
            "evidence_ids": [str(row.get("incident_id")) for row in (rows + comparison_rows)[:MAX_LIMIT]],
            "evidence": evidence,
            "claims": claims,
            "verified_result": {
                "rows": [_verified_result_row(fact) for fact in facts] if outcome.outcome == "verified_results" else [],
                "columns": list(dict.fromkeys(
                    [field for fact in facts for field in fact]
                )),
            },
            "status": outcome.outcome,
            "reason": outcome.reason,
            "fallback_reason": outcome.reason or fallback_reason,
            "response_attempts": response_attempts,
            "comparison_evidence": build_evidence(
                comparison_facts,
                query_plan=plan,
                semantic_plan=semantic_plan,
                from_at=from_at - (to_at - from_at) if from_at and to_at else None,
                to_at=from_at,
                truncated=truncated,
            ) if comparison_facts else [],
            "_analytics_model_calls": self._qwen.calls_since(response_model_index),
            "_conversation_state": {"plan": plan, "facts": facts},
        }

    def _execute_compiled_analytics(
        self,
        *,
        semantic_plan: SemanticPlan,
        plan: QueryPlan,
        compiled: ExecutedTsql,
        from_at: datetime | None,
        to_at: datetime | None,
    ) -> dict[str, Any]:
        """Execute and render one compiler-generated SQL Server aggregation."""

        try:
            database_rows = self._execute_incident_query(
                statement=compiled.statement,
                parameters=compiled.parameter_values,
            )
        except Exception as exc:
            return _execution_failure(
                "analytics_source_unavailable", exception=exc, timezone_name=self._config.timezone
            )
        truncated = len(database_rows) > _MAX_EXECUTED_RESULT_ROWS
        facts = _compiled_result_facts(database_rows[:_MAX_EXECUTED_RESULT_ROWS], plan)
        outcome = classify_execution(
            row_count=len(database_rows), fact_count=len(facts), truncated=truncated
        )
        evidence = build_evidence(
            facts if outcome.outcome == "verified_results" else [],
            query_plan=plan,
            semantic_plan=semantic_plan,
            from_at=from_at,
            to_at=to_at,
            truncated=truncated,
        )
        presentation = build_presentation_facts(
            semantic_plan,
            query_plan=plan,
            outcome=outcome,
            source_rows=facts,
            facts=facts,
            evidence=evidence,
            from_at=from_at,
            to_at=to_at,
            timezone_name=self._config.timezone,
        )
        response_model_index = self._qwen.call_count
        answer, response_source, fallback_reason, response_attempts, claims = self._response_composer.compose(
            presentation=presentation,
        )
        executed_query = compiled.to_public_dict(executed=True)
        evidence_ids = [
            identifier
            for fact in facts
            for identifier in fact.get("evidence_ids") or []
        ][:MAX_LIMIT]
        return {
            "answer": answer,
            "source": response_source,
            "sources": [{
                "source": "vConnectorIncidentFacts",
                "count": len(facts),
                "items": [json_safe(row) for row in facts[:MAX_LIMIT]],
            }],
            "query_plan": plan.to_dict(),
            "executed_query": executed_query,
            "from_at": from_at.isoformat() if from_at else None,
            "to_at": to_at.isoformat() if to_at else None,
            "time_range_applied": _time_range_applied(from_at, to_at, self._config.timezone),
            **outcome.to_dict(),
            "evidence_ids": evidence_ids,
            "evidence": evidence,
            "claims": claims,
            "verified_result": {
                "rows": [_verified_result_row(fact) for fact in facts] if outcome.outcome == "verified_results" else [],
                "columns": list(dict.fromkeys(field for fact in facts for field in fact)),
            },
            "status": outcome.outcome,
            "reason": outcome.reason,
            # This is technical-only metadata.  The UI intentionally does not
            # render grounding fallback diagnostics in its primary answer.
            "fallback_reason": outcome.reason or fallback_reason,
            "response_attempts": response_attempts,
            "_analytics_model_calls": self._qwen.calls_since(response_model_index),
            "_conversation_state": {"plan": plan, "facts": facts},
        }

    def _with_conversation(
        self,
        result: dict[str, Any],
        conversation_id: str | None,
        *,
        context_used: bool,
        action: str,
    ) -> dict[str, Any]:
        if conversation_id:
            result["conversation"] = {"id": conversation_id, "context_used": context_used, "action": action}
        return result

    def clear_conversation(self, conversation_id: str) -> None:
        value = _validate_conversation_id(conversation_id)
        if value is not None:
            with self._conversation_lock:
                self._conversation_states.pop(value, None)

    def _get_conversation(self, conversation_id: str) -> _ConversationState | None:
        with self._conversation_lock:
            self._evict_expired_conversations(self._now())
            state = self._conversation_states.get(conversation_id)
            if state:
                self._conversation_states.move_to_end(conversation_id)
            return state

    def _remember_conversation(
        self,
        conversation_id: str,
        semantic_plan: SemanticPlan,
        internal: dict[str, Any],
    ) -> None:
        query_plan = internal.get("plan")
        facts = internal.get("facts")
        if not isinstance(query_plan, QueryPlan) or not isinstance(facts, list):
            return
        state = _ConversationState(
            expires_at=self._now() + timedelta(seconds=self._config.conversation_ttl_seconds),
            semantic_plan=semantic_plan,
            query_plan=query_plan,
            facts=tuple(dict(item) for item in facts[:MAX_LIMIT]),
            evidence_ids=tuple(
                str(identifier)
                for fact in facts[:MAX_LIMIT]
                for identifier in fact.get("evidence_ids") or []
            )[:MAX_LIMIT],
        )
        with self._conversation_lock:
            self._evict_expired_conversations(self._now())
            self._conversation_states[conversation_id] = state
            self._conversation_states.move_to_end(conversation_id)
            while len(self._conversation_states) > self._config.conversation_max_entries:
                self._conversation_states.popitem(last=False)

    def _evict_expired_conversations(self, now: datetime) -> None:
        for key in [key for key, state in self._conversation_states.items() if state.expires_at <= now]:
            self._conversation_states.pop(key, None)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            close = getattr(self._rag_workflow, "close", None)
            if callable(close):
                close()
        finally:
            if self._owns_client:
                self._client.close()


def _planning_failure(reason: str, *, timezone_name: str = "Asia/Ho_Chi_Minh") -> dict[str, Any]:
    outcome = cannot_verify(reason=reason)
    presentation = build_fallback_presentation(outcome, timezone_name=timezone_name)
    return {
        "answer": SemanticResponseRenderer().render_outcome(presentation),
        "route": "unsupported",
        "source": "deterministic_fallback",
        **outcome.to_dict(),
        "status": outcome.outcome,
        "reason": reason,
        "fallback_reason": reason,
        "citations": [],
        "sources": [],
        "evidence": [],
        "recommended_runbooks": [],
        "candidates": [],
        "query_plan": None,
        "semantic_plan": None,
        "evidence_ids": [],
    }


def _execution_failure(
    reason: str,
    *,
    exception: Exception,
    timezone_name: str = "Asia/Ho_Chi_Minh",
) -> dict[str, Any]:
    """Return a public-safe source failure without converting it into no data."""

    outcome = degraded(reason=reason)
    presentation = build_fallback_presentation(outcome, timezone_name=timezone_name)
    return {
        "answer": SemanticResponseRenderer().render_outcome(presentation),
        "route": "analytics",
        "source": "deterministic_outcome_renderer",
        **outcome.to_dict(),
        "status": outcome.outcome,
        "reason": outcome.reason,
        "fallback_reason": outcome.reason,
        "citations": [],
        "sources": [],
        "evidence": [],
        "recommended_runbooks": [],
        "candidates": [],
        "query_plan": None,
        "semantic_plan": None,
        "evidence_ids": [],
        "diagnostics": {"execution_error": type(exception).__name__},
    }


def _time_range_applied(
    from_at: datetime | None, to_at: datetime | None, timezone_name: str
) -> dict[str, str | None]:
    return {
        "from_at": from_at.isoformat() if from_at else None,
        "to_at": to_at.isoformat() if to_at else None,
        "timezone": timezone_name,
        "timestamp": "failure_at",
    }


def _analytics_envelope(result: dict[str, Any]) -> dict[str, Any]:
    return {
        **result,
        "route": "analytics",
        "source": result.get("source") or "analytics",
        "citations": [],
        "status": result.get("status") or "ok",
        "reason": result.get("reason"),
        "fallback_reason": result.get("fallback_reason"),
        "recommended_runbooks": [],
        "candidates": [],
    }


def _retrieval_config(config: RagConfig, *, mode: str, collection: str, timeout_seconds: float | None = None) -> RagConfig:
    available = {item.name for item in fields(config)}
    changes: dict[str, Any] = {"search_mode": mode, "collection": collection}
    if "retrieval_mode" in available:
        changes["retrieval_mode"] = mode
    if timeout_seconds is not None:
        changes["request_timeout_seconds"] = timeout_seconds
    return replace(config, **changes)


def _validate_conversation_id(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _CONVERSATION_ID.fullmatch(value):
        raise ChatInputError("conversation_id is invalid")
    return value


def _compiled_result_facts(rows: list[dict[str, Any]], plan: QueryPlan) -> list[dict[str, Any]]:
    """Translate scalar aliases from the executed T-SQL result to facts.

    The mapping is fixed in the compiler; it is not a second aggregation and
    cannot alter counts, rank, or connector lineage returned by SQL Server.
    """

    dimensions = {
        "job_name": "root_connector_name",
        "connector_name": "current_connector_name",
        "error_code": "error_code",
        "failure_code": "error_signature",
        "final_outcome": "final_outcome",
    }
    metrics = {
        "failure_count": "incident_count",
        "recovered_count": "recovered_incident_count",
        "open_count": "open_incident_count",
        "average_recovery_minutes": "average_recovery_minutes",
        "recovery_rate_percent": "recovery_rate_percent",
    }
    facts: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        required = [dimensions[field] for field in plan.group_by] + [
            metrics[metric.name] for metric in plan.metrics
        ]
        if plan.group_by:
            required.extend(["rank", "tie_count", "row_number"])
        if any(field not in row for field in required):
            # The query did run, but a changed view/driver result cannot be
            # treated as complete evidence.  Returning no facts lets the
            # outcome classifier emit cannot_verify rather than a false empty.
            return []
        fact: dict[str, Any] = {}
        for field in plan.group_by:
            value = row.get(dimensions[field])
            if value is None and field in {"error_code", "failure_code"}:
                # The compiler filters these dimensions; a NULL now means the
                # source did not honour its result contract.
                continue
            fact[field] = value if value not in {None, ""} else "Chưa xác định"
        if not plan.group_by:
            fact["label"] = "Tất cả"
        for metric in plan.metrics:
            value = row.get(metrics[metric.name])
            # pyodbc may return Decimal.  Convert at the boundary so evidence
            # and the JSON response remain scalar and deterministic.
            if hasattr(value, "as_tuple"):
                value = float(value)
            fact[metric.name] = value
        for field in ("recovery_rate_numerator", "recovery_rate_denominator", "rank", "tie_count", "row_number"):
            value = row.get(field)
            if value is not None:
                fact[field] = int(value) if field in {"rank", "tie_count", "row_number", "recovery_rate_numerator", "recovery_rate_denominator"} else value
        raw_ids = row.get("evidence_ids")
        fact["evidence_ids"] = [
            item for item in str(raw_ids or "").split(";") if item
        ]
        facts.append(fact)
    if plan.tie_policy == "exact_limit" and facts and plan.group_by:
        boundary = facts[-1].get(plan.order_by)
        selected = sum(1 for item in facts if item.get(plan.order_by) == boundary)
        for fact in facts:
            if fact.get(plan.order_by) == boundary:
                fact["tie_truncated"] = int(fact.get("tie_count") or 0) > selected
    return facts


def _prepare_incident_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        message = row.get("error_message")
        if isinstance(message, str) and message.strip():
            row["error_message"] = redact_text(message.strip())[:_MAX_ERROR_MESSAGE_CHARS]
        else:
            row.pop("error_message", None)
        row["failure_code"] = _failure_signature(row)
        prepared.append(row)
    return prepared


def _failure_signature(row: dict[str, Any]) -> str | None:
    explicit = str(row.get("error_code") or "").strip().upper()
    if explicit:
        return explicit
    message = str(row.get("error_message") or "")
    code = _TECHNICAL_CODE.search(message)
    if code:
        return code.group(0).upper()
    error_code = re.search(r"\bError Code:\s*([A-Za-z][A-Za-z0-9_-]{2,80})", message, re.I)
    if error_code:
        return error_code.group(1)
    exceptions = _EXCEPTION_CLASS.findall(message)
    return exceptions[-1] if exceptions else None


def _database_error_filter(error_code: str | None) -> str | None:
    return error_code.upper() if error_code and re.fullmatch(r"ORA-\d{5}", error_code, re.I) else None


def _filter_failure_code(rows: list[dict[str, Any]], requested: str | None) -> list[dict[str, Any]]:
    if not requested:
        return rows
    expected = requested.upper()
    return [row for row in rows if str(row.get("failure_code") or "").upper() == expected]


def _aggregate(rows: list[dict[str, Any]], plan: QueryPlan) -> list[dict[str, Any]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in rows:
        if any(field in {"error_code", "failure_code"} and not row.get(field) for field in plan.group_by):
            continue
        key = tuple(str(row.get(field) or "Chưa xác định") for field in plan.group_by) or ("Tất cả",)
        groups.setdefault(key, []).append(row)
    facts: list[dict[str, Any]] = []
    for key, items in groups.items():
        fact: dict[str, Any] = dict(zip(plan.group_by or ("label",), key, strict=True))
        fact["evidence_ids"] = [str(item.get("incident_id")) for item in items if item.get("incident_id") is not None]
        for metric in plan.metrics:
            if metric.name == "failure_count":
                fact[metric.name] = len({item.get("incident_id") for item in items})
            elif metric.name == "recovered_count":
                fact[metric.name] = len({item.get("incident_id") for item in items if item.get("final_outcome") == "RECOVERED"})
            elif metric.name == "open_count":
                fact[metric.name] = len({item.get("incident_id") for item in items if item.get("final_outcome") == "OPEN"})
            elif metric.name == "average_recovery_minutes":
                durations = [duration for item in items if (duration := _duration(item)) is not None]
                fact[metric.name] = round(sum(durations) / len(durations), 2) if durations else None
                fact["valid_recovery_duration_count"] = len(durations)
                fact["excluded_recovery_duration_count"] = len(items) - len(durations)
            elif metric.name == "recovery_rate_percent":
                denominator = len({item.get("incident_id") for item in items if item.get("incident_id") is not None})
                numerator = len({
                    item.get("incident_id")
                    for item in items
                    if item.get("incident_id") is not None and item.get("final_outcome") == "RECOVERED"
                })
                fact[metric.name] = round((numerator / denominator) * 100, 2) if denominator else None
                fact["recovery_rate_denominator"] = denominator
                fact["recovery_rate_numerator"] = numerator
        for detail in plan.details:
            values = _unique_values(items, detail)
            if values:
                fact[detail] = values[0] if len(values) == 1 else values[:3]
        facts.append(fact)
    present = [item for item in facts if item.get(plan.order_by) is not None]
    missing = [item for item in facts if item.get(plan.order_by) is None]
    present.sort(key=lambda item: tuple(str(item.get(field) or "") for field in plan.group_by))
    present.sort(key=lambda item: item[plan.order_by], reverse=plan.direction == "desc")
    ordered = present + missing
    if not plan.group_by:
        return ordered[:plan.limit]
    # The fallback aggregation exists for offline fixtures and unsupported
    # projections.  It applies exactly the same ranking semantics as the
    # production T-SQL compiler: top N ranks include boundary ties by default.
    rank = 0
    previous: Any = object()
    tie_counts: dict[Any, int] = {}
    for item in present:
        tie_counts[item[plan.order_by]] = tie_counts.get(item[plan.order_by], 0) + 1
    for index, item in enumerate(ordered, start=1):
        value = item.get(plan.order_by)
        if value != previous:
            rank += 1
            previous = value
        item["rank"] = rank
        item["tie_count"] = tie_counts.get(value, 1)
        item["row_number"] = index
    if plan.tie_policy == "exact_limit":
        selected = ordered[:plan.limit]
        if selected:
            boundary = selected[-1].get(plan.order_by)
            selected_boundary_count = sum(1 for item in selected if item.get(plan.order_by) == boundary)
            for item in selected:
                if item.get(plan.order_by) == boundary:
                    item["tie_truncated"] = int(item["tie_count"]) > selected_boundary_count
        return selected
    return [item for item in ordered if int(item["rank"]) <= plan.limit]


def _unique_values(rows: list[dict[str, Any]], field: str) -> list[Any]:
    values: list[Any] = []
    for row in rows:
        value = row.get(field)
        if value not in {None, ""} and value not in values:
            values.append(value)
    return values


def _verified_result_row(fact: dict[str, Any]) -> dict[str, str | int | float | bool | None]:
    """Keep the browser table contract scalar while retaining all fact values."""

    row: dict[str, str | int | float | bool | None] = {}
    for key, value in fact.items():
        safe = json_safe(value)
        if safe is None or isinstance(safe, (str, int, float, bool)):
            row[key] = safe
        elif isinstance(safe, list):
            row[key] = "; ".join(str(item) for item in safe)
        else:
            row[key] = str(safe)
    return row


def _duration(item: dict[str, Any]) -> float | None:
    if item.get("final_outcome") != "RECOVERED" or item.get("queue_status") != "COMPLETED":
        return None
    start, end = item.get("failure_at"), item.get("recovered_at")
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return None
    if start.utcoffset() is None or end.utcoffset() is None:
        return None
    minutes = (end - start).total_seconds() / 60
    return minutes if minutes >= 0 else None
