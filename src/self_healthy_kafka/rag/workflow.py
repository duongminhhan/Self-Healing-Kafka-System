"""Runbook orchestration driven by a validated semantic plan.

There is intentionally no question router in this module.  The caller supplies
the backend-validated plan and this workflow derives the route from its data
and guidance requirements.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.answer_composer import GroundedAnswerComposer, bounded_verified_facts
from self_healthy_kafka.rag.models import (
    RagConfigurationError,
    RagStoreError,
    RetrievalQuery,
    RetrievedChunk,
    Route,
    SearchDiagnostics,
)
from self_healthy_kafka.rag.shadow import Retriever
from self_healthy_kafka.redaction import redact_text
from self_healthy_kafka.semantic.fact_source import is_incident_evidence_source
from self_healthy_kafka.semantic.planner import SemanticPlan

logger = logging.getLogger(__name__)


def _empty_analytics_fields() -> dict[str, Any]:
    return {
        "sources": [],
        "query_plan": None,
        "semantic_plan": None,
        "from_at": None,
        "to_at": None,
        "row_count": 0,
        "outcome": "cannot_verify",
        "query_executed": False,
        "evidence_complete": False,
        "source_kind": "historical_incident_snapshot",
        "snapshot_freshness": None,
        "time_range_applied": None,
        "presentation": None,
        "evidence_ids": [],
        "analytics_evidence": [],
        "claims": [],
        "runbook_claims": [],
    }


class RunbookRagWorkflow:
    def __init__(
        self,
        config: RagConfig,
        *,
        retriever: Retriever,
        composer: GroundedAnswerComposer,
    ):
        self._config = config
        self._retriever = retriever
        self._composer = composer

    def ask(
        self,
        question: str,
        *,
        plan: SemanticPlan,
        analytics_ask: Callable[[SemanticPlan], dict[str, Any]],
    ) -> dict[str, Any]:
        started = time.perf_counter()
        route = plan.route
        if plan.clarification:
            result = {
                **_empty_analytics_fields(),
                "answer": plan.clarification,
                "route": "clarification",
                "source": "deterministic_fallback",
                "citations": [],
                "fallback_reason": "material_ambiguity",
                "status": "needs_clarification",
                "outcome": "needs_clarification",
                "query_executed": False,
                "evidence_complete": False,
                "reason": "material_ambiguity",
                "evidence": [],
                "recommended_runbooks": [],
                "candidates": [],
            }
            self._log_result("clarification", started, result, retrieval_hits=0)
            return result
        if route is None:
            result = {
                **_empty_analytics_fields(),
                "answer": "Mình chưa có yêu cầu dữ liệu hoặc hướng dẫn có thể thực thi từ câu hỏi này.",
                "route": "unsupported",
                "source": "deterministic_fallback",
                "citations": [],
                "fallback_reason": "unsupported_semantic_plan",
                "status": "no_answer",
                "outcome": "cannot_verify",
                "query_executed": False,
                "evidence_complete": False,
                "reason": "unsupported_semantic_plan",
                "evidence": [],
                "recommended_runbooks": [],
                "candidates": [],
            }
            self._log_result("unsupported", started, result, retrieval_hits=0)
            return result
        if route is Route.ANALYTICS:
            result = _analytics_envelope(analytics_ask(plan))
            self._log_result(route.value, started, result, retrieval_hits=0)
            return result

        analytics_result: dict[str, Any] = {}
        facts: list[dict[str, Any]] = []
        analytics_evidence: list[dict[str, Any]] = []
        if route is Route.COMBINED:
            analytics_result = analytics_ask(plan)
            facts = extract_verified_facts(analytics_result)
            analytics_evidence = _analytics_evidence(analytics_result)

        guidance = plan.guidance_request
        error_codes = guidance.error_codes or tuple(
            dict.fromkeys(
                str(item.get("error_code") or item.get("failure_code") or "").upper()
                for item in facts
                if item.get("error_code") or item.get("failure_code")
            )
        )
        connector_class = guidance.connector_class or next(
            (str(item["connector_class"]).lower() for item in facts if item.get("connector_class")),
            None,
        )
        retrieval_query = RetrievalQuery(
            text=_retrieval_text(question, facts),
            tenant_id=self._config.tenant_id,
            environment=self._config.environment,
            connector_class=connector_class,
            error_codes=error_codes,
            purpose=guidance.purpose,
        )
        try:
            chunks = self._retriever.retrieve(retrieval_query)
            search_diagnostics = self._retriever.last_diagnostics
        except (RagStoreError, RagConfigurationError) as exc:
            fallback_reason = (
                "qdrant_configuration_error" if isinstance(exc, RagConfigurationError) else "qdrant_service_error"
            )
            result = _rag_failure_result(analytics_result, route, fallback_reason)
            self._log_result(
                route.value,
                started,
                result,
                retrieval_hits=0,
                search_diagnostics=self._retriever.last_diagnostics,
            )
            return result

        composed = self._composer.compose(
            question=question,
            route=route,
            analytics_facts=facts,
            chunks=chunks,
            guidance_purpose=guidance.purpose,
            analytics_evidence=analytics_evidence,
        )
        no_answer_reason = _no_answer_reason(search_diagnostics, composed.fallback_reason)
        recommended = _recommended_runbooks(chunks)
        # Analytics is authoritative for every combined request, not merely a
        # retrieval miss.  Runbook text is supplementary guidance appended to
        # the verified data answer and is never allowed to reinterpret it.
        keep_analytics = route is Route.COMBINED and bool(analytics_result)
        analytics_answer = str(analytics_result.get("answer") or "").strip()
        if keep_analytics:
            answer = analytics_answer
            if chunks and composed.answer.strip():
                answer = f"{analytics_answer}\n\nHướng dẫn liên quan:\n{composed.answer.strip()}"
            elif not chunks:
                # This is a fixed retrieval-system outcome, not a business
                # answer template.  Keep the verified data conclusion intact
                # and make the missing guidance visible to the user.
                answer = (
                    f"{analytics_answer}\n\n"
                    "Mình chưa tìm thấy runbook đã được phê duyệt đủ phù hợp để đề xuất bước xử lý."
                )
        else:
            answer = composed.answer if chunks or composed.source == "analytics" else _no_answer_message(no_answer_reason)
        runbook_evidence = _response_evidence(chunks, retrieval_query)
        result = {
            **_empty_analytics_fields(),
            **analytics_result,
            "answer": answer,
            "route": route.value,
            "source": "combined" if keep_analytics and chunks else ("analytics" if keep_analytics else composed.source),
            "citations": [item.to_dict() for item in composed.citations],
            "fallback_reason": composed.fallback_reason,
            "status": (
                str(analytics_result.get("status") or "ok")
                if keep_analytics else ("ok" if chunks or composed.source == "analytics" else "no_answer")
            ),
            "reason": (
                analytics_result.get("reason")
                if keep_analytics else (None if chunks or composed.source == "analytics" else no_answer_reason)
            ),
            # Preserve the existing ``evidence`` runbook shape for the UI and
            # expose typed analytics facts separately.  This avoids a breaking
            # union while retaining both provenance packets for technical
            # details and downstream auditing.
            "evidence": runbook_evidence,
            "analytics_evidence": analytics_evidence,
            "runbook_claims": list(composed.claims),
            "runbook_generation_attempts": composed.generation_attempts,
            "recommended_runbooks": recommended,
            "candidates": recommended,
        }
        if self._config.diagnostics_enabled:
            result["diagnostics"] = {
                "semantic_plan": plan.to_dict(),
                "retrieval": [
                    {"point_id": item.point_id, "runbook_id": item.runbook_id, "section": item.section}
                    for item in chunks
                ],
                "search": search_diagnostics.to_dict() if search_diagnostics is not None else None,
            }
        self._log_result(
            route.value,
            started,
            result,
            retrieval_hits=len(chunks),
            search_diagnostics=search_diagnostics,
        )
        return result

    def close(self) -> None:
        close = getattr(self._retriever, "close", None)
        if callable(close):
            close()

    @staticmethod
    def _log_result(
        route: str,
        started: float,
        result: dict[str, Any],
        *,
        retrieval_hits: int,
        search_diagnostics: SearchDiagnostics | None = None,
    ) -> None:
        search = search_diagnostics.to_dict() if search_diagnostics is not None else {}
        logger.info(
            "Runbook RAG request completed",
            extra={
                "event": "runbook_rag_completed",
                "route": route,
                "source": result.get("source"),
                "fallback_reason": result.get("fallback_reason"),
                "retrieval_hits": retrieval_hits,
                "latency_seconds": round(time.perf_counter() - started, 6),
                "retrieval": search,
            },
        )


def _analytics_envelope(result: dict[str, Any]) -> dict[str, Any]:
    return {
        **result,
        "route": "analytics",
        "source": result.get("source") or "analytics",
        "citations": [],
        "fallback_reason": result.get("fallback_reason"),
        "status": result.get("status") or "ok",
        "reason": result.get("reason"),
        "recommended_runbooks": [],
        "candidates": [],
    }


def _rag_failure_result(analytics_result: dict[str, Any], route: Route, reason: str) -> dict[str, Any]:
    if analytics_result:
        analytics_answer = str(analytics_result.get("answer") or "").strip()
        return {
            **analytics_result,
            "answer": (
                f"{analytics_answer}\n\n"
                "Phần hướng dẫn chưa thể tải từ kho runbook; kết quả dữ liệu ở trên vẫn dựa trên nguồn đã xác minh."
            ),
            "route": route.value,
            "source": analytics_result.get("source") or "analytics",
            "citations": [],
            "fallback_reason": reason,
            # RAG availability is separate from whether the analytics evidence
            # was complete.  Do not relabel a verified analytics answer as a
            # data failure merely because supplementary guidance failed.
            "status": analytics_result.get("status") or "ok",
            "reason": reason,
            "evidence": analytics_result.get("evidence") or [],
            "recommended_runbooks": [],
            "candidates": [],
        }
    return {
        **_empty_analytics_fields(),
        "answer": "Kho runbook hiện không truy cập được. Vui lòng thử lại sau.",
        "route": route.value,
        "source": "deterministic_fallback",
        "citations": [],
        "fallback_reason": reason,
        "status": "degraded",
        "outcome": "degraded",
        "query_executed": False,
        "evidence_complete": False,
        "reason": reason,
        "evidence": [],
        "recommended_runbooks": [],
        "candidates": [],
    }


def extract_verified_facts(result: dict[str, Any]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for source in result.get("sources") or []:
        if not isinstance(source, dict) or not is_incident_evidence_source(source.get("source")):
            continue
        items = source.get("items") or []
        if isinstance(items, list):
            facts.extend(item for item in items if isinstance(item, dict))
    return bounded_verified_facts(facts)


def _analytics_evidence(result: dict[str, Any]) -> list[dict[str, Any]]:
    evidence = result.get("evidence")
    if not isinstance(evidence, list):
        return []
    # Evidence is built in the analytics boundary, redacted there, and bounded
    # by the source row limit. Keep only recognisable fact packets here.
    return [
        item for item in evidence[:100]
        if isinstance(item, dict) and isinstance(item.get("fact_id"), str)
        and isinstance(item.get("metrics"), list) and isinstance(item.get("time_range"), dict)
    ]


def _retrieval_text(question: str, facts: list[dict[str, Any]]) -> str:
    identifiers: list[str] = []
    for item in facts[:10]:
        for field in ("connector_name", "connector_class", "error_code", "failure_code", "event_type"):
            value = str(item.get(field) or "").strip()
            if value and value not in identifiers:
                identifiers.append(value)
    suffix = " ".join(identifiers)
    return redact_text(f"{question.strip()}\nVerified identifiers: {suffix}" if suffix else question.strip())


def _no_answer_reason(diagnostics: SearchDiagnostics | None, fallback_reason: str | None) -> str:
    if diagnostics is not None and diagnostics.no_result_reason:
        return diagnostics.no_result_reason
    if fallback_reason is None or fallback_reason == "no_applicable_runbook":
        return "insufficient_retrieval_evidence"
    return fallback_reason


def _response_evidence(chunks: list[RetrievedChunk], query: RetrievalQuery) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    requested_codes = {value.upper() for value in query.error_codes}
    for chunk in chunks:
        identity = (chunk.runbook_id, chunk.version, chunk.section)
        if identity in seen:
            continue
        seen.add(identity)
        matched_codes = sorted(requested_codes & {str(value).upper() for value in chunk.error_codes})
        if matched_codes:
            match_basis = "error_code"
        else:
            match_basis = "semantic_plan"
        result.append({
            "runbook_id": chunk.runbook_id,
            "version": chunk.version,
            "section": chunk.section,
            "source": chunk.source,
            "match_basis": match_basis,
            "matched_error_codes": matched_codes,
            "matched_config_keys": [],
            "matched_exception_classes": [],
            "matched_error_signatures": [],
        })
        if len(result) >= 10:
            break
    return result


def _recommended_runbooks(chunks: list[RetrievedChunk]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    seen: set[tuple[str, int]] = set()
    for chunk in chunks:
        identity = (chunk.runbook_id, chunk.version)
        if identity in seen:
            continue
        seen.add(identity)
        result.append({"runbook_id": chunk.runbook_id, "title": chunk.title, "version": chunk.version})
        if len(result) >= 5:
            break
    return result


def _no_answer_message(reason: str) -> str:
    if reason == "strong_anchor_not_found":
        return "Mình chưa tìm thấy runbook đã được phê duyệt khớp với mã lỗi hoặc dấu hiệu kỹ thuật được cung cấp."
    if reason == "rank_margin_below_evidence_threshold":
        return "Mình thấy nhiều runbook có mức phù hợp gần nhau nên chưa thể đề xuất an toàn. Bạn vui lòng bổ sung dấu hiệu kỹ thuật cụ thể."
    return "Mình chưa tìm thấy runbook đủ phù hợp với yêu cầu này."
