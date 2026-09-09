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
from self_healthy_kafka.rag.router import RunbookRouter
from self_healthy_kafka.rag.shadow import Retriever
from self_healthy_kafka.redaction import redact_text

logger = logging.getLogger(__name__)


def _empty_analytics_fields() -> dict[str, Any]:
    """Keep the pre-RAG response shape for clients that render analytics fields."""
    return {
        "sources": [],
        "query_plan": None,
        "from_at": None,
        "to_at": None,
        "row_count": 0,
        "evidence_ids": [],
    }


class RunbookRagWorkflow:
    def __init__(
        self,
        config: RagConfig,
        *,
        retriever: Retriever,
        composer: GroundedAnswerComposer,
        router: RunbookRouter | None = None,
    ):
        self._config = config
        self._retriever = retriever
        self._composer = composer
        self._router = router or RunbookRouter()

    def ask(
        self,
        question: str,
        *,
        analytics_ask: Callable[[str], dict[str, Any]],
    ) -> dict[str, Any]:
        started = time.perf_counter()
        decision = self._router.route(question)
        if decision.needs_clarification:
            result = {
                **_empty_analytics_fields(),
                "answer": decision.clarification_question,
                "route": decision.route.value,
                "source": "deterministic_fallback",
                "citations": [],
                "fallback_reason": "material_ambiguity",
                "status": "needs_clarification",
                "reason": "material_ambiguity",
                "evidence": [],
                "recommended_runbooks": [],
                "candidates": [],
            }
            self._log_result(decision.route, started, result, retrieval_hits=0)
            return result
        if decision.route is Route.ANALYTICS:
            result = analytics_ask(question)
            result = {
                **result,
                "route": "analytics",
                "source": "analytics",
                "citations": [],
                "fallback_reason": None,
                "status": "ok",
                "reason": None,
                "evidence": [],
                "recommended_runbooks": [],
                "candidates": [],
            }
            self._log_result(decision.route, started, result, retrieval_hits=0)
            return result

        analytics_result: dict[str, Any] = {}
        facts: list[dict[str, Any]] = []
        if decision.route is Route.COMBINED:
            analytics_result = analytics_ask(question)
            facts = extract_verified_facts(analytics_result)
        error_codes = decision.error_codes or tuple(
            dict.fromkeys(
                str(item["error_code"]).upper() for item in facts if item.get("error_code")
            )
        )
        connector_class = decision.connector_class or next(
            (str(item["connector_class"]).lower() for item in facts if item.get("connector_class")),
            None,
        )
        retrieval_query = RetrievalQuery(
            text=_retrieval_text(question, facts),
            tenant_id=self._config.tenant_id,
            environment=self._config.environment,
            connector_class=connector_class,
            error_codes=error_codes,
        )
        try:
            chunks = self._retriever.retrieve(retrieval_query)
            search_diagnostics = self._retriever.last_diagnostics
        except (RagStoreError, RagConfigurationError) as exc:
            fallback_reason = (
                "qdrant_configuration_error"
                if isinstance(exc, RagConfigurationError)
                else "qdrant_service_error"
            )
            if analytics_result:
                result = {
                    **analytics_result,
                    "route": decision.route.value,
                    "source": "analytics",
                    "citations": [],
                    "fallback_reason": fallback_reason,
                    "status": "degraded",
                    "reason": fallback_reason,
                    "evidence": [],
                    "recommended_runbooks": [],
                    "candidates": [],
                }
            else:
                result = {
                    **_empty_analytics_fields(),
                    "answer": "Kho runbook hiện không truy cập được. Vui lòng thử lại sau.",
                    "route": decision.route.value,
                    "source": "deterministic_fallback",
                    "citations": [],
                    "fallback_reason": fallback_reason,
                    "status": "degraded",
                    "reason": fallback_reason,
                    "evidence": [],
                    "recommended_runbooks": [],
                    "candidates": [],
                }
            self._log_result(
                decision.route,
                started,
                result,
                retrieval_hits=0,
                search_diagnostics=self._retriever.last_diagnostics,
            )
            return result
        composed = self._composer.compose(
            question=question,
            route=decision.route,
            analytics_facts=facts,
            chunks=chunks,
        )
        no_answer_reason = _no_answer_reason(search_diagnostics, composed.fallback_reason)
        recommended = _recommended_runbooks(chunks)
        answer = composed.answer
        if not chunks and composed.source != "analytics":
            answer = _no_answer_message(no_answer_reason)
        result = {
            **_empty_analytics_fields(),
            **analytics_result,
            "answer": answer,
            "route": decision.route.value,
            "source": composed.source,
            "citations": [item.to_dict() for item in composed.citations],
            "fallback_reason": composed.fallback_reason,
            "status": "ok" if chunks else "no_answer",
            "reason": (
                None
                if chunks
                else no_answer_reason
            ),
            "evidence": _response_evidence(chunks, retrieval_query),
            "recommended_runbooks": recommended,
            "candidates": recommended,
        }
        if self._config.diagnostics_enabled:
            result["diagnostics"] = {
                "router": decision.to_dict(),
                "retrieval": [
                    {
                        "point_id": item.point_id,
                        "runbook_id": item.runbook_id,
                        "section": item.section,
                        "score": round(item.score, 6),
                    }
                    for item in chunks
                ],
                "search": (
                    search_diagnostics.to_dict()
                    if search_diagnostics is not None
                    else None
                ),
            }
        self._log_result(
            decision.route,
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
        route: Route,
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
                "route": route.value,
                "source": result.get("source"),
                "fallback_reason": result.get("fallback_reason"),
                "retrieval_hits": retrieval_hits,
                "latency_seconds": round(time.perf_counter() - started, 6),
                "retrieval": search,
            },
        )


def extract_verified_facts(result: dict[str, Any]) -> list[dict[str, Any]]:
    facts: list[dict[str, Any]] = []
    for source in result.get("sources") or []:
        if not isinstance(source, dict) or not str(source.get("source", "")).startswith(
            "vConnectorIncidentFacts"
        ):
            continue
        items = source.get("items") or []
        if isinstance(items, list):
            facts.extend(item for item in items if isinstance(item, dict))
    return bounded_verified_facts(facts)


def _retrieval_text(question: str, facts: list[dict[str, Any]]) -> str:
    identifiers: list[str] = []
    for item in facts[:10]:
        for field in ("connector_name", "connector_class", "error_code", "event_type"):
            value = str(item.get(field) or "").strip()
            if value and value not in identifiers:
                identifiers.append(value)
    suffix = " ".join(identifiers)
    text = f"{question.strip()}\nVerified identifiers: {suffix}" if suffix else question.strip()
    return redact_text(text)


def _no_answer_reason(
    diagnostics: SearchDiagnostics | None,
    fallback_reason: str | None,
) -> str:
    if diagnostics is not None and diagnostics.no_result_reason:
        return diagnostics.no_result_reason
    if fallback_reason is None or fallback_reason == "no_applicable_runbook":
        return "insufficient_retrieval_evidence"
    return fallback_reason


def _response_evidence(
    chunks: list[RetrievedChunk],
    query: RetrievalQuery,
) -> list[dict[str, Any]]:
    """Expose bounded evidence labels to clients, never raw scores or chunk text."""

    result: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    requested_codes = {value.upper() for value in query.error_codes}
    normalized_query = _canonical_evidence(query.text)
    for chunk in chunks:
        identity = (chunk.runbook_id, chunk.version, chunk.section)
        if identity in seen:
            continue
        seen.add(identity)
        matched_codes = sorted(
            requested_codes & {str(value).upper() for value in chunk.error_codes}
        )
        matched_config_keys = [
            value
            for value in chunk.config_keys
            if _canonical_evidence(value) in normalized_query
        ]
        matched_exceptions = [
            value
            for value in chunk.exception_classes
            if _canonical_evidence(value) in normalized_query
        ]
        matched_signatures = [
            value
            for value in chunk.error_signatures
            if _canonical_evidence(value) in normalized_query
        ]
        if matched_codes:
            match_basis = "error_code"
        elif matched_config_keys:
            match_basis = "config_key"
        elif matched_exceptions:
            match_basis = "exception_class"
        elif matched_signatures:
            match_basis = "error_signature"
        else:
            match_basis = "semantic_or_lexical"
        result.append(
            {
                "runbook_id": chunk.runbook_id,
                "version": chunk.version,
                "section": chunk.section,
                "source": chunk.source,
                "match_basis": match_basis,
                "matched_error_codes": matched_codes,
                "matched_config_keys": matched_config_keys[:5],
                "matched_exception_classes": matched_exceptions[:5],
                "matched_error_signatures": matched_signatures[:5],
            }
        )
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
        result.append(
            {
                "runbook_id": chunk.runbook_id,
                "title": chunk.title,
                "version": chunk.version,
            }
        )
        if len(result) >= 5:
            break
    return result


def _canonical_evidence(value: str) -> str:
    return "".join(character for character in value.casefold() if character.isalnum())


def _no_answer_message(reason: str) -> str:
    if reason in {"strong_anchor_not_found", "explicit_issue_guard_filtered_all"}:
        return (
            "Mình chưa tìm thấy runbook đã được phê duyệt khớp với mã lỗi hoặc dấu hiệu "
            "kỹ thuật bạn cung cấp."
        )
    if reason in {"explicit_domain_exclusion", "insufficient_domain_evidence"}:
        return "Câu hỏi này chưa có đủ bằng chứng liên quan đến sự cố Kafka Connect trong kho runbook."
    if reason == "rank_margin_below_evidence_threshold":
        return (
            "Mình thấy nhiều runbook có mức phù hợp gần nhau nên chưa thể đề xuất an toàn. "
            "Bạn vui lòng bổ sung tên loại connector hoặc một mã lỗi ngắn trong log."
        )
    return "Mình chưa tìm thấy runbook đủ phù hợp với lỗi được cung cấp."
