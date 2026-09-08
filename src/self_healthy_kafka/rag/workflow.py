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
    Route,
    SearchDiagnostics,
)
from self_healthy_kafka.rag.retriever import RunbookRetriever
from self_healthy_kafka.rag.router import RunbookRouter
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
        retriever: RunbookRetriever,
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
                }
            else:
                result = {
                    **_empty_analytics_fields(),
                    "answer": "Kho runbook hiện không truy cập được. Vui lòng thử lại sau.",
                    "route": decision.route.value,
                    "source": "deterministic_fallback",
                    "citations": [],
                    "fallback_reason": fallback_reason,
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
        result = {
            **_empty_analytics_fields(),
            **analytics_result,
            "answer": composed.answer,
            "route": decision.route.value,
            "source": composed.source,
            "citations": [item.to_dict() for item in composed.citations],
            "fallback_reason": composed.fallback_reason,
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
                    self._retriever.last_diagnostics.to_dict()
                    if self._retriever.last_diagnostics is not None
                    else None
                ),
            }
        self._log_result(
            decision.route,
            started,
            result,
            retrieval_hits=len(chunks),
            search_diagnostics=self._retriever.last_diagnostics,
        )
        return result

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
