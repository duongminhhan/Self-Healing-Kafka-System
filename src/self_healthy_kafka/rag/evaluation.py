from __future__ import annotations

import json
import math
import statistics
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from self_healthy_kafka.rag.models import RetrievedChunk, SearchDiagnostics

QUERY_TYPES = {"exact", "semantic", "mixed", "negative"}


@dataclass(frozen=True)
class GoldRetrievalCase:
    case_id: str
    question: str
    expected_runbook_ids: tuple[str, ...]
    expected_sections: tuple[str, ...]
    expected_error_codes: tuple[str, ...]
    expected_connector_class: str | None
    query_type: str
    notes: str
    tenant_id: str = "default"
    environment: str = "all"
    fixture_only: bool = False


def load_gold_retrieval(path: Path) -> list[GoldRetrievalCase]:
    records: list[GoldRetrievalCase] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        missing = [
            key
            for key in (
                "id",
                "question",
                "expected_runbook_ids",
                "expected_sections",
                "expected_error_codes",
                "expected_connector_class",
                "query_type",
                "notes",
            )
            if key not in value
        ]
        if missing:
            raise ValueError(f"{path}:{line_number}: missing fields: {', '.join(missing)}")
        query_type = str(value["query_type"]).strip().lower()
        if query_type not in QUERY_TYPES:
            raise ValueError(
                f"{path}:{line_number}: query_type must be one of {sorted(QUERY_TYPES)}"
            )
        records.append(
            GoldRetrievalCase(
                case_id=str(value["id"]),
                question=str(value["question"]),
                expected_runbook_ids=tuple(map(str, value["expected_runbook_ids"])),
                expected_sections=tuple(map(str, value["expected_sections"])),
                expected_error_codes=tuple(map(str, value["expected_error_codes"])),
                expected_connector_class=(
                    str(value["expected_connector_class"])
                    if value["expected_connector_class"] is not None
                    else None
                ),
                query_type=query_type,
                notes=str(value["notes"]),
                tenant_id=str(value.get("tenant_id") or "default"),
                environment=str(value.get("environment") or "all"),
                fixture_only=bool(value.get("fixture_only", False)),
            )
        )
    identities = [item.case_id for item in records]
    if len(identities) != len(set(identities)):
        raise ValueError(f"{path}: duplicate case id")
    return records


RetrievalFunction = Callable[
    [GoldRetrievalCase], tuple[list[RetrievedChunk], SearchDiagnostics | None]
]


def evaluate_retrieval(
    cases: Iterable[GoldRetrievalCase],
    retrieve: RetrievalFunction,
    *,
    mode: str,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    latencies: list[float] = []
    candidate_counts: list[int] = []
    fallback_count = 0
    failures = 0
    for case in cases:
        if case.fixture_only:
            results.append(
                {
                    "id": case.case_id,
                    "query_type": case.query_type,
                    "status": "not_run",
                    "reason": "fixture_only_case",
                }
            )
            continue
        started = time.perf_counter()
        try:
            chunks, diagnostics = retrieve(case)
            latency_ms = (time.perf_counter() - started) * 1000
        except Exception as exc:
            failures += 1
            results.append(
                {
                    "id": case.case_id,
                    "query_type": case.query_type,
                    "status": "failed",
                    "error_type": type(exc).__name__,
                }
            )
            continue
        if diagnostics is not None and diagnostics.total_retrieval_latency_ms:
            latency_ms = diagnostics.total_retrieval_latency_ms
            candidate_counts.append(diagnostics.fused_candidate_count)
            fallback_count += int(diagnostics.fallback_reason is not None)
        else:
            candidate_counts.append(len(chunks))
        latencies.append(latency_ms)
        ids = list(dict.fromkeys(item.runbook_id for item in chunks))
        expected = set(case.expected_runbook_ids)
        tenant_violations = sum(
            bool(item.tenant_id and item.tenant_id != case.tenant_id) for item in chunks
        )
        status_violations = sum(bool(item.status and item.status != "approved") for item in chunks)
        environment_violations = sum(
            bool(
                item.environments
                and case.environment not in item.environments
                and "all" not in item.environments
            )
            for item in chunks
        )
        results.append(
            {
                "id": case.case_id,
                "query_type": case.query_type,
                "status": "measured",
                "retrieved_runbook_ids": ids,
                "expected_runbook_ids": list(case.expected_runbook_ids),
                "recall_at_1": _recall(ids, expected, 1),
                "recall_at_3": _recall(ids, expected, 3),
                "recall_at_5": _recall(ids, expected, 5),
                "reciprocal_rank": _reciprocal_rank(ids, expected),
                "ndcg_at_5": _ndcg(ids, expected, 5),
                "no_result": not ids,
                "wrong_runbook": bool(ids) if not expected else ids[0] not in expected,
                "tenant_filter_violations": tenant_violations,
                "status_filter_violations": status_violations,
                "environment_filter_violations": environment_violations,
                "latency_ms": round(latency_ms, 3),
                "candidate_count": (
                    diagnostics.fused_candidate_count if diagnostics is not None else len(chunks)
                ),
                "fallback": bool(diagnostics and diagnostics.fallback_reason),
            }
        )
    measured = [item for item in results if item["status"] == "measured"]
    positive = [item for item in measured if item["expected_runbook_ids"]]
    return {
        "mode": mode,
        "case_count": len(results),
        "measured_count": len(measured),
        "not_run_count": sum(item["status"] == "not_run" for item in results),
        "retrieval_failures": failures,
        "recall_at_1": _mean(item["recall_at_1"] for item in positive),
        "recall_at_3": _mean(item["recall_at_3"] for item in positive),
        "recall_at_5": _mean(item["recall_at_5"] for item in positive),
        "mrr": _mean(item["reciprocal_rank"] for item in positive),
        "ndcg_at_5": _mean(item["ndcg_at_5"] for item in positive),
        "no_result_rate": _mean(item["no_result"] for item in measured),
        "wrong_runbook_rate": _mean(item["wrong_runbook"] for item in measured),
        "tenant_filter_violations": sum(item["tenant_filter_violations"] for item in measured),
        "status_filter_violations": sum(item["status_filter_violations"] for item in measured),
        "environment_filter_violations": sum(
            item["environment_filter_violations"] for item in measured
        ),
        "p50_latency_ms": _percentile(latencies, 50),
        "p95_latency_ms": _percentile(latencies, 95),
        "average_candidate_count": _mean(candidate_counts),
        "dense_fallback_rate": fallback_count / len(measured) if measured else None,
        "by_query_type": {
            query_type: _breakdown(measured, query_type) for query_type in sorted(QUERY_TYPES)
        },
        "case_results": results,
    }


def _recall(ids: list[str], expected: set[str], k: int) -> float | None:
    if not expected:
        return None
    return len(expected & set(ids[:k])) / len(expected)


def _reciprocal_rank(ids: list[str], expected: set[str]) -> float | None:
    if not expected:
        return None
    return next((1 / index for index, value in enumerate(ids, 1) if value in expected), 0.0)


def _ndcg(ids: list[str], expected: set[str], k: int) -> float | None:
    if not expected:
        return None
    dcg = sum(
        (1 / math.log2(index + 1)) if value in expected else 0
        for index, value in enumerate(ids[:k], 1)
    )
    ideal_hits = min(len(expected), k)
    ideal = sum(1 / math.log2(index + 1) for index in range(1, ideal_hits + 1))
    return dcg / ideal if ideal else 0.0


def _mean(values: Iterable[float | bool | int | None]) -> float | None:
    present = [float(value) for value in values if value is not None]
    return statistics.fmean(present) if present else None


def _percentile(values: list[float], percentile: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile / 100
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 3)
    value = ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)
    return round(value, 3)


def _breakdown(results: list[dict[str, Any]], query_type: str) -> dict[str, Any]:
    selected = [item for item in results if item["query_type"] == query_type]
    positive = [item for item in selected if item["expected_runbook_ids"]]
    return {
        "case_count": len(selected),
        "recall_at_5": _mean(item["recall_at_5"] for item in positive),
        "mrr": _mean(item["reciprocal_rank"] for item in positive),
        "wrong_runbook_rate": _mean(item["wrong_runbook"] for item in selected),
        "p50_latency_ms": _percentile(
            [float(item["latency_ms"]) for item in selected],
            50,
        ),
    }
