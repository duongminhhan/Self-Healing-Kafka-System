from __future__ import annotations

import json
import math
import statistics
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from self_healthy_kafka.rag.models import RetrievedChunk, SearchDiagnostics

QUERY_TYPES = {"exact", "semantic", "mixed", "negative"}
CATEGORIES = {
    "exact_error",
    "exact_config_key",
    "semantic",
    "mixed_vi_en",
    "connector_specific",
    "multi_symptom",
    "typo",
    "ambiguous",
    "negative",
    "filter_sensitive",
}
DIFFICULTIES = {"easy", "medium", "hard"}
SPLITS = {"tuning", "holdout"}


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
    category: str = "semantic"
    difficulty: str = "medium"
    split: str = "tuning"
    expected_no_answer: bool = False
    relevance: Mapping[str, float] = field(default_factory=dict)
    filters: Mapping[str, Any] = field(default_factory=dict)

    @property
    def relevance_map(self) -> dict[str, float]:
        return {
            runbook_id: float(self.relevance.get(runbook_id, 1.0))
            for runbook_id in self.expected_runbook_ids
        }


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
                "expected_runbook_ids",
                "expected_sections",
                "expected_error_codes",
                "expected_connector_class",
                "query_type",
                "notes",
            )
            if key not in value
        ]
        if "question" not in value and "query" not in value:
            missing.append("question/query")
        if missing:
            raise ValueError(f"{path}:{line_number}: missing fields: {', '.join(missing)}")

        query_type = str(value["query_type"]).strip().lower()
        if query_type not in QUERY_TYPES:
            raise ValueError(
                f"{path}:{line_number}: query_type must be one of {sorted(QUERY_TYPES)}"
            )
        category = str(value.get("category") or _default_category(value, query_type)).lower()
        difficulty = str(value.get("difficulty") or "medium").lower()
        split = str(value.get("split") or "tuning").lower()
        _validate_enum(path, line_number, "category", category, CATEGORIES)
        _validate_enum(path, line_number, "difficulty", difficulty, DIFFICULTIES)
        _validate_enum(path, line_number, "split", split, SPLITS)

        expected = tuple(map(str, value["expected_runbook_ids"]))
        expected_no_answer = bool(value.get("expected_no_answer", not expected))
        if expected_no_answer and expected:
            raise ValueError(
                f"{path}:{line_number}: expected_no_answer cannot be true with expected runbooks"
            )
        relevance_value = value.get("relevance") or {}
        if not isinstance(relevance_value, dict):
            raise ValueError(f"{path}:{line_number}: relevance must be an object")
        relevance = {str(key): float(score) for key, score in relevance_value.items()}
        if set(relevance) - set(expected):
            raise ValueError(
                f"{path}:{line_number}: relevance contains a non-expected runbook"
            )
        if any(score <= 0 for score in relevance.values()):
            raise ValueError(f"{path}:{line_number}: relevance scores must be positive")
        filters = value.get("filters") or {}
        if not isinstance(filters, dict):
            raise ValueError(f"{path}:{line_number}: filters must be an object")

        records.append(
            GoldRetrievalCase(
                case_id=str(value["id"]),
                question=str(value.get("question") or value.get("query")),
                expected_runbook_ids=expected,
                expected_sections=tuple(map(str, value["expected_sections"])),
                expected_error_codes=tuple(map(str, value["expected_error_codes"])),
                expected_connector_class=(
                    str(value["expected_connector_class"])
                    if value["expected_connector_class"] is not None
                    else None
                ),
                query_type=query_type,
                notes=str(value["notes"]),
                tenant_id=str(value.get("tenant_id") or filters.get("tenant_id") or "default"),
                environment=str(value.get("environment") or filters.get("environment") or "all"),
                fixture_only=bool(value.get("fixture_only", False)),
                category=category,
                difficulty=difficulty,
                split=split,
                expected_no_answer=expected_no_answer,
                relevance=relevance,
                filters=filters,
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
    attempted = 0
    for case in cases:
        common = {
            "id": case.case_id,
            "query_type": case.query_type,
            "category": case.category,
            "difficulty": case.difficulty,
            "split": case.split,
            "expected_no_answer": case.expected_no_answer,
        }
        if case.fixture_only:
            results.append({**common, "status": "not_run", "reason": "fixture_only_case"})
            continue
        attempted += 1
        started = time.perf_counter()
        try:
            chunks, diagnostics = retrieve(case)
            latency_ms = (time.perf_counter() - started) * 1000
        except Exception as exc:
            failures += 1
            results.append(
                {**common, "status": "failed", "error_type": type(exc).__name__}
            )
            continue

        if diagnostics is not None and diagnostics.total_retrieval_latency_ms:
            latency_ms = diagnostics.total_retrieval_latency_ms
            candidate_counts.append(
                diagnostics.candidate_chunk_count or diagnostics.fused_candidate_count
            )
            fallback_count += int(
                diagnostics.fallback_reason is not None
                or diagnostics.fusion_fallback_reason is not None
            )
        else:
            candidate_counts.append(len(chunks))
        latencies.append(latency_ms)

        ids = list(dict.fromkeys(item.runbook_id for item in chunks))
        expected = set(case.expected_runbook_ids)
        retrieved_sections = list(
            dict.fromkeys(
                item.section for item in chunks if not expected or item.runbook_id in expected
            )
        )
        expected_sections = set(case.expected_sections)
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
        no_result = not ids
        wrong_runbook = bool(ids) if case.expected_no_answer else (not ids or ids[0] not in expected)
        candidate_count = (
            (diagnostics.candidate_chunk_count or diagnostics.fused_candidate_count)
            if diagnostics is not None
            else len(chunks)
        )
        results.append(
            {
                **common,
                "status": "measured",
                "retrieved_runbook_ids": ids,
                "retrieved_sections": retrieved_sections,
                "expected_runbook_ids": list(case.expected_runbook_ids),
                "recall_at_1": _recall(ids, expected, 1),
                "recall_at_3": _recall(ids, expected, 3),
                "recall_at_5": _recall(ids, expected, 5),
                "section_recall_at_5": _recall(retrieved_sections, expected_sections, 5),
                "reciprocal_rank": _reciprocal_rank(ids, expected),
                "ndcg_at_5": _graded_ndcg(ids, case.relevance_map, 5),
                "no_result": no_result,
                "correct_no_answer": no_result if case.expected_no_answer else None,
                "wrong_runbook": wrong_runbook,
                "tenant_filter_violations": tenant_violations,
                "status_filter_violations": status_violations,
                "environment_filter_violations": environment_violations,
                "filter_violation": bool(
                    tenant_violations or status_violations or environment_violations
                ),
                "latency_ms": round(latency_ms, 3),
                "candidate_count": candidate_count,
                "candidate_diversity": len(ids) / len(chunks) if chunks else 1.0,
                "fallback": bool(
                    diagnostics
                    and (diagnostics.fallback_reason or diagnostics.fusion_fallback_reason)
                ),
                "no_result_reason": diagnostics.no_result_reason if diagnostics else None,
            }
        )

    measured = [item for item in results if item["status"] == "measured"]
    positive = [item for item in measured if not item["expected_no_answer"]]
    negative = [item for item in measured if item["expected_no_answer"]]
    report = {
        "mode": mode,
        "case_count": len(results),
        "measured_count": len(measured),
        "not_run_count": sum(item["status"] == "not_run" for item in results),
        "retrieval_failures": failures,
        "retrieval_failure_rate": failures / attempted if attempted else None,
        "recall_at_1": _mean(item["recall_at_1"] for item in positive),
        "recall_at_3": _mean(item["recall_at_3"] for item in positive),
        "recall_at_5": _mean(item["recall_at_5"] for item in positive),
        "section_recall_at_5": _mean(item["section_recall_at_5"] for item in positive),
        "mrr": _mean(item["reciprocal_rank"] for item in positive),
        "ndcg_at_5": _mean(item["ndcg_at_5"] for item in positive),
        "correct_no_answer_rate": _mean(item["correct_no_answer"] for item in negative),
        "no_result_rate": _mean(item["no_result"] for item in measured),
        "wrong_runbook_rate": _mean(item["wrong_runbook"] for item in measured),
        "negative_wrong_runbook_rate": _mean(item["wrong_runbook"] for item in negative),
        "tenant_filter_violations": sum(item["tenant_filter_violations"] for item in measured),
        "status_filter_violations": sum(item["status_filter_violations"] for item in measured),
        "environment_filter_violations": sum(
            item["environment_filter_violations"] for item in measured
        ),
        "filter_violation_rate": _mean(item["filter_violation"] for item in measured),
        "p50_latency_ms": _percentile(latencies, 50),
        "p95_latency_ms": _percentile(latencies, 95),
        "average_candidate_count": _mean(candidate_counts),
        "candidate_diversity": _mean(item["candidate_diversity"] for item in measured),
        "fallback_rate": fallback_count / len(measured) if measured else None,
        "dense_fallback_rate": fallback_count / len(measured) if measured else None,
        "by_query_type": {
            value: _breakdown(measured, "query_type", value) for value in sorted(QUERY_TYPES)
        },
        "by_category": {
            value: _breakdown(measured, "category", value) for value in sorted(CATEGORIES)
        },
        "by_difficulty": {
            value: _breakdown(measured, "difficulty", value) for value in sorted(DIFFICULTIES)
        },
        "by_split": {
            value: _breakdown(measured, "split", value) for value in sorted(SPLITS)
        },
        "case_results": results,
    }
    report["exact_error_recall_at_5"] = report["by_category"]["exact_error"]["recall_at_5"]
    report["exact_config_key_recall_at_5"] = report["by_category"]["exact_config_key"][
        "recall_at_5"
    ]
    report["semantic_recall_at_5"] = report["by_category"]["semantic"]["recall_at_5"]
    report["mixed_language_recall_at_5"] = report["by_category"]["mixed_vi_en"][
        "recall_at_5"
    ]
    return report


def build_promotion_gate(
    dense: dict[str, Any],
    hybrid: dict[str, Any],
    *,
    target_collection: str,
    dataset_split: str,
    benchmark_repetitions: int,
    quick: bool = False,
    dataset_contract_valid: bool = True,
    max_failure_rate: float = 0.0,
    max_p95_overhead_ratio: float = 0.20,
) -> dict[str, Any]:
    """Return the canonical, deterministic Hybrid Search promotion decision."""
    failures: list[str] = []
    if not dataset_contract_valid:
        failures.append("gold_dataset_contract_invalid")
    if dataset_split != "holdout":
        failures.append("dataset_split_must_be_holdout")
    if quick or benchmark_repetitions < 5:
        failures.append("benchmark_requires_at_least_5_repetitions")
    for metric in ("recall_at_1", "recall_at_3", "recall_at_5"):
        _require_not_lower(failures, dense, hybrid, metric)
    _require_not_lower(failures, dense, hybrid, "semantic_recall_at_5")
    _require_not_lower(failures, dense, hybrid, "exact_error_recall_at_5")
    _require_not_lower(failures, dense, hybrid, "exact_config_key_recall_at_5")
    _require_at_least(failures, hybrid, "correct_no_answer_rate", 0.90)
    _require_at_most(failures, hybrid, "wrong_runbook_rate", 0.10)
    _require_at_most(failures, hybrid, "filter_violation_rate", 0.0)
    _require_at_most(failures, hybrid, "retrieval_failure_rate", max_failure_rate)
    _require_at_most(failures, hybrid, "fallback_rate", 0.0)

    dense_p95 = dense.get("p95_latency_ms")
    hybrid_p95 = hybrid.get("p95_latency_ms")
    if dense_p95 is None or hybrid_p95 is None:
        failures.append("p95_latency_unmeasured")
    elif float(dense_p95) <= 0:
        failures.append("dense_p95_latency_invalid")
    else:
        overhead = (float(hybrid_p95) - float(dense_p95)) / float(dense_p95)
        if overhead > max_p95_overhead_ratio:
            failures.append(
                f"p95_latency_overhead_exceeds_{max_p95_overhead_ratio:.0%}:actual={overhead:.4f}"
            )
    return {
        "passed": not failures,
        "target_collection": target_collection,
        "dataset_split": dataset_split,
        "dataset_contract_valid": dataset_contract_valid,
        "benchmark_repetitions": benchmark_repetitions,
        "failures": failures,
    }


def _validate_enum(
    path: Path,
    line_number: int,
    name: str,
    value: str,
    allowed: set[str],
) -> None:
    if value not in allowed:
        raise ValueError(f"{path}:{line_number}: {name} must be one of {sorted(allowed)}")


def _default_category(value: dict[str, Any], query_type: str) -> str:
    if query_type == "negative":
        return "negative"
    if query_type == "semantic":
        return "semantic"
    if query_type == "mixed":
        return "mixed_vi_en"
    question = str(value.get("question") or value.get("query") or "")
    return "exact_config_key" if "." in question else "exact_error"


def _recall(ids: list[str], expected: set[str], k: int) -> float | None:
    if not expected:
        return None
    return len(expected & set(ids[:k])) / len(expected)


def _reciprocal_rank(ids: list[str], expected: set[str]) -> float | None:
    if not expected:
        return None
    return next((1 / index for index, value in enumerate(ids, 1) if value in expected), 0.0)


def _graded_ndcg(ids: list[str], relevance: Mapping[str, float], k: int) -> float | None:
    if not relevance:
        return None
    dcg = sum(
        ((2 ** relevance.get(value, 0.0)) - 1) / math.log2(index + 1)
        for index, value in enumerate(ids[:k], 1)
    )
    ideal_scores = sorted(relevance.values(), reverse=True)[:k]
    ideal = sum(
        ((2**score) - 1) / math.log2(index + 1)
        for index, score in enumerate(ideal_scores, 1)
    )
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


def _breakdown(results: list[dict[str, Any]], field_name: str, value: str) -> dict[str, Any]:
    selected = [item for item in results if item[field_name] == value]
    positive = [item for item in selected if not item["expected_no_answer"]]
    negative = [item for item in selected if item["expected_no_answer"]]
    return {
        "case_count": len(selected),
        "recall_at_1": _mean(item["recall_at_1"] for item in positive),
        "recall_at_3": _mean(item["recall_at_3"] for item in positive),
        "recall_at_5": _mean(item["recall_at_5"] for item in positive),
        "mrr": _mean(item["reciprocal_rank"] for item in positive),
        "correct_no_answer_rate": _mean(item["correct_no_answer"] for item in negative),
        "wrong_runbook_rate": _mean(item["wrong_runbook"] for item in selected),
        "p50_latency_ms": _percentile(
            [float(item["latency_ms"]) for item in selected],
            50,
        ),
    }


def _require_not_lower(
    failures: list[str],
    dense: dict[str, Any],
    hybrid: dict[str, Any],
    metric: str,
) -> None:
    baseline = dense.get(metric)
    candidate = hybrid.get(metric)
    if baseline is None or candidate is None:
        failures.append(f"{metric}_unmeasured")
    elif float(candidate) + 1e-12 < float(baseline):
        failures.append(f"{metric}_regressed:dense={baseline}:hybrid={candidate}")


def _require_at_least(
    failures: list[str], report: dict[str, Any], metric: str, minimum: float
) -> None:
    value = report.get(metric)
    if value is None:
        failures.append(f"{metric}_unmeasured")
    elif float(value) < minimum:
        failures.append(f"{metric}_below_{minimum}:actual={value}")


def _require_at_most(
    failures: list[str], report: dict[str, Any], metric: str, maximum: float
) -> None:
    value = report.get(metric)
    if value is None:
        failures.append(f"{metric}_unmeasured")
    elif float(value) > maximum:
        failures.append(f"{metric}_above_{maximum}:actual={value}")
