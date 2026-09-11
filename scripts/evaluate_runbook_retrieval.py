"""Tune and compare dense/hybrid Runbook retrieval without calling an answer LLM."""

# ruff: noqa: E402 -- direct script execution must prefer this checkout's src tree.

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
from dataclasses import replace
from itertools import product
from pathlib import Path
from typing import Any

if __package__:
    from ._repo_bootstrap import bootstrap_repo_src
else:
    from _repo_bootstrap import bootstrap_repo_src

bootstrap_repo_src()

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.evaluation import (
    CATEGORIES,
    GoldRetrievalCase,
    build_promotion_gate,
    evaluate_retrieval,
    load_gold_retrieval,
)
from self_healthy_kafka.rag.models import RetrievalQuery
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore
from self_healthy_kafka.rag.retriever import RunbookRetriever
from self_healthy_kafka.rag.router import RunbookRouter

_QUALITY_METRICS = (
    "recall_at_1",
    "recall_at_3",
    "recall_at_5",
    "section_recall_at_5",
    "mrr",
    "ndcg_at_5",
    "correct_no_answer_rate",
    "wrong_runbook_rate",
    "negative_wrong_runbook_rate",
    "filter_violation_rate",
    "retrieval_failure_rate",
    "candidate_diversity",
    "fallback_rate",
)
_LATENCY_METRICS = ("p50_latency_ms", "p95_latency_ms", "average_candidate_count")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("runbooks/evaluation/gold_retrieval.jsonl"),
    )
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--dense-collection")
    parser.add_argument("--hybrid-collection")
    parser.add_argument(
        "--repetitions",
        type=int,
        default=5,
        help="Repeat every live configuration; production comparison requires at least 5.",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Evaluate only current hybrid weights/candidate limits once; never promotable.",
    )
    parser.add_argument(
        "--profile",
        choices=("router", "controlled"),
        default="router",
        help="router is end-to-end and does not use expected labels as retrieval filters.",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Omit per-case rows while preserving aggregates and failed quality gates.",
    )
    parser.add_argument(
        "--require-gates",
        action="store_true",
        help="Return non-zero unless the untouched holdout passes every promotion gate.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Write the complete JSON report to this path and print only a bounded summary.",
    )
    args = parser.parse_args()
    if args.repetitions < 1:
        parser.error("--repetitions must be positive")

    cases = load_gold_retrieval(args.dataset)
    dataset = _dataset_summary(args.dataset, cases)
    if not args.live:
        offline_report = {
            "dataset": dataset,
            "benchmark_status": "not_run",
            "reason": "live_qdrant_not_requested",
            "promotion_gate": {
                "passed": False,
                "target_collection": None,
                "dataset_split": "holdout",
                "dataset_contract_valid": bool(dataset["contract_valid"]),
                "failures": ["live_holdout_benchmark_not_run"],
            },
        }
        _emit_report(offline_report, args.output)
        return 0

    if os.getenv("RUNBOOK_RAG_LIVE_TEST", "false").lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        parser.error("--live requires RUNBOOK_RAG_LIVE_TEST=true")

    base = RagConfig()
    dense_collection = args.dense_collection or base.dense_collection or base.collection
    hybrid_collection = args.hybrid_collection or base.hybrid_collection
    tuning_cases = [case for case in cases if case.split == "tuning"]
    holdout_cases = [case for case in cases if case.split == "holdout"]
    repetitions = 1 if args.quick else args.repetitions

    dense_config = replace(
        base,
        retrieval_mode="dense",
        search_mode="dense",
        collection=dense_collection,
    )
    dense_tuning = _evaluate_repeated(
        tuning_cases,
        dense_config,
        repetitions=repetitions,
        profile=args.profile,
    )

    hybrid_configs = _hybrid_grid(base, hybrid_collection, quick=args.quick)
    hybrid_tuning: list[dict[str, Any]] = []
    for config in hybrid_configs:
        report = _evaluate_repeated(
            tuning_cases,
            config,
            repetitions=repetitions,
            profile=args.profile,
        )
        hybrid_tuning.append(
            {
                "config": _safe_config(config),
                "report": report,
                "comparison_to_dense": compare_reports(dense_tuning, report),
            }
        )

    selected = max(hybrid_tuning, key=lambda item: _selection_key(item["report"]))
    selected_config = next(
        config
        for config in hybrid_configs
        if _safe_config(config) == selected["config"]
    )
    dense_holdout = _evaluate_repeated(
        holdout_cases,
        dense_config,
        repetitions=repetitions,
        profile=args.profile,
    )
    hybrid_holdout = _evaluate_repeated(
        holdout_cases,
        selected_config,
        repetitions=repetitions,
        profile=args.profile,
    )
    holdout_comparison = compare_reports(dense_holdout, hybrid_holdout)
    gate = build_promotion_gate(
        dense_holdout,
        hybrid_holdout,
        target_collection=hybrid_collection,
        dataset_split="holdout",
        benchmark_repetitions=repetitions,
        quick=args.quick,
        dataset_contract_valid=bool(dataset["contract_valid"]),
    )
    output: dict[str, Any] = {
        "dataset": dataset,
        "benchmark_status": "measured",
        "profile": args.profile,
        "repetitions": repetitions,
        "tuning": {
            "dense": {"config": _safe_config(dense_config), "report": dense_tuning},
            "hybrid_grid": hybrid_tuning,
            "selected_hybrid_config": selected["config"],
            "selection_rule": (
                "lexicographic: failures, Recall@1, semantic/config Recall@5, "
                "no-answer, wrong-runbook, nDCG, p95"
            ),
        },
        "holdout": {
            "dense": dense_holdout,
            "hybrid": hybrid_holdout,
            "comparison": holdout_comparison,
        },
        "promotion_gate": gate,
    }
    if args.summary_only:
        _strip_case_results(output)
    _emit_report(output, args.output)
    if args.require_gates and not gate["passed"]:
        return 2
    return int(bool(dense_holdout["retrieval_failures"] or hybrid_holdout["retrieval_failures"]))


def _evaluate_repeated(
    cases: list[GoldRetrievalCase],
    config: RagConfig,
    *,
    repetitions: int,
    profile: str,
) -> dict[str, Any]:
    reports = [_evaluate_mode(cases, config, profile=profile) for _ in range(repetitions)]
    aggregate = dict(reports[0])
    aggregate["repetitions"] = repetitions
    aggregate["run_metrics"] = {
        metric: [report.get(metric) for report in reports]
        for metric in (*_QUALITY_METRICS, *_LATENCY_METRICS)
    }
    for metric in (*_QUALITY_METRICS, *_LATENCY_METRICS):
        values = [float(report[metric]) for report in reports if report.get(metric) is not None]
        aggregate[metric] = statistics.fmean(values) if values else None
    aggregate["retrieval_failures"] = sum(report["retrieval_failures"] for report in reports)
    aggregate["case_results"] = _aggregate_case_results(reports)
    return aggregate


def _evaluate_mode(
    cases: list[GoldRetrievalCase],
    config: RagConfig,
    *,
    profile: str,
) -> dict[str, Any]:
    config.validate()
    retriever = RunbookRetriever(config, QdrantRunbookStore(config))
    router = RunbookRouter()

    def retrieve(case: GoldRetrievalCase):
        decision = router.route(case.question)
        controlled = profile == "controlled"
        filters = case.filters
        connector_class = (
            str(filters.get("connector_class") or case.expected_connector_class or "") or None
            if controlled
            else decision.connector_class
        )
        error_codes = (
            tuple(map(str, filters.get("error_codes") or case.expected_error_codes))
            if controlled
            else decision.error_codes
        )
        chunks = retriever.retrieve(
            RetrievalQuery(
                text=case.question,
                tenant_id=case.tenant_id,
                environment=case.environment,
                connector_class=connector_class,
                error_codes=error_codes,
            )
        )
        return chunks, retriever.last_diagnostics

    report = evaluate_retrieval(cases, retrieve, mode=config.search_mode)
    report["profile"] = profile
    return report


def _hybrid_grid(base: RagConfig, collection: str, *, quick: bool) -> list[RagConfig]:
    if quick:
        pairs = [
            (
                base.hybrid_dense_weight,
                base.hybrid_sparse_weight,
                base.effective_dense_candidate_limit,
            )
        ]
    else:
        pairs = [(dense, sparse, limit) for (dense, sparse), limit in product(
            ((1.0, 1.0), (2.0, 1.0), (3.0, 1.0)),
            (10, 20, 30),
        )]
    return [
        replace(
            base,
            retrieval_mode="hybrid",
            search_mode="hybrid",
            collection=collection,
            hybrid_dense_weight=dense,
            hybrid_sparse_weight=sparse,
            dense_candidate_limit=limit,
            sparse_candidate_limit=limit,
            fusion_limit=max(limit, base.effective_top_k),
        )
        for dense, sparse, limit in pairs
    ]


def compare_reports(dense: dict[str, Any], hybrid: dict[str, Any]) -> dict[str, Any]:
    metrics = (
        "recall_at_1",
        "recall_at_3",
        "recall_at_5",
        "mrr",
        "ndcg_at_5",
        "exact_error_recall_at_5",
        "exact_config_key_recall_at_5",
        "semantic_recall_at_5",
        "mixed_language_recall_at_5",
        "correct_no_answer_rate",
        "wrong_runbook_rate",
        "filter_violation_rate",
        "p50_latency_ms",
        "p95_latency_ms",
        "candidate_diversity",
        "fallback_rate",
    )
    deltas = {
        f"{metric}_delta": _delta(dense.get(metric), hybrid.get(metric))
        for metric in metrics
    }
    dense_cases = {
        item["id"]: item
        for item in dense.get("case_results", [])
        if item.get("status") == "measured"
    }
    hybrid_cases = {
        item["id"]: item
        for item in hybrid.get("case_results", [])
        if item.get("status") == "measured"
    }
    improved: list[str] = []
    regressed: list[str] = []
    false_positives: list[str] = []
    for case_id in sorted(dense_cases.keys() & hybrid_cases.keys()):
        dense_quality = _case_quality(dense_cases[case_id])
        hybrid_quality = _case_quality(hybrid_cases[case_id])
        if hybrid_quality > dense_quality:
            improved.append(case_id)
        elif hybrid_quality < dense_quality:
            regressed.append(case_id)
        if (
            hybrid_cases[case_id].get("expected_no_answer")
            and hybrid_cases[case_id].get("wrong_runbook")
        ):
            false_positives.append(case_id)
    return {
        **deltas,
        "improved_cases": improved,
        "regressed_cases": regressed,
        "hybrid_false_positives": false_positives,
        "interpretation": (
            "Positive deltas favor hybrid for quality/diversity, but favor dense for "
            "wrong-runbook, filter-violation, fallback, and latency metrics."
        ),
    }


def _dataset_summary(path: Path, cases: list[GoldRetrievalCase]) -> dict[str, Any]:
    measured = [case for case in cases if not case.fixture_only]
    holdout = [case for case in measured if case.split == "holdout"]
    return {
        "path": str(path),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "case_count": len(cases),
        "measured_case_count": len(measured),
        "fixture_only_count": len(cases) - len(measured),
        "split_counts": {
            split: sum(case.split == split and not case.fixture_only for case in cases)
            for split in ("tuning", "holdout")
        },
        "holdout_ratio": len(holdout) / len(measured) if measured else None,
        "category_counts": {
            category: sum(case.category == category for case in cases)
            for category in sorted(CATEGORIES)
        },
        "query_type_counts": _query_type_counts(cases),
        "contract_valid": len(measured) >= 75 and len(holdout) / len(measured) >= 0.20,
    }


def _safe_config(config: RagConfig) -> dict[str, Any]:
    return {
        "mode": config.search_mode,
        "collection": config.collection,
        "dense_weight": config.hybrid_dense_weight if config.search_mode == "hybrid" else None,
        "sparse_weight": config.hybrid_sparse_weight if config.search_mode == "hybrid" else None,
        "dense_candidates": config.effective_dense_candidate_limit,
        "sparse_candidates": (
            config.effective_sparse_candidate_limit if config.search_mode == "hybrid" else None
        ),
        "final_top_k": config.effective_top_k,
        "max_chunks_per_runbook": config.max_chunks_per_runbook,
        "evidence_gate_enabled": config.effective_evidence_gate_enabled,
    }


def _selection_key(report: dict[str, Any]) -> tuple[float, ...]:
    def value(name: str, default: float = 0.0) -> float:
        measured = report.get(name)
        return float(measured) if measured is not None else default

    return (
        -value("retrieval_failure_rate"),
        value("recall_at_1"),
        value("semantic_recall_at_5"),
        value("exact_config_key_recall_at_5"),
        value("correct_no_answer_rate"),
        -value("wrong_runbook_rate", 1.0),
        value("ndcg_at_5"),
        -value("p95_latency_ms", float("inf")),
    )


def _aggregate_case_results(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    first = reports[0].get("case_results", [])
    by_run = [{item["id"]: item for item in report.get("case_results", [])} for report in reports]
    result: list[dict[str, Any]] = []
    for item in first:
        current = dict(item)
        if item.get("status") == "measured":
            for metric in (
                "recall_at_1",
                "recall_at_3",
                "recall_at_5",
                "reciprocal_rank",
                "ndcg_at_5",
                "latency_ms",
            ):
                values = [
                    run[item["id"]].get(metric)
                    for run in by_run
                    if item["id"] in run and run[item["id"]].get(metric) is not None
                ]
                if values:
                    current[metric] = statistics.fmean(map(float, values))
        result.append(current)
    return result


def _query_type_counts(cases: list[GoldRetrievalCase]) -> dict[str, int]:
    return {
        query_type: sum(item.query_type == query_type for item in cases)
        for query_type in ("exact", "semantic", "mixed", "negative")
    }


def _case_quality(item: dict[str, Any]) -> float:
    if item.get("expected_no_answer"):
        return float(bool(item.get("correct_no_answer")))
    return float(item.get("recall_at_1") or 0.0)


def _delta(left: Any, right: Any) -> float | None:
    if left is None or right is None:
        return None
    return float(right) - float(left)


def _strip_case_results(value: Any) -> None:
    if isinstance(value, dict):
        value.pop("case_results", None)
        value.pop("run_metrics", None)
        for nested in value.values():
            _strip_case_results(nested)
    elif isinstance(value, list):
        for nested in value:
            _strip_case_results(nested)


def _emit_report(report: dict[str, Any], output_path: Path | None) -> None:
    serialized = json.dumps(report, ensure_ascii=False, indent=2) + "\n"
    if output_path is None:
        print(serialized, end="")
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(serialized, encoding="utf-8")
    print(
        json.dumps(
            {
                "report_path": str(output_path.resolve()),
                "benchmark_status": report.get("benchmark_status"),
                "promotion_gate": report.get("promotion_gate"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    raise SystemExit(main())
