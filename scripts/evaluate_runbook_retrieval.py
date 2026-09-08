"""Compare dense and hybrid Qdrant retrieval without invoking an answer LLM."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.evaluation import (
    GoldRetrievalCase,
    evaluate_retrieval,
    load_gold_retrieval,
)
from self_healthy_kafka.rag.models import RetrievalQuery
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore
from self_healthy_kafka.rag.retriever import RunbookRetriever
from self_healthy_kafka.rag.router import RunbookRouter


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("runbooks/evaluation/gold_retrieval.jsonl"),
    )
    parser.add_argument("--live", action="store_true")
    parser.add_argument("--dense-collection")
    parser.add_argument("--hybrid-collection", default="healing_runbooks_v2")
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Omit per-case rows while preserving aggregate and query-type metrics.",
    )
    args = parser.parse_args()

    cases = load_gold_retrieval(args.dataset)
    if not args.live:
        print(
            json.dumps(
                {
                    "dataset": str(args.dataset),
                    "case_count": len(cases),
                    "query_type_counts": _query_type_counts(cases),
                    "dense": {"status": "not_run", "reason": "live_qdrant_not_requested"},
                    "hybrid": {"status": "not_run", "reason": "live_qdrant_not_requested"},
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    if os.getenv("RUNBOOK_RAG_LIVE_TEST", "false").lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        parser.error("--live requires RUNBOOK_RAG_LIVE_TEST=true")
    base = RagConfig()
    dense_collection = args.dense_collection or base.collection
    dense = _evaluate_mode(
        cases,
        replace(base, search_mode="dense", collection=dense_collection),
    )
    hybrid = _evaluate_mode(
        cases,
        replace(base, search_mode="hybrid", collection=args.hybrid_collection),
    )
    if args.summary_only:
        dense.pop("case_results", None)
        hybrid.pop("case_results", None)
    print(
        json.dumps(
            {
                "dataset": str(args.dataset),
                "case_count": len(cases),
                "dense": dense,
                "hybrid": hybrid,
                "comparison": _comparison(dense, hybrid),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return int(bool(dense["retrieval_failures"] or hybrid["retrieval_failures"]))


def _evaluate_mode(cases: list[GoldRetrievalCase], config: RagConfig) -> dict[str, Any]:
    config.validate()
    retriever = RunbookRetriever(config, QdrantRunbookStore(config))
    router = RunbookRouter()

    def retrieve(case: GoldRetrievalCase):
        decision = router.route(case.question)
        chunks = retriever.retrieve(
            RetrievalQuery(
                text=case.question,
                tenant_id=case.tenant_id,
                environment=case.environment,
                connector_class=decision.connector_class,
                error_codes=decision.error_codes,
            )
        )
        return chunks, retriever.last_diagnostics

    return evaluate_retrieval(cases, retrieve, mode=config.search_mode)


def _query_type_counts(cases: list[GoldRetrievalCase]) -> dict[str, int]:
    return {
        query_type: sum(item.query_type == query_type for item in cases)
        for query_type in ("exact", "semantic", "mixed", "negative")
    }


def _comparison(dense: dict[str, Any], hybrid: dict[str, Any]) -> dict[str, Any]:
    def delta(path: tuple[str, ...]) -> float | None:
        left: Any = dense
        right: Any = hybrid
        for key in path:
            left = left.get(key) if isinstance(left, dict) else None
            right = right.get(key) if isinstance(right, dict) else None
        if left is None or right is None:
            return None
        return float(right) - float(left)

    return {
        "recall_at_1_delta": delta(("recall_at_1",)),
        "recall_at_3_delta": delta(("recall_at_3",)),
        "recall_at_5_delta": delta(("recall_at_5",)),
        "mrr_delta": delta(("mrr",)),
        "p50_latency_ms_delta": delta(("p50_latency_ms",)),
        "p95_latency_ms_delta": delta(("p95_latency_ms",)),
        "exact_recall_at_5_delta": delta(("by_query_type", "exact", "recall_at_5")),
        "semantic_recall_at_5_delta": delta(("by_query_type", "semantic", "recall_at_5")),
        "mixed_recall_at_5_delta": delta(("by_query_type", "mixed", "recall_at_5")),
        "interpretation": (
            "Positive deltas favor hybrid for quality metrics; positive latency deltas "
            "mean hybrid is slower. Do not claim improvement from null or unmeasured values."
        ),
    }


if __name__ == "__main__":
    raise SystemExit(main())
