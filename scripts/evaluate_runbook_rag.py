"""Evaluate routing offline and retrieval only during an explicit live Qdrant run."""

from __future__ import annotations

import argparse
import json
import os
import statistics
import time
from pathlib import Path
from typing import Any

import httpx

from self_healthy_kafka.config import AnalyticsChatConfig, RagConfig
from self_healthy_kafka.rag.answer_composer import GroundedAnswerComposer, QwenJsonGenerator
from self_healthy_kafka.rag.models import RetrievalQuery, Route
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore
from self_healthy_kafka.rag.retriever import RunbookRetriever
from self_healthy_kafka.rag.router import RunbookRouter


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset", type=Path, default=Path("runbooks/evaluation/gold_questions.jsonl")
    )
    parser.add_argument("--live", action="store_true")
    parser.add_argument(
        "--with-generation",
        action="store_true",
        help="Also call the configured Qwen/Hugging Face endpoint; requires --live.",
    )
    args = parser.parse_args()
    if args.with_generation and not args.live:
        parser.error("--with-generation requires --live")
    records = [json.loads(line) for line in args.dataset.read_text(encoding="utf-8").splitlines() if line.strip()]
    router = RunbookRouter()
    route_hits = sum(router.route(item["question"]).route.value == item["expected_route"] for item in records)
    report: dict[str, Any] = {
        "records": len(records),
        "pass_count": 0,
        "fail_count": 0,
        "not_run_count": 0,
        "case_results": [],
        "route_accuracy": route_hits / len(records) if records else 0.0,
        "live_retrieval": False,
        "retrieval_recall_at_k": None,
        "mean_reciprocal_rank": None,
        "citation_precision": None,
        "unsupported_claim_rate": None,
        "correct_no_answer_rate": None,
        "deterministic_fallback_rate": None,
        "required_claim_recall": None,
        "retrieval_latency_seconds": None,
        "end_to_end_latency_seconds": None,
        "not_measured": [
            "Qwen answer metrics require a separate opt-in end-to-end run with verified analytics facts"
        ],
    }
    if args.live:
        if os.getenv("RUNBOOK_RAG_LIVE_TEST", "false").lower() not in {"1", "true", "yes", "on"}:
            parser.error("--live requires RUNBOOK_RAG_LIVE_TEST=true")
        config = RagConfig()
        config.validate()
        retriever = RunbookRetriever(config, QdrantRunbookStore(config))
        recalls, reciprocal_ranks, latencies, no_answer_hits = [], [], [], []
        citation_scores: list[float] = []
        unsupported_claims: list[bool] = []
        fallback_hits: list[bool] = []
        answer_latencies: list[float] = []
        required_claim_hits: list[float] = []
        composer = None
        client = None
        if args.with_generation:
            chat_config = AnalyticsChatConfig()
            missing = [
                name
                for name, value in (
                    ("HF_CHAT_ENDPOINT_URL", chat_config.hf_endpoint_url),
                    ("HF_CHAT_TOKEN", chat_config.hf_token),
                    ("HF_CHAT_MODEL_ID", chat_config.hf_model_id),
                )
                if not value.strip()
            ]
            if missing:
                parser.error("--with-generation is missing: " + ", ".join(missing))
            client = httpx.Client()
            composer = GroundedAnswerComposer(QwenJsonGenerator(chat_config, client).generate)
        for item in records:
            if item.get("fault"):
                report["not_run_count"] += 1
                report["case_results"].append({
                    "id": item["id"],
                    "status": "not_run",
                    "reasons": ["fault_injection_not_executed"],
                })
                continue
            if item["expected_route"] == Route.ANALYTICS.value:
                report["not_run_count"] += 1
                report["case_results"].append({
                    "id": item["id"],
                    "status": "not_run",
                    "reasons": ["analytics_requires_live_api_evaluation"],
                })
                continue
            decision = router.route(item["question"])
            started = time.perf_counter()
            found = retriever.retrieve(RetrievalQuery(
                text=item["question"], tenant_id=config.tenant_id,
                environment=config.environment, connector_class=decision.connector_class,
                error_codes=decision.error_codes,
            ))
            latencies.append(time.perf_counter() - started)
            ids = [chunk.runbook_id for chunk in found]
            expected = item.get("expected_runbook_ids") or []
            reasons: list[str] = []
            if decision.route.value != item["expected_route"]:
                reasons.append("route_mismatch")
            if expected:
                recall = sum(value in ids for value in expected) / len(expected)
                recalls.append(recall)
                ranks = [ids.index(value) + 1 for value in expected if value in ids]
                reciprocal_ranks.append(1 / min(ranks) if ranks else 0.0)
                if recall < 1:
                    reasons.append("missing_expected_runbook")
                expected_sections = set(item.get("expected_sections") or [])
                found_sections = {
                    chunk.section for chunk in found if chunk.runbook_id in expected
                }
                if not expected_sections <= found_sections:
                    reasons.append("missing_expected_section")
            else:
                no_answer_hits.append(not ids)
                if ids:
                    reasons.append("unexpected_runbook_for_no_answer")
            answer_source = None
            fallback_reason = None
            if composer is not None:
                answer = composer.compose(
                    question=item["question"],
                    route=decision.route,
                    analytics_facts=[],
                    chunks=found,
                )
                answer_latencies.append(time.perf_counter() - started)
                answer_source = answer.source
                fallback_reason = answer.fallback_reason
                cited = [citation.runbook_id for citation in answer.citations]
                citation_score = (
                    sum(value in expected for value in cited) / len(cited)
                    if cited
                    else float(not expected)
                )
                citation_scores.append(citation_score)
                if expected and not cited:
                    reasons.append("missing_citation")
                elif citation_score < 1:
                    reasons.append("unexpected_citation")
                forbidden = [str(value).casefold() for value in item.get("forbidden_claims") or []]
                has_forbidden_claim = any(
                    value in answer.answer.casefold() for value in forbidden
                )
                unsupported_claims.append(has_forbidden_claim)
                if has_forbidden_claim:
                    reasons.append("forbidden_claim")
                fallback_hits.append(answer.source == "deterministic_fallback")
                required = [str(value).casefold() for value in item.get("required_claims") or []]
                if required:
                    required_recall = (
                        sum(value in answer.answer.casefold() for value in required)
                        / len(required)
                    )
                    required_claim_hits.append(required_recall)
                    if required_recall < 1:
                        reasons.append("missing_required_claim")
            status = "pass" if not reasons else "fail"
            report[f"{status}_count"] += 1
            report["case_results"].append({
                "id": item["id"],
                "status": status,
                "reasons": reasons,
                "retrieved_runbook_ids": list(dict.fromkeys(ids)),
                "retrieved_sections": list(dict.fromkeys(chunk.section for chunk in found)),
                "answer_source": answer_source,
                "fallback_reason": fallback_reason,
            })
        if client is not None:
            client.close()
        report.update({
            "live_retrieval": True,
            "retrieval_recall_at_k": statistics.fmean(recalls) if recalls else None,
            "mean_reciprocal_rank": statistics.fmean(reciprocal_ranks) if reciprocal_ranks else None,
            "correct_no_answer_rate": statistics.fmean(no_answer_hits) if no_answer_hits else None,
            "retrieval_latency_seconds": statistics.fmean(latencies) if latencies else None,
            "citation_precision": statistics.fmean(citation_scores) if citation_scores else None,
            "unsupported_claim_rate": statistics.fmean(unsupported_claims) if unsupported_claims else None,
            "deterministic_fallback_rate": statistics.fmean(fallback_hits) if fallback_hits else None,
            "required_claim_recall": statistics.fmean(required_claim_hits) if required_claim_hits else None,
            "end_to_end_latency_seconds": statistics.fmean(answer_latencies) if answer_latencies else None,
            "not_measured": [] if composer is not None else report["not_measured"],
        })
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
