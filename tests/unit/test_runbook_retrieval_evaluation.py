import json

import pytest

from self_healthy_kafka.rag.evaluation import (
    GoldRetrievalCase,
    evaluate_retrieval,
    load_gold_retrieval,
)
from self_healthy_kafka.rag.models import RetrievedChunk, SearchDiagnostics


def _case(case_id, expected, query_type="exact"):
    return GoldRetrievalCase(
        case_id=case_id,
        question="question",
        expected_runbook_ids=tuple(expected),
        expected_sections=(),
        expected_error_codes=(),
        expected_connector_class=None,
        query_type=query_type,
        notes="fixture",
    )


def _chunk(runbook_id, *, tenant_id="default", status="approved", environments=("all",)):
    return RetrievedChunk(
        point_id=runbook_id,
        score=0.5,
        runbook_id=runbook_id,
        title=runbook_id,
        version=1,
        section="diagnostic_steps",
        section_title="Diagnostic steps",
        source="runbooks/test.md",
        connector_class="kafka-connect",
        error_codes=("TASK_FAILED",),
        text="Evidence",
        tenant_id=tenant_id,
        status=status,
        environments=environments,
    )


def test_gold_loader_validates_contract_and_query_type(tmp_path):
    path = tmp_path / "gold.jsonl"
    value = {
        "id": "one",
        "question": "TASK_FAILED",
        "expected_runbook_ids": ["RB-KC-001"],
        "expected_sections": ["symptoms"],
        "expected_error_codes": ["TASK_FAILED"],
        "expected_connector_class": "kafka-connect",
        "query_type": "exact",
        "notes": "fixture",
    }
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")

    assert load_gold_retrieval(path)[0].case_id == "one"

    value["query_type"] = "unknown"
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="query_type"):
        load_gold_retrieval(path)


def test_evaluator_reports_rank_metrics_latency_candidates_and_fallback():
    cases = [_case("one", ["RB-1"]), _case("two", ["RB-2"], "semantic")]

    def retrieve(case):
        chunks = [_chunk("RB-X"), _chunk("RB-1")] if case.case_id == "one" else [_chunk("RB-2")]
        diagnostics = SearchDiagnostics(
            search_mode="hybrid",
            collection="v2",
            dense_model="dense",
            sparse_model="sparse",
            fused_candidate_count=len(chunks) + 2,
            total_retrieval_latency_ms=10 if case.case_id == "one" else 20,
            fallback_reason="hybrid_query_failed:TimeoutError" if case.case_id == "two" else None,
        )
        return chunks, diagnostics

    report = evaluate_retrieval(cases, retrieve, mode="hybrid")

    assert report["recall_at_1"] == 0.5
    assert report["recall_at_3"] == 1.0
    assert report["mrr"] == 0.75
    assert 0 <= report["ndcg_at_5"] <= 1
    assert report["p50_latency_ms"] == 15
    assert report["p95_latency_ms"] == 19.5
    assert report["average_candidate_count"] == 3.5
    assert report["dense_fallback_rate"] == 0.5


def test_evaluator_separates_failures_and_detects_payload_filter_violations():
    cases = [_case("bad-filter", ["RB-1"]), _case("service-failure", ["RB-2"])]

    def retrieve(case):
        if case.case_id == "service-failure":
            raise TimeoutError("offline")
        return [
            _chunk(
                "RB-1",
                tenant_id="other",
                status="draft",
                environments=("prod",),
            )
        ], None

    report = evaluate_retrieval(cases, retrieve, mode="dense")

    assert report["retrieval_failures"] == 1
    assert report["measured_count"] == 1
    assert report["tenant_filter_violations"] == 1
    assert report["status_filter_violations"] == 1
    assert report["environment_filter_violations"] == 1
