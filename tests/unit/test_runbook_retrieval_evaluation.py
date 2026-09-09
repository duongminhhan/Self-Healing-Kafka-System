import json
from pathlib import Path

import pytest

from scripts.evaluate_runbook_retrieval import _emit_report, build_promotion_gate
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


def test_checked_in_gold_dataset_has_holdout_and_all_required_categories():
    cases = load_gold_retrieval(Path("runbooks/evaluation/gold_retrieval.jsonl"))
    measured = [case for case in cases if not case.fixture_only]
    holdout = [case for case in measured if case.split == "holdout"]

    assert len(measured) >= 75
    assert len(holdout) / len(measured) >= 0.20
    assert {
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
    } <= {case.category for case in cases}


def test_gold_loader_rejects_relevance_for_non_expected_runbook(tmp_path):
    path = tmp_path / "gold.jsonl"
    value = {
        "id": "one",
        "query": "TASK_FAILED",
        "expected_runbook_ids": ["RB-KC-001"],
        "relevance": {"RB-OTHER": 3},
        "expected_sections": ["symptoms"],
        "expected_error_codes": ["TASK_FAILED"],
        "expected_connector_class": "kafka-connect",
        "query_type": "exact",
        "category": "exact_error",
        "difficulty": "easy",
        "split": "holdout",
        "notes": "fixture",
    }
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="non-expected"):
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
    assert report["filter_violation_rate"] == 1
    assert report["retrieval_failure_rate"] == 0.5


def test_evaluator_counts_negative_as_pass_only_for_no_answer():
    cases = [
        GoldRetrievalCase(
            case_id="good",
            question="GPU failure",
            expected_runbook_ids=(),
            expected_sections=(),
            expected_error_codes=(),
            expected_connector_class=None,
            query_type="negative",
            notes="fixture",
            category="negative",
            expected_no_answer=True,
        ),
        GoldRetrievalCase(
            case_id="bad",
            question="kernel panic",
            expected_runbook_ids=(),
            expected_sections=(),
            expected_error_codes=(),
            expected_connector_class=None,
            query_type="negative",
            notes="fixture",
            category="negative",
            expected_no_answer=True,
        ),
    ]

    report = evaluate_retrieval(
        cases,
        lambda case: ([], None) if case.case_id == "good" else ([_chunk("RB-X")], None),
        mode="hybrid",
    )

    assert report["correct_no_answer_rate"] == 0.5
    assert report["negative_wrong_runbook_rate"] == 0.5


def test_promotion_gate_requires_holdout_quality_and_five_runs():
    dense = {
        "recall_at_1": 0.9,
        "recall_at_3": 0.95,
        "recall_at_5": 0.95,
        "semantic_recall_at_5": 0.9,
        "exact_error_recall_at_5": 1.0,
        "exact_config_key_recall_at_5": 0.9,
        "p95_latency_ms": 100,
    }
    hybrid = {
        **dense,
        "correct_no_answer_rate": 0.95,
        "wrong_runbook_rate": 0.05,
        "filter_violation_rate": 0.0,
        "retrieval_failure_rate": 0.0,
        "fallback_rate": 0.0,
        "p95_latency_ms": 115,
    }

    passed = build_promotion_gate(
        dense,
        hybrid,
        target_collection="hybrid-v2",
        dataset_split="holdout",
        benchmark_repetitions=5,
    )
    failed = build_promotion_gate(
        dense,
        {**hybrid, "correct_no_answer_rate": 0.5},
        target_collection="hybrid-v2",
        dataset_split="tuning",
        benchmark_repetitions=1,
    )

    assert passed["passed"] is True
    assert failed["passed"] is False
    assert "dataset_split_must_be_holdout" in failed["failures"]
    assert any("correct_no_answer_rate" in reason for reason in failed["failures"])


def test_promotion_gate_rejects_invalid_gold_contract_and_hidden_fallback():
    dense = {
        "recall_at_1": 1.0,
        "recall_at_3": 1.0,
        "recall_at_5": 1.0,
        "semantic_recall_at_5": 1.0,
        "exact_error_recall_at_5": 1.0,
        "exact_config_key_recall_at_5": 1.0,
        "p95_latency_ms": 100,
    }
    hybrid = {
        **dense,
        "correct_no_answer_rate": 1.0,
        "wrong_runbook_rate": 0.0,
        "filter_violation_rate": 0.0,
        "retrieval_failure_rate": 0.0,
        "fallback_rate": 0.1,
        "p95_latency_ms": 100,
    }

    result = build_promotion_gate(
        dense,
        hybrid,
        target_collection="hybrid-v2",
        dataset_split="holdout",
        benchmark_repetitions=5,
        dataset_contract_valid=False,
    )

    assert result["passed"] is False
    assert "gold_dataset_contract_invalid" in result["failures"]
    assert any("fallback_rate" in reason for reason in result["failures"])


def test_evaluator_can_write_complete_report_to_explicit_output(tmp_path, capsys):
    output = tmp_path / "nested" / "report.json"
    report = {"benchmark_status": "measured", "promotion_gate": {"passed": False}}

    _emit_report(report, output)

    assert json.loads(output.read_text(encoding="utf-8")) == report
    printed = json.loads(capsys.readouterr().out)
    assert printed["report_path"] == str(output.resolve())
    assert printed["promotion_gate"] == {"passed": False}
