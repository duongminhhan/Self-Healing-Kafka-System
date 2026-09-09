import json
import sys

import pytest

from scripts.promote_qdrant_collection import (
    _validate_versioned_target,
    main,
    validate_promotion_report,
)


def _passing_report(collection="healing_runbooks_v2"):
    dense = {
        "recall_at_1": 0.8,
        "recall_at_3": 0.9,
        "recall_at_5": 0.9,
        "semantic_recall_at_5": 1.0,
        "exact_error_recall_at_5": 1.0,
        "exact_config_key_recall_at_5": 0.8,
        "p95_latency_ms": 100.0,
    }
    hybrid = {
        **dense,
        "recall_at_1": 0.9,
        "recall_at_3": 1.0,
        "recall_at_5": 1.0,
        "exact_config_key_recall_at_5": 1.0,
        "correct_no_answer_rate": 1.0,
        "wrong_runbook_rate": 0.05,
        "filter_violation_rate": 0.0,
        "retrieval_failure_rate": 0.0,
        "fallback_rate": 0.0,
        "p95_latency_ms": 115.0,
    }
    return {
        "benchmark_status": "measured",
        "repetitions": 5,
        "dataset": {
            "contract_valid": True,
            "measured_case_count": 92,
            "holdout_ratio": 0.22,
        },
        "holdout": {"dense": dense, "hybrid": hybrid},
        "promotion_gate": {
            "passed": True,
            "target_collection": collection,
            "dataset_split": "holdout",
            "dataset_contract_valid": True,
            "benchmark_repetitions": 5,
            "failures": [],
        }
    }


def test_promotion_report_must_match_target_holdout_and_five_runs():
    assert validate_promotion_report(_passing_report(), "healing_runbooks_v2") == []

    report = _passing_report("other_v2")
    report["promotion_gate"]["dataset_split"] = "tuning"
    report["promotion_gate"]["benchmark_repetitions"] = 1
    report["promotion_gate"]["failures"] = ["quality_regression"]
    errors = validate_promotion_report(report, "healing_runbooks_v2")

    assert "promotion_gate_target_collection_mismatch" in errors
    assert "promotion_gate_not_based_on_holdout" in errors
    assert "promotion_gate_contains_failures" in errors
    assert "promotion_gate_has_fewer_than_5_repetitions" in errors


def test_promotion_report_rejects_missing_dataset_contract():
    report = _passing_report()
    report.pop("dataset")
    report["promotion_gate"]["dataset_contract_valid"] = False

    errors = validate_promotion_report(report, "healing_runbooks_v2")

    assert "promotion_gate_dataset_contract_invalid" in errors
    assert "benchmark_dataset_contract_invalid" in errors


def test_promotion_report_recomputes_gate_from_holdout_metrics():
    report = _passing_report()
    report["holdout"]["hybrid"]["wrong_runbook_rate"] = 0.5

    errors = validate_promotion_report(report, "healing_runbooks_v2")

    assert "recomputed_promotion_gate_not_passed" in errors
    assert "promotion_gate_failures_do_not_match_metrics" in errors


def test_promotion_report_is_bound_to_current_gold_dataset(tmp_path):
    dataset = tmp_path / "gold.jsonl"
    dataset.write_text('{"id":"one"}\n', encoding="utf-8")
    report = _passing_report()
    report["dataset"]["sha256"] = "stale"

    errors = validate_promotion_report(
        report,
        "healing_runbooks_v2",
        dataset_path=dataset,
    )

    assert "benchmark_dataset_sha256_mismatch" in errors


def test_promotion_target_must_be_versioned_and_not_the_alias():
    _validate_versioned_target("healing_runbooks_v2", "healing_runbooks_current")
    with pytest.raises(ValueError, match="explicit version"):
        _validate_versioned_target("healing_runbooks_candidate", "healing_runbooks_current")
    with pytest.raises(ValueError, match="must differ"):
        _validate_versioned_target("healing_runbooks_v2", "healing_runbooks_v2")


def test_promotion_dry_run_makes_no_qdrant_request(monkeypatch, capsys):
    monkeypatch.delenv("QDRANT_URL", raising=False)
    monkeypatch.delenv("QDRANT_API_KEY", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "promote_qdrant_collection.py",
            "--collection",
            "healing_runbooks_v2",
            "--alias",
            "healing_runbooks_current",
        ],
    )

    assert main() == 0
    output = json.loads(capsys.readouterr().out)
    assert output["dry_run"] is True
    assert output["benchmark_gate_valid"] is False
    assert output["warning"] == "No Qdrant request was made."
    assert output["deletes_old_collection"] is False
