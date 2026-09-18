from __future__ import annotations

import copy
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

from self_healthy_kafka.evaluation.analytics_bench import (
    AnalyticsBenchError,
    evaluate_adapter,
    evaluate_offline,
    export_training_records,
    load_benchmark,
)
from self_healthy_kafka.evaluation.semantic_adapter import SemanticAdapterResult
from self_healthy_kafka.evaluation.snapshot import (
    SnapshotReference,
    SnapshotReferenceError,
    apply_snapshot_review,
    build_snapshot_manifest,
    content_hash,
    facts_hash,
    load_snapshot,
    source_identity_hash,
)
from self_healthy_kafka.semantic.planner import compile_analytics_request, parse_semantic_plan
from self_healthy_kafka.semantic.tsql import compile_incident_query, is_read_only_incident_query

_ROOT = Path(__file__).resolve().parents[2]
_BENCHMARK = _ROOT / "runbooks" / "evaluation" / "shk_analytics_bench"


def test_benchmark_offline_contracts_cover_versioned_splits_without_training_leakage():
    cases = load_benchmark(_BENCHMARK)
    report = evaluate_offline(cases)

    assert len(cases) == 25
    assert {case.split for case in cases} == {"train", "validation", "hidden_test"}
    assert report["summary"]["offline_contract_passed"] == len(cases)
    assert report["summary"]["valid_sql_rate"] == 1.0
    assert report["summary"]["training_eligible_cases"] == sum(
        case.split == "train" and case.raw["review"]["status"] == "approved" for case in cases
    )

    validation_cases = [case for case in cases if case.split == "validation"]
    assert validation_cases
    assert all(case.raw["review"]["status"] == "approved" for case in validation_cases)


def test_benchmark_plans_compile_to_the_fixed_read_only_tsql_shape():
    case = load_benchmark(_BENCHMARK, splits=("train",))[0]
    plan = parse_semantic_plan(case.raw["expected_plan"])
    statement = compile_incident_query(compile_analytics_request(plan), from_at=None, to_at=None)

    assert is_read_only_incident_query(statement.statement)
    assert "FROM [dbo].[vConnectorIncidentFacts]" in statement.statement


def test_benchmark_rejects_a_family_that_leaks_between_splits(tmp_path: Path):
    copied = tmp_path / "bench"
    shutil.copytree(_BENCHMARK, copied)
    validation = copied / "validation.jsonl"
    records = [json.loads(line) for line in validation.read_text(encoding="utf-8").splitlines()]
    records[0]["family_id"] = "failed-today"
    validation.write_text("\n".join(json.dumps(item) for item in records) + "\n", encoding="utf-8")

    with pytest.raises(AnalyticsBenchError, match="family leakage"):
        load_benchmark(copied)


def test_benchmark_rejects_sensitive_values_before_a_case_can_be_exported(tmp_path: Path):
    copied = tmp_path / "bench"
    shutil.copytree(_BENCHMARK, copied)
    train = copied / "train.jsonl"
    records = [json.loads(line) for line in train.read_text(encoding="utf-8").splitlines()]
    records[0]["question"] = "hf_abcdefghijklmnop"
    train.write_text("\n".join(json.dumps(item) for item in records) + "\n", encoding="utf-8")

    with pytest.raises(AnalyticsBenchError, match="sensitive-looking"):
        load_benchmark(copied)


def test_benchmark_rejects_personal_contact_values(tmp_path: Path):
    copied = tmp_path / "bench"
    shutil.copytree(_BENCHMARK, copied)
    train = copied / "train.jsonl"
    records = [json.loads(line) for line in train.read_text(encoding="utf-8").splitlines()]
    records[0]["question"] = "Liên hệ 0912345678 để kiểm tra connector"
    train.write_text("\n".join(json.dumps(item) for item in records) + "\n", encoding="utf-8")

    with pytest.raises(AnalyticsBenchError, match="sensitive-looking"):
        load_benchmark(copied)


def test_training_export_requires_approved_train_labels_and_excludes_answers_and_facts():
    cases = load_benchmark(_BENCHMARK, splits=("train",))
    records = export_training_records(cases)

    assert len(records) == len(cases)
    assert all(set(record) == {"id", "messages", "target_semantic_plan"} for record in records)
    assert all(
        "answer" not in json.dumps(record) and "reference_execution" not in json.dumps(record)
        for record in records
    )

    unapproved_cases = copy.deepcopy(cases)
    for case in unapproved_cases:
        case.raw["review"] = {"status": "seed", "reviewer": "", "reviewed_at": None}
    with pytest.raises(AnalyticsBenchError, match="no approved training records"):
        export_training_records(unapproved_cases)

    local_format_records = export_training_records(unapproved_cases, include_seed=True)

    assert len(local_format_records) == len(cases)
    assert all(
        "answer" not in json.dumps(record) and "reference_execution" not in json.dumps(record)
        for record in local_format_records
    )


class _EchoGoldAdapter:
    name = "fixture-echo"

    def __init__(self, cases) -> None:
        self._cases = {case.id: case for case in cases}

    def evaluate(self, *, question, prior_questions=(), conversation_id):
        case = self._cases[conversation_id.removeprefix("shk-bench:")]
        reference = case.raw["reference_execution"]
        return SemanticAdapterResult(
            semantic_plan=case.raw["expected_plan"],
            route=case.raw["expected_route"],
            outcome=case.raw["expected_outcome"],
            query_executed=reference.get("query_executed"),
            evidence_complete=reference.get("evidence_complete"),
            row_count=reference.get("row_count"),
            answer="Đây là kết quả fixture.",
            latency_seconds=0.1,
        )


def test_provider_neutral_adapter_scores_canonical_plan_and_grounded_safety():
    cases = load_benchmark(_BENCHMARK)
    report = evaluate_adapter(cases, _EchoGoldAdapter(cases))

    assert report["summary"]["canonical_plan_accuracy"] == 1.0
    assert report["summary"]["route_accuracy"] == 1.0
    assert report["summary"]["outcome_accuracy"] is None
    assert report["summary"]["execution_result_accuracy"] is None
    assert report["summary"]["outcome_accuracy_case_count"] == 0
    assert report["summary"]["safety_critical_gate_passed"] is True
    assert report["summary"]["grounded_negative_safety_rate"] == 1.0


def test_adapter_excludes_offline_fault_injection_from_model_accuracy():
    cases = load_benchmark(_BENCHMARK, splits=("validation",))
    report = evaluate_adapter(cases, _EchoGoldAdapter(cases))

    invalid = next(item for item in report["records"] if item["id"] == "invalid-semantic-plan")
    assert invalid["status"] == "offline_contract_only"
    assert report["summary"]["case_count"] == 8
    assert report["summary"]["adapter_scored_case_count"] == 7
    assert report["summary"]["offline_contract_only_case_count"] == 1


def _approved_snapshot_case():
    original = load_benchmark(_BENCHMARK, splits=("train",))[0]
    raw = copy.deepcopy(original.raw)
    raw["review"] = {"status": "approved", "reviewer": "Minh An", "reviewed_at": "2026-09-16"}
    return type(original)(
        raw=raw, source_path=original.source_path, line_number=original.line_number
    )


def _capture_approved_snapshot(case):
    captured_packets: list[tuple[str, tuple]] = []

    def execute(*, statement, parameters):
        assert is_read_only_incident_query(statement)
        captured_packets.append((statement, parameters))
        return [
            {
                "root_connector_name": "fixture-safe-connector",
                "incident_count": 2,
                "rank": 1,
                "tie_count": 1,
                "row_number": 1,
                # These source-only fields must not be retained by the manifest.
                "evidence_ids": "fixture-incident-id",
                "error_message": "password=should-not-appear",
            }
        ]

    manifest = build_snapshot_manifest(
        benchmark_version="2026-09-16.1",
        snapshot_id="baseline-safe-20260916",
        timezone_name="Asia/Ho_Chi_Minh",
        cases=[case],
        execute=execute,
        now=datetime(2026, 9, 16, 6, tzinfo=timezone.utc),
        source_identity="mssql-benchmark-fixture",
    )
    assert captured_packets
    assert "password" not in json.dumps(manifest)
    assert "evidence_ids" not in json.dumps(manifest)
    assert "fixture-safe-connector" not in json.dumps(manifest)
    assert "mssql-benchmark-fixture" not in json.dumps(manifest)
    manifest["status"] = "approved"
    manifest["review"] = {"reviewer": "Minh An", "reviewed_at": "2026-09-16"}
    manifest["integrity"]["content_sha256"] = content_hash(manifest)
    return manifest


class _SnapshotAdapter:
    name = "immutable-snapshot-fixture"

    def __init__(
        self,
        case,
        snapshot,
        *,
        source_identity_hash_override: str | None = None,
        semantic_plan_override: dict | None = None,
    ):
        self._case = case
        self._snapshot = snapshot
        self._source_identity_hash_override = source_identity_hash_override
        self._semantic_plan_override = semantic_plan_override

    def evaluate(self, *, question, prior_questions=(), conversation_id):
        entry = self._snapshot["cases"][0]
        execution = entry["execution"]
        return SemanticAdapterResult(
            semantic_plan=self._semantic_plan_override or self._case.raw["expected_plan"],
            route=self._case.raw["expected_route"],
            outcome=execution["outcome"],
            query_executed=True,
            evidence_complete=True,
            row_count=execution["row_count"],
            answer="Kết quả snapshot đã xác minh.",
            latency_seconds=0.1,
            source_snapshot_id=self._snapshot["snapshot_id"],
            source_integrity_hash=self._snapshot["integrity"]["content_sha256"],
            source_identity_hash=(
                self._source_identity_hash_override
                if self._source_identity_hash_override is not None
                else self._snapshot["source"]["identity_sha256"]
            ),
            source_timezone=self._snapshot["timezone"],
            time_range_applied=entry["time_range"],
            canonical_facts=tuple(execution["facts"]),
        )


def _immutable_reference(case, manifest):
    entry = manifest["cases"][0]
    execution = entry["execution"]
    case.raw["reference_execution"] = {
        "kind": "executed",
        "reference_classification": "immutable_snapshot",
        "query_executed": True,
        "evidence_complete": True,
        "row_count": execution["row_count"],
        "facts": execution["facts"],
        "truncated": False,
        "snapshot_id": manifest["snapshot_id"],
        "snapshot_integrity_sha256": manifest["integrity"]["content_sha256"],
        "source_identity_sha256": manifest["source"]["identity_sha256"],
        "timezone": manifest["timezone"],
        "time_range": entry["time_range"],
    }
    case.raw["expected_outcome"] = execution["outcome"]


def test_approved_matching_snapshot_is_required_before_execution_or_outcome_score():
    case = _approved_snapshot_case()
    manifest = _capture_approved_snapshot(case)
    _immutable_reference(case, manifest)
    snapshot = SnapshotReference(raw=manifest, source_path="test")
    report = evaluate_adapter(
        [case], _SnapshotAdapter(case, manifest), snapshots={snapshot.snapshot_id: snapshot}
    )

    assert report["summary"]["outcome_accuracy"] == 1.0
    assert report["summary"]["execution_result_accuracy"] == 1.0
    assert report["records"][0]["execution_reference"] == "approved_snapshot_match"


@pytest.mark.parametrize("mutation", ["unapproved", "id", "hash", "source", "timezone"])
def test_snapshot_mismatch_or_missing_approval_cannot_enable_execution_score(mutation: str):
    case = _approved_snapshot_case()
    manifest = _capture_approved_snapshot(case)
    _immutable_reference(case, manifest)
    if mutation == "unapproved":
        manifest["status"] = "draft"
        manifest["review"] = {"reviewer": "", "reviewed_at": None}
        manifest["integrity"]["content_sha256"] = content_hash(manifest)
    elif mutation == "id":
        case.raw["reference_execution"]["snapshot_id"] = "another-approved-snapshot"
    elif mutation == "hash":
        case.raw["reference_execution"]["snapshot_integrity_sha256"] = "0" * 64
    elif mutation == "source":
        case.raw["reference_execution"]["source_identity_sha256"] = source_identity_hash(
            "other-source"
        )
    else:
        case.raw["reference_execution"]["timezone"] = "UTC"
    snapshot = SnapshotReference(raw=manifest, source_path="test")
    report = evaluate_adapter(
        [case], _SnapshotAdapter(case, manifest), snapshots={snapshot.snapshot_id: snapshot}
    )

    assert report["summary"]["outcome_accuracy"] is None
    assert report["summary"]["execution_result_accuracy"] is None
    assert report["records"][0]["execution_reference"] != "approved_snapshot_match"


def test_snapshot_source_identity_must_match_the_actual_adapter_source():
    case = _approved_snapshot_case()
    manifest = _capture_approved_snapshot(case)
    _immutable_reference(case, manifest)
    snapshot = SnapshotReference(raw=manifest, source_path="test")
    report = evaluate_adapter(
        [case],
        _SnapshotAdapter(case, manifest, source_identity_hash_override="0" * 64),
        snapshots={snapshot.snapshot_id: snapshot},
    )

    assert report["summary"]["outcome_accuracy"] is None
    assert report["summary"]["execution_result_accuracy"] is None
    assert report["records"][0]["execution_reference"] == "snapshot_source_identity_mismatch"


def test_snapshot_score_requires_the_adapter_to_return_the_reviewed_plan():
    case = _approved_snapshot_case()
    manifest = _capture_approved_snapshot(case)
    _immutable_reference(case, manifest)
    changed_plan = copy.deepcopy(case.raw["expected_plan"])
    changed_plan["data_request"]["limit"] = 1
    snapshot = SnapshotReference(raw=manifest, source_path="test")
    report = evaluate_adapter(
        [case],
        _SnapshotAdapter(case, manifest, semantic_plan_override=changed_plan),
        snapshots={snapshot.snapshot_id: snapshot},
    )

    assert report["summary"]["outcome_accuracy"] is None
    assert report["summary"]["execution_result_accuracy"] is None
    assert report["records"][0]["execution_reference"] == "source_plan_mismatch"


def test_snapshot_from_another_benchmark_version_is_rejected_for_scoring():
    case = _approved_snapshot_case()
    manifest = _capture_approved_snapshot(case)
    _immutable_reference(case, manifest)
    manifest["benchmark_version"] = "2026-01-01.0"
    manifest["integrity"]["content_sha256"] = content_hash(manifest)
    snapshot = SnapshotReference(raw=manifest, source_path="test")

    with pytest.raises(AnalyticsBenchError, match="benchmark version"):
        evaluate_adapter(
            [case], _SnapshotAdapter(case, manifest), snapshots={snapshot.snapshot_id: snapshot}
        )


def test_snapshot_capture_rejects_unreviewed_cases_and_hidden_split_never_exports():
    approved_case = load_benchmark(_BENCHMARK, splits=("validation",))[0]
    unreviewed_raw = {
        **copy.deepcopy(approved_case.raw),
        "review": {"status": "seed", "reviewer": "", "reviewed_at": None},
    }
    unreviewed_case = type(approved_case)(
        raw=unreviewed_raw,
        source_path=approved_case.source_path,
        line_number=approved_case.line_number,
    )
    with pytest.raises(SnapshotReferenceError, match="reviewed"):
        build_snapshot_manifest(
            benchmark_version="2026-09-16.1",
            snapshot_id="unsafe-capture",
            timezone_name="Asia/Ho_Chi_Minh",
            cases=[unreviewed_case],
            execute=lambda **_: [],
            now=datetime(2026, 9, 16, tzinfo=timezone.utc),
            source_identity="mssql-benchmark-fixture",
        )

    hidden = load_benchmark(_BENCHMARK, splits=("hidden_test",))
    with pytest.raises(AnalyticsBenchError, match="no approved training records"):
        export_training_records(hidden)


def test_snapshot_capture_redacts_connector_identifiers_before_writing():
    case = _approved_snapshot_case()
    manifest = build_snapshot_manifest(
        benchmark_version="2026-09-16.1",
        snapshot_id="safe-fact-capture",
        timezone_name="Asia/Ho_Chi_Minh",
        cases=[case],
        execute=lambda **_: [
            {
                "root_connector_name": "person@example.com",
                "incident_count": 1,
                "rank": 1,
                "tie_count": 1,
                "row_number": 1,
            }
        ],
        now=datetime(2026, 9, 16, tzinfo=timezone.utc),
        source_identity="mssql-benchmark-fixture",
    )

    rendered = json.dumps(manifest)
    assert "person@example.com" not in rendered
    assert "sha256:" in rendered


def test_snapshot_manifest_rejects_tamper_and_approved_without_reviewer(tmp_path: Path):
    manifest = _capture_approved_snapshot(_approved_snapshot_case())
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    assert load_snapshot(path).approved is True

    manifest["integrity"]["content_sha256"] = "0" * 64
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SnapshotReferenceError, match="integrity hash mismatch"):
        load_snapshot(path)

    manifest["integrity"]["content_sha256"] = content_hash(manifest)
    manifest["cases"][0]["execution"]["facts"][0]["error_code"] = "SELECT source"
    manifest["cases"][0]["execution"]["facts_sha256"] = facts_hash(
        manifest["cases"][0]["execution"]["facts"]
    )
    manifest["integrity"]["content_sha256"] = content_hash(manifest)
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SnapshotReferenceError, match="cannot contain SQL"):
        load_snapshot(path)

    manifest["cases"][0]["execution"]["facts"][0].pop("error_code")
    manifest["cases"][0]["execution"]["facts_sha256"] = facts_hash(
        manifest["cases"][0]["execution"]["facts"]
    )
    manifest["integrity"]["content_sha256"] = content_hash(manifest)
    manifest["review"] = {"reviewer": "", "reviewed_at": None}
    manifest["integrity"]["content_sha256"] = content_hash(manifest)
    path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(SnapshotReferenceError, match="dated reviewer"):
        load_snapshot(path)


def test_snapshot_review_creates_a_new_integrity_bound_approval():
    draft = _capture_approved_snapshot(_approved_snapshot_case())
    draft["status"] = "draft"
    draft["review"] = {"reviewer": "", "reviewed_at": None}
    draft["integrity"]["content_sha256"] = content_hash(draft)
    reviewed = apply_snapshot_review(
        SnapshotReference(raw=draft, source_path="draft.json"),
        status="approved",
        reviewer="Minh An",
        reviewed_at="2026-09-16",
    )

    assert draft["status"] == "draft"
    assert reviewed["status"] == "approved"
    assert reviewed["review"] == {"reviewer": "Minh An", "reviewed_at": "2026-09-16"}
    assert reviewed["integrity"]["content_sha256"] == content_hash(reviewed)

    with pytest.raises(SnapshotReferenceError, match="requires reviewer"):
        apply_snapshot_review(
            SnapshotReference(raw=draft, source_path="draft.json"),
            status="approved",
            reviewer="",
            reviewed_at="2026-09-16",
        )
