"""Versioned, provider-neutral benchmark for semantic analytics planning.

The benchmark has two deliberately separate jobs:

* Offline validation checks independently authored labels, compilation safety,
  data leakage, and the execution-outcome invariants without a model or DB.
* An opt-in adapter can score a configured model against the exact same labels.

No case stores SQL, answers, credentials, or production snapshots.  Training
export contains only approved question/context to canonical-plan pairs.
"""

from __future__ import annotations

import json
import re
import statistics
import unicodedata
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from self_healthy_kafka.evaluation.semantic_adapter import (
    SemanticAdapterResult,
    SemanticPlanAdapter,
)
from self_healthy_kafka.evaluation.snapshot import (
    SnapshotReference,
    facts_hash,
    match_snapshot_reference,
    plan_hash,
)
from self_healthy_kafka.semantic.outcome import (
    cannot_verify,
    classify_execution,
    degraded,
    needs_clarification,
)
from self_healthy_kafka.semantic.planner import (
    SemanticPlanError,
    compile_analytics_request,
    parse_semantic_plan,
)
from self_healthy_kafka.semantic.tsql import (
    compile_incident_query,
    is_read_only_incident_query,
)

BENCHMARK_VERSION = "2026-09-16.1"
SPLITS = ("train", "validation", "hidden_test")
OUTCOMES = {
    "verified_results",
    "verified_empty",
    "cannot_verify",
    "needs_clarification",
    "degraded",
}
REVIEW_STATUSES = {"seed", "approved", "rejected"}
EXECUTION_KINDS = {
    "executed",
    "evidence_incomplete",
    "planner_invalid",
    "source_unavailable",
    "needs_clarification",
}
REFERENCE_CLASSIFICATIONS = {"synthetic_fixture", "immutable_snapshot"}
EVALUATION_SCOPES = {"adapter", "offline_contract"}
SAFETY_ASSERTIONS = {
    "must_use_read_only_compiler",
    "must_preserve_time_scope",
    "must_preserve_failed_state",
    "must_not_make_negative_claim_without_verified_empty",
    "must_enforce_exact_top_n",
    "must_include_ties_when_requested",
    "must_require_clarification",
    "must_not_route_to_analytics",
}
_CASE_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,100}$")
_TAG = re.compile(r"^[a-z0-9][a-z0-9-]{1,100}$")
_SENSITIVE_PATTERNS = (
    re.compile(r"(?i)\b(?:hf|sk|sk-ant|ghp)_[a-z0-9_-]{12,}\b"),
    re.compile(r"(?i)\b(?:bearer|basic)\s+[a-z0-9._~+/-]{8,}\b"),
    re.compile(
        r"(?i)\b(?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|passwd|pwd|secret)\s*[:=]\s*\S+"
    ),
    re.compile(r"(?i)(?:driver|server|uid|user id|password)\s*=\s*[^;\s]+"),
    re.compile(r"(?i)\b[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}\b"),
    re.compile(r"(?<!\d)(?:\+?84|0)\d{8,10}(?!\d)"),
)
_SQL_PATTERN = re.compile(
    r"(?i)\b(?:select|insert|update|delete|merge|drop|alter|create|exec(?:ute)?)\b"
)
_NEGATIVE_CLAIM = re.compile(
    r"(?i)(?:khong\s+co|chua\s+ghi\s+nhan|no\s+(?:failed|failure|connector|incident))"
)


class AnalyticsBenchError(ValueError):
    """The benchmark data or a model result violated its safe contract."""


@dataclass(frozen=True)
class BenchmarkCase:
    """A validated case whose labels are independent from runtime responses."""

    raw: dict[str, Any]
    source_path: Path
    line_number: int

    @property
    def id(self) -> str:
        return self.raw["id"]

    @property
    def split(self) -> str:
        return self.raw["split"]

    @property
    def question(self) -> str:
        return self.raw["question"]

    @property
    def evaluation_scope(self) -> str:
        return self.raw.get("evaluation_scope", "adapter")

    @property
    def prior_questions(self) -> tuple[str, ...]:
        return tuple(item["question"] for item in self.raw.get("conversation", []))


def load_benchmark(directory: Path, *, splits: Sequence[str] = SPLITS) -> list[BenchmarkCase]:
    """Load and validate selected benchmark splits without model/network access."""

    selected = _validate_splits(splits)
    _validate_manifest(directory)
    cases: list[BenchmarkCase] = []
    for split in selected:
        path = directory / f"{split}.jsonl"
        if not path.is_file():
            raise AnalyticsBenchError(f"missing benchmark split: {path}")
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as exc:
                raise AnalyticsBenchError(f"{path}:{line_number}: invalid JSON") from exc
            if not isinstance(raw, dict):
                raise AnalyticsBenchError(f"{path}:{line_number}: case must be an object")
            case = BenchmarkCase(raw=raw, source_path=path, line_number=line_number)
            _validate_case(case, expected_split=split)
            cases.append(case)
    _validate_collection(cases)
    return cases


def evaluate_offline(cases: Sequence[BenchmarkCase]) -> dict[str, Any]:
    """Verify labels, outcome invariants, and compiler safety deterministically."""

    records: list[dict[str, Any]] = []
    executable_cases = 0
    compilable_cases = 0
    safety_passed = 0
    for case in cases:
        raw = case.raw
        plan = _expected_plan(raw)
        compiled = None
        statement = None
        if plan is not None and plan.data_request is not None:
            executable_cases += 1
            compiled = compile_analytics_request(plan)
            # Validate the actual fixed T-SQL statement, not only its DSL.
            statement = compile_incident_query(compiled, from_at=None, to_at=None)
            if not is_read_only_incident_query(statement.statement):
                raise AnalyticsBenchError(f"{case.id}: compiler emitted a non-read-only statement")
            compilable_cases += 1
        outcome = _reference_outcome(raw["reference_execution"])
        if outcome != raw["expected_outcome"]:
            raise AnalyticsBenchError(
                f"{case.id}: reference execution does not match expected outcome"
            )
        _validate_safety_invariants(
            raw,
            plan=plan,
            compiled=compiled,
            statement=statement,
            reference_outcome=outcome,
        )
        safety_passed += 1
        records.append(
            {
                "id": case.id,
                "split": case.split,
                "status": "offline_contract_passed",
                "expected_route": raw["expected_route"],
                "expected_outcome": raw["expected_outcome"],
                "compiled_read_only": statement is not None,
                "safety_assertion_count": len(raw["safety_assertions"]),
            }
        )
    return {
        "benchmark_version": BENCHMARK_VERSION,
        "mode": "offline",
        "records": records,
        "summary": {
            "case_count": len(cases),
            "split_counts": dict(sorted(Counter(case.split for case in cases).items())),
            "tag_counts": dict(
                sorted(Counter(tag for case in cases for tag in case.raw["tags"]).items())
            ),
            "offline_contract_passed": len(records),
            "safety_case_passed": safety_passed,
            "valid_sql_rate": compilable_cases / executable_cases if executable_cases else None,
            "training_eligible_cases": sum(
                case.split == "train" and _is_training_eligible(case.raw) for case in cases
            ),
            "not_measured": [
                "Offline mode validates independently authored labels and contracts; it does not run a model, database, or provider.",
                "Execution-result accuracy against a live source requires an approved immutable snapshot reference.",
            ],
        },
    }


def evaluate_adapter(
    cases: Sequence[BenchmarkCase],
    adapter: SemanticPlanAdapter,
    *,
    snapshots: Mapping[str, SnapshotReference] | None = None,
) -> dict[str, Any]:
    """Score one provider through the shared planner service contract.

    Live execution results are intentionally not compared to synthetic fixtures:
    a changed database snapshot is not a model error.  Canonical plan, route,
    outcome safety, latency, and token metadata remain measurable.
    """

    records: list[dict[str, Any]] = []
    available_snapshots = snapshots or {}
    if any(
        snapshot.raw["benchmark_version"] != BENCHMARK_VERSION
        for snapshot in available_snapshots.values()
    ):
        raise AnalyticsBenchError("snapshot benchmark version does not match this scorecard")
    for case in cases:
        if case.evaluation_scope == "offline_contract":
            records.append(
                {
                    "id": case.id,
                    "split": case.split,
                    "status": "offline_contract_only",
                    "safety_critical": "safety-critical" in case.raw["tags"],
                }
            )
            continue
        result = adapter.evaluate(
            question=case.question,
            prior_questions=case.prior_questions,
            conversation_id=f"shk-bench:{case.id}",
        )
        expected_plan = _expected_plan(case.raw)
        actual_plan = _try_canonical_plan(result.semantic_plan)
        plan_match = (
            actual_plan == _canonical_plan(expected_plan.to_dict())
            if expected_plan
            else result.semantic_plan is None
        )
        route_match = result.route == case.raw["expected_route"]
        expected_plan_hash = (
            plan_hash(_canonical_plan(expected_plan.to_dict())) if expected_plan else None
        )
        actual_plan_hash = plan_hash(actual_plan) if actual_plan is not None else None
        baseline_match = match_snapshot_reference(
            case_id=case.id,
            reference_execution=case.raw["reference_execution"],
            expected_plan_hash=expected_plan_hash,
            adapter_plan_hash=actual_plan_hash,
            adapter_snapshot_id=result.source_snapshot_id,
            adapter_integrity_hash=result.source_integrity_hash,
            adapter_source_identity_hash=result.source_identity_hash,
            adapter_timezone=result.source_timezone,
            adapter_time_range=result.time_range_applied,
            snapshots=available_snapshots,
        )
        outcome_match = (
            result.outcome == baseline_match.reference["outcome"]
            if baseline_match.reference is not None
            else None
        )
        execution_match = (
            _execution_result_match(result, baseline_match.reference)
            if baseline_match.reference is not None
            else None
        )
        negative_safe = not (
            result.outcome != "verified_empty"
            and _NEGATIVE_CLAIM.search(_normalize_text(result.answer))
        )
        safety_passed = _adapter_safety_safe(
            case.raw,
            result=result,
            actual_plan=actual_plan,
            grounded_negative_safe=negative_safe,
        )
        records.append(
            {
                "id": case.id,
                "split": case.split,
                "status": "attempted",
                "plan_valid": actual_plan is not None,
                "plan_match": plan_match,
                "route_match": route_match,
                "outcome": result.outcome,
                "outcome_match": outcome_match,
                "execution_result_match": execution_match,
                "execution_reference": baseline_match.reason,
                "safety_gate_passed": safety_passed,
                "grounded_negative_safe": negative_safe,
                "safety_critical": "safety-critical" in case.raw["tags"],
                "time_scope_match": _time_scope_match(expected_plan, actual_plan),
                "ranking_match": _ranking_match(expected_plan, actual_plan),
                "clarification_match": _clarification_match(case.raw, result),
                "latency_seconds": result.latency_seconds,
                "api_calls": result.api_calls,
                "input_tokens": result.input_tokens,
                "output_tokens": result.output_tokens,
                "correction_count": result.correction_count,
            }
        )
    return _adapter_report(records, adapter_name=adapter.name)


def export_training_records(
    cases: Sequence[BenchmarkCase], *, include_seed: bool = False
) -> list[dict[str, Any]]:
    """Return safe question/context -> canonical semantic-plan LoRA records.

    Only human-approved labels are eligible by default.  The output deliberately
    excludes answers, SQL, execution facts, and model/provider information.
    """

    records: list[dict[str, Any]] = []
    for case in cases:
        raw = case.raw
        if raw["split"] != "train" or not _is_training_eligible(raw, include_seed=include_seed):
            continue
        plan = _expected_plan(raw)
        if plan is None:
            continue
        records.append(
            {
                "id": case.id,
                "messages": [
                    *({"role": "user", "content": item} for item in case.prior_questions),
                    {"role": "user", "content": case.question},
                ],
                "target_semantic_plan": _plan_for_model(plan.to_dict()),
            }
        )
    if not records:
        raise AnalyticsBenchError("no approved training records; review labels before exporting")
    _scan_records_for_sensitive_content(records, context="training export")
    return records


def write_jsonl(path: Path, records: Iterable[Mapping[str, Any]]) -> int:
    """Write a caller-authorized, bounded JSONL artifact for a training job."""

    rendered = [json.dumps(dict(record), ensure_ascii=False, sort_keys=True) for record in records]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(rendered) + "\n", encoding="utf-8")
    return len(rendered)


def _validate_manifest(directory: Path) -> None:
    manifest_path = directory / "schema.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AnalyticsBenchError(f"benchmark schema is unavailable: {manifest_path}") from exc
    if not isinstance(manifest, dict) or manifest.get("benchmark_version") != BENCHMARK_VERSION:
        raise AnalyticsBenchError("benchmark schema version is unsupported")


def _validate_splits(splits: Sequence[str]) -> tuple[str, ...]:
    selected = tuple(dict.fromkeys(splits))
    if not selected or any(split not in SPLITS for split in selected):
        raise AnalyticsBenchError(f"splits must be selected from {', '.join(SPLITS)}")
    return selected


def _validate_case(case: BenchmarkCase, *, expected_split: str) -> None:
    raw = case.raw
    required = {
        "id",
        "split",
        "family_id",
        "question",
        "conversation",
        "tags",
        "expected_plan",
        "expected_route",
        "expected_outcome",
        "reference_execution",
        "safety_assertions",
        "review",
    }
    unsupported = set(raw) - required - {"evaluation_scope"}
    missing = required - set(raw)
    location = f"{case.source_path}:{case.line_number}"
    if missing or unsupported:
        raise AnalyticsBenchError(f"{location}: unsupported or missing case fields")
    if not isinstance(raw["id"], str) or not _CASE_ID.fullmatch(raw["id"]):
        raise AnalyticsBenchError(f"{location}: id is unsupported")
    if raw["split"] != expected_split:
        raise AnalyticsBenchError(f"{location}: split does not match its file")
    if raw.get("evaluation_scope", "adapter") not in EVALUATION_SCOPES:
        raise AnalyticsBenchError(f"{location}: evaluation_scope is unsupported")
    if not isinstance(raw["family_id"], str) or not _CASE_ID.fullmatch(raw["family_id"]):
        raise AnalyticsBenchError(f"{location}: family_id is unsupported")
    _validate_text(raw["question"], label="question", location=location)
    _validate_conversation(raw["conversation"], location=location)
    if (
        not isinstance(raw["tags"], list)
        or not raw["tags"]
        or len(raw["tags"]) > 12
        or not all(isinstance(item, str) and _TAG.fullmatch(item) for item in raw["tags"])
    ):
        raise AnalyticsBenchError(f"{location}: tags are unsupported")
    plan = _expected_plan(raw)
    expected_route = raw["expected_route"]
    derived_route = (
        plan.route.value
        if plan and plan.route
        else "unsupported"
        if plan is None
        else "clarification"
    )
    if (
        expected_route not in {"analytics", "runbook", "combined", "clarification", "unsupported"}
        or expected_route != derived_route
    ):
        raise AnalyticsBenchError(f"{location}: expected_route does not match expected_plan")
    if plan is None and raw["reference_execution"].get("kind") != "planner_invalid":
        raise AnalyticsBenchError(f"{location}: only planner_invalid cases may omit expected_plan")
    if raw["expected_outcome"] not in OUTCOMES:
        raise AnalyticsBenchError(f"{location}: expected_outcome is unsupported")
    _validate_reference_execution(raw["reference_execution"], location=location)
    if (
        not isinstance(raw["safety_assertions"], list)
        or not raw["safety_assertions"]
        or not set(raw["safety_assertions"]) <= SAFETY_ASSERTIONS
    ):
        raise AnalyticsBenchError(f"{location}: safety_assertions are unsupported")
    _validate_review(raw["review"], location=location)
    _scan_records_for_sensitive_content([raw], context=location)
    _reject_raw_sql(raw, location=location)


def _expected_plan(raw: Mapping[str, Any]):
    value = raw["expected_plan"]
    if value is None:
        return None
    try:
        plan = parse_semantic_plan(value)
        if plan.data_request is not None:
            compile_analytics_request(plan)
        return plan
    except SemanticPlanError as exc:
        raise AnalyticsBenchError(
            f"{raw.get('id', 'unknown')}: expected_plan is invalid: {exc}"
        ) from exc


def _validate_conversation(value: Any, *, location: str) -> None:
    if not isinstance(value, list) or len(value) > 6:
        raise AnalyticsBenchError(f"{location}: conversation is unsupported")
    for item in value:
        if not isinstance(item, dict) or set(item) != {"question"}:
            raise AnalyticsBenchError(f"{location}: conversation may only contain prior questions")
        _validate_text(item["question"], label="conversation question", location=location)


def _validate_text(value: Any, *, label: str, location: str) -> None:
    if not isinstance(value, str) or not value.strip() or len(value) > 1_000:
        raise AnalyticsBenchError(f"{location}: {label} is unsupported")


def _validate_reference_execution(value: Any, *, location: str) -> None:
    if not isinstance(value, dict) or value.get("kind") not in EXECUTION_KINDS:
        raise AnalyticsBenchError(f"{location}: reference_execution kind is unsupported")
    kind = value["kind"]
    if kind == "executed":
        required = {
            "kind",
            "reference_classification",
            "query_executed",
            "evidence_complete",
            "row_count",
            "facts",
            "truncated",
        }
        classification = value.get("reference_classification")
        if classification == "synthetic_fixture":
            required.add("fixture_id")
        elif classification == "immutable_snapshot":
            required.update(
                {
                    "snapshot_id",
                    "snapshot_integrity_sha256",
                    "source_identity_sha256",
                    "timezone",
                    "time_range",
                }
            )
        if (
            set(value) != required
            or value["query_executed"] is not True
            or value["evidence_complete"] is not True
        ):
            raise AnalyticsBenchError(f"{location}: executed reference must be complete")
        if (
            not isinstance(value["row_count"], int)
            or value["row_count"] < 0
            or not isinstance(value["facts"], list)
        ):
            raise AnalyticsBenchError(f"{location}: executed reference counts are unsupported")
        if not isinstance(value["truncated"], bool):
            raise AnalyticsBenchError(f"{location}: executed reference metadata is unsupported")
        _validate_reference_identity(value, location=location)
        if value["row_count"] == 0 and value["facts"]:
            raise AnalyticsBenchError(f"{location}: empty reference cannot contain facts")
        if value["row_count"] > 0 and not value["facts"]:
            raise AnalyticsBenchError(f"{location}: result reference needs at least one fact")
        return
    required = {"kind", "reason", "reference_classification", "fixture_id"}
    if (
        set(value) != required
        or not isinstance(value["reason"], str)
        or not value["reason"].strip()
    ):
        raise AnalyticsBenchError(f"{location}: non-executed reference is unsupported")
    _validate_reference_identity(value, location=location)


def _validate_reference_identity(value: Mapping[str, Any], *, location: str) -> None:
    classification = value.get("reference_classification")
    if classification not in REFERENCE_CLASSIFICATIONS:
        raise AnalyticsBenchError(f"{location}: reference classification is unsupported")
    if classification == "synthetic_fixture":
        if not isinstance(value.get("fixture_id"), str) or not value["fixture_id"].strip():
            raise AnalyticsBenchError(f"{location}: fixture reference identity is unsupported")
        return
    if not isinstance(value.get("snapshot_id"), str) or not _CASE_ID.fullmatch(
        value["snapshot_id"]
    ):
        raise AnalyticsBenchError(f"{location}: snapshot reference identity is unsupported")
    if not isinstance(value.get("snapshot_integrity_sha256"), str) or not re.fullmatch(
        r"[a-f0-9]{64}", value["snapshot_integrity_sha256"]
    ):
        raise AnalyticsBenchError(f"{location}: snapshot reference hash is unsupported")
    if not isinstance(value.get("source_identity_sha256"), str) or not re.fullmatch(
        r"[a-f0-9]{64}", value["source_identity_sha256"]
    ):
        raise AnalyticsBenchError(f"{location}: snapshot reference source identity is unsupported")
    if not isinstance(value.get("timezone"), str) or not value["timezone"].strip():
        raise AnalyticsBenchError(f"{location}: snapshot reference timezone is unsupported")
    time_range = value.get("time_range")
    if not isinstance(time_range, dict) or set(time_range) != {
        "kind",
        "from_at",
        "to_at",
        "timezone",
        "timestamp_field",
    }:
        raise AnalyticsBenchError(f"{location}: snapshot reference time range is unsupported")
    if (
        time_range.get("timezone") != value["timezone"]
        or time_range.get("timestamp_field") != "failure_at"
    ):
        raise AnalyticsBenchError(f"{location}: snapshot reference time range is inconsistent")


def _validate_review(value: Any, *, location: str) -> None:
    if not isinstance(value, dict) or set(value) != {"status", "reviewer", "reviewed_at"}:
        raise AnalyticsBenchError(f"{location}: review is unsupported")
    if value["status"] not in REVIEW_STATUSES or not isinstance(value["reviewer"], str):
        raise AnalyticsBenchError(f"{location}: review status is unsupported")
    reviewed_at = value["reviewed_at"]
    if reviewed_at is not None and (
        not isinstance(reviewed_at, str) or not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", reviewed_at)
    ):
        raise AnalyticsBenchError(f"{location}: review date is unsupported")
    if value["status"] == "approved" and (not value["reviewer"].strip() or reviewed_at is None):
        raise AnalyticsBenchError(f"{location}: approved cases require a dated reviewer")


def _validate_collection(cases: Sequence[BenchmarkCase]) -> None:
    if not cases:
        raise AnalyticsBenchError("benchmark is empty")
    ids: set[str] = set()
    normalized_questions: dict[str, str] = {}
    family_splits: dict[str, str] = {}
    for case in cases:
        if case.id in ids:
            raise AnalyticsBenchError(f"duplicate benchmark id: {case.id}")
        ids.add(case.id)
        fingerprint = _conversation_fingerprint(case)
        previous = normalized_questions.get(fingerprint)
        if previous is not None:
            raise AnalyticsBenchError(
                f"duplicate question/context across cases: {previous}, {case.id}"
            )
        normalized_questions[fingerprint] = case.id
        family = case.raw["family_id"]
        prior_split = family_splits.setdefault(family, case.split)
        if prior_split != case.split:
            raise AnalyticsBenchError(f"family leakage across splits: {family}")


def _reference_outcome(reference: Mapping[str, Any]) -> str:
    kind = reference["kind"]
    if kind == "executed":
        return classify_execution(
            row_count=reference["row_count"],
            fact_count=len(reference["facts"]),
            truncated=reference["truncated"],
        ).outcome
    if kind == "evidence_incomplete":
        return cannot_verify(reason=reference["reason"], query_executed=True, row_count=1).outcome
    if kind == "planner_invalid":
        return cannot_verify(reason=reference["reason"]).outcome
    if kind == "source_unavailable":
        return degraded(reason=reference["reason"]).outcome
    return needs_clarification(reason=reference["reason"]).outcome


def _validate_safety_invariants(
    raw: Mapping[str, Any],
    *,
    plan: Any,
    compiled: Any,
    statement: Any,
    reference_outcome: str,
) -> None:
    assertions = set(raw["safety_assertions"])
    request = plan.data_request if plan is not None else None
    if "must_use_read_only_compiler" in assertions and (
        compiled is None
        or statement is None
        or not is_read_only_incident_query(statement.statement)
    ):
        raise AnalyticsBenchError(f"{raw['id']}: executable case did not compile")
    if "must_preserve_time_scope" in assertions:
        if (
            request is None
            or request.get("time_scope") is None
            or request.get("filters", {}).get("time_range") != request["time_scope"]
            or compiled is None
            or compiled.time_range is None
        ):
            raise AnalyticsBenchError(f"{raw['id']}: time scope was not preserved")
    if "must_preserve_failed_state" in assertions:
        if compiled is None or compiled.outcomes != ("FAILED",):
            raise AnalyticsBenchError(f"{raw['id']}: FAILED state was not preserved")
    if "must_enforce_exact_top_n" in assertions:
        if (
            compiled is None
            or compiled.tie_policy != "exact_limit"
            or compiled.limit < 1
            or not compiled.group_by
        ):
            raise AnalyticsBenchError(f"{raw['id']}: exact top-N was not preserved")
    if "must_include_ties_when_requested" in assertions:
        if (
            request is None
            or request.get("ranking") is None
            or compiled is None
            or compiled.tie_policy != "include_ties"
            or not compiled.group_by
        ):
            raise AnalyticsBenchError(f"{raw['id']}: requested ties were not preserved")
    if "must_require_clarification" in assertions and reference_outcome != "needs_clarification":
        raise AnalyticsBenchError(f"{raw['id']}: clarification assertion has the wrong outcome")
    if "must_not_route_to_analytics" in assertions and raw["expected_route"] == "analytics":
        raise AnalyticsBenchError(f"{raw['id']}: non-analytics assertion has the wrong route")


def _adapter_safety_safe(
    raw: Mapping[str, Any],
    *,
    result: SemanticAdapterResult,
    actual_plan: dict[str, Any] | None,
    grounded_negative_safe: bool,
) -> bool:
    assertions = set(raw["safety_assertions"])
    request = actual_plan.get("data_request") if actual_plan is not None else None
    if "must_use_read_only_compiler" in assertions:
        if not isinstance(request, dict):
            return False
        try:
            planner_value = dict(actual_plan) if actual_plan is not None else {}
            planner_value.pop("derived_route", None)
            planner_value.pop("time_scope_resolution", None)
            statement = compile_incident_query(
                compile_analytics_request(parse_semantic_plan(planner_value)),
                from_at=None,
                to_at=None,
            )
        except (SemanticPlanError, ValueError, TypeError):
            return False
        if not is_read_only_incident_query(statement.statement):
            return False
    if "must_preserve_time_scope" in assertions:
        expected_plan = _expected_plan(raw)
        if not _time_scope_match(expected_plan, actual_plan):
            return False
    if "must_preserve_failed_state" in assertions:
        filters = request.get("filters") if isinstance(request, dict) else None
        if not isinstance(filters, dict) or filters.get("outcome") != ["FAILED"]:
            return False
    if "must_enforce_exact_top_n" in assertions:
        if not _ranking_match(_expected_plan(raw), actual_plan):
            return False
        if not isinstance(request, dict) or request.get("tie_policy") != "exact_limit":
            return False
    if "must_include_ties_when_requested" in assertions:
        if not _ranking_match(_expected_plan(raw), actual_plan):
            return False
        if not isinstance(request, dict) or request.get("tie_policy") != "include_ties":
            return False
    if "must_not_make_negative_claim_without_verified_empty" in assertions:
        if result.outcome == "verified_empty":
            return grounded_negative_safe and (
                result.query_executed is True
                and result.evidence_complete is True
                and result.row_count == 0
            )
        if not grounded_negative_safe:
            return False
    if "must_require_clarification" in assertions:
        return result.outcome == "needs_clarification"
    if "must_not_route_to_analytics" in assertions:
        return result.route != "analytics"
    return True


def _execution_result_match(result: SemanticAdapterResult, reference: Mapping[str, Any]) -> bool:
    """Compare only a verified adapter execution to its immutable reference."""

    if (
        result.outcome != reference["outcome"]
        or result.query_executed is not reference["query_executed"]
        or result.evidence_complete is not reference["evidence_complete"]
        or result.row_count != reference["row_count"]
    ):
        return False
    return facts_hash(result.canonical_facts) == reference["facts_sha256"]


def _adapter_report(records: Sequence[Mapping[str, Any]], *, adapter_name: str) -> dict[str, Any]:
    attempted = [item for item in records if item["status"] == "attempted"]
    valid_plans = [bool(item["plan_valid"]) for item in attempted]
    plans = [bool(item["plan_match"]) for item in attempted]
    routes = [bool(item["route_match"]) for item in attempted]
    outcome_matches = [
        bool(item["outcome_match"]) for item in attempted if item["outcome_match"] is not None
    ]
    execution_matches = [
        bool(item["execution_result_match"])
        for item in attempted
        if item["execution_result_match"] is not None
    ]
    safety = [bool(item["safety_gate_passed"]) for item in attempted]
    negative = [bool(item["grounded_negative_safe"]) for item in attempted]
    latencies = [float(item["latency_seconds"]) for item in attempted]
    time_scope = [
        bool(item["time_scope_match"]) for item in attempted if item["time_scope_match"] is not None
    ]
    ranking = [bool(item["ranking_match"]) for item in attempted if item["ranking_match"] is not None]
    clarification = [
        bool(item["clarification_match"])
        for item in attempted
        if item["clarification_match"] is not None
    ]
    safety_critical = [item for item in attempted if item["safety_critical"]]
    safety_critical_pass = all(item["safety_gate_passed"] for item in safety_critical)
    return {
        "benchmark_version": BENCHMARK_VERSION,
        "mode": "adapter",
        "adapter": adapter_name,
        "records": list(records),
        "summary": {
            "case_count": len(records),
            "adapter_scored_case_count": len(attempted),
            "offline_contract_only_case_count": len(records) - len(attempted),
            "valid_semantic_plan_rate": statistics.fmean(valid_plans) if valid_plans else None,
            "canonical_plan_accuracy": statistics.fmean(plans) if plans else None,
            "route_accuracy": statistics.fmean(routes) if routes else None,
            "outcome_accuracy": statistics.fmean(outcome_matches) if outcome_matches else None,
            "outcome_accuracy_case_count": len(outcome_matches),
            "time_scope_accuracy": statistics.fmean(time_scope) if time_scope else None,
            "ranking_top_n_tie_accuracy": statistics.fmean(ranking) if ranking else None,
            "clarification_accuracy": statistics.fmean(clarification) if clarification else None,
            "safety_gate_rate": statistics.fmean(safety) if safety else None,
            "safety_critical_case_count": len(safety_critical),
            "safety_critical_gate_passed": safety_critical_pass,
            "grounded_negative_safety_rate": statistics.fmean(negative) if negative else None,
            "input_tokens": sum(int(item["input_tokens"]) for item in attempted),
            "output_tokens": sum(int(item["output_tokens"]) for item in attempted),
            "api_calls": sum(int(item["api_calls"]) for item in attempted),
            "correction_rate": statistics.fmean(
                int(item["correction_count"]) > 0 for item in attempted
            )
            if attempted
            else None,
            "latency_median_seconds": statistics.median(latencies) if latencies else None,
            "latency_p95_seconds": _p95(latencies),
            "execution_result_accuracy": (
                statistics.fmean(execution_matches) if execution_matches else None
            ),
            "execution_result_accuracy_case_count": len(execution_matches),
            "not_measured": [
                "Outcome and execution metrics exclude synthetic fixtures and any source without a matching approved immutable snapshot identity, integrity hash, timezone, and time boundary.",
                "Estimated provider cost is intentionally omitted until an approved pricing table is configured outside this benchmark.",
            ],
        },
    }


def _canonical_plan(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    # Service responses expose derived diagnostics alongside the original
    # planner payload.  They are not model-owned fields and must not make a
    # valid live plan look invalid to the benchmark.
    planner_value = dict(value)
    planner_value.pop("derived_route", None)
    planner_value.pop("time_scope_resolution", None)
    plan = parse_semantic_plan(planner_value)
    canonical = plan.to_dict()
    canonical.pop("derived_route", None)
    canonical.pop("time_scope_resolution", None)
    request = canonical.get("data_request")
    if isinstance(request, dict):
        for field in ("metrics", "dimensions", "detail_fields"):
            if isinstance(request.get(field), list):
                request[field] = sorted(request[field])
        filters = request.get("filters")
        if isinstance(filters, dict):
            for field in ("event_type", "outcome"):
                if isinstance(filters.get(field), list):
                    filters[field] = sorted(filters[field])
    guidance = canonical.get("guidance_request")
    if isinstance(guidance, dict) and isinstance(guidance.get("error_codes"), list):
        guidance["error_codes"] = sorted(guidance["error_codes"])
    canonical["inherited_fields"] = sorted(canonical.get("inherited_fields") or [])
    return canonical


def _try_canonical_plan(value: Any) -> dict[str, Any] | None:
    """Turn an invalid adapter payload into a failed score, never a runner crash."""

    try:
        return _canonical_plan(value)
    except (SemanticPlanError, TypeError, ValueError):
        return None


def _time_scope_match(expected_plan: Any, actual_plan: Mapping[str, Any] | None) -> bool | None:
    expected_request = expected_plan.data_request if expected_plan is not None else None
    if not isinstance(expected_request, dict) or expected_request.get("time_scope") is None:
        return None
    actual_request = actual_plan.get("data_request") if actual_plan is not None else None
    if not isinstance(actual_request, dict):
        return False
    return (
        actual_request.get("time_scope") == expected_request["time_scope"]
        and actual_request.get("filters", {}).get("time_range")
        == expected_request["filters"]["time_range"]
    )


def _ranking_match(expected_plan: Any, actual_plan: Mapping[str, Any] | None) -> bool | None:
    expected_request = expected_plan.data_request if expected_plan is not None else None
    if not isinstance(expected_request, dict) or expected_request.get("ranking") is None:
        return None
    actual_request = actual_plan.get("data_request") if actual_plan is not None else None
    if not isinstance(actual_request, dict):
        return False
    return all(
        actual_request.get(field) == expected_request[field]
        for field in ("ranking", "dimensions", "sort", "limit", "tie_policy")
    )


def _clarification_match(raw: Mapping[str, Any], result: SemanticAdapterResult) -> bool | None:
    if "must_require_clarification" not in set(raw["safety_assertions"]):
        return None
    return result.route == "clarification" and result.outcome == "needs_clarification"


def _plan_for_model(value: Mapping[str, Any]) -> dict[str, Any]:
    """Strip backend-derived diagnostics from an SFT target."""

    return {
        key: value.get(key)
        for key in (
            "version",
            "data_request",
            "guidance_request",
            "clarification",
            "conversation_action",
            "inherited_fields",
        )
    }


def _is_training_eligible(raw: Mapping[str, Any], *, include_seed: bool = False) -> bool:
    return raw["review"]["status"] == "approved" or (
        include_seed and raw["review"]["status"] == "seed"
    )


def _conversation_fingerprint(case: BenchmarkCase) -> str:
    return "\n".join(_normalize_text(item) for item in (*case.prior_questions, case.question))


def _normalize_text(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value.casefold())
    no_marks = "".join(char for char in decomposed if unicodedata.category(char) != "Mn").replace(
        "đ", "d"
    )
    return " ".join(re.sub(r"[^a-z0-9]+", " ", no_marks).split())


def _scan_records_for_sensitive_content(records: Iterable[Any], *, context: str) -> None:
    for value in _strings(records):
        if any(pattern.search(value) for pattern in _SENSITIVE_PATTERNS):
            raise AnalyticsBenchError(f"{context}: sensitive-looking value is not allowed")


def _reject_raw_sql(value: Any, *, location: str) -> None:
    for item in _strings(value):
        if _SQL_PATTERN.search(item):
            raise AnalyticsBenchError(f"{location}: raw SQL is not allowed in benchmark data")


def _strings(value: Any) -> Iterable[str]:
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield from _strings(key)
            yield from _strings(item)
    elif isinstance(value, list) or isinstance(value, tuple):
        for item in value:
            yield from _strings(item)


def _p95(values: Sequence[float]) -> float | None:
    if len(values) < 2:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, max(0, round(0.95 * (len(ordered) - 1))))]
