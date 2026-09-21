"""Immutable, redacted execution references for SHK-AnalyticsBench.

The benchmark labels describe the intended semantic plan.  This module keeps
the separate evidence needed to score an execution result: an explicitly
captured, reviewed snapshot of the compiler's bounded query result.  It never
stores SQL, bound parameter values, raw source rows, logs, or credentials.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from self_healthy_kafka.semantic.outcome import classify_execution
from self_healthy_kafka.semantic.planner import compile_analytics_request, parse_semantic_plan
from self_healthy_kafka.semantic.tsql import compile_incident_query, is_read_only_incident_query
from self_healthy_kafka.webhook.analytics import QueryPlan, resolve_canonical_time_range

SNAPSHOT_FORMAT_VERSION = "2026-09-16.1"
SNAPSHOT_STATUSES = {"draft", "approved", "rejected"}
SNAPSHOT_SOURCE_CLASSIFICATIONS = {"mssql_historical_incident_snapshot"}
SNAPSHOT_REDACTION = "canonical_identifier_hash_v1"
_SNAPSHOT_ID = re.compile(r"^[a-z0-9][a-z0-9-]{2,100}$")
_DATE = re.compile(r"20\d{2}-\d{2}-\d{2}$")
_MAX_RESULT_ROWS = 500
_DIMENSIONS = {
    "job_name": "root_connector_name",
    "connector_name": "current_connector_name",
    "error_code": "error_code",
    "failure_code": "error_signature",
    "final_outcome": "final_outcome",
}
_METRICS = {
    "failure_count": "incident_count",
    "recovered_count": "recovered_incident_count",
    "open_count": "open_incident_count",
    "average_recovery_minutes": "average_recovery_minutes",
    "recovery_rate_percent": "recovery_rate_percent",
}
_SAFE_FACT_FIELDS = frozenset(
    {
        *_DIMENSIONS,
        *_METRICS,
        "recovery_rate_numerator",
        "recovery_rate_denominator",
        "rank",
        "tie_count",
        "row_number",
        "tie_truncated",
    }
)
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


class SnapshotReferenceError(ValueError):
    """A snapshot is malformed, unsafe, or cannot be used as a baseline."""


@dataclass(frozen=True)
class SnapshotReference:
    """One validated immutable snapshot and its safe case references."""

    raw: dict[str, Any]
    source_path: str

    @property
    def snapshot_id(self) -> str:
        return self.raw["snapshot_id"]

    @property
    def integrity_hash(self) -> str:
        return self.raw["integrity"]["content_sha256"]

    @property
    def approved(self) -> bool:
        return self.raw["status"] == "approved"


@dataclass(frozen=True)
class SnapshotMatch:
    """Whether an adapter result is comparable to a reviewed snapshot."""

    reference: Mapping[str, Any] | None
    reason: str

    @property
    def available(self) -> bool:
        return self.reference is not None


def canonical_json(value: Any) -> str:
    """Return the only serialisation used for integrity and fact digests."""

    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def content_hash(value: Mapping[str, Any]) -> str:
    """Hash a snapshot payload without its self-referential integrity block."""

    content = {key: item for key, item in value.items() if key != "integrity"}
    return hashlib.sha256(canonical_json(content).encode("utf-8")).hexdigest()


def plan_hash(value: Mapping[str, Any]) -> str:
    """Hash a canonical semantic plan without retaining model text or SQL."""

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def source_identity_hash(value: str) -> str:
    """Hash a logical source alias without storing its raw identifier.

    Snapshot operators provide a short stable alias rather than a DSN, host,
    or credential.  The alias exists solely to prevent a snapshot-aware
    adapter from claiming evidence from a different source.
    """

    if not isinstance(value, str) or not _SNAPSHOT_ID.fullmatch(value):
        raise SnapshotReferenceError("snapshot source identity is unsupported")
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def facts_hash(facts: Sequence[Mapping[str, Any]]) -> str:
    """Hash the canonical fact projection used for execution comparison."""

    return hashlib.sha256(canonical_json(list(facts)).encode("utf-8")).hexdigest()


def load_snapshot(path: str | Any) -> SnapshotReference:
    """Load and validate one snapshot manifest without contacting a source."""

    source_path = Path(path)
    source = str(source_path)
    try:
        raw = json.loads(source_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SnapshotReferenceError("snapshot manifest is unavailable or invalid JSON") from exc
    if not isinstance(raw, dict):
        raise SnapshotReferenceError("snapshot manifest must be an object")
    _validate_snapshot(raw)
    return SnapshotReference(raw=raw, source_path=source)


def load_snapshots(paths: Sequence[str | Any]) -> dict[str, SnapshotReference]:
    """Load a unique set of manifests and reject duplicate snapshot IDs."""

    snapshots: dict[str, SnapshotReference] = {}
    for path in paths:
        snapshot = load_snapshot(path)
        if snapshot.snapshot_id in snapshots:
            raise SnapshotReferenceError("duplicate snapshot id")
        snapshots[snapshot.snapshot_id] = snapshot
    return snapshots


def apply_snapshot_review(
    snapshot: SnapshotReference,
    *,
    status: str,
    reviewer: str,
    reviewed_at: str,
) -> dict[str, Any]:
    """Apply an explicit reviewer decision and recalculate immutable content hash."""

    if status not in {"approved", "rejected"}:
        raise SnapshotReferenceError("snapshot review status is unsupported")
    if not reviewer.strip() or not _DATE.fullmatch(reviewed_at):
        raise SnapshotReferenceError("snapshot review requires reviewer and date")
    updated = json.loads(canonical_json(snapshot.raw))
    updated["status"] = status
    updated["review"] = {"reviewer": reviewer.strip(), "reviewed_at": reviewed_at}
    updated["integrity"] = {
        "algorithm": "sha256",
        "content_sha256": content_hash(updated),
    }
    _validate_snapshot(updated)
    return updated


def match_snapshot_reference(
    *,
    case_id: str,
    reference_execution: Mapping[str, Any],
    expected_plan_hash: str | None,
    adapter_plan_hash: str | None,
    adapter_snapshot_id: str | None,
    adapter_integrity_hash: str | None,
    adapter_source_identity_hash: str | None,
    adapter_timezone: str | None,
    adapter_time_range: Mapping[str, Any] | None,
    snapshots: Mapping[str, SnapshotReference],
) -> SnapshotMatch:
    """Return a reference only when identity, approval, and scope all match."""

    if reference_execution.get("reference_classification") != "immutable_snapshot":
        return SnapshotMatch(None, "synthetic_fixture_reference")
    expected_snapshot_id = reference_execution.get("snapshot_id")
    if not isinstance(expected_snapshot_id, str) or not expected_snapshot_id:
        return SnapshotMatch(None, "reference_snapshot_id_missing")
    if adapter_snapshot_id != expected_snapshot_id:
        return SnapshotMatch(None, "source_snapshot_id_mismatch")
    snapshot = snapshots.get(expected_snapshot_id)
    if snapshot is None:
        return SnapshotMatch(None, "snapshot_manifest_not_supplied")
    if not snapshot.approved:
        return SnapshotMatch(None, "snapshot_not_approved")
    if reference_execution.get("snapshot_integrity_sha256") != snapshot.integrity_hash:
        return SnapshotMatch(None, "reference_snapshot_integrity_mismatch")
    if reference_execution.get("timezone") != snapshot.raw["timezone"]:
        return SnapshotMatch(None, "reference_snapshot_timezone_mismatch")
    if (
        reference_execution.get("source_identity_sha256")
        != snapshot.raw["source"]["identity_sha256"]
    ):
        return SnapshotMatch(None, "reference_snapshot_source_identity_mismatch")
    if adapter_integrity_hash != snapshot.integrity_hash:
        return SnapshotMatch(None, "snapshot_integrity_mismatch")
    if adapter_source_identity_hash != snapshot.raw["source"]["identity_sha256"]:
        return SnapshotMatch(None, "snapshot_source_identity_mismatch")
    if adapter_timezone != snapshot.raw["timezone"]:
        return SnapshotMatch(None, "snapshot_timezone_mismatch")
    entry = next((item for item in snapshot.raw["cases"] if item["case_id"] == case_id), None)
    if entry is None:
        return SnapshotMatch(None, "snapshot_case_missing")
    if expected_plan_hash is None or entry["semantic_plan_sha256"] != expected_plan_hash:
        return SnapshotMatch(None, "snapshot_plan_mismatch")
    if adapter_plan_hash != entry["semantic_plan_sha256"]:
        return SnapshotMatch(None, "source_plan_mismatch")
    if _canonical_time_range(reference_execution.get("time_range")) != entry["time_range"]:
        return SnapshotMatch(None, "reference_snapshot_time_boundary_mismatch")
    if adapter_time_range is None:
        return SnapshotMatch(None, "source_time_boundary_missing")
    try:
        actual_time_range = _canonical_time_range(adapter_time_range)
    except SnapshotReferenceError:
        return SnapshotMatch(None, "source_time_boundary_mismatch")
    if actual_time_range != entry["time_range"]:
        return SnapshotMatch(None, "snapshot_time_boundary_mismatch")
    return SnapshotMatch(entry["execution"], "approved_snapshot_match")


def build_snapshot_manifest(
    *,
    benchmark_version: str,
    snapshot_id: str,
    timezone_name: str,
    cases: Sequence[Any],
    execute: Callable[..., list[dict[str, Any]]],
    now: datetime,
    source_classification: str = "mssql_historical_incident_snapshot",
    source_identity: str,
) -> dict[str, Any]:
    """Capture reviewed plans through the fixed compiler into a draft manifest.

    ``execute`` is intentionally given the compiler's positional statement and
    parameters.  The function verifies the statement again before calling it;
    callers cannot use this capture route to run arbitrary SQL.
    """

    if not _SNAPSHOT_ID.fullmatch(snapshot_id):
        raise SnapshotReferenceError("snapshot_id is unsupported")
    if source_classification not in SNAPSHOT_SOURCE_CLASSIFICATIONS:
        raise SnapshotReferenceError("source classification is unsupported")
    if now.tzinfo is None:
        raise SnapshotReferenceError("capture time must include a timezone")
    if not cases:
        raise SnapshotReferenceError("snapshot capture requires at least one case")
    captured_cases: list[dict[str, Any]] = []
    for case in cases:
        raw = getattr(case, "raw", None)
        if not isinstance(raw, dict) or raw.get("review", {}).get("status") != "approved":
            raise SnapshotReferenceError("only separately reviewed cases may be captured")
        expected = raw.get("expected_plan")
        if not isinstance(expected, dict):
            raise SnapshotReferenceError(
                "snapshot capture requires a reviewed executable semantic plan"
            )
        plan = parse_semantic_plan(expected)
        if plan.data_request is None:
            raise SnapshotReferenceError("snapshot capture requires an analytics semantic plan")
        query_plan = compile_analytics_request(plan)
        time_range = resolve_canonical_time_range(
            query_plan.time_range, now=now, timezone_name=timezone_name
        )
        compiled = compile_incident_query(
            query_plan,
            from_at=time_range.from_at,
            to_at=time_range.to_at,
            row_limit=_MAX_RESULT_ROWS + 1,
        )
        if not is_read_only_incident_query(compiled.statement):
            raise SnapshotReferenceError(
                "capture compiler did not produce an approved read-only statement"
            )
        rows = execute(statement=compiled.statement, parameters=compiled.parameter_values)
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise SnapshotReferenceError("snapshot executor returned an unsupported result")
        if len(rows) > _MAX_RESULT_ROWS:
            raise SnapshotReferenceError("snapshot result is truncated and cannot be approved")
        facts = canonicalize_compiled_facts(rows, query_plan)
        outcome = classify_execution(row_count=len(rows), fact_count=len(facts), truncated=False)
        if outcome.outcome == "cannot_verify":
            raise SnapshotReferenceError("snapshot evidence is incomplete")
        execution = {
            "outcome": outcome.outcome,
            "query_executed": True,
            "evidence_complete": True,
            "row_count": len(rows),
            "facts": facts,
            "facts_sha256": facts_hash(facts),
        }
        captured_cases.append(
            {
                "case_id": raw["id"],
                "semantic_plan_sha256": plan_hash(_canonical_semantic_plan(plan.to_dict())),
                "time_range": _canonical_time_range(time_range.to_dict()),
                "execution": execution,
            }
        )
    manifest: dict[str, Any] = {
        "format_version": SNAPSHOT_FORMAT_VERSION,
        "snapshot_id": snapshot_id,
        "captured_at": now.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
        "timezone": timezone_name,
        "source": {
            "classification": source_classification,
            "identity_sha256": source_identity_hash(source_identity),
            "redaction": SNAPSHOT_REDACTION,
        },
        "benchmark_version": benchmark_version,
        "status": "draft",
        "review": {"reviewer": "", "reviewed_at": None},
        "cases": captured_cases,
    }
    manifest["integrity"] = {"algorithm": "sha256", "content_sha256": content_hash(manifest)}
    _validate_snapshot(manifest)
    return manifest


def canonicalize_compiled_facts(
    rows: Sequence[Mapping[str, Any]], plan: QueryPlan
) -> list[dict[str, Any]]:
    """Project only the compiler's approved aggregate aliases into facts."""

    facts: list[dict[str, Any]] = []
    for source in rows:
        required = [*_required_dimension_aliases(plan), *_required_metric_aliases(plan)]
        if plan.group_by:
            required.extend(["rank", "tie_count", "row_number"])
        if any(field not in source for field in required):
            raise SnapshotReferenceError("snapshot source did not honour the compiler result shape")
        fact: dict[str, Any] = {}
        for field in plan.group_by:
            fact[field] = _snapshot_dimension_value(field, source[_DIMENSIONS[field]])
        if not plan.group_by:
            fact["label"] = "all"
        for metric in plan.metrics:
            fact[metric.name] = _metric_value(source[_METRICS[metric.name]])
        for field in (
            "recovery_rate_numerator",
            "recovery_rate_denominator",
            "rank",
            "tie_count",
            "row_number",
        ):
            if field in source and source[field] is not None:
                fact[field] = _rank_value(source[field])
        facts.append(fact)
    _ensure_safe_snapshot_value(facts)
    return facts


def project_canonical_facts(rows: Any) -> tuple[dict[str, Any], ...]:
    """Drop non-baseline fields from an adapter result before comparison.

    In particular, evidence IDs and technical details never enter an adapter
    report.  An unsupported scalar produces no comparable facts rather than a
    best-effort coercion that could turn a data-contract change into a pass.
    """

    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        return ()
    facts: list[dict[str, Any]] = []
    for row in rows:
        fact: dict[str, Any] = {}
        for key in _SAFE_FACT_FIELDS | {"label"}:
            if key in row:
                if key in {
                    "job_name",
                    "connector_name",
                    "error_code",
                    "failure_code",
                    "final_outcome",
                }:
                    fact[key] = _snapshot_dimension_value(key, row[key])
                elif key in {
                    "rank",
                    "tie_count",
                    "row_number",
                    "recovery_rate_numerator",
                    "recovery_rate_denominator",
                }:
                    fact[key] = _rank_value(row[key])
                elif key == "tie_truncated":
                    if not isinstance(row[key], bool):
                        raise SnapshotReferenceError("snapshot tie marker is unsupported")
                    fact[key] = row[key]
                elif key == "label":
                    fact[key] = _snapshot_dimension_value(key, row[key])
                else:
                    fact[key] = _metric_value(row[key])
        if not fact:
            return ()
        facts.append(fact)
    _ensure_safe_snapshot_value(facts)
    return tuple(facts)


def _required_dimension_aliases(plan: QueryPlan) -> list[str]:
    return [_DIMENSIONS[field] for field in plan.group_by]


def _required_metric_aliases(plan: QueryPlan) -> list[str]:
    return [_METRICS[metric.name] for metric in plan.metrics]


def _safe_scalar(value: Any) -> str | int | float | bool | None:
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SnapshotReferenceError("snapshot fact contains a non-finite number")
        return value
    if hasattr(value, "as_tuple"):
        numeric = float(value)
        if not math.isfinite(numeric):
            raise SnapshotReferenceError("snapshot fact contains a non-finite decimal")
        return numeric
    if isinstance(value, str):
        if not value.strip() or len(value) > 255:
            raise SnapshotReferenceError("snapshot fact text is unsupported")
        return value.strip()
    raise SnapshotReferenceError("snapshot fact contains an unsupported value")


def _snapshot_dimension_value(field: str, value: Any) -> str | int | float | bool | None:
    """Hash connector identifiers while retaining states, ranks, and metrics."""

    safe = _safe_scalar(value)
    if not isinstance(safe, str):
        raise SnapshotReferenceError("snapshot dimension is unsupported")
    if field in {"job_name", "connector_name"} and isinstance(safe, str):
        return f"sha256:{hashlib.sha256(safe.encode('utf-8')).hexdigest()}"
    return safe


def _metric_value(value: Any) -> int | float | None:
    safe = _safe_scalar(value)
    if safe is None or (isinstance(safe, (int, float)) and not isinstance(safe, bool)):
        return safe
    raise SnapshotReferenceError("snapshot metric is unsupported")


def _rank_value(value: Any) -> int:
    safe = _safe_scalar(value)
    if isinstance(safe, int) and not isinstance(safe, bool):
        return safe
    raise SnapshotReferenceError("snapshot rank is unsupported")


def _validate_fact_value(field: str, value: Any) -> None:
    if field in {"job_name", "connector_name"}:
        if not isinstance(value, str) or not re.fullmatch(r"sha256:[a-f0-9]{64}", value):
            raise SnapshotReferenceError("snapshot connector identifier is not redacted")
        return
    if field in _METRICS:
        _metric_value(value)
        return
    if field in {
        "rank",
        "tie_count",
        "row_number",
        "recovery_rate_numerator",
        "recovery_rate_denominator",
    }:
        _rank_value(value)
        return
    if field == "tie_truncated":
        if not isinstance(value, bool):
            raise SnapshotReferenceError("snapshot tie marker is unsupported")
        return
    if field == "label":
        if value != "all":
            raise SnapshotReferenceError("snapshot aggregate label is unsupported")
        return
    _snapshot_dimension_value(field, value)


def _validate_snapshot(raw: Mapping[str, Any]) -> None:
    required = {
        "format_version",
        "snapshot_id",
        "captured_at",
        "timezone",
        "source",
        "benchmark_version",
        "status",
        "review",
        "cases",
        "integrity",
    }
    if set(raw) != required or raw.get("format_version") != SNAPSHOT_FORMAT_VERSION:
        raise SnapshotReferenceError("snapshot manifest fields or version are unsupported")
    if not isinstance(raw["snapshot_id"], str) or not _SNAPSHOT_ID.fullmatch(raw["snapshot_id"]):
        raise SnapshotReferenceError("snapshot_id is unsupported")
    _parse_timestamp(raw["captured_at"], "captured_at")
    if not isinstance(raw["timezone"], str) or not raw["timezone"].strip():
        raise SnapshotReferenceError("snapshot timezone is unsupported")
    source = raw["source"]
    if (
        not isinstance(source, dict)
        or set(source) != {"classification", "identity_sha256", "redaction"}
        or source.get("classification") not in SNAPSHOT_SOURCE_CLASSIFICATIONS
        or not isinstance(source.get("identity_sha256"), str)
        or not re.fullmatch(r"[a-f0-9]{64}", source["identity_sha256"])
        or source.get("redaction") != SNAPSHOT_REDACTION
    ):
        raise SnapshotReferenceError("snapshot source or redaction metadata is unsupported")
    if not isinstance(raw["benchmark_version"], str) or not raw["benchmark_version"]:
        raise SnapshotReferenceError("snapshot benchmark version is unsupported")
    if raw["status"] not in SNAPSHOT_STATUSES:
        raise SnapshotReferenceError("snapshot status is unsupported")
    review = raw["review"]
    if not isinstance(review, dict) or set(review) != {"reviewer", "reviewed_at"}:
        raise SnapshotReferenceError("snapshot review is unsupported")
    if not isinstance(review["reviewer"], str):
        raise SnapshotReferenceError("snapshot reviewer is unsupported")
    if review["reviewed_at"] is not None and (
        not isinstance(review["reviewed_at"], str) or not _DATE.fullmatch(review["reviewed_at"])
    ):
        raise SnapshotReferenceError("snapshot review date is unsupported")
    if raw["status"] == "approved" and (
        not review["reviewer"].strip() or review["reviewed_at"] is None
    ):
        raise SnapshotReferenceError("approved snapshot requires a dated reviewer")
    cases = raw["cases"]
    if not isinstance(cases, list) or not cases:
        raise SnapshotReferenceError("snapshot must contain case references")
    seen_case_ids: set[str] = set()
    for item in cases:
        _validate_snapshot_case(item)
        if item["case_id"] in seen_case_ids:
            raise SnapshotReferenceError("snapshot contains duplicate case references")
        seen_case_ids.add(item["case_id"])
    integrity = raw["integrity"]
    if (
        not isinstance(integrity, dict)
        or set(integrity) != {"algorithm", "content_sha256"}
        or integrity.get("algorithm") != "sha256"
        or not isinstance(integrity.get("content_sha256"), str)
        or not re.fullmatch(r"[a-f0-9]{64}", integrity["content_sha256"])
    ):
        raise SnapshotReferenceError("snapshot integrity metadata is unsupported")
    if integrity["content_sha256"] != content_hash(raw):
        raise SnapshotReferenceError("snapshot integrity hash mismatch")
    _ensure_safe_snapshot_value(raw)


def _validate_snapshot_case(value: Any) -> None:
    required = {"case_id", "semantic_plan_sha256", "time_range", "execution"}
    if not isinstance(value, dict) or set(value) != required:
        raise SnapshotReferenceError("snapshot case fields are unsupported")
    if not isinstance(value["case_id"], str) or not _SNAPSHOT_ID.fullmatch(value["case_id"]):
        raise SnapshotReferenceError("snapshot case id is unsupported")
    if not isinstance(value["semantic_plan_sha256"], str) or not re.fullmatch(
        r"[a-f0-9]{64}", value["semantic_plan_sha256"]
    ):
        raise SnapshotReferenceError("snapshot semantic plan hash is unsupported")
    _canonical_time_range(value["time_range"])
    execution = value["execution"]
    required_execution = {
        "outcome",
        "query_executed",
        "evidence_complete",
        "row_count",
        "facts",
        "facts_sha256",
    }
    if not isinstance(execution, dict) or set(execution) != required_execution:
        raise SnapshotReferenceError("snapshot execution fields are unsupported")
    if execution["outcome"] not in {"verified_results", "verified_empty"}:
        raise SnapshotReferenceError("snapshot execution outcome is unsupported")
    if execution["query_executed"] is not True or execution["evidence_complete"] is not True:
        raise SnapshotReferenceError("snapshot execution must be verified")
    if not isinstance(execution["row_count"], int) or execution["row_count"] < 0:
        raise SnapshotReferenceError("snapshot execution row count is unsupported")
    if not isinstance(execution["facts"], list) or not all(
        isinstance(item, dict) for item in execution["facts"]
    ):
        raise SnapshotReferenceError("snapshot facts are unsupported")
    for fact in execution["facts"]:
        if not set(fact) <= _SAFE_FACT_FIELDS | {"label"}:
            raise SnapshotReferenceError("snapshot fact field is not allowlisted")
        for field, item in fact.items():
            _validate_fact_value(field, item)
    if execution["outcome"] == "verified_empty" and (
        execution["row_count"] != 0 or execution["facts"]
    ):
        raise SnapshotReferenceError("empty snapshot reference is inconsistent")
    if execution["outcome"] == "verified_results" and (
        execution["row_count"] < 1 or not execution["facts"]
    ):
        raise SnapshotReferenceError("result snapshot reference is incomplete")
    if not isinstance(execution["facts_sha256"], str) or execution["facts_sha256"] != facts_hash(
        execution["facts"]
    ):
        raise SnapshotReferenceError("snapshot fact hash mismatch")


def _canonical_time_range(value: Mapping[str, Any] | None) -> dict[str, str | None]:
    if not isinstance(value, Mapping) or set(value) != {
        "kind",
        "from_at",
        "to_at",
        "timezone",
        "timestamp_field",
    }:
        raise SnapshotReferenceError("snapshot time range is unsupported")
    if not isinstance(value["kind"], str) or not isinstance(value["timezone"], str):
        raise SnapshotReferenceError("snapshot time range metadata is unsupported")
    if value["timestamp_field"] != "failure_at":
        raise SnapshotReferenceError("snapshot timestamp field is unsupported")
    for key in ("from_at", "to_at"):
        if value[key] is not None:
            _parse_timestamp(value[key], key)
    return {
        "kind": value["kind"],
        "from_at": value["from_at"],
        "to_at": value["to_at"],
        "timezone": value["timezone"],
        "timestamp_field": value["timestamp_field"],
    }


def _parse_timestamp(value: Any, label: str) -> None:
    if not isinstance(value, str):
        raise SnapshotReferenceError(f"snapshot {label} is unsupported")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SnapshotReferenceError(f"snapshot {label} is unsupported") from exc
    if parsed.tzinfo is None:
        raise SnapshotReferenceError(f"snapshot {label} must include a timezone")


def _canonical_semantic_plan(value: Mapping[str, Any]) -> dict[str, Any]:
    canonical = dict(value)
    canonical.pop("time_scope_resolution", None)
    canonical.pop("derived_route", None)
    request = canonical.get("data_request")
    if isinstance(request, dict):
        request = dict(request)
        canonical["data_request"] = request
        for field in ("metrics", "dimensions", "detail_fields"):
            if isinstance(request.get(field), list):
                request[field] = sorted(request[field])
        filters = request.get("filters")
        if isinstance(filters, dict):
            filters = dict(filters)
            request["filters"] = filters
            for field in ("event_type", "outcome"):
                if isinstance(filters.get(field), list):
                    filters[field] = sorted(filters[field])
    guidance = canonical.get("guidance_request")
    if isinstance(guidance, dict) and isinstance(guidance.get("error_codes"), list):
        guidance = dict(guidance)
        guidance["error_codes"] = sorted(guidance["error_codes"])
        canonical["guidance_request"] = guidance
    canonical["inherited_fields"] = sorted(canonical.get("inherited_fields") or [])
    return canonical


def _ensure_safe_snapshot_value(value: Any) -> None:
    for text in _strings(value):
        if _SQL_PATTERN.search(text):
            raise SnapshotReferenceError("snapshot cannot contain SQL text")
        # SHA-256 integrity values are opaque machine identifiers, not phone
        # numbers.  Scan all source-derived values, but do not reject a hash
        # merely because a decimal-looking substring happens to start with 0.
        if re.fullmatch(r"(?:sha256:)?[a-f0-9]{64}", text):
            continue
        if any(pattern.search(text) for pattern in _SENSITIVE_PATTERNS):
            raise SnapshotReferenceError("snapshot contains sensitive-looking content")


def _strings(value: Any):
    if isinstance(value, str):
        yield value
    elif isinstance(value, Mapping):
        for key, item in value.items():
            yield from _strings(key)
            yield from _strings(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            yield from _strings(item)
