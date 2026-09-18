"""Allowlisted incident-fact sources and bounded shadow comparison helpers.

The semantic planner never receives a physical database object.  This module
is the one place that maps an operator-selected source mode to a fixed query
target.  Shadow comparisons intentionally work on compiler result fields only
so diagnostics cannot contain raw error messages or credentials.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import json
import re
from typing import Any, Iterable

from self_healthy_kafka.storage.common import json_safe


_SCHEMA_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")
_FACT_SOURCE_MODES = frozenset({"legacy", "shadow", "dbt"})


@dataclass(frozen=True)
class IncidentFactSource:
    """One compiler-approved physical source and its safe evidence label."""

    key: str
    qualified_name: str
    evidence_source: str


def normalize_fact_source_mode(value: str) -> str:
    mode = value.strip().lower()
    if mode not in _FACT_SOURCE_MODES:
        raise ValueError("CHAT_ANALYTICS_FACT_SOURCE must be legacy, shadow, or dbt")
    return mode


def incident_fact_source(*, mode: str, dbt_schema: str) -> IncidentFactSource:
    """Resolve a feature-flag mode without accepting arbitrary table names."""

    normalized_mode = normalize_fact_source_mode(mode)
    if normalized_mode in {"legacy", "shadow"}:
        return IncidentFactSource(
            key="legacy",
            qualified_name="[dbo].[vConnectorIncidentFacts]",
            evidence_source="vConnectorIncidentFacts",
        )
    schema = dbt_schema.strip()
    if not _SCHEMA_NAME.fullmatch(schema):
        raise ValueError("CHAT_ANALYTICS_DBT_SCHEMA must be a SQL Server identifier")
    return IncidentFactSource(
        key="dbt",
        qualified_name=f"[{schema}].[vSemanticConnectorIncidentFacts]",
        evidence_source="vSemanticConnectorIncidentFacts",
    )


def is_incident_evidence_source(value: object) -> bool:
    """Recognise only source labels created by this allowlisted boundary."""

    return value in {"vConnectorIncidentFacts", "vSemanticConnectorIncidentFacts"}


def is_valid_incident_fact_source(value: object) -> bool:
    """Reject hand-crafted physical sources at the final repository boundary."""

    if not isinstance(value, IncidentFactSource):
        return False
    if value.key == "legacy":
        return value == incident_fact_source(mode="legacy", dbt_schema="analytics")
    if value.key != "dbt" or value.evidence_source != "vSemanticConnectorIncidentFacts":
        return False
    return bool(
        re.fullmatch(
            r"\[[A-Za-z_][A-Za-z0-9_]{0,127}\]\.\[vSemanticConnectorIncidentFacts\]",
            value.qualified_name,
        )
    )


def plan_fingerprint(plan: dict[str, Any]) -> str:
    """Return a compact, non-sensitive correlation key for audit events."""

    encoded = json.dumps(plan, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(encoded.encode("utf-8")).hexdigest()[:16]


def result_fingerprint(
    rows: Iterable[dict[str, Any]], *, result_shape: Iterable[str]
) -> str:
    """Hash only expected compiler result columns, never raw row payloads."""

    allowed = tuple(dict.fromkeys((*result_shape, "evidence_ids", "row_number")))
    normalized = [
        {field: json_safe(row.get(field)) for field in allowed}
        for row in rows
        if isinstance(row, dict)
    ]
    encoded = json.dumps(normalized, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(encoded.encode("utf-8")).hexdigest()[:16]


def classify_shadow_comparison(
    *,
    legacy_rows: list[dict[str, Any]] | None,
    dbt_rows: list[dict[str, Any]] | None,
    legacy_rows_after: list[dict[str, Any]] | None,
    result_shape: Iterable[str],
    snapshot_consistent: bool,
) -> str:
    """Classify shadow output without turning a moving source into a mismatch."""

    if legacy_rows is None:
        return "legacy_query_failed"
    if dbt_rows is None:
        return "dbt_query_failed"
    if legacy_rows_after is None:
        return "legacy_query_failed"
    before = result_fingerprint(legacy_rows, result_shape=result_shape)
    if before != result_fingerprint(
        legacy_rows_after, result_shape=result_shape
    ):
        return "source_changed_during_comparison"
    if not snapshot_consistent:
        return "inconclusive"
    return "match" if before == result_fingerprint(dbt_rows, result_shape=result_shape) else "transformation_mismatch"
