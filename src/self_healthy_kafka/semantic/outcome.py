"""Typed execution outcomes for the analytics boundary.

An empty source result is not interchangeable with an unavailable source or an
incomplete aggregation.  This small module makes that distinction explicit at
the boundary where a natural-language answer is composed and later passed to
the browser.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

OutcomeKind = Literal[
    "verified_results",
    "verified_empty",
    "cannot_verify",
    "needs_clarification",
    "degraded",
]


@dataclass(frozen=True)
class AnalyticsOutcome:
    """Public-safe evidence state for a single analytics request.

    ``row_count`` is the number of source incident rows after the validated
    filters have been applied, not a rendered-table count.  A negative claim
    is permitted only through :meth:`permits_empty_claim`.
    """

    outcome: OutcomeKind
    query_executed: bool
    evidence_complete: bool
    row_count: int | None
    source_kind: str = "historical_incident_snapshot"
    snapshot_freshness: str | None = None
    reason: str | None = None

    @property
    def permits_empty_claim(self) -> bool:
        return (
            self.outcome == "verified_empty"
            and self.query_executed
            and self.evidence_complete
            and self.row_count == 0
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def verified_results(*, row_count: int, snapshot_freshness: str | None = None) -> AnalyticsOutcome:
    if row_count < 1:
        raise ValueError("verified results require at least one row")
    return AnalyticsOutcome(
        outcome="verified_results",
        query_executed=True,
        evidence_complete=True,
        row_count=row_count,
        snapshot_freshness=snapshot_freshness,
    )


def verified_empty(*, snapshot_freshness: str | None = None) -> AnalyticsOutcome:
    return AnalyticsOutcome(
        outcome="verified_empty",
        query_executed=True,
        evidence_complete=True,
        row_count=0,
        snapshot_freshness=snapshot_freshness,
    )


def cannot_verify(*, reason: str, query_executed: bool = False, row_count: int | None = None) -> AnalyticsOutcome:
    return AnalyticsOutcome(
        outcome="cannot_verify",
        query_executed=query_executed,
        evidence_complete=False,
        row_count=row_count,
        reason=reason,
    )


def needs_clarification(*, reason: str = "material_ambiguity") -> AnalyticsOutcome:
    return AnalyticsOutcome(
        outcome="needs_clarification",
        query_executed=False,
        evidence_complete=False,
        row_count=None,
        reason=reason,
    )


def degraded(*, reason: str, query_executed: bool = False, row_count: int | None = None) -> AnalyticsOutcome:
    return AnalyticsOutcome(
        outcome="degraded",
        query_executed=query_executed,
        evidence_complete=False,
        row_count=row_count,
        reason=reason,
    )


def classify_execution(*, row_count: int, fact_count: int, truncated: bool) -> AnalyticsOutcome:
    """Classify a completed source call without inspecting user phrasing."""

    if truncated:
        return cannot_verify(
            reason="analytics_evidence_truncated", query_executed=True, row_count=row_count
        )
    if row_count == 0:
        return verified_empty(snapshot_freshness="historical_incident_snapshot")
    if fact_count == 0:
        return cannot_verify(
            reason="incomplete_result_coverage", query_executed=True, row_count=row_count
        )
    return verified_results(row_count=row_count, snapshot_freshness="historical_incident_snapshot")
