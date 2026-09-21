"""Model-neutral adapter boundary used by SHK-AnalyticsBench.

The benchmark evaluates semantic planning, never arbitrary SQL generation.  A
provider adapter returns only the public-safe, structured result required for
scoring; it deliberately does not expose prompts, answers, or credentials.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol, Sequence


@dataclass(frozen=True)
class SemanticAdapterResult:
    """One bounded planner execution suitable for benchmark scoring."""

    semantic_plan: dict[str, Any] | None
    route: str | None
    outcome: str | None
    query_executed: bool | None
    evidence_complete: bool | None
    row_count: int | None
    answer: str
    latency_seconds: float
    api_calls: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    correction_count: int = 0
    # These fields are populated only by a source that can identify the exact
    # immutable snapshot it read.  A mutable live database must leave them
    # unset, which keeps execution/outcome scoring unavailable by design.
    source_snapshot_id: str | None = None
    source_integrity_hash: str | None = None
    source_identity_hash: str | None = None
    source_timezone: str | None = None
    time_range_applied: dict[str, Any] | None = None
    canonical_facts: tuple[dict[str, Any], ...] = ()


class SemanticPlanAdapter(Protocol):
    """A swappable provider implementation for the same semantic contract."""

    name: str

    def evaluate(
        self,
        *,
        question: str,
        prior_questions: Sequence[str] = (),
        conversation_id: str,
    ) -> SemanticAdapterResult:
        """Run the supplied conversation without revealing provider internals."""
