from __future__ import annotations

from self_healthy_kafka.semantic.outcome import classify_execution


def test_execution_outcome_keeps_source_kind_separate_from_unknown_freshness():
    result = classify_execution(row_count=1, fact_count=1, truncated=False)
    empty = classify_execution(row_count=0, fact_count=0, truncated=False)

    assert result.source_kind == "historical_incident_snapshot"
    assert result.snapshot_freshness == "unknown"
    assert empty.source_kind == "historical_incident_snapshot"
    assert empty.snapshot_freshness == "unknown"
