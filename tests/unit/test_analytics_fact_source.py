from __future__ import annotations

import pytest

from self_healthy_kafka.semantic.fact_source import (
    classify_shadow_comparison,
    incident_fact_source,
    normalize_fact_source_mode,
)
from self_healthy_kafka.semantic.tsql import compile_incident_query, is_read_only_incident_query
from self_healthy_kafka.webhook.analytics import parse_plan


def _plan():
    return parse_plan({
        "dataset": "connector_incidents",
        "metrics": [{"name": "failure_count", "aggregation": "count_distinct_incident"}],
        "group_by": ["job_name"],
        "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
        "order_by": [{"field": "failure_count", "direction": "desc"}],
        "limit": 3,
    })


def test_compiler_accepts_only_resolved_allowlisted_fact_sources():
    legacy = incident_fact_source(mode="legacy", dbt_schema="analytics")
    dbt = incident_fact_source(mode="dbt", dbt_schema="analytics")

    legacy_query = compile_incident_query(_plan(), from_at=None, to_at=None, fact_source=legacy)
    dbt_query = compile_incident_query(_plan(), from_at=None, to_at=None, fact_source=dbt)

    assert "FROM [dbo].[vConnectorIncidentFacts]" in legacy_query.statement
    assert "FROM [analytics].[vSemanticConnectorIncidentFacts]" in dbt_query.statement
    assert is_read_only_incident_query(dbt_query.statement, fact_source=dbt)
    assert not is_read_only_incident_query(dbt_query.statement, fact_source=legacy)
    with pytest.raises(ValueError, match="FACT_SOURCE"):
        normalize_fact_source_mode("other")
    with pytest.raises(ValueError, match="identifier"):
        incident_fact_source(mode="dbt", dbt_schema="analytics]; DROP TABLE x;--")


@pytest.mark.parametrize(
    ("legacy", "dbt", "legacy_after", "consistent", "expected"),
    [
        ([{"root_connector_name": "orders", "incident_count": 2}], [{"root_connector_name": "orders", "incident_count": 2}], [{"root_connector_name": "orders", "incident_count": 2}], True, "match"),
        ([{"root_connector_name": "orders", "incident_count": 2}], [{"root_connector_name": "orders", "incident_count": 3}], [{"root_connector_name": "orders", "incident_count": 2}], True, "transformation_mismatch"),
        ([{"root_connector_name": "orders", "incident_count": 2}], [{"root_connector_name": "orders", "incident_count": 2}], [{"root_connector_name": "orders", "incident_count": 3}], True, "source_changed_during_comparison"),
        ([{"root_connector_name": "orders", "incident_count": 2}], [{"root_connector_name": "orders", "incident_count": 2}], [{"root_connector_name": "orders", "incident_count": 2}], False, "inconclusive"),
        (None, [{"root_connector_name": "orders", "incident_count": 2}], None, True, "legacy_query_failed"),
        ([{"root_connector_name": "orders", "incident_count": 2}], None, None, True, "dbt_query_failed"),
    ],
)
def test_shadow_comparison_categories_are_data_bounded(legacy, dbt, legacy_after, consistent, expected):
    assert classify_shadow_comparison(
        legacy_rows=legacy,
        dbt_rows=dbt,
        legacy_rows_after=legacy_after,
        result_shape=("root_connector_name", "incident_count", "rank", "tie_count"),
        snapshot_consistent=consistent,
    ) == expected
