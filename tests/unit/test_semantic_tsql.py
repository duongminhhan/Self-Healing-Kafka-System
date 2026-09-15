from datetime import datetime, timezone

import pytest

from self_healthy_kafka.config import AnalyticsChatConfig
from self_healthy_kafka.semantic.catalog import CATALOG_VERSION
from self_healthy_kafka.semantic.planner import SemanticPlanner, parse_semantic_plan
from self_healthy_kafka.semantic.tsql import compile_incident_query, is_read_only_incident_query
from self_healthy_kafka.webhook.analytics import parse_plan
from self_healthy_kafka.webhook.analytics_chat import AnalyticsChatService


def _query_plan(*, tie_policy="include_ties"):
    return parse_plan({
        "dataset": "connector_incidents",
        "metrics": [{"name": "failure_count", "aggregation": "count_distinct_incident"}],
        "group_by": ["job_name"],
        "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
        "order_by": [{"field": "failure_count", "direction": "desc"}],
        "limit": 3,
        "tie_policy": tie_policy,
    })


def _semantic_plan():
    return {
        "version": CATALOG_VERSION,
        "data_request": {
            "intent": "incidents", "subject": "root_connector", "metric": "incident_count",
            "ranking": "descending", "time_scope": None, "time_scope_origin": "unspecified",
            "metrics": ["incident_count"], "dimensions": ["root_connector"],
            "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
            "sort": {"metric": "incident_count", "direction": "desc"}, "limit": 3,
            "comparison": None, "detail_fields": [],
        },
        "guidance_request": {"needed": False, "purpose": None, "error_codes": [], "connector_class": None},
        "clarification": None, "conversation_action": "none", "inherited_fields": [],
    }


def test_compiler_uses_root_connector_and_parameterized_dense_rank_query():
    query = compile_incident_query(
        _query_plan(), from_at=None, to_at=None, row_limit=501
    )

    assert "[JobName] AS [root_connector_name]" in query.statement
    assert "DENSE_RANK()" in query.statement
    assert "MIN(CONVERT(nvarchar(36), [IncidentId])) AS [evidence_ids]" in query.statement
    assert "STRING_AGG" not in query.statement
    assert "WHERE [rank] <= ?" in query.statement
    assert query.parameter_values == ("HEALTH_FAILED_CONFIRMED", 501, 3)
    assert query.result_shape == ("root_connector_name", "incident_count", "rank", "tie_count")
    assert "DECLARE @event_type nvarchar(80)" in query.display_statement
    assert is_read_only_incident_query(query.statement)
    assert "EXEC " not in query.statement.upper()


def test_exact_top_n_uses_stable_row_number_only_when_explicitly_requested():
    query = compile_incident_query(
        _query_plan(tie_policy="exact_limit"), from_at=None, to_at=None, row_limit=501
    )

    assert "ROW_NUMBER()" in query.statement
    assert "WHERE [row_number] <= ?" in query.statement
    assert "[root_connector_name] ASC" in query.statement


def test_compiled_execution_is_used_for_root_ranking_and_keeps_boundary_ties():
    calls = []

    def execute(**kwargs):
        calls.append(kwargs)
        return [
            {"root_connector_name": "sample-oracle-orders", "incident_count": 4, "tie_count": 1, "rank": 1, "row_number": 1, "evidence_ids": "one;two;three;four"},
            {"root_connector_name": "sample-jdbc-orders", "incident_count": 3, "tie_count": 1, "rank": 2, "row_number": 2, "evidence_ids": "five;six;seven"},
            {"root_connector_name": "sample-jdbc-auth", "incident_count": 1, "tie_count": 2, "rank": 3, "row_number": 3, "evidence_ids": "eight"},
            {"root_connector_name": "sample-oracle-redo", "incident_count": 1, "tie_count": 2, "rank": 3, "row_number": 4, "evidence_ids": "nine"},
        ]

    planner = SemanticPlanner(lambda _messages, **_kwargs: _semantic_plan(), enforce_cues=False)
    service = AnalyticsChatService(
        AnalyticsChatConfig(enabled=True, timezone="UTC", hf_endpoint_url="", hf_token="", hf_model_id=""),
        incident_facts=lambda **_kwargs: pytest.fail("legacy fact fetch must not run"),
        execute_incident_query=execute,
        semantic_planner=planner,
        now=lambda: datetime(2026, 9, 15, tzinfo=timezone.utc),
    )

    result = service.ask("Câu hỏi ranking từ fixture")

    assert result["outcome"] == "verified_results"
    assert result["source"] == "deterministic_evidence_renderer"
    assert result["fallback_reason"] == "response_model_not_configured"
    assert [row["job_name"] for row in result["verified_result"]["rows"]] == [
        "sample-oracle-orders", "sample-jdbc-orders", "sample-jdbc-auth", "sample-oracle-redo",
    ]
    assert result["evidence"][2]["rank"] == 3
    assert result["evidence"][2]["tie_count"] == 2
    assert result["executed_query"]["executed"] is True
    assert result["executed_query"]["read_only"] is True
    assert "sample-oracle-orders" in result["answer"]
    assert len(calls) == 1
