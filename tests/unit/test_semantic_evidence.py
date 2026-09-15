from __future__ import annotations

import json

from self_healthy_kafka.semantic.catalog import CATALOG_VERSION
from self_healthy_kafka.semantic.evidence import AnalyticsResponseComposer
from self_healthy_kafka.semantic.planner import parse_semantic_plan
from self_healthy_kafka.semantic.presentation import PresentationFacts
from self_healthy_kafka.webhook.analytics import parse_plan


def _plan():
    return parse_semantic_plan({
        "version": CATALOG_VERSION,
        "data_request": {
            "metrics": ["incident_count"], "dimensions": ["connector"], "filters": {},
            "sort": {"metric": "incident_count", "direction": "desc"}, "limit": 5,
            "comparison": None, "detail_fields": [],
        },
        "guidance_request": {"needed": False, "purpose": None, "error_codes": [], "connector_class": None},
        "clarification": None, "conversation_action": "none", "inherited_fields": [],
    })


def _query_plan():
    return parse_plan({
        "dataset": "connector_incidents",
        "metrics": [{"name": "failure_count", "aggregation": "count_distinct_incident"}],
        "group_by": ["connector_name"], "filters": {},
        "order_by": [{"field": "failure_count", "direction": "desc"}], "limit": 5,
        "comparison": None, "details": [],
    })


def _evidence():
    return [{
        "fact_id": "analytics:orders:2", "rank": 1, "entity": {"connector": "orders"},
        "metrics": [{"name": "failure_count", "label": "số incident", "value": 2,
                     "unit": "incident", "aggregation": "count_distinct_incident"}],
        "details": {}, "detail_values": {},
        "time_range": {"from_at": None, "to_at": None, "timestamp": "failure_at"},
        "status": None, "grain": "aggregated connector incident facts",
        "source": "vConnectorIncidentFacts", "evidence_ids": ["one", "two"], "complete": True,
        "semantic_catalog_version": CATALOG_VERSION,
    }]


def _presentation():
    return PresentationFacts(
        outcome="verified_results",
        subject="connector",
        conditions=(),
        metric="incident_count",
        metric_value=None,
        result_count=1,
        rows=tuple(_evidence()),
        row_count=1,
        time_scope="trong phạm vi thời gian đã áp dụng",
        from_at=None,
        to_at=None,
        timezone="UTC",
        source="historical_incident_snapshot",
        snapshot_freshness="historical_incident_snapshot",
        evidence_complete=True,
        query_executed=True,
        clarification_question=None,
        safe_failure_reason=None,
        sort_metric="incident_count",
    )


def _candidate(*, value=2, entity="orders", text="Connector orders có số incident là 2."):
    return {
        "answer": text,
        "claims": [{
            "fact_id": "analytics:orders:2", "entity": {"connector": entity},
            "metric": "failure_count", "value": value,
            "time_range": {"from_at": None, "to_at": None, "timestamp": "failure_at"},
            "status": None, "text": text,
        }],
    }


def test_analytics_response_retries_once_after_claim_scope_mismatch():
    values = iter([_candidate(entity="payments"), _candidate()])
    calls = []

    def generate(messages, **_kwargs):
        calls.append(messages)
        return next(values)

    answer, source, fallback_reason, attempts, claims = AnalyticsResponseComposer(generate).compose(
        presentation=_presentation(),
    )

    assert answer == "Connector orders có số incident là 2."
    assert source == "huggingface"
    assert fallback_reason is None
    assert attempts == 2
    assert claims[0]["fact_id"] == "analytics:orders:2"
    assert "validation_feedback" in calls[1][1]["content"]
    assert "question" not in json.loads(calls[0][1]["content"])


def test_analytics_response_rejects_value_that_does_not_match_the_fact():
    answer, source, fallback_reason, attempts, claims = AnalyticsResponseComposer(
        lambda _messages, **_kwargs: _candidate(value=3, text="Connector orders có số incident là 3.")
    ).compose(
        presentation=_presentation(),
    )

    assert source == "deterministic_evidence_renderer"
    assert fallback_reason == "grounding_failure:ValueError"
    assert attempts == 2
    assert "orders" in answer and "2" in answer
    assert claims[0]["metric"] == "failure_count"


def test_verified_results_cannot_add_an_unproven_negative_conclusion():
    answer, source, fallback_reason, attempts, _claims = AnalyticsResponseComposer(
        lambda _messages, **_kwargs: _candidate(
            text="Connector orders có số incident là 2. Không có connector nào failed."
        )
    ).compose(
        presentation=_presentation(),
    )

    assert source == "deterministic_evidence_renderer"
    assert fallback_reason == "grounding_failure:ValueError"
    assert attempts == 2
    assert "không có connector" not in answer.lower()
