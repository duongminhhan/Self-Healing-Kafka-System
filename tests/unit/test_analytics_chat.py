from datetime import datetime, timezone

from self_healthy_kafka.config import AnalyticsChatConfig
from self_healthy_kafka.webhook.analytics_chat import AnalyticsChatService


def _fallback_config() -> AnalyticsChatConfig:
    """Keep unit tests independent from developer-shell HF environment variables."""
    return AnalyticsChatConfig(
        enabled=True,
        timezone="UTC",
        hf_endpoint_url="",
        hf_token="",
        hf_model_id="",
    )


class _Response:
    def raise_for_status(self):
        return None

    def json(self):
        return {"choices": [{"message": {"content": '{"dataset":"connector_incidents","metrics":[{"name":"failure_count","aggregation":"count_distinct_incident"}],"group_by":["job_name"],"filters":{"time_range":{"kind":"relative","value":"today"},"event_type":["HEALTH_FAILED_CONFIRMED"]},"order_by":[{"field":"failure_count","direction":"desc"}],"limit":5}'}}]}


class _Client:
    def __init__(self):
        self.request = None

    def post(self, url, **kwargs):
        self.request = (url, kwargs)
        return _Response()


def test_analytics_chat_uses_backend_time_range_and_parameterized_fact_callable():
    calls = []

    def facts(**kwargs):
        calls.append(kwargs)
        return [{
            "incident_id": "incident-1",
            "job_name": "TOPO-CLI-G043",
            "connector_name": "TOPO-CLI-G043.008",
            "failure_at": datetime(2026, 9, 3, 2, tzinfo=timezone.utc),
            "recovered_at": None,
            "final_outcome": "OPEN",
            "event_type": "HEALTH_FAILED_CONFIRMED",
            "severity": "ERROR",
            "error_code": "ORA-01013",
        }]

    service = AnalyticsChatService(
        _fallback_config(),
        incident_facts=facts,
        now=lambda: datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
    )

    result = service.ask("Connector nào gặp lỗi hôm nay?")

    assert calls[0]["from_at"] == datetime(2026, 9, 3, tzinfo=timezone.utc)
    assert calls[0]["to_at"] == datetime(2026, 9, 4, tzinfo=timezone.utc)
    assert calls[0]["event_type"] == "HEALTH_FAILED_CONFIRMED"
    assert calls[0]["limit"] == 100
    assert result["query_plan"]["dataset"] == "connector_incidents"
    assert result["evidence_ids"] == ["incident-1"]
    assert "TOPO-CLI-G043" in result["answer"]


def test_hugging_face_planner_receives_only_catalog_and_returns_validated_json():
    client = _Client()
    service = AnalyticsChatService(
        AnalyticsChatConfig(
            enabled=True, timezone="UTC", hf_endpoint_url="https://hf.example",
            hf_token="hf-private", hf_model_id="text-to-sql", hf_request_timeout_seconds=10,
        ),
        incident_facts=lambda **_kwargs: [], client=client,
        now=lambda: datetime(2026, 9, 3, tzinfo=timezone.utc),
    )

    result = service.ask("Connector nào lỗi hôm nay?")

    assert client.request[0] == "https://hf.example/v1/chat/completions"
    assert client.request[1]["headers"] == {"Authorization": "Bearer hf-private"}
    assert "credential" in client.request[1]["json"]["messages"][0]["content"]
    assert result["query_plan"]["dataset"] == "connector_incidents"


def test_trend_query_keeps_previous_period_rows_as_evidence():
    calls = []

    def facts(**kwargs):
        calls.append(kwargs)
        return [{"incident_id": f"incident-{len(calls)}", "job_name": "A"}]

    service = AnalyticsChatService(
        _fallback_config(), incident_facts=facts,
        now=lambda: datetime(2026, 9, 3, tzinfo=timezone.utc),
    )

    result = service.ask("ORA-01013 tăng hay giảm so với tuần trước?")

    assert len(calls) == 2
    assert calls[0]["error_code"] == "ORA-01013"
    assert result["query_plan"]["comparison"] == "previous_period"
    assert result["evidence_ids"] == ["incident-1", "incident-2"]
    assert result["sources"][1]["source"] == "vConnectorIncidentFacts.previous_period"


def test_ranking_answer_states_when_connectors_are_tied():
    service = AnalyticsChatService(
        _fallback_config(),
        incident_facts=lambda **_kwargs: [
            {"incident_id": "one", "job_name": "A"},
            {"incident_id": "two", "job_name": "B"},
        ],
        now=lambda: datetime(2026, 9, 3, tzinfo=timezone.utc),
    )

    result = service.ask("Connector nào gặp nhiều lỗi hơn?")

    assert "Không có connector nào gặp lỗi nhiều hơn" in result["answer"]
    assert "A, B đồng hạng" in result["answer"]
