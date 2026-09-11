from datetime import datetime, timezone

import pytest

from self_healthy_kafka.config import AnalyticsChatConfig, RagConfig
from self_healthy_kafka.webhook.analytics_chat import (
    AnalyticsChatService,
    ChatPlanningError,
)


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
    assert calls[0]["limit"] == 1001
    assert result["query_plan"]["dataset"] == "connector_incidents"
    assert result["evidence_ids"] == ["incident-1"]
    assert "TOPO-CLI-G043" in result["answer"]
    assert "evidence=" not in result["answer"]
    assert "incident-1" not in result["answer"]


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
    assert client.request[1]["json"]["temperature"] == 0
    assert client.request[1]["json"]["max_tokens"] == 700
    prompt = client.request[1]["json"]["messages"][0]["content"]
    assert "metrics must be an array of objects" in prompt
    assert "A question that also asks for causes or actions" in prompt
    assert result["query_plan"]["dataset"] == "connector_incidents"


def test_versioned_openai_compatible_endpoint_is_not_duplicated():
    from self_healthy_kafka.rag.answer_composer import chat_completions_url

    assert (
        chat_completions_url("https://openrouter.example/api/v1/")
        == "https://openrouter.example/api/v1/chat/completions"
    )


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


def test_structured_follow_up_reuses_verified_ranking_without_querying_again():
    calls = []

    def facts(**kwargs):
        calls.append(kwargs)
        return [
            {"incident_id": "a-1", "job_name": "connector-a"},
            {"incident_id": "a-2", "job_name": "connector-a"},
            {"incident_id": "b-1", "job_name": "connector-b"},
        ]

    service = AnalyticsChatService(_fallback_config(), incident_facts=facts)

    first = service.ask(
        "Connector nào thường xuyên gặp sự cố nhất?",
        conversation_id="conversation-a",
    )
    follow_up = service.ask(
        "Còn connector thứ hai thì sao?",
        conversation_id="conversation-a",
    )

    assert len(calls) == 1
    assert first["conversation"]["context_used"] is False
    assert follow_up["answer"].startswith("Ở vị trí thứ 2 là connector-b")
    assert follow_up["evidence_ids"] == ["b-1"]
    assert follow_up["source"] == "conversation_verified_result"
    assert follow_up["conversation"] == {
        "id": "conversation-a",
        "context_used": True,
        "action": "reuse_verified_ranking",
    }


def test_conversation_context_is_isolated_and_explicitly_resettable():
    service = AnalyticsChatService(
        _fallback_config(),
        incident_facts=lambda **_kwargs: [
            {"incident_id": "a", "job_name": "connector-a"},
            {"incident_id": "b", "job_name": "connector-b"},
        ],
    )
    service.ask("Connector nào gặp nhiều sự cố nhất?", conversation_id="conversation-a")

    isolated = service.ask("Còn connector thứ hai?", conversation_id="conversation-b")
    reset = service.ask("Xóa ngữ cảnh", conversation_id="conversation-a")
    after_reset = service.ask("Còn connector thứ hai?", conversation_id="conversation-a")

    assert isolated["status"] == "needs_clarification"
    assert isolated["reason"] == "missing_compatible_context"
    assert reset["conversation"]["action"] == "reset"
    assert after_reset["status"] == "needs_clarification"


def test_temporal_follow_up_preserves_verified_plan_and_changes_only_time_range():
    calls = []

    def facts(**kwargs):
        calls.append(kwargs)
        return [{"incident_id": str(len(calls)), "job_name": "connector-a"}]

    service = AnalyticsChatService(
        _fallback_config(),
        incident_facts=facts,
        now=lambda: datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
    )
    service.ask("Connector nào lỗi hôm nay?", conversation_id="conversation-a")
    follow_up = service.ask("Còn hôm qua thì sao?", conversation_id="conversation-a")

    assert calls[0]["from_at"] == datetime(2026, 9, 3, tzinfo=timezone.utc)
    assert calls[1]["from_at"] == datetime(2026, 9, 2, tzinfo=timezone.utc)
    assert calls[1]["to_at"] == datetime(2026, 9, 3, tzinfo=timezone.utc)
    assert follow_up["query_plan"]["time_range"] == {
        "kind": "relative",
        "value": "yesterday",
    }
    assert follow_up["conversation"]["action"] == "inherit_plan_with_time_override"


def test_most_common_error_returns_one_named_error_with_connector_and_message():
    rows = [
        {
            "incident_id": "timeout-1",
            "job_name": "sample-oracle-orders",
            "error_code": "ORA-01013",
            "error_message": "java.sql.SQLTimeoutException: ORA-01013: user requested cancel of current operation",
        },
        {
            "incident_id": "timeout-2",
            "job_name": "sample-oracle-orders",
            "error_code": "ORA-01013",
            "error_message": "java.sql.SQLTimeoutException: ORA-01013: user requested cancel of current operation",
        },
        {
            "incident_id": "login-1",
            "job_name": "sample-oracle-login",
            "error_code": "ORA-01017",
            "error_message": "java.sql.SQLException: ORA-01017: invalid username/password; logon denied",
        },
        {"incident_id": "unknown-1", "job_name": "unknown", "error_code": None},
    ]
    service = AnalyticsChatService(_fallback_config(), incident_facts=lambda **_kwargs: rows)

    result = service.ask("Lỗi phổ biến nhất là gì?")

    assert result["query_plan"]["group_by"] == ("failure_code",)
    assert result["query_plan"]["limit"] == 1
    assert result["answer"].startswith(
        "Lỗi phổ biến nhất là ORA-01013, xuất hiện trong 2 incident"
    )
    assert "sample-oracle-orders" in result["answer"]
    assert "user requested cancel of current operation" in result["answer"]
    assert "ORA-01017" not in result["answer"]
    assert "—" not in result["answer"]


def test_non_oracle_failure_is_classified_from_the_leaf_exception():
    service = AnalyticsChatService(
        _fallback_config(),
        incident_facts=lambda **_kwargs: [
            {
                "incident_id": "jdbc-1",
                "job_name": "sample-jdbc-orders",
                "error_message": (
                    "org.postgresql.util.PSQLException: connection failed\n"
                    "Caused by: java.net.ConnectException: Connection refused"
                ),
            }
        ],
    )

    result = service.ask("Lỗi phổ biến nhất là gì?")

    assert result["answer"].startswith("Lỗi phổ biến nhất là ConnectException")


def test_error_detail_uses_redacted_verified_log_message_without_calling_model():
    client = _Client()
    service = AnalyticsChatService(
        AnalyticsChatConfig(
            enabled=True,
            timezone="UTC",
            hf_endpoint_url="https://hf.example",
            hf_token="hf-private",
            hf_model_id="qwen-test",
        ),
        incident_facts=lambda **_kwargs: [
            {
                "incident_id": "ora-1",
                "job_name": "sample-oracle-orders",
                "error_code": "ORA-01013",
                "error_message": (
                    "java.sql.SQLTimeoutException: ORA-01013: user requested cancel of "
                    "current operation; password=do-not-expose"
                ),
            }
        ],
        client=client,
    )

    result = service.ask("Nội dung lỗi đầy đủ của mã lỗi ORA-01013 là gì?")

    assert client.request is None
    assert "user requested cancel of current operation" in result["answer"]
    assert "do-not-expose" not in result["answer"]
    assert "[REDACTED]" in result["answer"]
    assert "sample-oracle-orders" in result["answer"]


def test_generic_remediation_follow_up_inherits_the_verified_error_code():
    class Workflow:
        def __init__(self):
            self.questions = []

        def ask(self, question, *, analytics_ask):
            self.questions.append(question)
            if len(self.questions) == 1:
                result = analytics_ask(question)
                return {**result, "route": "analytics", "source": "analytics", "citations": []}
            return {
                "answer": "Hãy kiểm tra nguyên nhân hủy thao tác.",
                "route": "combined",
                "source": "runbook",
                "citations": [],
            }

    workflow = Workflow()
    service = AnalyticsChatService(
        _fallback_config(),
        incident_facts=lambda **_kwargs: [
            {
                "incident_id": "ora-1",
                "job_name": "sample-oracle-orders",
                "error_code": "ORA-01013",
                "error_message": "ORA-01013: user requested cancel of current operation",
            }
        ],
        rag_workflow=workflow,
    )
    service.ask("Lỗi phổ biến nhất là gì?", conversation_id="context-1")

    result = service.ask("Cách xử lý là gì?", conversation_id="context-1")

    assert "ORA-01013" in workflow.questions[-1]
    assert result["answer"] == "Hãy kiểm tra nguyên nhân hủy thao tác."
    assert result["conversation"] == {
        "id": "context-1",
        "context_used": True,
        "action": "inherit_verified_error_for_remediation",
    }


def test_previous_week_fallback_uses_last_week_not_this_week():
    service = AnalyticsChatService(_fallback_config(), incident_facts=lambda **_kwargs: [])

    result = service.ask("Có bao nhiêu incident tuần trước?")

    assert result["query_plan"]["time_range"] == {
        "kind": "relative",
        "value": "last_week",
    }


def test_null_metric_is_sorted_after_valid_values():
    service = AnalyticsChatService(
        _fallback_config(),
        incident_facts=lambda **_kwargs: [
            {"incident_id": "open", "job_name": "missing", "final_outcome": "OPEN"},
            {
                "incident_id": "done",
                "job_name": "valid",
                "final_outcome": "RECOVERED",
                "queue_status": "COMPLETED",
                "failure_at": datetime(2026, 9, 3, tzinfo=timezone.utc),
                "recovered_at": datetime(2026, 9, 3, 0, 10, tzinfo=timezone.utc),
            },
        ],
    )

    result = service.ask("Connector nào có thời gian phục hồi trung bình thấp nhất?")

    assert "valid" in result["answer"]
    assert not result["answer"].startswith("missing")


def test_invalid_model_contract_is_an_internal_planning_error():
    class InvalidResponse(_Response):
        def json(self):
            return {"choices": [{"message": {"content": "not-json"}}]}

    class InvalidClient(_Client):
        def post(self, url, **kwargs):
            self.request = (url, kwargs)
            return InvalidResponse()

    service = AnalyticsChatService(
        AnalyticsChatConfig(
            enabled=True,
            timezone="UTC",
            hf_endpoint_url="https://hf.example",
            hf_token="hf-private",
            hf_model_id="qwen-test",
        ),
        incident_facts=lambda **_kwargs: [],
        client=InvalidClient(),
    )

    with pytest.raises(ChatPlanningError):
        service.ask("Connector nào gặp sự cố nhiều nhất?")


def test_rag_can_own_existing_chat_endpoint_without_enabling_legacy_analytics_flag():
    class Workflow:
        def ask(self, question, *, analytics_ask):
            return {"answer": question, "route": "runbook", "citations": []}

    service = AnalyticsChatService(
        AnalyticsChatConfig(
            enabled=False,
            timezone="UTC",
            hf_endpoint_url="https://hf.example",
            hf_token="hf-test",
            hf_model_id="qwen-test",
        ),
        incident_facts=lambda **_kwargs: [],
        rag_config=RagConfig(
            enabled=True,
            qdrant_url="https://qdrant.example",
            qdrant_api_key="qdrant-test",
            embedding_model="configured-cluster-model",
        ),
        rag_workflow=Workflow(),
    )

    assert service.enabled is True
    assert service.ask("Hướng xử lý?")["route"] == "runbook"


def test_close_stops_rag_workflow_but_does_not_close_injected_client():
    class Workflow:
        def __init__(self):
            self.close_calls = 0

        def close(self):
            self.close_calls += 1

    class Client:
        def __init__(self):
            self.close_calls = 0

        def close(self):
            self.close_calls += 1

    workflow = Workflow()
    client = Client()
    service = AnalyticsChatService(
        _fallback_config(),
        incident_facts=lambda **_kwargs: [],
        client=client,
        rag_workflow=workflow,
    )

    service.close()
    service.close()

    assert workflow.close_calls == 1
    assert client.close_calls == 0


def test_close_releases_internally_owned_http_client_without_rag():
    service = AnalyticsChatService(
        _fallback_config(),
        incident_facts=lambda **_kwargs: [],
    )
    client = service._client

    service.close()

    assert client.is_closed is True
