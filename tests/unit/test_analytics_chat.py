from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest

from self_healthy_kafka.config import AnalyticsChatConfig
from self_healthy_kafka.semantic.catalog import CATALOG_VERSION
from self_healthy_kafka.semantic.planner import SemanticPlanner, parse_semantic_plan
from self_healthy_kafka.storage.conversation import ConversationStoreError, RedisConversationStore
from self_healthy_kafka.webhook.analytics_chat import AnalyticsChatService


def _config():
    # Unit tests must not inherit live dev.env provider or source-selection state.
    return AnalyticsChatConfig(
        enabled=True,
        timezone="UTC",
        hf_endpoint_url="",
        hf_token="",
        hf_model_id="",
        jev_mode="off",
        fact_source="legacy",
    )


class _ImmediateShadowExecutor:
    """Execute background work deterministically without adding a test race."""

    def submit(self, callback, *args):
        callback(*args)

    def shutdown(self, **_kwargs):
        return None


class _HoldingShadowExecutor:
    """Accept work without running it, so the queue budget remains occupied."""

    def submit(self, callback, *args):
        self.callback = callback
        self.args = args

    def shutdown(self, **_kwargs):
        return None


def _plan(*, time="today", details=None, guidance=False, inherited=()):
    return {
        "version": CATALOG_VERSION,
        "data_request": {
            "metrics": ["incident_count"],
            "dimensions": ["connector", "error"],
            "filters": {"time_range": {"kind": "relative", "value": time}},
            "sort": {"metric": "incident_count", "direction": "desc"},
            "limit": 5,
            "comparison": None,
            "detail_fields": details or [],
        },
        "guidance_request": {
            "needed": guidance,
            "purpose": "remediation" if guidance else None,
            "error_codes": ["ORA-01013"] if guidance else [],
            "connector_class": "oracle" if guidance else None,
        },
        "clarification": None,
        "conversation_action": "none",
        "inherited_fields": list(inherited),
    }


def _planner(*values):
    planned = [parse_semantic_plan(value) for value in values]
    calls = []

    def generate(messages, **_kwargs):
        calls.append(messages)
        return planned.pop(0).to_dict() | {"derived_route": None}  # rejected if passed directly

    # Parsed values must be returned as raw model JSON, not the diagnostic view.
    source = [value for value in values]
    def raw_generate(messages, **_kwargs):
        calls.append(messages)
        return source.pop(0)

    planner = SemanticPlanner(raw_generate, enforce_cues=False)
    planner.calls = calls  # type: ignore[attr-defined]
    return planner


class _SharedRedisClient:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def setex(self, key, _ttl, value):
        self.values[key] = value

    def delete(self, key):
        self.values.pop(key, None)

    def close(self):
        return None


class _UnavailableConversationStore:
    def get(self, _conversation_id):
        raise ConversationStoreError("unavailable")

    def set(self, _conversation_id, _value):
        raise ConversationStoreError("unavailable")

    def delete(self, _conversation_id):
        raise ConversationStoreError("unavailable")

    def close(self):
        return None


class _WriteFailingConversationStore(_UnavailableConversationStore):
    def get(self, _conversation_id):
        return None


def test_shared_redis_context_survives_service_recreation_for_runbook_follow_up():
    client = _SharedRedisClient()
    first_store = RedisConversationStore("redis://unused", ttl_seconds=1800, client=client)
    first = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [{
            "incident_id": "incident-1",
            "job_name": "orders-root",
            "connector_name": "orders",
            "error_code": "ORA-01013",
            "error_message": "ORA-01013: user requested cancel",
        }],
        semantic_planner=_planner(_plan(details=["error_message"])),
        conversation_store=first_store,
    )
    first_result = first.ask(
        "Nội dung lỗi connector orders là gì?", conversation_id="shared-context"
    )
    first.close()

    guidance = {
        "version": CATALOG_VERSION,
        "data_request": None,
        "guidance_request": {
            "needed": True,
            "purpose": "remediation",
            "error_codes": [],
            "connector_class": None,
        },
        "clarification": None,
        "conversation_action": "none",
        "inherited_fields": [],
    }
    second = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: pytest.fail("runbook follow-up must not query DB"),
        semantic_planner=_planner(guidance),
        conversation_store=RedisConversationStore(
            "redis://unused", ttl_seconds=1800, client=client
        ),
    )
    follow_up = second.ask(
        "Lỗi vừa nêu fix như thế nào?", conversation_id="shared-context"
    )

    assert first_result["outcome"] == "verified_results"
    assert follow_up["route"] == "runbook"
    assert follow_up["semantic_plan"]["guidance_request"]["error_codes"] == ["ORA-01013"]
    assert follow_up["conversation"]["context_used"] is True
    second.close()


def test_unavailable_conversation_store_fails_closed_before_planner_or_database():
    planner_calls = []
    database_calls = []
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **kwargs: database_calls.append(kwargs) or [],
        semantic_planner=SemanticPlanner(
            lambda messages, **_kwargs: planner_calls.append(messages) or _plan(),
            enforce_cues=False,
        ),
        conversation_store=_UnavailableConversationStore(),
    )

    result = service.ask("Connector orders gặp lỗi gì?", conversation_id="unavailable")

    assert result["outcome"] == "degraded"
    assert result["reason"] == "conversation_store_unavailable"
    assert result["query_executed"] is False
    assert planner_calls == []
    assert database_calls == []


def test_conversation_write_failure_keeps_current_verified_answer():
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [{
            "incident_id": "incident-1",
            "connector_name": "orders",
            "error_code": "ORA-01013",
        }],
        semantic_planner=_planner(_plan()),
        conversation_store=_WriteFailingConversationStore(),
    )

    result = service.ask("Connector orders gặp lỗi gì?", conversation_id="write-failure")

    assert result["outcome"] == "verified_results"
    assert result["query_executed"] is True
    assert result["conversation"]["action"] == "context_not_saved"


def test_analytics_execution_uses_backend_time_and_parameterized_fact_callable():
    calls = []
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **kwargs: calls.append(kwargs) or [{
            "incident_id": "one", "job_name": "root", "connector_name": "orders",
            "failure_at": datetime(2026, 9, 3, 2, tzinfo=timezone.utc),
            "final_outcome": "OPEN", "event_type": "HEALTH_FAILED_CONFIRMED",
            "error_code": "ORA-01013",
        }],
        now=lambda: datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
        semantic_planner=_planner(_plan()),
    )

    result = service.ask("Cách hỏi tự nhiên không chứa schema")

    assert calls[0]["from_at"] == datetime(2026, 9, 3, tzinfo=timezone.utc)
    assert calls[0]["to_at"] == datetime(2026, 9, 3, 12, tzinfo=timezone.utc)
    assert calls[0]["limit"] == 1001
    assert result["route"] == "analytics"
    assert result["query_plan"]["group_by"] == ("connector_name", "failure_code")
    assert result["evidence"][0]["entity"] == {"connector": "orders", "lỗi": "ORA-01013"}
    assert "orders" in result["answer"]


def test_detail_field_uses_redacted_verified_message_not_question_template():
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [{
            "incident_id": "one", "job_name": "root", "connector_name": "orders",
            "error_code": "ORA-01013",
            "error_message": "ORA-01013: user requested cancel; password=must-not-leak",
            "failure_at": datetime(2026, 9, 3, tzinfo=timezone.utc),
        }],
        semantic_planner=_planner(_plan(details=["error_message"])),
    )

    result = service.ask("Hãy cho tôi chi tiết")

    details = result["evidence"][0]["details"]
    assert "password=must-not-leak" not in str(details)
    assert "[REDACTED]" in str(details)
    assert "Nội dung lỗi đã ghi nhận" in result["answer"]


def test_connector_detail_keeps_verified_error_and_severity_fields():
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [{
            "incident_id": "one", "job_name": "root", "connector_name": "orders",
            "error_code": "ORA-01013", "error_message": "ORA-01013: user requested cancel",
            "severity": "CRITICAL", "failure_at": datetime(2026, 9, 3, tzinfo=timezone.utc),
        }],
        semantic_planner=_planner(_plan(details=["error_message", "severity"])),
    )

    result = service.ask("Chi tiết lỗi của connector orders")

    assert result["outcome"] == "verified_results"
    assert result["evidence"][0]["entity"]["connector"] == "orders"
    assert result["evidence"][0]["detail_values"] == {
        "error_message": "ORA-01013: user requested cancel",
        "severity": "CRITICAL",
    }


@pytest.mark.parametrize(
    "rows",
    [
        [{"incident_id": "one", "connector_name": "orders", "error_code": "ORA-01013"}],
        [
            {"incident_id": "one", "connector_name": "orders", "error_code": "ORA-01013", "severity": "WARNING"},
            {"incident_id": "two", "connector_name": "orders", "error_code": "ORA-01013", "severity": "CRITICAL"},
        ],
    ],
)
def test_missing_or_mixed_severity_cannot_be_presented_as_verified(rows):
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: rows,
        semantic_planner=_planner(_plan(details=["severity"])),
    )

    result = service.ask("Mức độ nghiêm trọng đã ghi nhận của connector orders là gì?")

    assert result["outcome"] == "cannot_verify"
    assert result["verified_result"]["rows"] == []
    assert "WARNING" not in result["answer"]
    assert "CRITICAL" not in result["answer"]


def test_dbt_compiled_detail_is_redacted_before_evidence_and_ui_output():
    service = AnalyticsChatService(
        AnalyticsChatConfig(
            enabled=True,
            timezone="UTC",
            hf_endpoint_url="",
            hf_token="",
            hf_model_id="",
            jev_mode="off",
            fact_source="dbt",
        ),
        incident_facts=lambda **_kwargs: pytest.fail("dbt mode must not use legacy facts"),
        execute_incident_query=lambda **_kwargs: [{
            "current_connector_name": "orders",
            "error_signature": "ORA-01013",
            "incident_count": 1,
            "error_message": "ORA-01013: password=must-not-leak",
            "rank": 1,
            "tie_count": 1,
            "row_number": 1,
            "evidence_ids": "one",
        }],
        semantic_planner=_planner(_plan(details=["error_message"])),
    )

    result = service.ask("Chi tiết lỗi dbt")

    assert result["outcome"] == "verified_results"
    details = result["evidence"][0]["details"]
    assert "password=must-not-leak" not in str(details)
    assert "[REDACTED]" in str(details)
    assert result["executed_query"]["fact_source"] == "dbt"


def test_response_model_failure_uses_generic_evidence_renderer_without_question_specific_branch():
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [{"incident_id": "one", "connector_name": "orders", "error_code": "ORA-01013"}],
        semantic_planner=_planner(_plan()),
    )

    result = service.ask("Bất kỳ cách diễn đạt nào")

    assert result["source"] == "deterministic_evidence_renderer"
    assert result["fallback_reason"] == "response_model_not_configured"
    assert "số incident là 1" in result["answer"]


def test_planner_failure_does_not_query_data_or_fallback_to_keywords():
    service = AnalyticsChatService(_config(), incident_facts=lambda **_kwargs: (_ for _ in ()).throw(AssertionError()))

    result = service.ask("Hôm nay có connector nào lỗi không?")

    assert result["status"] == "cannot_verify"
    assert result["outcome"] == "cannot_verify"
    assert result["query_executed"] is False
    assert result["evidence_complete"] is False
    assert "không có connector" not in result["answer"].lower()
    assert result["reason"] == "semantic_planner_not_configured"
    assert result["query_plan"] is None


def test_vague_severity_without_verified_context_clarifies_before_planner_or_database():
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: pytest.fail("database must not be called"),
        semantic_planner=pytest.fail,
    )

    result = service.ask("Lỗi này có nghiêm trọng không?")

    assert result["outcome"] == "needs_clarification"
    assert result["route"] == "clarification"
    assert result["query_executed"] is False
    assert result["semantic_plan"] is None


def test_multi_turn_sends_bounded_semantic_context_and_reexecutes_new_request():
    first = _plan(time="today")
    second = _plan(time="yesterday", inherited=("metrics", "dimensions"))
    planner = _planner(first, second)
    calls = []
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **kwargs: calls.append(kwargs) or [{
            "incident_id": str(len(calls)),
            "job_name": "orders",
            "connector_name": "orders",
            "failure_at": datetime(2026, 9, 3, 2, tzinfo=timezone.utc),
            "final_outcome": "FAILED",
            "event_type": "HEALTH_FAILED_CONFIRMED",
            "error_code": "ORA-01013",
        }],
        now=lambda: datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
        semantic_planner=planner,
    )

    service.ask("Một câu hỏi đầu", conversation_id="a")
    result = service.ask("Một câu hỏi tiếp theo", conversation_id="a")

    assert len(calls) == 2
    assert calls[1]["from_at"] == datetime(2026, 9, 2, tzinfo=timezone.utc)
    assert result["conversation"] == {"id": "a", "context_used": True, "action": "semantic_plan"}
    assert "previous_plan" in planner.calls[1][1]["content"]


def test_follow_up_for_returned_root_connector_queries_error_without_cannot_verify():
    first = _failed_connectors_plan()
    first["data_request"]["time_scope"] = {"kind": "relative", "value": "today"}
    second = _plan(time="today", details=["error_message"])
    second["data_request"].update({
        "intent": "incidents",
        "subject": "incident",
        "metric": "incident_count",
        "ranking": None,
        "dimensions": ["error"],
        "filters": {
            "time_range": {"kind": "relative", "value": "today"},
            "connector": "test-connector-ora-01013-20260921",
        },
        "sort": {"metric": "incident_count", "direction": "desc"},
        "limit": 20,
    })
    second["data_request"]["time_scope_origin"] = "explicit"
    planned = iter([first, second])
    service = AnalyticsChatService(
        AnalyticsChatConfig(
            enabled=True,
            timezone="UTC",
            hf_endpoint_url="",
            hf_token="",
            hf_model_id="",
            jev_mode="off",
            fact_source="legacy",
        ),
        incident_facts=lambda **_kwargs: [{
            "incident_id": "one",
            "job_name": "test-connector-ora-01013-20260921",
            "connector_name": "test-connector-ora-01013-20260921.001",
            "error_code": "ORA-01013",
            "error_message": "ORA-01013: user requested cancel",
            "failure_at": datetime(2026, 9, 3, 2, tzinfo=timezone.utc),
            "final_outcome": "FAILED",
            "event_type": "HEALTH_FAILED_CONFIRMED",
        }],
        now=lambda: datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
        semantic_planner=SemanticPlanner(lambda _messages, **_kwargs: next(planned)),
    )

    service.ask("Hôm nay có connector nào có trạng thái FAILED không?", conversation_id="root-follow-up")
    result = service.ask(
        "Lỗi của connector root connector test-connector-ora-01013-20260921 ngày hôm nay là gì?",
        conversation_id="root-follow-up",
    )

    assert result["outcome"] == "verified_results"
    assert result["query_executed"] is True
    assert result["query_plan"]["connector_name"] == "test-connector-ora-01013-20260921"
    assert result["evidence"][0]["entity"]["lỗi"] == "ORA-01013"


def test_clear_context_is_a_model_plan_action_not_phrase_dispatch():
    clear = {
        "version": CATALOG_VERSION,
        "data_request": None,
        "guidance_request": {"needed": False, "purpose": None, "error_codes": [], "connector_class": None},
        "clarification": None,
        "conversation_action": "clear_context",
        "inherited_fields": [],
    }
    service = AnalyticsChatService(_config(), incident_facts=lambda **_kwargs: [], semantic_planner=_planner(clear))

    result = service.ask("Một câu lệnh do planner hiểu", conversation_id="a")

    assert result["conversation"]["action"] == "clear_context"
    assert result["route"] == "conversation"


def _failed_connectors_plan():
    plan = _plan()
    plan["data_request"].update({
        "intent": "failed_connectors",
        "subject": "root_connector",
        "dimensions": ["root_connector"],
        "filters": {
            "time_range": {"kind": "relative", "value": "today"},
            "outcome": ["FAILED"],
        },
    })
    return plan


def _weekly_failed_connectors_plan():
    plan = _failed_connectors_plan()
    plan["data_request"].update({
        "intent": "failed_connectors",
        "subject": "root_connector",
        "metric": "incident_count",
        "ranking": "descending",
        "time_scope": {"kind": "relative", "value": "this_week"},
        "time_scope_origin": "explicit",
        "dimensions": ["root_connector"],
        "filters": {
            "time_range": {"kind": "relative", "value": "this_week"},
            "outcome": ["FAILED"],
        },
    })
    return plan


def _yesterday_failed_connectors_plan():
    plan = _failed_connectors_plan()
    plan["data_request"].update({
        "metric": "incident_count",
        "ranking": "descending",
        "time_scope": {"kind": "relative", "value": "yesterday"},
        "time_scope_origin": "explicit",
        "filters": {
            "time_range": {"kind": "relative", "value": "yesterday"},
            "outcome": ["FAILED"],
        },
        "limit": 20,
    })
    return plan


def _connector_detail_plan(connector: str):
    plan = _plan(details=["error_message"])
    plan["data_request"].update({
        "intent": "incidents",
        "subject": "incident",
        "metric": "incident_count",
        "ranking": None,
        "time_scope": None,
        "time_scope_origin": "unspecified",
        "dimensions": ["connector", "error"],
        "filters": {"connector": connector},
        "limit": 20,
    })
    return plan


def _connector_error_code_plan(connector: str):
    plan = _plan()
    plan["data_request"].update({
        "intent": "incidents",
        "subject": "incident",
        "metric": "incident_count",
        "ranking": None,
        "time_scope": None,
        "time_scope_origin": "unspecified",
        "dimensions": ["connector", "error_code"],
        "filters": {"connector": connector},
        "limit": 20,
    })
    return plan


def test_weekly_failed_connector_population_executes_and_resolves_this_week():
    calls = []
    service = AnalyticsChatService(
        AnalyticsChatConfig(**{**_config().__dict__, "timezone": "Asia/Ho_Chi_Minh"}),
        incident_facts=lambda **kwargs: calls.append(kwargs) or [{
            "incident_id": "incident-1",
            "job_name": "orders",
            "connector_name": "orders",
            "failure_at": datetime(2026, 9, 22, 2, tzinfo=timezone.utc),
            "final_outcome": "FAILED",
            "event_type": "HEALTH_FAILED_CONFIRMED",
            "error_code": "ORA-01013",
        }],
        now=lambda: datetime(2026, 9, 24, 5, tzinfo=timezone.utc),
        semantic_planner=SemanticPlanner(lambda _messages, **_kwargs: _weekly_failed_connectors_plan()),
    )

    result = service.ask("tuần này có connector nào bị lỗi không?")

    assert result["outcome"] == "verified_results"
    assert result["query_executed"] is True
    assert result["verified_result"]["rows"]
    assert "orders" in str(result["verified_result"]["rows"])
    assert result["answer"].startswith("Có.")
    assert "orders: 1 incident" in result["answer"]
    assert "trong tuần này theo múi giờ Asia/Ho_Chi_Minh" in result["answer"]
    assert "Hạng" not in result["answer"]
    assert "root connector" not in result["answer"].lower()
    assert "snapshot incident hiện tại" not in result["answer"].lower()
    assert len(calls) == 1
    assert calls[0]["from_at"] == datetime(2026, 9, 21, tzinfo=timezone(timedelta(hours=7)))
    assert calls[0]["to_at"] == datetime(2026, 9, 28, tzinfo=timezone(timedelta(hours=7)))


def test_yesterday_failed_connector_list_is_unranked_and_time_bounded():
    calls = []
    service = AnalyticsChatService(
        AnalyticsChatConfig(**{**_config().__dict__, "timezone": "Asia/Ho_Chi_Minh"}),
        incident_facts=lambda **kwargs: calls.append(kwargs) or [
            {"incident_id": "one", "job_name": "orders", "connector_name": "orders.001"},
            {"incident_id": "two", "job_name": "payments", "connector_name": "payments.001"},
        ],
        now=lambda: datetime(2026, 9, 24, 5, tzinfo=timezone.utc),
        semantic_planner=SemanticPlanner(
            lambda _messages, **_kwargs: _yesterday_failed_connectors_plan()
        ),
    )

    result = service.ask("Liệt kê tên các connector gặp lỗi hôm qua")

    assert result["outcome"] == "verified_results"
    assert result["query_plan"]["ranking"] is None
    assert result["answer"].startswith("Có.")
    assert "orders: 1 incident" in result["answer"]
    assert "payments: 1 incident" in result["answer"]
    assert "trong ngày hôm qua theo múi giờ Asia/Ho_Chi_Minh" in result["answer"]
    assert "Hạng" not in result["answer"]
    assert calls[0]["from_at"] == datetime(2026, 9, 23, tzinfo=timezone(timedelta(hours=7)))
    assert calls[0]["to_at"] == datetime(2026, 9, 24, tzinfo=timezone(timedelta(hours=7)))


@pytest.mark.parametrize(
    ("rows", "expected_outcome"),
    [
        ([{
            "incident_id": "one",
            "connector_name": "test-connector-ora-01013-20260921",
            "error_code": "ORA-01013",
            "error_message": "ORA-01013: user requested cancel",
        }], "verified_results"),
        ([], "verified_empty"),
        ([{
            "incident_id": "one",
            "connector_name": "test-connector-ora-01013-20260921",
        }], "cannot_verify"),
    ],
)
def test_specific_connector_keeps_entity_across_verified_empty_and_unverified_outcomes(
    rows, expected_outcome
):
    connector = "test-connector-ora-01013-20260921"
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: rows,
        semantic_planner=SemanticPlanner(
            lambda _messages, **_kwargs: _connector_detail_plan(connector)
        ),
    )

    result = service.ask(f"Nội dung lỗi connector {connector} là gì?")

    assert result["outcome"] == expected_outcome
    assert result["query_plan"]["connector_name"] == connector
    assert connector in result["answer"]
    assert result["query_executed"] is True
    assert result["evidence_complete"] is (expected_outcome != "cannot_verify")


def test_exact_prose_error_question_returns_verified_connector_message():
    connector = "test-connector-ora-01013-20260921"
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [{
            "incident_id": "one",
            "connector_name": connector,
            "error_code": "ORA-01013",
            "error_message": "ORA-01013: user requested cancel of current operation",
        }],
        semantic_planner=SemanticPlanner(
            lambda _messages, **_kwargs: _connector_detail_plan(connector)
        ),
    )

    result = service.ask(f"nội dung lỗi của {connector} là gì")

    assert result["outcome"] == "verified_results"
    assert result["query_executed"] is True
    assert result["query_plan"]["connector_name"] == connector
    assert result["evidence"][0]["detail_values"]["error_message"] == (
        "ORA-01013: user requested cancel of current operation"
    )


def test_named_connector_error_code_question_executes_once_and_returns_verified_code():
    connector = "test-connector-ora-01013-20260921"
    calls = []

    def incident_facts(**kwargs):
        calls.append(kwargs)
        return [{
            "incident_id": "one",
            "connector_name": connector,
            "error_code": "ORA-01013",
        }]

    service = AnalyticsChatService(
        _config(),
        incident_facts=incident_facts,
        semantic_planner=SemanticPlanner(
            lambda _messages, **_kwargs: _connector_error_code_plan(connector)
        ),
    )

    result = service.ask(f"mã lỗi của connector {connector} là gì")

    assert len(calls) == 1
    assert result["outcome"] == "verified_results"
    assert result["query_executed"] is True
    assert result["evidence"][0]["entity"] == {
        "connector": connector,
        "mã lỗi": "ORA-01013",
    }


def test_weekly_failed_connector_population_returns_verified_empty_without_rows():
    service = AnalyticsChatService(
        AnalyticsChatConfig(**{**_config().__dict__, "timezone": "Asia/Ho_Chi_Minh"}),
        incident_facts=lambda **_kwargs: [],
        now=lambda: datetime(2026, 9, 24, 5, tzinfo=timezone.utc),
        semantic_planner=SemanticPlanner(lambda _messages, **_kwargs: _weekly_failed_connectors_plan()),
    )

    result = service.ask("tuần này có connector nào bị lỗi không?")

    assert result["outcome"] == "verified_empty"
    assert result["query_executed"] is True
    assert result["row_count"] == 0
    assert "chưa ghi nhận" in result["answer"].lower()


def test_verified_empty_is_the_only_outcome_that_may_state_no_failed_connectors():
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [],
        now=lambda: datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
        semantic_planner=_planner(_failed_connectors_plan()),
    )

    result = service.ask("Một cách nói bất kỳ cho trạng thái lỗi")

    assert result["outcome"] == "verified_empty"
    assert result["query_executed"] is True
    assert result["evidence_complete"] is True
    assert result["row_count"] == 0
    assert result["answer"].startswith("Không.")
    assert "chưa ghi nhận connector nào có trạng thái failed" in result["answer"].lower()
    assert "root connector" not in result["answer"].lower()
    assert result["time_range_applied"]["timezone"] == "UTC"


def test_failed_connector_today_executes_with_canonical_time_diagnostics():
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [{
            "incident_id": "one", "job_name": "orders", "connector_name": "orders",
            "failure_at": datetime(2026, 9, 3, 2, tzinfo=timezone.utc),
            "final_outcome": "FAILED", "event_type": "HEALTH_FAILED_CONFIRMED",
        }],
        now=lambda: datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
        semantic_planner=_planner(_failed_connectors_plan()),
    )

    result = service.ask("Có connector nào có trạng thái FAILED hôm nay không?")

    assert result["outcome"] == "verified_results"
    assert result["query_executed"] is True
    assert result["time_range_applied"] == {
        "kind": "relative", "from_at": "2026-09-03T00:00:00+00:00",
        "to_at": "2026-09-03T12:00:00+00:00", "timezone": "UTC", "timestamp_field": "failure_at",
    }
    assert result["diagnostics"]["time_range"]["canonical_time_scope"] == {"kind": "relative", "value": "today"}
    assert result["diagnostics"]["time_range"]["validation_reason"] is None


def test_incomplete_grouping_never_becomes_a_negative_claim():
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [{"incident_id": "one", "connector_name": "orders"}],
        semantic_planner=_planner(_plan()),
    )

    result = service.ask("Một cách nói bất kỳ cho lỗi phổ biến")

    assert result["outcome"] == "cannot_verify"
    assert result["query_executed"] is True
    assert result["evidence_complete"] is False
    assert result["row_count"] == 1
    assert "chưa thể xác minh" in result["answer"].lower()
    assert "không có connector" not in result["answer"].lower()


def test_source_failure_is_degraded_and_not_reported_as_no_data():
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("database unavailable")),
        semantic_planner=_planner(_failed_connectors_plan()),
    )

    result = service.ask("Một cách nói bất kỳ")

    assert result["outcome"] == "degraded"
    assert result["reason"] == "analytics_source_unavailable"
    assert result["query_executed"] is False
    assert result["evidence_complete"] is False
    assert "Nguồn dữ liệu incident hiện không truy cập được" in result["answer"]
    assert "chưa thể xác minh" in result["answer"].lower()
    assert "chưa ghi nhận" not in result["answer"].lower()


def test_recovery_rate_keeps_its_verified_numerator_and_denominator():
    plan = _plan(time="last_7_days")
    plan["data_request"].update({
        "intent": "recovery_rate",
        "metrics": ["recovery_rate"],
        "dimensions": [],
        "filters": {},
        "sort": {"metric": "recovery_rate", "direction": "desc"},
    })
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **_kwargs: [
            {"incident_id": "one", "final_outcome": "RECOVERED"},
            {"incident_id": "two", "final_outcome": "FAILED"},
        ],
        semantic_planner=_planner(plan),
    )

    result = service.ask("Tỷ lệ phục hồi trong tuần qua là bao nhiêu?")

    assert result["outcome"] == "verified_results"
    assert result["evidence"][0]["detail_values"] == {
        "recovery_rate_numerator": 1,
        "recovery_rate_denominator": 2,
    }
    assert result["verified_result"]["rows"] == [{
        "label": "Tất cả",
        "evidence_ids": "one; two",
        "recovery_rate_percent": 50.0,
        "recovery_rate_denominator": 2,
        "recovery_rate_numerator": 1,
    }]


def _compiled_ranking_plan():
    return {
        "version": CATALOG_VERSION,
        "data_request": {
            "intent": "incidents", "subject": "root_connector", "metric": "incident_count",
            "ranking": "descending", "time_scope": None, "time_scope_origin": "unspecified",
            "metrics": ["incident_count"], "dimensions": ["root_connector"],
            "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
            "sort": {"metric": "incident_count", "direction": "desc"}, "limit": 3,
            "comparison": None, "detail_fields": [], "tie_policy": "exact_limit",
        },
        "guidance_request": {"needed": False, "purpose": None, "error_codes": [], "connector_class": None},
        "clarification": None, "conversation_action": "none", "inherited_fields": [],
    }


def test_shadow_mode_returns_legacy_response_when_dbt_shadow_times_out(caplog):
    caplog.set_level(logging.INFO, logger="self_healthy_kafka.webhook.analytics_chat")
    calls = []

    def execute(**kwargs):
        calls.append(kwargs)
        if "vSemanticConnectorIncidentFacts" in kwargs["statement"]:
            raise TimeoutError("dbt query exceeded budget")
        return [{
            "root_connector_name": "orders", "incident_count": 1,
            "rank": 1, "tie_count": 1, "row_number": 1, "evidence_ids": "one",
        }]

    config = AnalyticsChatConfig(
        enabled=True, timezone="UTC", hf_endpoint_url="", hf_token="", hf_model_id="",
        jev_mode="off",
        fact_source="shadow", shadow_snapshot_consistent=True,
    )
    service = AnalyticsChatService(
        config,
        incident_facts=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("no fallback")),
        execute_incident_query=execute,
        semantic_planner=_planner(_compiled_ranking_plan()),
    )
    service._shadow_executor = _ImmediateShadowExecutor()  # type: ignore[assignment]

    result = service.ask("Xếp hạng incident")

    assert result["outcome"] == "verified_results"
    assert result["fact_source"] == "legacy"
    assert "[dbo].[vConnectorIncidentFacts]" in result["executed_query"]["statement"]
    assert any("vSemanticConnectorIncidentFacts" in item["statement"] for item in calls)
    assert any(
        getattr(record, "comparison_result", None) == "dbt_query_failed"
        for record in caplog.records
    )
    service.close()


def test_dbt_mode_never_silently_falls_back_to_the_legacy_source():
    calls = []
    config = AnalyticsChatConfig(
        enabled=True, timezone="UTC", hf_endpoint_url="", hf_token="", hf_model_id="",
        jev_mode="off", fact_source="dbt",
    )
    service = AnalyticsChatService(
        config,
        incident_facts=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("legacy fallback")),
        execute_incident_query=lambda **kwargs: calls.append(kwargs) or (_ for _ in ()).throw(RuntimeError("dbt unavailable")),
        semantic_planner=_planner(_compiled_ranking_plan()),
    )

    result = service.ask("Xếp hạng incident")

    assert result["outcome"] == "degraded"
    assert result["reason"] == "query_failed"
    assert "Truy vấn dữ liệu incident không hoàn tất" in result["answer"]
    assert len(calls) == 1
    assert "[analytics].[vSemanticConnectorIncidentFacts]" in calls[0]["statement"]
    assert calls[0]["fact_source"].key == "dbt"
    service.close()


def test_shadow_queue_saturation_never_blocks_the_legacy_response(caplog):
    caplog.set_level(logging.INFO, logger="self_healthy_kafka.webhook.analytics_chat")
    config = AnalyticsChatConfig(
        enabled=True, timezone="UTC", hf_endpoint_url="", hf_token="", hf_model_id="",
        jev_mode="off",
        fact_source="shadow", shadow_queue_size=1,
    )
    service = AnalyticsChatService(
        config,
        incident_facts=lambda **_kwargs: pytest.fail("legacy fact fetch must not run"),
        execute_incident_query=lambda **_kwargs: [{
            "root_connector_name": "orders", "incident_count": 1,
            "rank": 1, "tie_count": 1, "row_number": 1, "evidence_ids": "one",
        }],
        semantic_planner=_planner(_compiled_ranking_plan(), _compiled_ranking_plan()),
    )
    service._shadow_executor.shutdown(wait=False, cancel_futures=True)  # type: ignore[union-attr]
    service._shadow_executor = _HoldingShadowExecutor()  # type: ignore[assignment]

    first = service.ask("Xếp hạng incident lần một")
    second = service.ask("Xếp hạng incident lần hai")

    assert first["fact_source"] == second["fact_source"] == "legacy"
    assert any(
        getattr(record, "comparison_reason", None) == "shadow_queue_full"
        for record in caplog.records
    )
    service.close()
