from __future__ import annotations

from datetime import datetime, timezone

from self_healthy_kafka.config import AnalyticsChatConfig
from self_healthy_kafka.semantic.catalog import CATALOG_VERSION
from self_healthy_kafka.semantic.planner import SemanticPlanner, parse_semantic_plan
from self_healthy_kafka.webhook.analytics_chat import AnalyticsChatService


def _config():
    return AnalyticsChatConfig(enabled=True, timezone="UTC", hf_endpoint_url="", hf_token="", hf_model_id="")


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


def test_multi_turn_sends_bounded_semantic_context_and_reexecutes_new_request():
    first = _plan(time="today")
    second = _plan(time="yesterday", inherited=("metrics", "dimensions"))
    planner = _planner(first, second)
    calls = []
    service = AnalyticsChatService(
        _config(),
        incident_facts=lambda **kwargs: calls.append(kwargs) or [{"incident_id": str(len(calls)), "connector_name": "orders"}],
        now=lambda: datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
        semantic_planner=planner,
    )

    service.ask("Một câu hỏi đầu", conversation_id="a")
    result = service.ask("Một câu hỏi tiếp theo", conversation_id="a")

    assert len(calls) == 2
    assert calls[1]["from_at"] == datetime(2026, 9, 2, tzinfo=timezone.utc)
    assert result["conversation"] == {"id": "a", "context_used": True, "action": "semantic_plan"}
    assert "previous_plan" in planner.calls[1][1]["content"]


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
    assert "chưa ghi nhận root connector nào có trạng thái failed" in result["answer"].lower()
    assert result["time_range_applied"]["timezone"] == "UTC"


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
    assert result["query_executed"] is False
    assert result["evidence_complete"] is False
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
