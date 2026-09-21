from __future__ import annotations

import httpx
import pytest

from self_healthy_kafka.rag.models import Route
from self_healthy_kafka.semantic.catalog import CATALOG_VERSION
from self_healthy_kafka.semantic.planner import (
    SemanticPlanError,
    SemanticPlanner,
    compile_analytics_request,
    parse_semantic_plan,
    semantic_cue_contract,
)


def _value(**changes):
    value = {
        "version": CATALOG_VERSION,
        "data_request": {
            "metrics": ["incident_count"],
            "dimensions": ["connector"],
            "filters": {"time_range": {"kind": "relative", "value": "today"}},
            "sort": {"metric": "incident_count", "direction": "desc"},
            "limit": 5,
            "comparison": None,
            "detail_fields": [],
        },
        "guidance_request": {
            "needed": False,
            "purpose": None,
            "error_codes": [],
            "connector_class": None,
        },
        "clarification": None,
        "conversation_action": "none",
        "inherited_fields": [],
    }
    value.update(changes)
    return value


def test_backend_derives_route_from_data_and_guidance_requirements():
    analytics = parse_semantic_plan(_value())
    combined = parse_semantic_plan(_value(guidance_request={
        "needed": True, "purpose": "remediation", "error_codes": ["ORA-01013"], "connector_class": None,
    }))
    runbook = parse_semantic_plan(_value(data_request=None, guidance_request={
        "needed": True, "purpose": "meaning", "error_codes": ["ORA-01013"], "connector_class": "oracle",
    }))

    assert analytics.route is Route.ANALYTICS
    assert combined.route is Route.COMBINED
    assert runbook.route is Route.RUNBOOK


def test_route_cannot_be_selected_by_model_output():
    with pytest.raises(SemanticPlanError, match="unsupported field"):
        parse_semantic_plan(_value(route="combined"))


def test_guidance_requires_a_validated_purpose_and_no_hidden_retrieval_default():
    missing_purpose = _value(guidance_request={
        "needed": True, "purpose": None, "error_codes": ["ORA-01013"], "connector_class": "oracle",
    })
    stray_metadata = _value(guidance_request={
        "needed": False, "purpose": "meaning", "error_codes": [], "connector_class": None,
    })

    with pytest.raises(SemanticPlanError, match="purpose is required"):
        parse_semantic_plan(missing_purpose)
    with pytest.raises(SemanticPlanError, match="metadata requires guidance"):
        parse_semantic_plan(stray_metadata)


@pytest.mark.parametrize("bad", [
    _value(data_request={"sql": "DELETE FROM ConnectorHealingQueue"}),
    _value(data_request={**_value()["data_request"], "metrics": ["healing_log_count"]}),
    _value(data_request={**_value()["data_request"], "filters": {"connector": ""}}),
])
def test_plan_rejects_raw_sql_unsupported_metric_and_empty_filter(bad):
    with pytest.raises(SemanticPlanError):
        parse_semantic_plan(bad)


def test_compiler_maps_business_concepts_to_safe_dsl_without_database_objects():
    plan = parse_semantic_plan(_value(data_request={
        "metrics": ["incident_count", "average_recovery_minutes"],
        "dimensions": ["root_connector", "error"],
        "filters": {"connector": "orders", "error_code": "ORA-01013"},
        "sort": {"metric": "incident_count", "direction": "desc"},
        "limit": 10,
        "comparison": None,
        "detail_fields": ["error_message", "queue_status"],
    }))

    query = compile_analytics_request(plan)

    assert query.dataset == "connector_incidents"
    assert query.group_by == ("job_name", "failure_code")
    assert query.connector_name == "orders"
    assert query.error_code == "ORA-01013"
    assert query.details == ("error_message", "queue_status")
    assert "ConnectorHealing" not in str(query.to_dict())


def test_failed_connector_intent_enforces_the_catalogued_failed_state():
    plan = parse_semantic_plan(_value(data_request={
        **_value()["data_request"],
        "intent": "failed_connectors",
        "filters": {"outcome": ["FAILED"]},
    }))

    query = compile_analytics_request(plan)

    assert plan.data_request["intent"] == "failed_connectors"
    assert query.outcomes == ("FAILED",)
    assert query.event_types == ("HEALTH_FAILED_CONFIRMED",)


@pytest.mark.parametrize("intent,request_fields", [
    ("failed_connectors", {"metrics": ["incident_count"], "dimensions": ["error"], "filters": {}}),
    ("top_error_signature", {"metrics": ["incident_count"], "dimensions": ["connector"], "filters": {}}),
])
def test_intent_cannot_claim_a_business_shape_its_fields_do_not_support(intent, request_fields):
    value = _value(data_request={
        **_value()["data_request"], **request_fields, "intent": intent,
    })

    with pytest.raises(SemanticPlanError, match="requires"):
        parse_semantic_plan(value)


def test_planner_retries_once_with_local_validation_feedback():
    calls = []
    corrected = _value()
    corrected["data_request"]["filters"] = {}

    def generate(messages, **_kwargs):
        calls.append(messages)
        return {"version": CATALOG_VERSION, "data_request": {"sql": "select 1"}} if len(calls) == 1 else corrected

    plan, attempts = SemanticPlanner(generate).plan("Một câu hỏi bất kỳ")

    assert plan.route is Route.ANALYTICS
    assert attempts == 2
    assert "validation_feedback" in calls[1][1]["content"]


def test_planner_does_not_retry_service_failure_indefinitely():
    calls = 0

    def generate(_messages, **_kwargs):
        nonlocal calls
        calls += 1
        raise RuntimeError("provider unavailable")

    with pytest.raises(SemanticPlanError, match="service_failure"):
        SemanticPlanner(generate).plan("Một câu hỏi")
    assert calls == 1


def test_clear_context_and_clarification_are_non_executable():
    clear = parse_semantic_plan(_value(
        data_request=None,
        guidance_request={"needed": False, "purpose": None, "error_codes": [], "connector_class": None},
        conversation_action="clear_context",
    ))
    clarification = parse_semantic_plan(_value(
        data_request=None,
        clarification="Bạn muốn đếm incident hay healing log?",
    ))

    assert clear.route is None
    assert clarification.route is None


def test_planner_rejects_a_multi_outcome_filter_that_repository_cannot_execute_exactly():
    value = _value(data_request={
        **_value()["data_request"],
        "filters": {"outcome": ["RECOVERED", "FAILED"]},
    })

    with pytest.raises(SemanticPlanError, match="outcome"):
        parse_semantic_plan(value)


def test_inherited_fields_must_match_the_actual_prior_semantic_request():
    first = _value()
    changed = _value(inherited_fields=["filters"])
    changed["data_request"]["filters"] = {"time_range": {"kind": "relative", "value": "yesterday"}}

    with pytest.raises(SemanticPlanError, match="does not match prior context"):
        SemanticPlanner(lambda _messages, **_kwargs: changed).plan(
            "Hôm qua thì sao?", context={"previous_plan": first}
        )


def test_planner_classifies_authentication_failure_without_keyword_fallback():
    request = httpx.Request("POST", "https://hf.example/v1/chat/completions")
    response = httpx.Response(401, request=request)

    def generate(_messages, **_kwargs):
        raise httpx.HTTPStatusError("unauthorized", request=request, response=response)

    with pytest.raises(SemanticPlanError, match="semantic_planner_authentication"):
        SemanticPlanner(generate).plan("Một câu hỏi")


def _connector_ranking_plan(*, time_range=None, origin="unspecified", inherited=()):
    filters = {"event_type": ["HEALTH_FAILED_CONFIRMED"]}
    if time_range is not None:
        filters["time_range"] = {"kind": "relative", "value": time_range}
    return _value(data_request={
        "intent": "incidents",
        "subject": "connector",
        "metric": "incident_count",
        "ranking": "descending",
        "time_scope": filters.get("time_range"),
        "time_scope_origin": origin,
        "metrics": ["incident_count"],
        "dimensions": ["root_connector"],
        "filters": filters,
        "sort": {"metric": "incident_count", "direction": "desc"},
        "limit": 1,
        "comparison": None,
        "detail_fields": [],
    }, inherited_fields=list(inherited))


def test_cue_contract_is_accent_insensitive_and_prefers_explicit_connector_subject():
    cues = semantic_cue_contract("Liên kê tên conector có nhiều lỗi nhất")

    assert cues.subject == "root_connector"
    assert cues.metric == "incident_count"
    assert cues.ranking == "descending"
    assert cues.limit == 1
    assert cues.time_scope is None


def test_cue_contract_preserves_an_explicit_top_n_limit():
    cues = semantic_cue_contract("Top 3 connector gặp sự cố nhiều nhất")

    assert cues.subject == "root_connector"
    assert cues.ranking == "descending"
    assert cues.limit == 3


def test_top_n_defaults_to_exact_limit_but_explicit_ties_are_preserved():
    exact = _connector_ranking_plan()
    exact["data_request"]["limit"] = 3
    ties = _connector_ranking_plan()
    ties["data_request"]["limit"] = 3

    exact_plan, _ = SemanticPlanner(lambda _messages, **_kwargs: exact).plan(
        "Liệt kê top 3 connector gặp nhiều sự cố nhất"
    )
    ties_plan, _ = SemanticPlanner(lambda _messages, **_kwargs: ties).plan(
        "Liệt kê top 3 connector gặp nhiều sự cố nhất, bao gồm các connector đồng hạng"
    )

    assert exact_plan.data_request["tie_policy"] == "exact_limit"
    assert ties_plan.data_request["tie_policy"] == "include_ties"


def test_plan_normalizes_dimension_level_subject_aliases_to_the_neutral_contract():
    value = _value(data_request={
        "intent": "top_error_signature",
        "subject": "error_code",
        "metric": "incident_count",
        "ranking": "descending",
        "time_scope": None,
        "time_scope_origin": "unspecified",
        "metrics": ["incident_count"],
        "dimensions": ["error_code"],
        "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
        "sort": {"metric": "incident_count", "direction": "desc"},
        "limit": 1,
        "comparison": None,
        "detail_fields": [],
    })

    plan = parse_semantic_plan(value)

    assert plan.data_request["subject"] == "error_signature"


def test_planner_corrects_error_ranking_and_invented_today_for_connector_ranking():
    wrong = _value(data_request={
        "intent": "top_error_signature",
        "subject": "error_signature",
        "metric": "incident_count",
        "ranking": "descending",
        "time_scope": {"kind": "relative", "value": "today"},
        "time_scope_origin": "explicit",
        "metrics": ["incident_count"],
        "dimensions": ["error"],
        "filters": {
            "time_range": {"kind": "relative", "value": "today"},
            "event_type": ["HEALTH_FAILED_CONFIRMED"],
        },
        "sort": {"metric": "incident_count", "direction": "desc"},
        "limit": 1,
        "comparison": None,
        "detail_fields": [],
    })
    calls = []

    def generate(messages, **_kwargs):
        calls.append(messages)
        return wrong if len(calls) == 1 else _connector_ranking_plan()

    plan, attempts = SemanticPlanner(generate).plan("Liệt kê tên connector có nhiều lỗi nhất")

    assert attempts == 2
    assert plan.semantic_enforced is True
    assert plan.data_request["subject"] == "root_connector"
    assert plan.data_request["dimensions"] == ["root_connector"]
    assert plan.data_request["time_scope"] is None
    assert plan.data_request["time_scope_origin"] == "unspecified"
    assert "semantic subject mismatch" in calls[1][1]["content"]
    assert compile_analytics_request(plan, require_semantic_enforcement=True).time_range is None


def test_planner_rejects_an_added_time_filter_when_question_has_no_time_scope():
    with pytest.raises(SemanticPlanError, match="time scope mismatch"):
        SemanticPlanner(lambda _messages, **_kwargs: _connector_ranking_plan(
            time_range="today", origin="explicit"
        )).plan("Connector nào thường xuyên gặp sự cố nhất?")


def test_planner_preserves_explicit_error_code_time_scope_without_connector_conversion():
    value = _value(data_request={
        "intent": "top_error_signature",
        "subject": "error_signature",
        "metric": "incident_count",
        "ranking": "descending",
        "time_scope": {"kind": "relative", "value": "today"},
        "time_scope_origin": "explicit",
        "metrics": ["incident_count"],
        "dimensions": ["error_code"],
        "filters": {
            "time_range": {"kind": "relative", "value": "today"},
            "event_type": ["HEALTH_FAILED_CONFIRMED"],
        },
        "sort": {"metric": "incident_count", "direction": "desc"},
        "limit": 1,
        "comparison": None,
        "detail_fields": [],
    })

    plan, _ = SemanticPlanner(lambda _messages, **_kwargs: value).plan(
        "Mã lỗi nào xuất hiện nhiều nhất hôm nay?"
    )

    assert plan.data_request["subject"] == "error_signature"
    assert plan.data_request["time_scope"] == {"kind": "relative", "value": "today"}
    assert plan.data_request["time_scope_origin"] == "explicit"


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("Có connector nào FAILED hôm nay không?", {"kind": "relative", "value": "today"}),
        ("Có connector nào FAILED hôm qua không?", {"kind": "relative", "value": "yesterday"}),
        ("Có connector nào FAILED trong 3 ngày gần đây không?", {"kind": "relative", "value": "last_n_days", "days": 3}),
        ("Which connectors FAILED in the last 3 days?", {"kind": "relative", "value": "last_n_days", "days": 3}),
        ("Có connector nào FAILED vào 2026-09-03 không?", {"kind": "absolute_date", "value": "2026-09-03"}),
    ],
)
def test_cue_contract_extracts_canonical_time_scope(question, expected):
    assert semantic_cue_contract(question).time_scope == expected


def test_planner_canonicalizes_unsupported_model_time_shape_from_explicit_question_scope():
    raw = _connector_ranking_plan()
    raw["data_request"].update({
        "intent": "failed_connectors",
        "subject": "root_connector",
        "filters": {"outcome": ["FAILED"], "time_range": {
            "kind": "calendar", "from_at": "2026-09-15T00:00:00+07:00", "to_at": "2026-09-15T10:00:00+07:00",
        }},
        "time_scope": {"kind": "calendar", "from_at": "2026-09-15T00:00:00+07:00"},
        "time_scope_origin": "explicit",
    })

    plan, attempts = SemanticPlanner(lambda _messages, **_kwargs: raw).plan(
        "Có connector nào có trạng thái FAILED hôm nay không?"
    )

    assert attempts == 1
    assert plan.data_request["time_scope"] == {"kind": "relative", "value": "today"}
    assert plan.data_request["filters"]["time_range"] == {"kind": "relative", "value": "today"}
    assert plan.time_scope_resolution == {
        "model_time_scope": {"kind": "calendar"},
        "canonical_time_scope": {"kind": "relative", "value": "today"},
        "validation_reason": None,
    }


def test_planner_corrects_conflicting_semantic_time_scope_instead_of_overwriting_it():
    wrong = _connector_ranking_plan(time_range="yesterday", origin="explicit")
    wrong["data_request"].update({
        "intent": "failed_connectors", "subject": "root_connector",
        "filters": {"outcome": ["FAILED"], "time_range": {"kind": "relative", "value": "yesterday"}},
    })
    correct = _connector_ranking_plan(time_range="today", origin="explicit")
    correct["data_request"].update({
        "intent": "failed_connectors", "subject": "root_connector",
        "filters": {"outcome": ["FAILED"], "time_range": {"kind": "relative", "value": "today"}},
    })
    calls = []

    def generate(messages, **_kwargs):
        calls.append(messages)
        return wrong if len(calls) == 1 else correct

    plan, attempts = SemanticPlanner(generate).plan("Có connector nào FAILED hôm nay không?")

    assert attempts == 2
    assert plan.data_request["time_scope"] == {"kind": "relative", "value": "today"}
    assert "time scope mismatch" in calls[1][1]["content"]


def test_planner_allows_a_declared_time_scope_inherited_from_valid_context():
    prior = _connector_ranking_plan(time_range="today", origin="explicit")
    follow_up = _connector_ranking_plan(
        time_range="today", origin="inherited", inherited=("filters", "time_scope")
    )

    plan, _ = SemanticPlanner(lambda _messages, **_kwargs: follow_up).plan(
        "Connector nào thường xuyên gặp sự cố nhất?",
        context={"previous_plan": parse_semantic_plan(prior).to_dict()},
    )

    assert plan.semantic_enforced is True
    assert plan.data_request["time_scope_origin"] == "inherited"
