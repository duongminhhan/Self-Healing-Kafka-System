from __future__ import annotations

import pytest

from self_healthy_kafka.semantic.catalog import CATALOG_VERSION
from self_healthy_kafka.semantic.evidence import render_evidence
from self_healthy_kafka.semantic.outcome import (
    cannot_verify,
    degraded,
    verified_empty,
    verified_results,
)
from self_healthy_kafka.semantic.planner import compile_analytics_request, parse_semantic_plan
from self_healthy_kafka.semantic.presentation import (
    PresentationFacts,
    SemanticResponseRenderer,
    build_presentation_facts,
)


def _plan(*, dimensions=None, filters=None, metrics=None, intent="incidents"):
    return parse_semantic_plan({
        "version": CATALOG_VERSION,
        "data_request": {
            "intent": intent,
            "metrics": metrics or ["incident_count"],
            "dimensions": dimensions or [],
            "filters": filters or {},
            "sort": {"metric": (metrics or ["incident_count"])[0], "direction": "desc"},
            "limit": 20 if intent != "top_error_signature" else 1,
            "comparison": None,
            "detail_fields": [],
        },
        "guidance_request": {
            "needed": False, "purpose": None, "error_codes": [], "connector_class": None,
        },
        "clarification": None,
        "conversation_action": "none",
        "inherited_fields": [],
    })


def _presentation(plan, outcome, *, rows=None, facts=None, evidence=None):
    return build_presentation_facts(
        plan,
        query_plan=compile_analytics_request(plan),
        outcome=outcome,
        source_rows=rows or [],
        facts=facts or [],
        evidence=evidence or [],
        from_at=None,
        to_at=None,
        timezone_name="Asia/Ho_Chi_Minh",
    )


def test_empty_renderer_uses_catalog_conditions_not_an_intent_sentence():
    plan = _plan(
        dimensions=["root_connector"],
        filters={"outcome": ["FAILED"], "time_range": {"kind": "relative", "value": "today"}},
        intent="failed_connectors",
    )

    presentation = _presentation(plan, verified_empty())
    answer = SemanticResponseRenderer().render_outcome(presentation)

    assert "intent" not in presentation.to_dict()
    assert presentation.subject == "root_connector"
    assert presentation.condition == {"field": "outcome", "operator": "equals", "value": "FAILED"}
    assert answer == (
        "Trong snapshot incident hiện tại, chưa ghi nhận root connector nào có trạng thái FAILED "
        "trong ngày hôm nay theo múi giờ Asia/Ho_Chi_Minh."
    )


@pytest.mark.parametrize(
    ("dimensions", "expected_subject"),
    [([], "incident"), (["error_code"], "error_code"), (["root_connector"], "root_connector")],
)
def test_empty_renderer_is_generic_across_catalog_subjects(dimensions, expected_subject):
    plan = _plan(dimensions=dimensions)
    presentation = _presentation(plan, verified_empty())

    answer = SemanticResponseRenderer().render_outcome(presentation)

    assert presentation.subject == expected_subject
    assert "chưa ghi nhận" in answer
    assert "snapshot incident hiện tại" in answer


def test_unspecified_time_scope_renders_all_snapshot_without_today():
    plan = _plan(dimensions=["root_connector"])
    presentation = _presentation(plan, verified_empty())

    answer = SemanticResponseRenderer().render_outcome(presentation)

    assert presentation.time_scope == "trên toàn bộ snapshot hiện có"
    assert presentation.time_scope_origin == "unspecified"
    assert "hôm nay" not in answer.lower()
    assert "toàn bộ snapshot" in answer


def test_non_empty_outcome_never_uses_the_empty_claim_renderer():
    plan = _plan(dimensions=["root_connector"])
    presentation = _presentation(
        plan,
        verified_results(row_count=1),
        rows=[{"incident_id": "one"}],
        facts=[{"job_name": "orders", "failure_count": 1}],
    )

    answer = SemanticResponseRenderer().render_outcome(presentation)

    assert "đã xác minh 1 root connector" in answer
    assert "chưa ghi nhận" not in answer


@pytest.mark.parametrize(
    "outcome",
    [
        cannot_verify(reason="incomplete_result_coverage", query_executed=True, row_count=1),
        degraded(reason="analytics_source_unavailable"),
    ],
)
def test_unverified_outcomes_do_not_make_negative_data_claims(outcome):
    presentation = _presentation(_plan(dimensions=["root_connector"]), outcome, rows=[{"incident_id": "one"}])

    answer = SemanticResponseRenderer().render_outcome(presentation).lower()

    assert "chưa ghi nhận" not in answer
    assert "không có connector" not in answer
    assert "xác minh" in answer


def test_clarification_uses_the_validated_clarification_fact_only():
    presentation = PresentationFacts(
        outcome="needs_clarification",
        subject="incident",
        conditions=(),
        metric=None,
        metric_value=None,
        result_count=0,
        rows=(),
        row_count=0,
        time_scope="trong phạm vi thời gian đã áp dụng",
        from_at=None,
        to_at=None,
        timezone="Asia/Ho_Chi_Minh",
        source="historical_incident_snapshot",
        snapshot_freshness=None,
        evidence_complete=False,
        query_executed=False,
        clarification_question="Bạn muốn xem incident hay healing log?",
        safe_failure_reason="material_ambiguity",
        sort_metric=None,
    )

    answer = SemanticResponseRenderer().render_outcome(presentation)

    assert answer == "Bạn muốn xem incident hay healing log?"


@pytest.mark.parametrize(
    ("name", "label", "value", "unit", "expected"),
    [
        ("failure_count", "số incident", 2, "incident", "số incident là 2"),
        ("recovery_rate_percent", "tỷ lệ phục hồi", 50.0, "%", "tỷ lệ phục hồi là 50.0"),
        ("average_recovery_minutes", "thời gian phục hồi trung bình", 35.0, "phút", "35.0 phút"),
        ("average_recovery_minutes", "thời gian phục hồi trung bình", None, "phút", "chưa thể tính"),
    ],
)
def test_evidence_renderer_formats_verified_metric_values(name, label, value, unit, expected):
    evidence = [{
        "entity": {},
        "metrics": [{"name": name, "label": label, "value": value, "unit": unit}],
        "details": {},
    }]
    presentation = PresentationFacts(
        outcome="verified_results",
        subject="incident",
        conditions=(),
        metric="incident_count",
        metric_value=value,
        result_count=1,
        rows=tuple(evidence),
        row_count=1,
        time_scope="trong ngày hôm nay theo múi giờ Asia/Ho_Chi_Minh",
        from_at=None,
        to_at=None,
        timezone="Asia/Ho_Chi_Minh",
        source="historical_incident_snapshot",
        snapshot_freshness="historical_incident_snapshot",
        evidence_complete=True,
        query_executed=True,
        clarification_question=None,
        safe_failure_reason=None,
        sort_metric="incident_count",
    )
    answer = render_evidence(presentation)

    assert expected in answer


def test_evidence_renderer_previews_verified_rows_and_summarizes_boundary_ties():
    def fact(name: str, value: int, rank: int, tie_count: int) -> dict:
        return {
            "rank": rank,
            "tie_count": tie_count,
            "entity": {"root connector": name},
            "metrics": [{"name": "failure_count", "label": "số incident", "value": value, "unit": "incident"}],
            "details": {},
        }

    presentation = PresentationFacts(
        outcome="verified_results", subject="root_connector", conditions=(), metric="incident_count",
        metric_value=None, result_count=5,
        rows=(
            fact("orders", 4, 1, 1), fact("payments", 3, 2, 1),
            fact("auth", 1, 3, 3), fact("oracle", 1, 3, 3), fact("s3", 1, 3, 3),
        ),
        row_count=5, time_scope="trên toàn bộ snapshot hiện có", from_at=None, to_at=None,
        timezone="Asia/Ho_Chi_Minh", source="historical_incident_snapshot", snapshot_freshness=None,
        evidence_complete=True, query_executed=True, clarification_question=None, safe_failure_reason=None,
        sort_metric="incident_count", ranking="descending", summary_item_limit=3, result_total_count=5,
        displayed_count=3, remaining_count=2, tie_policy="include_ties", boundary_tie_count=3,
        boundary_tie_truncated=True, has_more_verified_results=True, detail_accessible=True,
    )

    answer = render_evidence(presentation)

    assert answer.count("root connector") == 3
    assert "oracle" not in answer and "s3" not in answer
    assert "Có thêm 2 kết quả đồng hạng với vị trí thứ 3" in answer
    assert presentation.summary_metadata() == {
        "summary_item_limit": 3, "summary_detail_limit": 1, "result_total_count": 5,
        "displayed_count": 3, "remaining_count": 2, "ranking": "descending",
        "tie_policy": "include_ties", "boundary_tie_count": 3,
        "boundary_tie_truncated": True, "has_more_verified_results": True,
        "detail_accessible": True,
    }
