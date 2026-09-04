from datetime import datetime, timezone

import pytest

from self_healthy_kafka.webhook.analytics import parse_plan, resolve_time_range


def _plan(**changes):
    value = {
        "dataset": "connector_incidents",
        "metrics": [{"name": "failure_count", "aggregation": "count_distinct_incident"}],
        "group_by": ["job_name"],
        "filters": {"time_range": {"kind": "relative", "value": "today"}},
        "order_by": [{"field": "failure_count", "direction": "desc"}],
        "limit": 5,
    }
    value.update(changes)
    return value


def test_query_plan_allows_only_semantic_analytics_fields():
    plan = parse_plan(_plan())

    assert plan.dataset == "connector_incidents"
    assert plan.metrics[0].name == "failure_count"
    assert plan.group_by == ("job_name",)
    assert plan.limit == 5


@pytest.mark.parametrize(
    "change, message",
    [
        ({"dataset": "sys.tables"}, "dataset"),
        ({"group_by": ["message"]}, "group_by"),
        ({"metrics": [{"name": "raw_sql"}]}, "metric"),
        ({"metrics": [{"name": "failure_count", "aggregation": "count(*)"}]}, "aggregation"),
        ({"limit": 101}, "limit"),
        ({"filters": {"sql": "DELETE FROM ConnectorHealingLogs"}}, "filters"),
    ],
)
def test_query_plan_rejects_database_objects_and_unbounded_input(change, message):
    with pytest.raises(ValueError, match=message):
        parse_plan(_plan(**change))


def test_today_is_resolved_by_backend_timezone_not_a_keyword_search():
    plan = parse_plan(_plan())

    from_at, to_at = resolve_time_range(
        plan.time_range,
        now=datetime(2026, 9, 3, 12, tzinfo=timezone.utc),
        timezone_name="Asia/Ho_Chi_Minh",
    )

    assert from_at.isoformat() == "2026-09-03T00:00:00+07:00"
    assert to_at.isoformat() == "2026-09-04T00:00:00+07:00"
