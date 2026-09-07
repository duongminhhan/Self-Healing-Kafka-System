from datetime import datetime

import pytest

from self_healthy_kafka.webhook.analytics import parse_plan
from self_healthy_kafka.webhook.analytics_chat import _aggregate, _duration


@pytest.mark.parametrize(
    "status,outcome,start,end,expected",
    [
        ("COMPLETED", "RECOVERED", "2026-01-01T00:00:00+00:00", "2026-01-01T01:00:00+00:00", 60),
        ("ESCALATED", "ESCALATED", "2026-01-01T00:00:00+00:00", "2026-01-01T01:00:00+00:00", None),
        ("PROCESSING", "RECOVERED", "2026-01-01T00:00:00+00:00", "2026-01-01T01:00:00+00:00", None),
        ("COMPLETED", "RECOVERED", None, "2026-01-01T01:00:00+00:00", None),
        ("COMPLETED", "RECOVERED", "2026-01-01T00:00:00+00:00", None, None),
        ("COMPLETED", "RECOVERED", "2026-01-01T01:00:00+00:00", "2026-01-01T00:00:00+00:00", None),
        ("COMPLETED", "RECOVERED", "2026-01-01T07:00:00+07:00", "2026-01-01T00:00:00+00:00", 0),
        ("COMPLETED", "RECOVERED", "2026-01-01T00:00:00", "2026-01-01T01:00:00+00:00", None),
    ],
)
def test_successful_recovery_duration(status, outcome, start, end, expected):
    assert _duration({
        "queue_status": status,
        "final_outcome": outcome,
        "failure_at": datetime.fromisoformat(start) if start else None,
        "recovered_at": datetime.fromisoformat(end) if end else None,
    }) == expected


@pytest.mark.parametrize("outcome,expected,valid", [("ESCALATED", None, 0), ("RECOVERED", 35, 2)])
def test_average_duration_population_and_exclusions(outcome, expected, valid):
    plan = parse_plan({"dataset": "connector_incidents", "metrics": [
        {"name": "average_recovery_minutes", "aggregation": "average_recovery_minutes"}
    ]})
    start = datetime.fromisoformat("2026-01-01T00:00:00+00:00")
    rows = [{"incident_id": str(index), "queue_status": "COMPLETED" if outcome == "RECOVERED" else "ESCALATED",
             "final_outcome": outcome, "failure_at": start,
             "recovered_at": datetime.fromisoformat(end) if end else None}
            for index, end in enumerate(["2026-01-01T01:00:00+00:00", "2026-01-01T00:10:00+00:00", None])]
    fact = _aggregate(rows, plan)[0]
    assert fact["average_recovery_minutes"] == expected
    assert fact["valid_recovery_duration_count"] == valid
    assert fact["excluded_recovery_duration_count"] == len(rows) - valid
