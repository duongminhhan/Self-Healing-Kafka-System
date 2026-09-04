"""Safe semantic query plans for connector incident analytics.

This module deliberately accepts a small JSON DSL rather than SQL.  A model may
choose facts and aggregations, but it can never choose a database object or
execute an arbitrary statement.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

DATASET = "connector_incidents"
MAX_LIMIT = 100
MAX_RANGE_DAYS = 366
ALLOWED_GROUP_BY = {"job_name", "connector_name", "error_code", "final_outcome"}
ALLOWED_METRICS = {
    "failure_count",
    "recovered_count",
    "open_count",
    "average_recovery_minutes",
}
ALLOWED_AGGREGATIONS = {
    "failure_count": "count_distinct_incident",
    "recovered_count": "count_distinct_incident",
    "open_count": "count_distinct_incident",
    "average_recovery_minutes": "average_recovery_minutes",
}
ALLOWED_EVENT_TYPES = {"HEALTH_FAILED_CONFIRMED"}
ALLOWED_OUTCOMES = {"RECOVERED", "FAILED", "ESCALATED", "OPEN"}


@dataclass(frozen=True)
class TimeRange:
    kind: str
    value: str


@dataclass(frozen=True)
class Metric:
    name: str
    aggregation: str


@dataclass(frozen=True)
class QueryPlan:
    dataset: str
    metrics: tuple[Metric, ...]
    group_by: tuple[str, ...]
    time_range: TimeRange | None
    event_types: tuple[str, ...]
    outcomes: tuple[str, ...]
    connector_name: str | None
    error_code: str | None
    order_by: str
    direction: str
    limit: int
    comparison: str | None

    def to_dict(self) -> dict:
        result = asdict(self)
        result["metrics"] = [asdict(metric) for metric in self.metrics]
        return result


def parse_plan(value: object) -> QueryPlan:
    """Parse and validate the only model output format accepted by the app."""
    if not isinstance(value, dict):
        raise ValueError("query plan must be a JSON object")
    if not set(value) <= {"dataset", "metrics", "group_by", "filters", "order_by", "limit", "comparison"}:
        raise ValueError("query plan contains an unsupported field")
    if value.get("dataset") != DATASET:
        raise ValueError("dataset is not allowed")
    raw_metrics = value.get("metrics")
    if not isinstance(raw_metrics, list) or not raw_metrics:
        raise ValueError("at least one metric is required")
    metrics: list[Metric] = []
    for item in raw_metrics:
        if not isinstance(item, dict) or item.get("name") not in ALLOWED_METRICS:
            raise ValueError("metric is not allowed")
        name = str(item["name"])
        aggregation = str(item.get("aggregation") or "")
        if aggregation != ALLOWED_AGGREGATIONS[name]:
            raise ValueError("metric aggregation is not allowed")
        metrics.append(Metric(name, aggregation))
    raw_group_by = value.get("group_by") or []
    if not isinstance(raw_group_by, list) or not set(raw_group_by) <= ALLOWED_GROUP_BY:
        raise ValueError("group_by field is not allowed")
    filters = value.get("filters") or {}
    if not isinstance(filters, dict):
        raise ValueError("filters must be an object")
    if not set(filters) <= {
        "time_range",
        "event_type",
        "final_outcome",
        "connector_name",
        "error_code",
    }:
        raise ValueError("filters contain an unsupported field")
    time_range = _parse_time_range(filters.get("time_range"))
    event_types = _enum_values(filters.get("event_type"), ALLOWED_EVENT_TYPES, "event_type")
    outcomes = _enum_values(filters.get("final_outcome"), ALLOWED_OUTCOMES, "final_outcome")
    connector_name = _optional_text(filters.get("connector_name"), "connector_name")
    error_code = _optional_text(filters.get("error_code"), "error_code")
    raw_order = value.get("order_by") or []
    order_field = metrics[0].name
    direction = "desc"
    if raw_order:
        if not isinstance(raw_order, list) or len(raw_order) != 1 or not isinstance(raw_order[0], dict):
            raise ValueError("only one order_by is allowed")
        order_field = str(raw_order[0].get("field"))
        direction = str(raw_order[0].get("direction", "desc")).lower()
    if order_field not in ALLOWED_METRICS or direction not in {"asc", "desc"}:
        raise ValueError("order_by is not allowed")
    try:
        limit = int(value.get("limit", 20))
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {MAX_LIMIT}")
    comparison = value.get("comparison")
    if comparison not in {None, "previous_period"}:
        raise ValueError("comparison is not allowed")
    return QueryPlan(
        dataset=DATASET,
        metrics=tuple(metrics), group_by=tuple(raw_group_by), time_range=time_range,
        event_types=event_types, outcomes=outcomes, connector_name=connector_name,
        error_code=error_code, order_by=order_field, direction=direction, limit=limit,
        comparison=comparison,
    )


def resolve_time_range(time_range: TimeRange | None, *, now: datetime, timezone_name: str) -> tuple[datetime | None, datetime | None]:
    if time_range is None:
        return None, None
    try:
        zone = ZoneInfo(timezone_name)
    except Exception as exc:
        raise ValueError("chat timezone is invalid") from exc
    local_now = now.astimezone(zone)
    today = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    value = time_range.value
    if value == "today":
        return today, today + timedelta(days=1)
    if value == "yesterday":
        return today - timedelta(days=1), today
    if value == "last_7_days":
        return today - timedelta(days=6), today + timedelta(days=1)
    if value == "this_month":
        return today.replace(day=1), today + timedelta(days=1)
    if value == "last_week":
        start = today - timedelta(days=today.weekday() + 7)
        return start, start + timedelta(days=7)
    if value == "this_week":
        start = today - timedelta(days=today.weekday())
        return start, start + timedelta(days=7)
    raise ValueError("time range is not allowed")


def _parse_time_range(value: object) -> TimeRange | None:
    if value is None:
        return None
    if not isinstance(value, dict) or value.get("kind") != "relative":
        raise ValueError("time_range is not allowed")
    raw = str(value.get("value") or "")
    if raw not in {"today", "yesterday", "last_7_days", "this_month", "last_week", "this_week"}:
        raise ValueError("time_range is not allowed")
    return TimeRange("relative", raw)


def _enum_values(value: object, allowed: set[str], name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list) or not set(value) <= allowed:
        raise ValueError(f"{name} is not allowed")
    return tuple(str(item) for item in value)


def _optional_text(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip() or len(value) > 255:
        raise ValueError(f"{name} is not allowed")
    return value.strip()
