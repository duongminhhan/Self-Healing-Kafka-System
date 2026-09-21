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
ALLOWED_GROUP_BY = {
    "job_name",
    "connector_name",
    "error_code",
    "failure_code",
    "final_outcome",
}
ALLOWED_DETAILS = {
    "error_message",
    "connector_name",
    "job_name",
    "final_outcome",
    "queue_status",
}
ALLOWED_METRICS = {
    "failure_count",
    "recovered_count",
    "open_count",
    "average_recovery_minutes",
    "recovery_rate_percent",
}
ALLOWED_AGGREGATIONS = {
    "failure_count": "count_distinct_incident",
    "recovered_count": "count_distinct_incident",
    "open_count": "count_distinct_incident",
    "average_recovery_minutes": "average_recovery_minutes",
    "recovery_rate_percent": "recovery_rate_percent",
}
ALLOWED_EVENT_TYPES = {"HEALTH_FAILED_CONFIRMED"}
ALLOWED_OUTCOMES = {"RECOVERED", "FAILED", "ESCALATED", "OPEN"}


@dataclass(frozen=True)
class TimeRange:
    """A bounded semantic time scope; it never contains model timestamps."""

    kind: str
    value: str
    days: int | None = None

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {"kind": self.kind, "value": self.value}
        if self.days is not None:
            result["days"] = self.days
        return result


@dataclass(frozen=True)
class ResolvedTimeRange:
    """Canonical boundary derived by the backend from one semantic scope."""

    kind: str
    from_at: datetime | None
    to_at: datetime | None
    timezone: str
    timestamp_field: str = "failure_at"

    def to_dict(self) -> dict[str, str | None]:
        return {
            "kind": self.kind,
            "from_at": self.from_at.isoformat() if self.from_at else None,
            "to_at": self.to_at.isoformat() if self.to_at else None,
            "timezone": self.timezone,
            "timestamp_field": self.timestamp_field,
        }


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
    details: tuple[str, ...] = ()
    # Ranking defaults to business ranks.  A fixed row count is only used when
    # the user explicitly asks for exactly N connectors.
    tie_policy: str = "include_ties"

    def to_dict(self) -> dict:
        result = asdict(self)
        result["metrics"] = [asdict(metric) for metric in self.metrics]
        if isinstance(result.get("time_range"), dict) and result["time_range"].get("days") is None:
            result["time_range"].pop("days")
        return result


def parse_plan(value: object) -> QueryPlan:
    """Parse and validate the only model output format accepted by the app."""
    if not isinstance(value, dict):
        raise ValueError("query plan must be a JSON object")
    if not set(value) <= {
        "dataset", "metrics", "group_by", "filters", "order_by", "limit", "comparison", "details", "tie_policy"
    }:
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
    time_range = parse_time_range(filters.get("time_range"))
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
    selected_metrics = {metric.name for metric in metrics}
    if order_field not in selected_metrics or direction not in {"asc", "desc"}:
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
    tie_policy = value.get("tie_policy", "include_ties")
    if tie_policy not in {"include_ties", "exact_limit"}:
        raise ValueError("tie_policy is not allowed")
    if tie_policy == "exact_limit" and not raw_group_by:
        raise ValueError("tie_policy requires a ranked dimension")
    raw_details = value.get("details") or []
    if not isinstance(raw_details, list) or len(raw_details) > len(ALLOWED_DETAILS):
        raise ValueError("details are not allowed")
    if not all(isinstance(item, str) and item in ALLOWED_DETAILS for item in raw_details):
        raise ValueError("details are not allowed")
    return QueryPlan(
        dataset=DATASET,
        metrics=tuple(metrics), group_by=tuple(raw_group_by), time_range=time_range,
        event_types=event_types, outcomes=outcomes, connector_name=connector_name,
        error_code=error_code, order_by=order_field, direction=direction, limit=limit,
        comparison=comparison, details=tuple(dict.fromkeys(raw_details)), tie_policy=tie_policy,
    )


def resolve_time_range(time_range: TimeRange | None, *, now: datetime, timezone_name: str) -> tuple[datetime | None, datetime | None]:
    """Backward-compatible tuple form for repository callers."""

    resolved = resolve_canonical_time_range(time_range, now=now, timezone_name=timezone_name)
    return resolved.from_at, resolved.to_at


def resolve_canonical_time_range(
    time_range: TimeRange | None, *, now: datetime, timezone_name: str
) -> ResolvedTimeRange:
    """Resolve one catalogued semantic scope in the configured business zone."""

    try:
        zone = ZoneInfo(timezone_name)
    except Exception as exc:
        raise ValueError("chat timezone is invalid") from exc
    if time_range is None:
        return ResolvedTimeRange("all_snapshot", None, None, timezone_name)
    local_now = now.astimezone(zone)
    today = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
    value = time_range.value
    if value == "today":
        # "Today" is the elapsed business day, not the entire calendar day
        # including future rows that could be inserted later.
        return ResolvedTimeRange(time_range.kind, today, local_now, timezone_name)
    if value == "yesterday":
        return ResolvedTimeRange(time_range.kind, today - timedelta(days=1), today, timezone_name)
    if value == "last_7_days":
        return ResolvedTimeRange(time_range.kind, today - timedelta(days=6), today + timedelta(days=1), timezone_name)
    if value == "last_n_days" and time_range.days is not None:
        return ResolvedTimeRange(time_range.kind, today - timedelta(days=time_range.days - 1), local_now, timezone_name)
    if value == "this_month":
        return ResolvedTimeRange(time_range.kind, today.replace(day=1), today + timedelta(days=1), timezone_name)
    if value == "last_week":
        start = today - timedelta(days=today.weekday() + 7)
        return ResolvedTimeRange(time_range.kind, start, start + timedelta(days=7), timezone_name)
    if value == "this_week":
        start = today - timedelta(days=today.weekday())
        return ResolvedTimeRange(time_range.kind, start, start + timedelta(days=7), timezone_name)
    if time_range.kind == "absolute_date":
        try:
            start = datetime.fromisoformat(time_range.value).replace(tzinfo=zone)
        except ValueError as exc:
            raise ValueError("absolute date is not allowed") from exc
        return ResolvedTimeRange(time_range.kind, start, start + timedelta(days=1), timezone_name)
    raise ValueError("time range is not allowed")


def parse_time_range(value: object) -> TimeRange | None:
    """Validate the sole semantic time-range schema accepted from a plan."""

    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError("time_range is not allowed")
    kind = value.get("kind")
    raw = value.get("value")
    if not isinstance(raw, str):
        raise ValueError("time_range is not allowed")
    if kind == "relative":
        if raw in {"today", "yesterday", "last_7_days", "this_month", "last_week", "this_week"}:
            if set(value) != {"kind", "value"}:
                raise ValueError("time_range is not allowed")
            return TimeRange("relative", raw)
        if raw == "last_n_days":
            days = value.get("days")
            if set(value) != {"kind", "value", "days"} or not isinstance(days, int) or isinstance(days, bool) or not 1 <= days <= MAX_RANGE_DAYS:
                raise ValueError("time_range is not allowed")
            return TimeRange("relative", raw, days=days)
    if kind == "absolute_date" and set(value) == {"kind", "value"}:
        try:
            datetime.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError("time_range is not allowed") from exc
        if len(raw) == 10:
            return TimeRange("absolute_date", raw)
    raise ValueError("time_range is not allowed")


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
