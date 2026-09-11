"""Typed, source-backed analytics plans and a closed SQLite compiler.

No SQL expressions or physical identifiers are accepted from the model. This
validates plan semantics, not that a plan perfectly captures natural language.
"""

import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone


class PlanError(ValueError):
    """Bounded, non-sensitive validation feedback."""


@dataclass(frozen=True)
class Field:
    table: str
    column: str
    kind: str
    meaning: str
    source: str


@dataclass(frozen=True)
class Metric:
    entities: tuple[str, ...]
    meaning: str
    required: tuple[str, ...]
    source: str


QUEUE = "ConnectorHealingQueue"
LOGS = "ConnectorHealingLogs"
QUEUE_SOURCE = "sql/init-table/ConnectorHealingQueue.sql"
LOG_SOURCE = "sql/init-table/ConnectorHealingLogs.sql"
BUSINESS_SOURCE = "src/self_healthy_kafka/storage/connector_repository.py:complete"

FIELDS = {
    "root": Field(
        QUEUE,
        "RootConnectorName",
        "text",
        "Stable connector identity across replacements",
        QUEUE_SOURCE,
    ),
    "current_connector": Field(
        QUEUE,
        "CurrentConnectorName",
        "text",
        "Physical connector name for this incident, not root identity",
        QUEUE_SOURCE,
    ),
    "incident_id": Field(
        QUEUE, "QueueId", "text", "Unique persisted incident, not retry", QUEUE_SOURCE
    ),
    "queue_status": Field(
        QUEUE, "QueueStatus", "text", "Incident state, not live connector health", QUEUE_SOURCE
    ),
    "outcome": Field(
        QUEUE, "FinalOutcome", "text", "RECOVERED/FAILED/ESCALATED, nullable", BUSINESS_SOURCE
    ),
    "mode": Field(QUEUE, "HealingMode", "text", "RESTART_ONLY or RECOVERY", QUEUE_SOURCE),
    "received_at": Field(QUEUE, "ReceivedAt", "timestamp", "Incident receipt time", QUEUE_SOURCE),
    "started_at": Field(
        QUEUE, "StartedAt", "timestamp", "Worker start time, nullable", QUEUE_SOURCE
    ),
    "completed_at": Field(
        QUEUE, "CompletedAt", "timestamp", "Terminal time, not proof of success", BUSINESS_SOURCE
    ),
    "event_id": Field(LOGS, "Id", "text", "Unique recorded event", LOG_SOURCE),
    "event_connector": Field(
        LOGS, "ConnectorName", "text", "Physical name at event time", LOG_SOURCE
    ),
    "event_type": Field(
        LOGS,
        "EventType",
        "text",
        "Recorded action/observation; not all events are errors",
        LOG_SOURCE,
    ),
    "severity": Field(
        LOGS,
        "Severity",
        "text",
        "Recorded severity; WARN and WARNING are not silently merged",
        LOG_SOURCE,
    ),
    "attempt": Field(
        LOGS, "AttemptNo", "integer", "Nullable attempt label, not count of incidents", LOG_SOURCE
    ),
    "step": Field(
        LOGS,
        "HealingStep",
        "integer",
        "Healing step 1..4, nullable",
        "src/self_healthy_kafka/domain/healing.py:HealingStep",
    ),
    "event_at": Field(LOGS, "CreatedAt", "timestamp", "Event recording time", LOG_SOURCE),
}
METRICS = {
    "incident_count": Metric(
        ("incidents", "events"),
        "Count unique incident IDs in selected population",
        ("incident_id",),
        QUEUE_SOURCE,
    ),
    "log_count": Metric(
        ("incidents", "events"),
        "Count events including informational actions; preserve zero-log incidents",
        ("event_id",),
        LOG_SOURCE,
    ),
    "confirmed_failure_count": Metric(
        ("events",), "Recorded HEALTH_FAILED_CONFIRMED events; not every log is a failure",
        ("event_id", "event_type"), "src/self_healthy_kafka/healing/phases.py:EventType",
    ),
    "task_restart_count": Metric(
        ("events",), "Recorded task restart actions, not a count of attempts",
        ("event_id", "event_type"), "src/self_healthy_kafka/healing/phases.py:EventType",
    ),
    "connector_restart_count": Metric(
        ("events",), "Recorded connector restart actions, not a count of incidents",
        ("event_id", "event_type"), "src/self_healthy_kafka/healing/phases.py:EventType",
    ),
    "recovery_count": Metric(
        ("incidents",), "Persisted incidents completed with RECOVERED outcome",
        ("incident_id", "outcome"), BUSINESS_SOURCE,
    ),
    "recovery_rate_percent": Metric(
        ("incidents",),
        "Recovered incidents divided by terminal incidents with RECOVERED, FAILED or ESCALATED outcome; NULL when no terminal incidents",
        ("incident_id", "outcome"), BUSINESS_SOURCE,
    ),
    "escalation_count": Metric(
        ("incidents",), "Persisted incidents with ESCALATED outcome",
        ("incident_id", "outcome"), BUSINESS_SOURCE,
    ),
    "avg_duration_minutes": Metric(
        ("incidents",),
        "Receipt to completion, exclude missing/unparseable/negative durations; retain zero; round 2 decimals",
        ("received_at", "completed_at"),
        BUSINESS_SOURCE,
    ),
    "matched_count": Metric(
        ("incidents",),
        "All incident rows matching filters before duration quality exclusions",
        ("incident_id",),
        QUEUE_SOURCE,
    ),
    "valid_duration_count": Metric(
        ("incidents",),
        "Parseable, nonnegative receipt-to-completion durations",
        ("received_at", "completed_at"),
        BUSINESS_SOURCE,
    ),
    "excluded_duration_count": Metric(
        ("incidents",),
        "Matched minus valid duration count",
        ("received_at", "completed_at"),
        BUSINESS_SOURCE,
    ),
}

# Presentation metadata is part of the semantic contract. It lets both the
# response model and deterministic fallback use business language without
# reverse-engineering SQL aliases. Provenance describes how the returned value
# is produced; it does not claim that a model-selected intent is correct.
METRIC_PRESENTATION = {
    "incident_count": {
        "label_vi": "incident", "unit": "incident", "provenance": "database_aggregate",
    },
    "log_count": {
        "label_vi": "healing log", "unit": "healing log", "provenance": "database_aggregate",
    },
    "confirmed_failure_count": {
        "label_vi": "lần lỗi được xác nhận", "unit": "sự kiện", "provenance": "database_aggregate",
    },
    "task_restart_count": {
        "label_vi": "lần khởi động lại task", "unit": "sự kiện", "provenance": "database_aggregate",
    },
    "connector_restart_count": {
        "label_vi": "lần khởi động lại connector", "unit": "sự kiện", "provenance": "database_aggregate",
    },
    "recovery_count": {
        "label_vi": "incident đã phục hồi", "unit": "incident", "provenance": "database_aggregate",
    },
    "recovery_rate_percent": {
        "label_vi": "tỷ lệ phục hồi", "unit": "percent", "provenance": "deterministic_calculation",
    },
    "escalation_count": {
        "label_vi": "incident đã chuyển cấp", "unit": "incident", "provenance": "database_aggregate",
    },
    "avg_duration_minutes": {
        "label_vi": "thời gian phục hồi trung bình", "unit": "minute", "provenance": "deterministic_calculation",
    },
    "matched_count": {
        "label_vi": "incident phù hợp", "unit": "incident", "provenance": "database_aggregate",
    },
    "valid_duration_count": {
        "label_vi": "incident có thời lượng hợp lệ", "unit": "incident", "provenance": "database_aggregate",
    },
    "excluded_duration_count": {
        "label_vi": "incident bị loại khỏi phép tính thời lượng", "unit": "incident", "provenance": "deterministic_calculation",
    },
}

# Closed, auditable metric semantics.  These rules are prompt/catalog metadata;
# SQL generation still goes through the compiler below.  Keeping them separate
# from presentation labels prevents a wording change from changing a measure.
METRIC_RULES = {
    "incident_count": {
        "source_fields": ["incident_id"], "grain": "unique incident_id",
        "aggregation": "count_distinct", "denominator": None,
        "valid_statuses": None, "timestamp_field": "received_at",
        "null_handling": "Do not count NULL incident identities",
        "allowed_joins": ["incidents_to_events_many_to_one"],
        "duplicate_policy": "COUNT DISTINCT incident_id",
    },
    "log_count": {
        "source_fields": ["event_id"], "grain": "unique event_id",
        "aggregation": "count", "denominator": None, "valid_statuses": None,
        "timestamp_field": "event_at", "null_handling": "Do not count NULL event identities",
        "allowed_joins": ["events_to_incidents_many_to_one"],
        "duplicate_policy": "Count each event_id once at event grain",
    },
    "confirmed_failure_count": {
        "source_fields": ["event_id", "event_type"], "grain": "unique event_id",
        "aggregation": "conditional_count", "denominator": None,
        "valid_statuses": ["HEALTH_FAILED_CONFIRMED"], "timestamp_field": "event_at",
        "null_handling": "Ignore events without the required event type",
        "allowed_joins": ["events_to_incidents_many_to_one"],
        "duplicate_policy": "Count each matching event_id once",
    },
    "task_restart_count": {
        "source_fields": ["event_id", "event_type"], "grain": "unique event_id",
        "aggregation": "conditional_count", "denominator": None,
        "valid_statuses": ["TASK_RESTART"], "timestamp_field": "event_at",
        "null_handling": "Ignore events without the required event type",
        "allowed_joins": ["events_to_incidents_many_to_one"],
        "duplicate_policy": "Count each matching event_id once",
    },
    "connector_restart_count": {
        "source_fields": ["event_id", "event_type"], "grain": "unique event_id",
        "aggregation": "conditional_count", "denominator": None,
        "valid_statuses": ["CONNECTOR_RESTART"], "timestamp_field": "event_at",
        "null_handling": "Ignore events without the required event type",
        "allowed_joins": ["events_to_incidents_many_to_one"],
        "duplicate_policy": "Count each matching event_id once",
    },
    "recovery_count": {
        "source_fields": ["incident_id", "outcome"], "grain": "unique incident_id",
        "aggregation": "conditional_count_distinct", "denominator": None,
        "valid_statuses": ["RECOVERED"], "timestamp_field": "received_at",
        "null_handling": "Ignore NULL outcomes",
        "allowed_joins": ["incidents_to_events_many_to_one"],
        "duplicate_policy": "COUNT DISTINCT incident_id",
    },
    "recovery_rate_percent": {
        "source_fields": ["incident_id", "outcome"], "grain": "unique incident_id",
        "aggregation": "ratio_percent", "denominator": "terminal incidents",
        "valid_statuses": ["RECOVERED", "FAILED", "ESCALATED"],
        "timestamp_field": "received_at",
        "null_handling": "Return NULL when the denominator is zero",
        "allowed_joins": [], "duplicate_policy": "One outcome per incident_id",
    },
    "escalation_count": {
        "source_fields": ["incident_id", "outcome"], "grain": "unique incident_id",
        "aggregation": "conditional_count_distinct", "denominator": None,
        "valid_statuses": ["ESCALATED"], "timestamp_field": "received_at",
        "null_handling": "Ignore NULL outcomes", "allowed_joins": [],
        "duplicate_policy": "COUNT DISTINCT incident_id",
    },
    "avg_duration_minutes": {
        "source_fields": ["received_at", "completed_at"], "grain": "unique incident_id",
        "aggregation": "mean_duration_minutes", "denominator": "valid_duration_count",
        "valid_statuses": ["COMPLETED", "RECOVERED"], "timestamp_field": "received_at",
        "null_handling": "Exclude missing, unparseable and negative durations; retain zero",
        "allowed_joins": [], "duplicate_policy": "One duration per incident_id",
    },
    "matched_count": {
        "source_fields": ["incident_id"], "grain": "unique incident_id",
        "aggregation": "count_distinct", "denominator": None, "valid_statuses": None,
        "timestamp_field": "received_at", "null_handling": "Do not count NULL identities",
        "allowed_joins": [], "duplicate_policy": "COUNT DISTINCT incident_id",
    },
    "valid_duration_count": {
        "source_fields": ["received_at", "completed_at"], "grain": "unique incident_id",
        "aggregation": "conditional_count_distinct", "denominator": None,
        "valid_statuses": None, "timestamp_field": "received_at",
        "null_handling": "Count only parseable nonnegative durations",
        "allowed_joins": [], "duplicate_policy": "Count each incident_id once",
    },
    "excluded_duration_count": {
        "source_fields": ["received_at", "completed_at"], "grain": "unique incident_id",
        "aggregation": "matched_minus_valid", "denominator": None,
        "valid_statuses": None, "timestamp_field": "received_at",
        "null_handling": "Missing, unparseable and negative durations are excluded",
        "allowed_joins": [], "duplicate_policy": "Count each incident_id once",
    },
}
DURATION = {
    "avg_duration_minutes",
    "matched_count",
    "valid_duration_count",
    "excluded_duration_count",
}
ENUMS = {
    "queue_status": {"PENDING", "PROCESSING", "WAITING", "COMPLETED", "ESCALATED"},
    "outcome": {"RECOVERED", "FAILED", "ESCALATED"},
    "mode": {"RESTART_ONLY", "RECOVERY"},
}

BUSINESS_TERMS = {
    "incident": ["incident", "sự cố", "ca healing", "lần cần healing", "hàng chờ tự sửa"],
    "healing_log": [
        "healing log", "log healing", "nhật ký healing", "nhật ký tự sửa", "sự kiện healing",
    ],
    "confirmed_failure": ["lỗi", "failure", "thất bại được xác nhận"],
    "recovery": ["phục hồi", "recovery", "khôi phục"],
    "unfinished": [
        "chưa xử lý xong", "chưa hoàn tất", "vẫn đang xử lý", "chưa giải quyết", "unresolved",
        "still pending",
    ],
    "root": ["connector", "kết nối", "root connector", "logical connector"],
}


def catalog(snapshot):
    fields = {
        k: v
        for k, v in FIELDS.items()
        if any(c["name"] == v.column for c in snapshot.schema.get(v.table, []))
    }
    metrics = {k: v for k, v in METRICS.items() if set(v.required) <= fields.keys()}
    return {
        "version": 1,
        "fields": {k: asdict(v) for k, v in fields.items()},
        "metrics": {
            k: {
                **asdict(v),
                **METRIC_RULES[k],
                "default_timezone": "Asia/Ho_Chi_Minh",
                "presentation": METRIC_PRESENTATION[k],
            }
            for k, v in metrics.items()
        },
        "success": {"queue_status": "COMPLETED", "outcome": "RECOVERED", "source": BUSINESS_SOURCE},
        "relationship": {
            "from": "events.QueueId",
            "to": "incidents.QueueId",
            "cardinality": "many-to-one",
            "source": LOG_SOURCE,
            "missing_parent_policy": "Events retain orphan logs with NULL parent attributes; incident_count counts only matched incident IDs",
        },
        "business_terms": BUSINESS_TERMS,
        "time_basis": {
            "incidents": {"field": "received_at", "meaning": "Incident receipt time"},
            "events": {"field": "event_at", "meaning": "Healing-log recording time"},
            "ingestion": "Unavailable: the snapshot does not record database insert time.",
        },
        "timezone": "UTC; timestamp filter values must include an explicit offset",
        "filter_logic": "All explicit filters use one allowlisted AND or OR connective; success semantics remain mandatory AND constraints.",
        "time_bucket": {
            "units": ["day", "week", "month"],
            "timezones": ["UTC", "Asia/Ho_Chi_Minh"],
            "week_definition": "Monday-start local calendar week, returned as its start date",
        },
        "limits": {
            "plan_bytes": 16000,
            "dimensions": 8,
            "filters": 20,
            "metrics": 6,
            "rows": snapshot.row_limit,
        },
    }


def semantic_representation(plan, snapshot):
    """Return a complete, source-backed audit view of an accepted plan.

    This representation is never compiled as SQL and contains no physical SQL
    fragments.  It makes implicit catalog decisions visible to validation,
    telemetry and review tooling without asking the user for column names.
    """

    semantic_catalog = catalog(snapshot)
    children = plan.get("queries", []) if plan.get("kind") == "independent" else [plan]
    populations = []
    for child in children:
        metrics = child.get("metrics", [])
        rules = [semantic_catalog["metrics"][name] for name in metrics]
        temporal_filters = [
            item for item in child.get("filters", [])
            if item.get("field") in {"received_at", "event_at", "started_at", "completed_at"}
        ]
        time_bucket = child.get("time_bucket") or {}
        populations.append(
            {
                "intent": "analytics_query",
                "entity": child.get("entity"),
                "metrics": metrics,
                "aggregation": [rule["aggregation"] for rule in rules],
                "grain": {
                    "entity": "incident_id" if child.get("entity") == "incidents" else "event_id",
                    "dimensions": child.get("dimensions", []),
                },
                "dimensions": child.get("dimensions", []),
                "filters": child.get("filters", []),
                "time_range": temporal_filters,
                "time_column": time_bucket.get("field") or (
                    "received_at" if child.get("entity") == "incidents" else "event_at"
                ),
                "timezone": time_bucket.get("timezone", "Asia/Ho_Chi_Minh"),
                "status_semantics": [rule["valid_statuses"] for rule in rules],
                "ordering": child.get("order_by", []),
                "limit": child.get("limit"),
                "denominator": [rule["denominator"] for rule in rules],
                "comparison_period": None,
                "ambiguity_flags": [],
                "assumptions": child.get("assumptions", []),
            }
        )
    return {"kind": plan.get("kind"), "populations": populations}


@dataclass(frozen=True)
class CompiledQuery:
    sql: str
    parameters: dict
    plan: dict
    assumptions: tuple[str, ...]


def _keys(value, allowed, required=()):
    if not isinstance(value, dict) or set(value) - set(allowed) or set(required) - set(value):
        raise PlanError("Unknown or missing plan fields")


def _list(value, maximum):
    if not isinstance(value, list) or len(value) > maximum:
        raise PlanError("Expected bounded list")
    return value


def compile_plan(plan, snapshot):
    """Validation is mandatory inside compilation: callers cannot skip it."""
    try:
        encoded = json.dumps(plan, allow_nan=False)
    except (ValueError, TypeError, RecursionError):
        raise PlanError("Plan must contain finite JSON values") from None
    if len(encoded.encode("utf-8")) > 16000:
        raise PlanError("Plan exceeds 16000 bytes")
    if isinstance(plan, dict) and plan.get("kind") == "independent":
        return _compile_independent(plan, snapshot, encoded)
    _keys(
        plan,
        {
            "kind",
            "entity",
            "dimensions",
            "metrics",
            "filters",
            "filter_logic",
            "success_only",
            "having",
            "order_by",
            "limit",
            "latest_status",
            "time_bucket",
            "assumptions",
        },
        {"kind", "entity", "dimensions", "metrics"},
    )
    if (
        plan["kind"] != "query"
        or not isinstance(plan["entity"], str)
        or plan["entity"] not in {"incidents", "events"}
    ):
        raise PlanError("Expected query over incidents or events")
    available = catalog(snapshot)
    entity = plan["entity"]
    dimensions = _list(plan["dimensions"], 8)
    metrics = _list(plan["metrics"], 6)
    if any(not isinstance(k, str) for k in dimensions + metrics):
        raise PlanError("Dimensions/metrics must be catalog IDs")
    if len(set(dimensions + metrics)) != len(dimensions + metrics) or not dimensions + metrics:
        raise PlanError("Select unique dimensions/metrics")
    if any(k not in available["fields"] for k in dimensions):
        raise PlanError("Unknown or unavailable dimension")
    if any(k not in available["metrics"] or entity not in METRICS[k].entities for k in metrics):
        raise PlanError("Metric unavailable or incompatible with entity grain")
    if "avg_duration_minutes" in metrics and not DURATION <= set(metrics):
        raise PlanError(
            "Duration average requires matched_count, valid_duration_count and excluded_duration_count"
        )
    for flag in ("success_only", "latest_status"):
        if flag in plan and type(plan[flag]) is not bool:
            raise PlanError("Plan flags must be booleans")
    filter_logic = plan.get("filter_logic", "and")
    if filter_logic not in {"and", "or"}:
        raise PlanError("filter_logic must be and or or")
    time_bucket = plan.get("time_bucket")
    if time_bucket is not None:
        _keys(time_bucket, {"field", "unit", "timezone"}, {"field", "unit", "timezone"})
        expected_time_field = "received_at" if entity == "incidents" else "event_at"
        if (
            time_bucket.get("field") != expected_time_field
            or time_bucket.get("unit") not in {"day", "week", "month"}
            or time_bucket.get("timezone") not in {"UTC", "Asia/Ho_Chi_Minh"}
            or not metrics
        ):
            raise PlanError(
                "time_bucket requires the entity time field, day/week/month, a supported timezone and a metric"
            )
    if plan.get("latest_status") and (
        dimensions != ["root"] or not metrics or entity != "incidents" or time_bucket
    ):
        raise PlanError("Latest status requires incident aggregation by root only")
    if (
        plan.get("latest_status")
        and not {"root", "queue_status", "received_at", "incident_id"} <= available["fields"].keys()
    ):
        raise PlanError("Latest-status source fields unavailable")
    limit = plan.get("limit", snapshot.row_limit)
    if type(limit) is not int or not 1 <= limit <= snapshot.row_limit:
        raise PlanError("Limit exceeds snapshot result budget")
    parameters = {}

    def bind(value):
        name = f"p{len(parameters)}"
        parameters[name] = value
        return ":" + name

    def field(name):
        if not isinstance(name, str) or name not in available["fields"]:
            raise PlanError("Unknown or unavailable field")
        f = FIELDS[name]
        if f.table == LOGS and entity == "incidents":
            raise PlanError(
                "Event dimensions/filters require events entity; do not fan out incident measures"
            )
        return f'{"q" if f.table == QUEUE else "l"}."{f.column}"'

    filters = _list(plan.get("filters", []), 20)
    user_conditions = []
    system_conditions = []
    ops = {"eq": "=", "ne": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
    for f in filters:
        _keys(f, {"field", "op", "value"}, {"field", "op"})
        name, op = f["field"], f["op"]
        col = field(name)
        if not isinstance(op, str):
            raise PlanError("Filter operator must be a string")
        if op in {"is_null", "not_null"}:
            if "value" in f:
                raise PlanError("NULL predicates do not take values")
            user_conditions.append(f"{col} IS {'NOT ' if op == 'not_null' else ''}NULL")
            continue
        if op not in ops or "value" not in f:
            raise PlanError("Unsupported filter operator or missing value")
        value = f["value"]
        kind = FIELDS[name].kind
        if kind == "integer":
            if type(value) is not int or not -(2**63) <= value < 2**63:
                raise PlanError("Integer filter requires integer value")
        elif not isinstance(value, str) or len(value) > 255:
            raise PlanError("Text/timestamp filter requires bounded string")
        if name in ENUMS and value not in ENUMS[name]:
            raise PlanError("Value outside business enum")
        if kind == "timestamp":
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    raise ValueError
                value = parsed.astimezone(timezone.utc).isoformat()
            except (ValueError, OverflowError):
                raise PlanError("Timestamp filter must be valid ISO-8601 with timezone") from None
            user_conditions.append(
                f"julianday({col}) {ops[op]} julianday({bind(value)})"
            )
        else:
            user_conditions.append(f"{col} {ops[op]} {bind(value)}")
    if plan.get("success_only"):
        for name, value in (("queue_status", "COMPLETED"), ("outcome", "RECOVERED")):
            if any(
                f["field"] == name and not (f["op"] == "eq" and f.get("value") == value)
                for f in filters
            ):
                raise PlanError("Explicit status/outcome filter conflicts with success_only")
            system_conditions.append(f"{field(name)} = {bind(value)}")
    if QUEUE not in snapshot.allowed_tables or "incident_id" not in available["fields"]:
        raise PlanError("Incident identity unavailable")
    queue_pk = [c["name"] for c in snapshot.schema[QUEUE] if c["primary_key_position"]]
    if queue_pk != ["QueueId"]:
        raise PlanError(
            "Strict mode requires a proven unique QueueId primary key for grain/JOIN safety"
        )
    # SQLite allows NULL in ordinary TEXT PRIMARY KEY columns unless NOT NULL
    # is explicit. INTEGER PRIMARY KEY is a non-null rowid alias.
    identity = next(c for c in snapshot.schema[QUEUE] if c["name"] == "QueueId")
    if identity["nullable"] and (identity["declared_type"] or "").upper() != "INTEGER":
        raise PlanError(
            "QueueId must be a non-null identity; nullable SQLite primary keys are unsafe"
        )
    if entity == "events" or "log_count" in metrics:
        log_pk = [c for c in snapshot.schema.get(LOGS, []) if c["primary_key_position"]]
        if len(log_pk) != 1 or log_pk[0]["name"] != "Id":
            raise PlanError("Strict event counting requires a unique Id primary key")
        if log_pk[0]["nullable"] and (log_pk[0]["declared_type"] or "").upper() != "INTEGER":
            raise PlanError("Event Id must be a non-null identity")
    if entity == "events":
        if LOGS not in snapshot.allowed_tables or not any(
            c["name"] == "QueueId" for c in snapshot.schema[LOGS]
        ):
            raise PlanError("Documented event-to-incident relationship unavailable")
        source = f'"{LOGS}" l LEFT JOIN "{QUEUE}" q ON l."QueueId"=q."QueueId"'
    else:
        source = f'"{QUEUE}" q'
    expressions = {}
    group_expressions = []
    if time_bucket:
        time_column = field(time_bucket["field"])
        modifiers = ", '+7 hours'" if time_bucket["timezone"] == "Asia/Ho_Chi_Minh" else ""
        if time_bucket["unit"] == "day":
            bucket_expression = f"strftime('%Y-%m-%d', {time_column}{modifiers})"
        elif time_bucket["unit"] == "week":
            bucket_expression = (
                f"date({time_column}{modifiers}, '-6 days', 'weekday 1')"
            )
        else:
            bucket_expression = f"strftime('%Y-%m', {time_column}{modifiers})"
        expressions["time_bucket"] = bucket_expression
        group_expressions.append(bucket_expression)
    expressions.update({k: field(k) for k in dimensions})
    group_expressions.extend(field(k) for k in dimensions)
    duration = '(julianday(q."CompletedAt")-julianday(q."ReceivedAt"))*1440.0'
    valid = f"({duration}) >= 0"
    metric_sql = {
        "incident_count": 'COUNT(DISTINCT q."QueueId")',
        "matched_count": "COUNT(*)",
        "avg_duration_minutes": f"ROUND(AVG(CASE WHEN {valid} THEN {duration} END),2)",
        "valid_duration_count": f"COUNT(CASE WHEN {valid} THEN 1 END)",
        "excluded_duration_count": f"COUNT(*)-COUNT(CASE WHEN {valid} THEN 1 END)",
        "recovery_count": "COUNT(CASE WHEN q.\"FinalOutcome\"='RECOVERED' THEN 1 END)",
        "recovery_rate_percent": (
            "ROUND(100.0*COUNT(CASE WHEN q.\"FinalOutcome\"='RECOVERED' THEN 1 END)"
            "/NULLIF(COUNT(CASE WHEN q.\"FinalOutcome\" IN ('RECOVERED','FAILED','ESCALATED') THEN 1 END),0),2)"
        ),
        "escalation_count": "COUNT(CASE WHEN q.\"FinalOutcome\"='ESCALATED' THEN 1 END)",
    }
    if "log_count" in metrics:
        if not any(c["name"] == "QueueId" for c in snapshot.schema.get(LOGS, [])):
            raise PlanError("Log-to-incident key unavailable")
        metric_sql["log_count"] = (
            'COUNT(l."Id")'
            if entity == "events"
            else f'COALESCE(SUM((SELECT COUNT(*) FROM "{LOGS}" lx WHERE lx."QueueId"=q."QueueId")),0)'
        )
    if entity == "events":
        metric_sql.update(
            {
                "confirmed_failure_count": "COUNT(CASE WHEN l.\"EventType\"='HEALTH_FAILED_CONFIRMED' THEN 1 END)",
                "task_restart_count": "COUNT(CASE WHEN l.\"EventType\"='TASK_RESTART' THEN 1 END)",
                "connector_restart_count": "COUNT(CASE WHEN l.\"EventType\"='CONNECTOR_RESTART' THEN 1 END)",
            }
        )
    expressions.update({k: metric_sql[k] for k in metrics})
    selected = ", ".join(f'{v} AS "{k}"' for k, v in expressions.items())
    sql = f"SELECT {selected} FROM {source}"
    where_groups = []
    if user_conditions:
        where_groups.append(
            "(" + (" OR " if filter_logic == "or" else " AND ").join(user_conditions) + ")"
        )
    where_groups.extend(system_conditions)
    if where_groups:
        sql += " WHERE " + " AND ".join(where_groups)
    if metrics and group_expressions:
        sql += " GROUP BY " + ", ".join(group_expressions)
    ctes = [f"base AS ({sql})"]
    outputs = (["time_bucket"] if time_bucket else []) + dimensions + metrics
    having = _list(plan.get("having", []), 8)
    conditions = []
    for h in having:
        _keys(h, {"metric", "op", "value", "compare_to"}, {"metric", "op"})
        if (
            not isinstance(h["metric"], str)
            or h["metric"] not in metrics
            or not isinstance(h["op"], str)
            or h["op"] not in ops
        ):
            raise PlanError("Having requires selected metric and supported comparison")
        if "compare_to" in h:
            if (
                h["compare_to"] != "population_mean"
                or "value" in h
                or not group_expressions
            ):
                raise PlanError("Population mean compares grouped metric before having/limit")
            rhs = f'(SELECT AVG("{h["metric"]}") FROM base)'
        else:
            value = h.get("value")
            if (
                type(value) not in (int, float)
                or (type(value) is int and not -(2**63) <= value < 2**63)
                or (type(value) is float and not math.isfinite(value))
            ):
                raise PlanError("Having requires finite numeric value")
            rhs = bind(value)
        conditions.append(f'"{h["metric"]}" {ops[h["op"]]} {rhs}')
    if plan.get("latest_status"):
        ctes.append(
            f'latest AS (SELECT "RootConnectorName" AS root, "QueueStatus", ROW_NUMBER() OVER (PARTITION BY "RootConnectorName" ORDER BY julianday("ReceivedAt") DESC,"QueueId" DESC) AS rn FROM "{QUEUE}")'
        )
        select = 'b.*, latest."QueueStatus" AS latest_queue_status'
        tail = "base b LEFT JOIN latest ON latest.root IS b.root AND latest.rn=1"
        outputs = outputs + ["latest_queue_status"]
    else:
        select, tail = "*", "base"
    sql = "WITH " + ", ".join(ctes) + f" SELECT {select} FROM {tail}"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    ordering = []
    for order in _list(plan.get("order_by", []), 12):
        _keys(order, {"field", "direction"}, {"field", "direction"})
        if order["field"] not in outputs or order["direction"] not in ("asc", "desc"):
            raise PlanError("Ordering references unselected field or invalid direction")
        name = order["field"]
        # Timestamp rows sort by instant, not textual UTC-offset spelling.
        expr = f'"{name}"'
        if name in FIELDS and FIELDS[name].kind == "timestamp":
            expr = f"julianday({expr})"
        ordering.append(expr + " " + order["direction"].upper())
    if ordering:
        ordered_names = {item.split('"', 2)[1] for item in ordering}
        if plan.get("limit") is not None and metrics:
            for name in (["time_bucket"] if time_bucket else []) + dimensions:
                if name not in ordered_names:
                    ordering.append(f'"{name}" ASC')
        sql += " ORDER BY " + ", ".join(ordering)
    sql += " LIMIT " + bind(limit)
    assumptions = _list(plan.get("assumptions", []), 8)
    if any(not isinstance(s, str) or len(s) > 300 for s in assumptions):
        raise PlanError("Assumptions must be bounded strings")
    snapshot.validate(sql)
    return CompiledQuery(sql, parameters, json.loads(encoded), tuple(assumptions))


def _compile_independent(plan, snapshot, encoded):
    """Combine proven scalar aggregates, never their underlying populations."""
    _keys(plan, {"kind", "queries"}, {"kind", "queries"})
    queries = _list(plan["queries"], 4)
    if len(queries) < 2:
        raise PlanError("Independent aggregation requires two to four populations")
    columns, sources, parameters, seen = [], [], {}, set()
    for index, child in enumerate(queries):
        _keys(child, {"kind", "entity", "dimensions", "metrics", "filters", "filter_logic", "success_only"},
              {"kind", "entity", "dimensions", "metrics"})
        if child["kind"] != "query" or child["dimensions"] != [] or not child["metrics"]:
            raise PlanError("Independent populations must be ungrouped aggregate queries")
        compiled = compile_plan(child, snapshot)
        for metric in child["metrics"]:
            if metric in seen or len(seen) >= 6:
                raise PlanError("Independent outputs require unique metrics, at most six")
            seen.add(metric)
            columns.append(f'b{index}."{metric}" AS "{metric}"')
        sql = re.sub(r":(p\d+)\b", lambda m: f":b{index}_{m[1]}", compiled.sql)
        parameters.update({f"b{index}_{key}": value for key, value in compiled.parameters.items()})
        sources.append(f"({sql}) b{index}")
    # Each child has aggregates, no grouping/HAVING: exactly one row, even empty.
    sql = "SELECT " + ", ".join(columns) + " FROM " + " CROSS JOIN ".join(sources)
    snapshot.validate(sql)
    return CompiledQuery(sql, parameters, json.loads(encoded), ())


_NONNEGATIVE_INTEGER_METRICS = {
    "incident_count",
    "log_count",
    "confirmed_failure_count",
    "task_restart_count",
    "connector_restart_count",
    "recovery_count",
    "escalation_count",
    "matched_count",
    "valid_duration_count",
    "excluded_duration_count",
}


def validate_result_invariants(plan, result):
    """Validate compiler-owned result shape and arithmetic after execution.

    These checks prove internal invariants of a validated plan. They do not
    claim that the plan perfectly captured the user's natural-language intent.
    """
    if not isinstance(result, dict):
        raise PlanError("Executed result must be an object")
    rows = result.get("rows")
    columns = result.get("columns")
    if not isinstance(rows, list) or not isinstance(columns, list):
        raise PlanError("Executed result is missing rows or columns")
    names = [column.get("name") for column in columns if isinstance(column, dict)]
    if len(names) != len(columns) or any(not isinstance(name, str) for name in names):
        raise PlanError("Executed result has malformed column metadata")
    if len(names) != len(set(names)):
        raise PlanError("Executed result contains duplicate output aliases")
    if result.get("returned_row_count") != len(rows):
        raise PlanError("Returned-row metadata contradicts the result rows")
    if result.get("truncated"):
        raise PlanError("Strict compiled result exceeded its declared row limit")

    children = plan.get("queries", []) if plan.get("kind") == "independent" else [plan]
    dimensions = [] if plan.get("kind") == "independent" else list(plan.get("dimensions", []))
    if plan.get("time_bucket"):
        dimensions.insert(0, "time_bucket")
    metrics = [metric for child in children for metric in child.get("metrics", [])]
    expected = dimensions + metrics
    if plan.get("latest_status"):
        expected.append("latest_queue_status")
    if names != expected:
        raise PlanError(
            "Executed result columns do not match the compiled semantic plan: "
            + ", ".join(names)
        )

    seen_groups = set()
    integer_metrics = _NONNEGATIVE_INTEGER_METRICS & set(metrics)
    for row in rows:
        if not isinstance(row, dict) or list(row) != names:
            raise PlanError("Executed result row shape contradicts column metadata")
        group = tuple(row[name] for name in dimensions)
        if dimensions and group in seen_groups:
            raise PlanError("Grouped result contains duplicate dimension grain")
        seen_groups.add(group)
        for metric in integer_metrics:
            value = row[metric]
            if type(value) is not int or value < 0:
                raise PlanError(f"{metric} must be a nonnegative integer")
        if "recovery_rate_percent" in metrics:
            value = row["recovery_rate_percent"]
            if value is not None and (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not 0 <= value <= 100
            ):
                raise PlanError("recovery_rate_percent must be NULL or between 0 and 100")
        if "avg_duration_minutes" in metrics:
            average = row["avg_duration_minutes"]
            matched = row["matched_count"]
            valid = row["valid_duration_count"]
            excluded = row["excluded_duration_count"]
            if matched != valid + excluded:
                raise PlanError("Duration counts must satisfy matched = valid + excluded")
            if average is None and valid != 0:
                raise PlanError("Duration average is NULL despite valid duration rows")
            if average is not None and (
                not isinstance(average, (int, float))
                or isinstance(average, bool)
                or average < 0
                or valid == 0
            ):
                raise PlanError("Duration average contradicts valid-duration evidence")

    if len(rows) > int(plan.get("limit", 1000)):
        raise PlanError("Executed result exceeds the semantic plan limit")
    return {
        "status": "passed",
        "checks": [
            "shape",
            "row_count",
            "declared_limit",
            "dimension_grain",
            "metric_domains",
            "duration_arithmetic",
        ],
    }
