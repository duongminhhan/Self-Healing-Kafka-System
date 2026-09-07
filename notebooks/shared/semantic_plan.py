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
            k: {**asdict(v), "presentation": METRIC_PRESENTATION[k]}
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
        "limits": {
            "plan_bytes": 16000,
            "dimensions": 8,
            "filters": 20,
            "metrics": 6,
            "rows": snapshot.row_limit,
        },
    }


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
            "success_only",
            "having",
            "order_by",
            "limit",
            "latest_status",
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
    if plan.get("latest_status") and (
        dimensions != ["root"] or not metrics or entity != "incidents"
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
    where = []
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
            where.append(f"{col} IS {'NOT ' if op == 'not_null' else ''}NULL")
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
            where.append(f"julianday({col}) {ops[op]} julianday({bind(value)})")
        else:
            where.append(f"{col} {ops[op]} {bind(value)}")
    if plan.get("success_only"):
        for name, value in (("queue_status", "COMPLETED"), ("outcome", "RECOVERED")):
            if any(
                f["field"] == name and not (f["op"] == "eq" and f.get("value") == value)
                for f in filters
            ):
                raise PlanError("Explicit status/outcome filter conflicts with success_only")
            where.append(f"{field(name)} = {bind(value)}")
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
    expressions = {k: field(k) for k in dimensions}
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
    if where:
        sql += " WHERE " + " AND ".join(where)
    if metrics and dimensions:
        sql += " GROUP BY " + ", ".join(field(k) for k in dimensions)
    ctes = [f"base AS ({sql})"]
    outputs = dimensions + metrics
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
            if h["compare_to"] != "population_mean" or "value" in h or not dimensions:
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
        _keys(child, {"kind", "entity", "dimensions", "metrics", "filters", "success_only"},
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
