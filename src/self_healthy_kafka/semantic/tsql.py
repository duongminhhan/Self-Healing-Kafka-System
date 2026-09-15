"""Deterministic, read-only T-SQL compiler for incident analytics.

The language model never provides SQL.  This module converts an already
validated :class:`QueryPlan` into the one bounded query shape that the
analytics service is allowed to execute.  The resulting display statement is
also a truthful representation of the query sent to SQL Server: only the
driver's positional placeholders are replaced by typed ``DECLARE`` variables.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import re
from typing import Any

from self_healthy_kafka.webhook.analytics import QueryPlan


_SOURCE = "[dbo].[vConnectorIncidentFacts]"
_MAX_ROWS = 500
_DIMENSION_COLUMNS = {
    "job_name": ("[JobName]", "root_connector_name"),
    "connector_name": ("[ConnectorName]", "current_connector_name"),
    "error_code": ("[ErrorCode]", "error_code"),
    # ``ErrorCode`` is already the approved normalized code projected by the
    # source view.  A source without that value cannot claim an error-signature
    # ranking; it is intentionally excluded rather than fabricated from logs.
    "failure_code": ("[ErrorCode]", "error_signature"),
    "final_outcome": ("[FinalOutcome]", "final_outcome"),
}
_METRIC_COLUMNS = {
    "failure_count": "incident_count",
    "recovered_count": "recovered_incident_count",
    "open_count": "open_incident_count",
    "average_recovery_minutes": "average_recovery_minutes",
    "recovery_rate_percent": "recovery_rate_percent",
}


@dataclass(frozen=True)
class QueryParameter:
    name: str
    type: str
    value: str | int | None

    def to_public_dict(self) -> dict[str, str | int | None]:
        return {"name": self.name, "type": self.type, "value": self.value}


@dataclass(frozen=True)
class ExecutedTsql:
    """A bounded SELECT packet that can be executed through ``pyodbc``."""

    statement: str
    parameters: tuple[QueryParameter, ...]
    result_shape: tuple[str, ...]

    @property
    def parameter_values(self) -> tuple[str | int | None, ...]:
        return tuple(item.value for item in self.parameters)

    @property
    def display_statement(self) -> str:
        declarations = "\n".join(_declaration(item) for item in self.parameters)
        named_statement = self.statement
        for parameter in self.parameters:
            named_statement = named_statement.replace("?", parameter.name, 1)
        return f"{declarations}\n\n{named_statement}" if declarations else named_statement

    def to_public_dict(self, *, executed: bool) -> dict[str, Any]:
        return {
            "kind": "tsql_select",
            "dialect": "tsql",
            "statement": self.statement,
            "display_statement": self.display_statement,
            "parameters": [item.to_public_dict() for item in self.parameters],
            "executed": executed,
            "read_only": True,
            "result_shape": list(self.result_shape),
        }


def compile_incident_query(
    plan: QueryPlan,
    *,
    from_at: datetime | None,
    to_at: datetime | None,
    row_limit: int = _MAX_ROWS,
) -> ExecutedTsql:
    """Compile a validated analytics plan into one parameterized CTE SELECT.

    ``row_limit`` is the bounded transport limit, not a semantic top-N.  One
    additional row is fetched by the caller so a tie boundary that exceeds the
    UI limit is classified as incomplete rather than silently truncated.
    """

    if not 1 <= row_limit <= _MAX_ROWS + 1:
        raise ValueError("row_limit is outside the approved analytics bound")
    if plan.details:
        raise ValueError("details require an unsupported non-aggregated projection")
    if plan.comparison:
        raise ValueError("comparison requires a separate verified execution")
    if not all(field in _DIMENSION_COLUMNS for field in plan.group_by):
        raise ValueError("query plan has an unsupported T-SQL dimension")
    if not all(metric.name in _METRIC_COLUMNS for metric in plan.metrics):
        raise ValueError("query plan has an unsupported T-SQL metric")

    parameters: list[QueryParameter] = []
    predicates: list[str] = []

    def bind(name: str, type_name: str, value: str | int | None) -> str:
        parameters.append(QueryParameter(name=name, type=type_name, value=value))
        return "?"

    if from_at is not None:
        predicates.append(f"[FailureAt] >= {bind('@from_at', 'datetimeoffset(3)', from_at.isoformat())}")
    if to_at is not None:
        predicates.append(f"[FailureAt] < {bind('@to_at', 'datetimeoffset(3)', to_at.isoformat())}")
    if plan.event_types:
        predicates.append(f"[EventType] = {bind('@event_type', 'nvarchar(80)', plan.event_types[0])}")
    if plan.outcomes:
        predicates.append(f"[FinalOutcome] = {bind('@final_outcome', 'nvarchar(20)', plan.outcomes[0])}")
    if plan.connector_name:
        predicates.append(
            "([JobName] = " + bind("@connector_name", "nvarchar(255)", plan.connector_name)
            + " OR [ConnectorName] = " + bind("@connector_name_current", "nvarchar(255)", plan.connector_name) + ")"
        )
    if plan.error_code:
        predicates.append(f"[ErrorCode] = {bind('@error_code', 'nvarchar(20)', plan.error_code)}")
    if "failure_code" in plan.group_by:
        predicates.append("[ErrorCode] IS NOT NULL")

    where = " AND ".join(predicates) if predicates else "1 = 1"
    dimensions = [
        f"{column} AS [{alias}]" for field in plan.group_by for column, alias in (_DIMENSION_COLUMNS[field],)
    ]
    group_columns = [column for field in plan.group_by for column, _ in (_DIMENSION_COLUMNS[field],)]
    group_by = ", ".join(group_columns)

    metric_selects = [_metric_expression(metric.name) for metric in plan.metrics]
    if any(metric.name == "recovery_rate_percent" for metric in plan.metrics):
        metric_selects.extend([
            "COUNT(DISTINCT CASE WHEN [FinalOutcome] = 'RECOVERED' THEN [IncidentId] END) AS [recovery_rate_numerator]",
            "COUNT(DISTINCT [IncidentId]) AS [recovery_rate_denominator]",
        ])
    # One deterministic evidence identifier is sufficient to prove the source
    # row for a grouped metric.  Returning every incident id would make a
    # legitimate high-volume group exceed the response byte limit and does not
    # improve the count, rank, or tie evidence exposed to the user.
    grouped_selects = ",\n        ".join([*dimensions, *metric_selects, "MIN(CONVERT(nvarchar(36), [IncidentId])) AS [evidence_ids]"])
    grouped_group = f"\n    GROUP BY {group_by}" if group_by else ""
    public_order = _METRIC_COLUMNS[plan.order_by]
    direction = "DESC" if plan.direction == "desc" else "ASC"
    # A deterministic name is only a stable presentation order.  It must not
    # decide whether a member belongs to a rank; DENSE_RANK does that first.
    tie_order = ", ".join(f"[{alias}] ASC" for _, alias in (_DIMENSION_COLUMNS[field] for field in plan.group_by))
    if not tie_order:
        tie_order = "[evidence_ids] ASC"

    ranking = bool(plan.group_by and plan.limit)
    rank_selects = ""
    rank_filter = ""
    if ranking:
        rank_selects = (
            f",\n        DENSE_RANK() OVER (ORDER BY [{public_order}] {direction}) AS [rank]"
            f",\n        COUNT(1) OVER (PARTITION BY [{public_order}]) AS [tie_count]"
            f",\n        ROW_NUMBER() OVER (ORDER BY [{public_order}] {direction}, {tie_order}) AS [row_number]"
        )
        rank_filter = "WHERE [row_number] <= ?" if plan.tie_policy == "exact_limit" else "WHERE [rank] <= ?"
    # This placeholder appears before the rank predicate in the final SQL, so
    # bind it first.  pyodbc binds positionally even though the display form
    # uses readable parameter names.
    transport_limit = bind("@result_limit", "int", row_limit)
    if ranking:
        bind("@rank_limit", "int", plan.limit)
    result_shape = [alias for field in plan.group_by for _, alias in (_DIMENSION_COLUMNS[field],)]
    result_shape.extend(_METRIC_COLUMNS[metric.name] for metric in plan.metrics)
    if any(metric.name == "recovery_rate_percent" for metric in plan.metrics):
        result_shape.extend(["recovery_rate_numerator", "recovery_rate_denominator"])
    if ranking:
        result_shape.extend(["rank", "tie_count"])
    final_columns = [*result_shape, "evidence_ids"]
    if ranking:
        final_columns.append("row_number")
    projection = ", ".join(f"[{field}]" for field in final_columns)

    statement = f"""WITH [filtered] AS (
    SELECT [IncidentId], [JobName], [ConnectorName], [FailureAt], [RecoveredAt],
           [FinalOutcome], [EventType], [QueueStatus], [ErrorCode]
    FROM {_SOURCE}
    WHERE {where}
), [grouped] AS (
    SELECT
        {grouped_selects}
    FROM [filtered]{grouped_group}
), [ranked] AS (
    SELECT *{rank_selects}
    FROM [grouped]
)
SELECT TOP ({transport_limit}) {projection}
FROM [ranked]
{rank_filter}
ORDER BY [{public_order}] {direction}, {tie_order};"""
    if not is_read_only_incident_query(statement):
        raise ValueError("compiled SQL did not pass the read-only guard")
    return ExecutedTsql(statement=statement, parameters=tuple(parameters), result_shape=tuple(result_shape))


def is_read_only_incident_query(statement: str) -> bool:
    """Defence in depth for the repository boundary.

    This intentionally recognizes the single compiler-generated CTE shape;
    unlike a general SQL parser it does not accept arbitrary ``SELECT``.
    """

    normalized = " ".join(statement.upper().split())
    # Do not mistake the semicolon used as the bounded evidence-id delimiter
    # for a second SQL statement.  The compiler always escapes string values,
    # and this guard only accepts its fixed CTE source shape.
    without_literals = re.sub(r"N?'(?:''|[^'])*'", "''", normalized)
    banned = (" INSERT ", " UPDATE ", " DELETE ", " MERGE ", " DROP ", " ALTER ", " CREATE ", " EXEC ", " TRUNCATE ")
    return (
        normalized.startswith("WITH [FILTERED] AS")
        and "FROM [DBO].[VCONNECTORINCIDENTFACTS]" in normalized
        and all(word not in normalized for word in banned)
        and without_literals.count(";") == 1
    )


def _metric_expression(name: str) -> str:
    alias = _METRIC_COLUMNS[name]
    if name == "failure_count":
        expression = "COUNT(DISTINCT [IncidentId])"
    elif name == "recovered_count":
        expression = "COUNT(DISTINCT CASE WHEN [FinalOutcome] = 'RECOVERED' THEN [IncidentId] END)"
    elif name == "open_count":
        expression = "COUNT(DISTINCT CASE WHEN [FinalOutcome] = 'OPEN' THEN [IncidentId] END)"
    elif name == "average_recovery_minutes":
        expression = (
            "AVG(CASE WHEN [FinalOutcome] = 'RECOVERED' AND [QueueStatus] = 'COMPLETED' "
            "AND [FailureAt] IS NOT NULL AND [RecoveredAt] IS NOT NULL "
            "AND DATEDIFF_BIG(second, [FailureAt], [RecoveredAt]) >= 0 "
            "THEN CAST(DATEDIFF_BIG(second, [FailureAt], [RecoveredAt]) / 60.0 AS decimal(18, 2)) END)"
        )
    elif name == "recovery_rate_percent":
        expression = (
            "CAST(100.0 * COUNT(DISTINCT CASE WHEN [FinalOutcome] = 'RECOVERED' THEN [IncidentId] END) "
            "/ NULLIF(COUNT(DISTINCT [IncidentId]), 0) AS decimal(9, 2))"
        )
    else:  # pragma: no cover - protected by ``compile_incident_query``.
        raise ValueError("unsupported metric")
    return f"{expression} AS [{alias}]"


def _declaration(parameter: QueryParameter) -> str:
    if parameter.value is None:
        literal = "NULL"
    elif isinstance(parameter.value, int):
        literal = str(parameter.value)
    else:
        literal = "N'" + parameter.value.replace("'", "''") + "'"
    return f"DECLARE {parameter.name} {parameter.type} = {literal};"
