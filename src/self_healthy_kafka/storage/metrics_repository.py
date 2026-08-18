from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from self_healthy_kafka.storage.common import rows_to_dicts

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MetricQuery:
    procedure: str
    sql: str
    fields: tuple[str, ...]
    uses_window: bool = False


METRIC_QUERIES = {
    "healing_attempt": MetricQuery(
        "spGetHealingAttemptMetrics",
        "EXEC dbo.spGetHealingAttemptMetrics",
        (
            "connector_name",
            "base_connector_name",
            "event_type",
            "healing_step",
            "total_attempts",
        ),
    ),
    "healing_recovered": MetricQuery(
        "spGetHealingRecoveredMetrics",
        "EXEC dbo.spGetHealingRecoveredMetrics",
        (
            "connector_name",
            "last_healing_recovered_timestamp_seconds",
        ),
        uses_window=True,
    ),
    "healing_escalated": MetricQuery(
        "spGetHealingEscalatedMetrics",
        "EXEC dbo.spGetHealingEscalatedMetrics",
        (
            "connector_name",
            "event_type",
            "last_healing_escalated_timestamp_seconds",
        ),
        uses_window=True,
    ),
    "topic_lag": MetricQuery(
        "spGetTopicLagStateMetrics",
        "EXEC dbo.spGetTopicLagStateMetrics",
        (
            "connector_name",
            "topic_name",
            "is_over_threshold",
            "last_message_timestamp_seconds",
        ),
    ),
    "topic_lag_event": MetricQuery(
        "spGetTopicLagEventMetrics",
        "EXEC dbo.spGetTopicLagEventMetrics",
        (
            "connector_name",
            "topic_name",
            "event_status",
            "condition",
            "total_events",
        ),
        uses_window=True,
    ),
}

SYNC_METRIC_GROUPS = (
    "healing_attempt",
    "healing_recovered",
    "healing_escalated",
    "topic_lag_event",
)


class MssqlOperationalMetricsRepository:
    """Loads DB-backed metric groups through isolated stored procedures."""

    def __init__(self, get_conn: Callable[[], Any]):
        self._get_conn = get_conn

    def load(
        self,
        window_minutes: int = 15,
        metric_group: str | None = None,
    ) -> dict[str, list[dict[str, Any]] | None]:
        if metric_group is not None and metric_group not in METRIC_QUERIES:
            raise ValueError(f"unsupported operational metric group: {metric_group}")

        grouped: dict[str, list[dict[str, Any]] | None] = {group: [] for group in METRIC_QUERIES}
        groups = (metric_group,) if metric_group is not None else SYNC_METRIC_GROUPS
        for group in groups:
            started_at = time.monotonic()
            try:
                grouped[group] = self._load_group(
                    group,
                    window_minutes=window_minutes,
                )
            except Exception as exc:
                if metric_group is not None:
                    raise
                grouped[group] = None
                logger.warning(
                    "DB-backed metric group synchronization failed: %s",
                    exc,
                    extra={
                        "event": "db_metric_group_sync_failed",
                        "metric_group": group,
                        "procedure": METRIC_QUERIES[group].procedure,
                        "duration_ms": round(
                            (time.monotonic() - started_at) * 1000,
                            2,
                        ),
                        "row_count": 0,
                    },
                )
        return grouped

    def _load_group(
        self,
        metric_group: str,
        *,
        window_minutes: int,
    ) -> list[dict[str, Any]]:
        query = METRIC_QUERIES[metric_group]
        sql = query.sql
        params = None
        if query.uses_window:
            sql += " @WindowMinutes = ?"
            params = (window_minutes,)

        started_at = time.monotonic()
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                if params is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, params)
                rows = rows_to_dicts(cur)

        result = [{field: row.get(field) for field in query.fields} for row in rows]
        logger.info(
            "DB-backed metric group synchronized",
            extra={
                "event": "db_metric_group_synchronized",
                "metric_group": metric_group,
                "procedure": query.procedure,
                "duration_ms": round((time.monotonic() - started_at) * 1000, 2),
                "row_count": len(result),
            },
        )
        return result
