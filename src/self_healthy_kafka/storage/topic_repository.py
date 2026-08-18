from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from self_healthy_kafka.monitoring.metrics import record_topic_lag_event
from self_healthy_kafka.storage.common import json_value, rows_to_dicts

logger = logging.getLogger(__name__)


class MssqlTopicLagRepository:
    """Loads topic lag jobs and persists threshold transition logs."""

    def __init__(self, get_conn: Callable[[], Any]):
        self._get_conn = get_conn

    def list_monitored_topics(self, default_threshold_seconds: int) -> list[dict[str, Any]]:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("EXEC dbo.spGetMonitoredTopics")
                rows = []
                for item in rows_to_dicts(cur):
                    item["idle_threshold_seconds"] = default_threshold_seconds
                    rows.append(item)
                return rows

    def update_topic_lag_state(
        self,
        *,
        topic_lag_job_id: str,
        last_end_offset: int,
        last_message_at,
        is_over_threshold: bool,
    ) -> None:
        sql = (
            "EXEC dbo.spUpdateTopicLagState "
            "@TopicLagJobId = ?, @LastEndOffset = ?, "
            "@LastMessageAt = ?, @IsOverThreshold = ?"
        )
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        topic_lag_job_id,
                        last_end_offset,
                        last_message_at,
                        is_over_threshold,
                    ),
                )

    def reset_after_connector_recreate(
        self,
        *,
        connector_id: str,
        old_connector_name: str,
        new_connector_name: str,
    ) -> int:
        """
        Close active lag state for the previous physical connector name and
        reset jobs so the next poll evaluates the replacement connector fresh.
        """
        select_sql = (
            "EXEC dbo.spGetMonitoredTopics "
            "@ConnectorId = ?, @OverThresholdOnly = 1"
        )
        insert_sql = (
            "EXEC dbo.spInsertTopicLagLog "
            "@TopicLagJobId = ?, @ConnectorId = ?, @JobName = ?, "
            "@ConnectorName = ?, @TopicName = ?, @EndOffset = ?, "
            "@EventStatus = ?, @Message = ?, @Details = ?"
        )
        reset_sql = (
            "EXEC dbo.spUpdateTopicLagState "
            "@ConnectorId = ?, @ResetConnectorTopics = 1"
        )
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(select_sql, (connector_id,))
                rows = rows_to_dicts(cur)
                for row in rows:
                    topic_name = row["topic_name"]
                    message = (
                        f"Topic lag state for {topic_name} was closed because "
                        f"connector {old_connector_name} was recreated as "
                        f"{new_connector_name}"
                    )
                    details = {
                        "old_connector_name": old_connector_name,
                        "new_connector_name": new_connector_name,
                        "lag_condition": "connector_recreated",
                    }
                    cur.execute(
                        insert_sql,
                        (
                            row["topic_lag_job_id"],
                            connector_id,
                            row["job_name"],
                            old_connector_name,
                            topic_name,
                            row.get("last_end_offset"),
                            "SUPERSEDED",
                            message,
                            json_value(details),
                        ),
                    )
                    _emit_topic_lag_log(
                        connector_name=old_connector_name,
                        topic_name=topic_name,
                        event_status="SUPERSEDED",
                        message=message,
                        details=details,
                    )
                cur.execute(reset_sql, (connector_id,))
        return len(rows)

    def record_topic_lag_log(
        self,
        *,
        topic_lag_job_id: str | None,
        connector_id: str | None,
        job_name: str,
        connector_name: str | None,
        topic_name: str,
        event_status: str,
        message: str,
        end_offset: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        sql = (
            "EXEC dbo.spInsertTopicLagLog "
            "@TopicLagJobId = ?, @ConnectorId = ?, @JobName = ?, "
            "@ConnectorName = ?, @TopicName = ?, @EndOffset = ?, "
            "@EventStatus = ?, @Message = ?, @Details = ?"
        )
        payload = details or {}
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        topic_lag_job_id,
                        connector_id,
                        job_name,
                        connector_name,
                        topic_name,
                        end_offset,
                        event_status,
                        message,
                        json_value(payload),
                    ),
                )
        _emit_topic_lag_log(
            connector_name=connector_name,
            topic_name=topic_name,
            event_status=event_status,
            message=message,
            details=payload,
        )


def _emit_topic_lag_log(
    *,
    connector_name: str | None,
    topic_name: str,
    event_status: str,
    message: str,
    details: dict[str, Any],
) -> None:
    event = f"TOPIC_LAG_{event_status}"
    record_topic_lag_event(
        connector_name=connector_name,
        topic_name=topic_name,
        event_status=event_status,
        details=details,
    )
    log_fn = logger.info if event_status == "RECOVERED" else logger.warning
    log_fn(
        message,
        extra={
            "event": event,
            "connector_name": connector_name,
            "topic": topic_name,
            "event_status": event_status,
            "details": details,
        },
    )
