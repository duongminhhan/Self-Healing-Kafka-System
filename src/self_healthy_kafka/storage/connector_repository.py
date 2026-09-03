from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from self_healthy_kafka.domain.healing import ConnectorJob
from self_healthy_kafka.healing.phases import EventType, Phase
from self_healthy_kafka.storage.common import (
    dict_or_empty,
    json_value,
    parse_datetime,
    row_to_dict,
    rows_to_dicts,
)

QUEUE_FIELDS = {
    "current_connector_name",
    "queue_status",
    "final_outcome",
    "started_at",
    "completed_at",
    "next_attempt_at",
}

PHASE_BY_EVENT = {
    EventType.TASK_RESTART: Phase.TASK_RESTARTING,
    EventType.CONNECTOR_RESTART: Phase.CONNECTOR_RESTARTING,
    EventType.CONNECTOR_RECREATE_WITH_OFFSET: Phase.RECREATE_WITH_OFFSET_VERIFYING,
    EventType.CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT: Phase.RECREATE_WITH_OFFSET_FAILED,
    EventType.CONNECTOR_RECREATE_WITH_OFFSET_FAILED: Phase.RECREATE_WITH_OFFSET_FAILED,
    EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET: Phase.RECREATE_WITHOUT_OFFSET_VERIFYING,
    EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED: Phase.RECREATE_WITHOUT_OFFSET_FAILED,
    EventType.HEALING_ESCALATED: Phase.ESCALATED,
    EventType.HEALING_LEVEL_LIMIT_REACHED: Phase.ESCALATED,
}


class MssqlConnectorRepository:
    """Persists open connector-healing incidents as queue items."""

    def __init__(self, get_conn: Callable[[], Any]):
        self._get_conn = get_conn

    def list_connectors(self) -> list[ConnectorJob]:
        return self._get_queue_items(due_only=True)

    def get_connector(
        self,
        connector_name: str,
        *,
        include_runtime_config: bool = False,
    ) -> ConnectorJob | None:
        del include_runtime_config
        rows = self._get_queue_items(connector_name=connector_name)
        return rows[0] if rows else None

    def get_connector_by_id(
        self,
        connector_id: Any,
        *,
        include_runtime_config: bool = False,
    ) -> ConnectorJob | None:
        del include_runtime_config
        rows = self._get_queue_items(queue_id=connector_id)
        return rows[0] if rows else None

    def list_queue_for_chat(
        self,
        *,
        queue_id: Any | None = None,
        connector_name: str | None = None,
    ) -> list[ConnectorJob]:
        return self._get_queue_items(
            queue_id=queue_id,
            connector_name=connector_name,
            open_only=False,
        )

    def get_failure_ranking(
        self,
        *,
        from_at: datetime | None,
        to_at: datetime | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spGetConnectorFailureRanking "
                    "@FromAt = ?, @ToAt = ?, @Limit = ?",
                    (from_at, to_at, limit),
                )
                return rows_to_dicts(cur)

    def enqueue_connector(
        self,
        *,
        root_connector_name: str,
        current_connector_name: str,
        connector_class: str | None,
        healing_mode: str,
    ) -> ConnectorJob:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spEnqueueConnectorHealing "
                    "@RootConnectorName = ?, @CurrentConnectorName = ?, "
                    "@ConnectorClass = ?, @HealingMode = ?",
                    (
                        root_connector_name,
                        current_connector_name,
                        connector_class,
                        healing_mode,
                    ),
                )
                row = cur.fetchone()
                if not row:
                    raise RuntimeError("spEnqueueConnectorHealing returned no queue item")
                return self._hydrate_queue_row(row_to_dict(cur, row))

    def update_queue_fields(self, queue_id: Any, **fields: Any) -> None:
        if not fields:
            return
        unknown = set(fields) - QUEUE_FIELDS
        if unknown:
            raise ValueError(f"unknown queue field(s): {', '.join(sorted(unknown))}")
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spUpdateConnectorHealingQueue "
                    "@QueueId = ?, @Fields = ?",
                    (queue_id, json_value(fields)),
                )

    def start_processing(self, queue_id: Any) -> None:
        self.update_queue_fields(
            queue_id,
            queue_status="PROCESSING",
            started_at=datetime.now(timezone.utc),
        )

    def wait_for_next_attempt(self, queue_id: Any, next_attempt_at: datetime) -> None:
        self.update_queue_fields(
            queue_id,
            queue_status="WAITING",
            next_attempt_at=next_attempt_at,
        )

    def complete(self, queue_id: Any, outcome: str) -> None:
        self.update_queue_fields(
            queue_id,
            queue_status="ESCALATED" if outcome == "ESCALATED" else "COMPLETED",
            final_outcome=outcome,
            completed_at=datetime.now(timezone.utc),
            next_attempt_at=None,
        )

    @staticmethod
    def ensure_active_incident(connector_id: Any) -> str:
        return str(connector_id)

    def _get_queue_items(
        self,
        *,
        queue_id: Any | None = None,
        connector_name: str | None = None,
        due_only: bool = False,
        open_only: bool = True,
    ) -> list[ConnectorJob]:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spGetConnectorHealingQueue "
                    "@QueueId = ?, @ConnectorName = ?, @OpenOnly = ?, @DueOnly = ?",
                    (queue_id, connector_name, open_only, due_only),
                )
                return [self._hydrate_queue_row(row) for row in rows_to_dicts(cur)]

    @staticmethod
    def _hydrate_queue_row(row: dict[str, Any]) -> ConnectorJob:
        latest_details = dict_or_empty(row.get("latest_event_details"))
        row["active_incident_id"] = row.get("active_incident_id") or row.get("id")
        row["active_config"] = None
        row["connector_type"] = "source"
        row["is_active"] = row.get("queue_status") not in {"COMPLETED", "ESCALATED"}
        row["failed_connector"] = False
        row["failed_task"] = bool(latest_details.get("task_ids"))
        row["last_failed_task_ids"] = _latest_task_ids(latest_details)
        row["last_checked_at"] = _parse_log_detail_dt(latest_details, "checked_at")
        row["last_error"] = row.get("latest_message")
        row["latest_event_details"] = latest_details
        row["current_phase"] = _derive_phase(row)
        return ConnectorJob.from_mapping(row)


def _derive_phase(row: dict[str, Any]) -> str:
    event_type = row.get("latest_event_type")
    if event_type in PHASE_BY_EVENT:
        return PHASE_BY_EVENT[event_type]
    return Phase.FAILED_DEBOUNCE


def _latest_task_ids(details: dict[str, Any]) -> list[int]:
    raw_ids = details.get("task_ids") or []
    return [int(task_id) for task_id in raw_ids if task_id is not None]


def _parse_log_detail_dt(details: dict[str, Any], key: str) -> datetime | None:
    value = details.get(key)
    return parse_datetime(value) if value else None
