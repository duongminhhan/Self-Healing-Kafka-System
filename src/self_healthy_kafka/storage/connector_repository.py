from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime
from typing import Any

from self_healthy_kafka.domain.healing import ConnectorJob
from self_healthy_kafka.healing.config_resolution import (
    has_external_placeholders,
    resolve_connector_config,
)
from self_healthy_kafka.healing.phases import EventType, Phase
from self_healthy_kafka.storage.common import (
    dict_or_empty,
    json_value,
    parse_datetime,
    row_to_dict,
    rows_to_dicts,
)

PHYSICAL_CONNECTOR_FIELDS = {
    "job_name",
    "connector_name",
    "connector_type",
    "is_active",
    "level",
    "config_template",
    "config_id",
    "failed_count",
    "failed_connector",
    "failed_task",
    "last_scn",
    "last_commit_scn",
    "last_failed_at",
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
    """Loads active connector rows and derives healing state from log history."""

    def __init__(self, get_conn: Callable[[], Any]):
        self._get_conn = get_conn

    def list_connectors(self) -> list[ConnectorJob]:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spGetConnectorContext @IncludeRuntimeConfig = 0"
                )
                rows = rows_to_dicts(cur)
                return [self._hydrate_connector_row(row) for row in rows]

    def list_connector_activity(self) -> list[dict[str, Any]]:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spGetConnectorContext "
                    "@ActiveOnly = 0, @IncludeRuntimeConfig = 0, "
                    "@IncludeHealingState = 0"
                )
                return rows_to_dicts(cur)

    def get_connector(
        self,
        connector_name: str,
        *,
        include_runtime_config: bool = False,
    ) -> ConnectorJob | None:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spGetConnectorContext "
                    f"@ConnectorName = ?, @IncludeRuntimeConfig = "
                    f"{int(include_runtime_config)}",
                    (connector_name,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return self._hydrate_connector_row(row_to_dict(cur, row))

    def get_connector_by_id(
        self,
        connector_id: Any,
        *,
        include_runtime_config: bool = False,
    ) -> ConnectorJob | None:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spGetConnectorContext "
                    f"@ConnectorId = ?, @IncludeRuntimeConfig = "
                    f"{int(include_runtime_config)}",
                    (connector_id,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return self._hydrate_connector_row(row_to_dict(cur, row))

    def update_connector_fields(self, connector_id: Any, **fields: Any) -> None:
        if not fields:
            return
        unknown = set(fields) - PHYSICAL_CONNECTOR_FIELDS
        if unknown:
            raise ValueError(f"unknown connector field(s): {', '.join(sorted(unknown))}")

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spUpdateConnector "
                    "@ConnectorId = ?, @Fields = ?",
                    (connector_id, json_value(fields)),
                )

    def ensure_active_incident(self, connector_id: Any) -> str:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spGetConnectorContext "
                    "@ConnectorId = ?, @IncludeRuntimeConfig = 0",
                    (connector_id,),
                )
                row = cur.fetchone()
                context = row_to_dict(cur, row) if row else {}
                incident_id = context.get("active_incident_id")
        return incident_id or str(uuid.uuid4())

    def _hydrate_connector_row(self, row: dict[str, Any]) -> ConnectorJob:
        credential = row.pop("credential", None)
        kafka_server = row.pop("kafka_server", None)
        active_incident_id = row.get("active_incident_id")
        if not int(row.get("failed_count") or 0):
            active_incident_id = None
        latest_log = _latest_log_from_context(row, active_incident_id)

        row["config_template"] = dict_or_empty(row.get("config_template"))
        if has_external_placeholders(row["config_template"]) and (
            credential is not None or kafka_server is not None
        ):
            row["active_config"] = resolve_connector_config(
                row["config_template"],
                config_id=row.get("config_id"),
                load_credential=lambda config_id: _required_runtime_value(
                    credential,
                    f"spGetConnectorContext returned no credential for "
                    f"ConfigId={config_id}",
                ),
                load_kafka_server=lambda: _required_runtime_value(
                    kafka_server,
                    "spGetConnectorContext returned no Kafka server",
                ),
            )
        row["active_incident_id"] = active_incident_id
        row["latest_event_type"] = latest_log.get("event_type")
        row["latest_event_at"] = latest_log.get("created_at")
        row["latest_event_details"] = latest_log.get("details") or {}
        row["latest_has_next_step"] = latest_log.get("has_next_step")
        row["current_phase"] = _derive_phase(row, latest_log)
        row["task_restart_count"] = int(row.get("task_restart_count") or 0)
        row["connector_restart_count"] = int(
            row.get("connector_restart_count") or 0
        )
        row["recreate_with_offset_count"] = int(
            row.get("recreate_with_offset_count") or 0
        )
        row["recreate_with_offset_timeout_count"] = int(
            row.get("recreate_with_offset_timeout_count") or 0
        )
        row["recreate_without_offset_count"] = int(
            row.get("recreate_without_offset_count") or 0
        )
        row["last_failed_task_ids"] = _latest_task_ids(latest_log)
        row["last_checked_at"] = _parse_log_detail_dt(latest_log, "checked_at")
        row["last_error"] = latest_log.get("message")
        for key in (
            "latest_attempt_no",
            "latest_message",
        ):
            row.pop(key, None)
        return ConnectorJob.from_mapping(row)


def _latest_log_from_context(
    row: dict[str, Any],
    active_incident_id: Any,
) -> dict[str, Any]:
    if not active_incident_id or not row.get("latest_event_type"):
        return {}
    return {
        "event_type": row.get("latest_event_type"),
        "attempt_no": row.get("latest_attempt_no"),
        "has_next_step": row.get("latest_has_next_step"),
        "message": row.get("latest_message"),
        "details": dict_or_empty(row.get("latest_event_details")),
        "created_at": row.get("latest_event_at"),
    }


def _required_runtime_value(value: Any, error_message: str) -> Any:
    if value is None or not str(value).strip():
        raise ValueError(error_message)
    return value


def _derive_phase(row: dict[str, Any], latest_log: dict[str, Any]) -> str:
    if not int(row.get("failed_count") or 0):
        return Phase.HEALTHY
    event_type = latest_log.get("event_type")
    if event_type in PHASE_BY_EVENT:
        return PHASE_BY_EVENT[event_type]
    return Phase.FAILED_DEBOUNCE


def _latest_task_ids(latest_log: dict[str, Any]) -> list[int]:
    details = dict_or_empty(latest_log.get("details"))
    raw_ids = details.get("task_ids") or []
    return [int(task_id) for task_id in raw_ids if task_id is not None]


def _parse_log_detail_dt(latest_log: dict[str, Any], key: str) -> datetime | None:
    details = dict_or_empty(latest_log.get("details"))
    value = details.get(key)
    if not value:
        return None
    return parse_datetime(value)
