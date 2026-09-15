from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

from self_healthy_kafka.redaction import redact, redact_text
from self_healthy_kafka.semantic.tsql import is_read_only_incident_query
from self_healthy_kafka.storage.common import json_value, rows_to_dicts

logger = logging.getLogger(__name__)

_COMPILED_ANALYTICS_QUERY_TIMEOUT_SECONDS = 10


class MssqlConnectorLogRepository:
    """Persists connector healing/audit events into connector_healing_logs."""

    def __init__(self, get_conn: Callable[[], Any]):
        self._get_conn = get_conn

    def record_connector_log(
        self,
        *,
        connector_name: str,
        event_type: str,
        message: str,
        severity: str = "INFO",
        job_name: str | None = None,
        connector_id: str | None = None,
        incident_id: str | None = None,
        attempt_no: int | None = None,
        healing_step: int | None = None,
        has_next_step: bool = True,
        task_id: int | None = None,
        scn: str | None = None,
        commit_scn: str | None = None,
        details: dict[str, Any] | None = None,
        **ignored: Any,
    ) -> None:
        message = redact_text(message)
        log_details = redact(_connector_log_details(
            details=details,
            severity=severity,
            task_id=task_id,
            scn=scn,
            commit_scn=commit_scn,
        ))
        if connector_id is None:
            raise ValueError("queue id is required to persist a healing log")
        sql = (
            "EXEC dbo.spInsertConnectorHealingLog "
            "@QueueId = ?, @ConnectorName = ?, @EventType = ?, "
            "@AttemptNo = ?, @HealingStep = ?, @Severity = ?, "
            "@Message = ?, @Details = ?"
        )
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        connector_id,
                        connector_name,
                        event_type,
                        attempt_no,
                        healing_step,
                        severity,
                        message,
                        json_value(log_details),
                    ),
                )
        _emit_connector_healing_log(
            connector_name=connector_name,
            event_type=event_type,
            message=message,
            severity=severity,
            job_name=job_name,
            connector_id=connector_id,
            incident_id=incident_id,
            attempt_no=attempt_no,
            healing_step=healing_step,
            has_next_step=has_next_step,
            details=log_details,
        )

    def list_for_chat(
        self,
        *,
        queue_id: str | None,
        connector_name: str | None,
        from_at: datetime | None,
        to_at: datetime | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spGetConnectorHealingLogs "
                    "@QueueId = ?, @ConnectorName = ?, @FromAt = ?, @ToAt = ?, @Limit = ?",
                    (queue_id, connector_name, from_at, to_at, limit),
                )
                return rows_to_dicts(cur)

    def search_for_chat(self, *, question: str, limit: int) -> list[dict[str, Any]]:
        """Retrieve relevant persisted Kafka Connect logs without accepting SQL."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spSearchConnectorHealingLogs @SearchText = ?, @Limit = ?",
                    (question, limit),
                )
                return rows_to_dicts(cur)

    def list_incident_facts_for_chat(
        self,
        *,
        from_at: datetime | None,
        to_at: datetime | None,
        event_type: str | None,
        final_outcome: str | None,
        connector_name: str | None,
        error_code: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Run the fixed, read-only analytics procedure with bound parameters."""
        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "EXEC dbo.spGetConnectorIncidentFacts "
                    "@FromAt = ?, @ToAt = ?, @EventType = ?, @FinalOutcome = ?, "
                    "@ConnectorName = ?, @ErrorCode = ?, @Limit = ?",
                    (
                        from_at,
                        to_at,
                        event_type,
                        final_outcome,
                        connector_name,
                        error_code,
                        limit,
                    ),
                )
                return rows_to_dicts(cur)

    def execute_compiled_incident_query(
        self,
        *,
        statement: str,
        parameters: tuple[str | int | None, ...],
    ) -> list[dict[str, Any]]:
        """Execute only the compiler's bounded analytics SELECT shape.

        The service owns compilation and this repository independently checks
        that a caller did not substitute DDL/DML, a stored procedure, or a
        different database object at the last boundary before SQL Server.
        """

        if not isinstance(statement, str) or not is_read_only_incident_query(statement):
            raise ValueError("compiled incident SQL is not an approved read-only query")
        if not isinstance(parameters, tuple) or len(parameters) > 12:
            raise ValueError("compiled incident SQL parameters are invalid")
        with self._get_conn() as conn:
            # pyodbc exposes the statement timeout on Connection, not Cursor.
            # Assigning it here makes the bound apply to the following cursor
            # execution without relying on a driver-specific cursor attribute.
            conn.timeout = _COMPILED_ANALYTICS_QUERY_TIMEOUT_SECONDS
            with conn.cursor() as cur:
                cur.execute(statement, parameters)
                return rows_to_dicts(cur)

def _connector_log_details(
    *,
    details: dict[str, Any] | None,
    severity: str | None,
    task_id: int | None,
    scn: str | None,
    commit_scn: str | None,
) -> dict[str, Any]:
    payload = dict(details or {})
    if severity:
        payload.setdefault("severity", severity)
    if task_id is not None:
        payload.setdefault("task_id", task_id)
    if scn is not None:
        payload.setdefault("scn", scn)
    if commit_scn is not None:
        payload.setdefault("commit_scn", commit_scn)
    return payload


def _emit_connector_healing_log(
    *,
    connector_name: str,
    event_type: str,
    message: str,
    severity: str,
    job_name: str | None,
    connector_id: str | None,
    incident_id: str | None,
    attempt_no: int | None,
    healing_step: int | None,
    has_next_step: bool,
    details: dict[str, Any],
) -> None:
    log_fn = logger.info
    if severity.upper() in {"ERROR", "CRITICAL"}:
        log_fn = logger.error
    elif severity.upper() == "WARNING":
        log_fn = logger.warning

    log_fn(
        message,
        extra={
            "event": event_type,
            "connector_id": connector_id,
            "connector_name": connector_name,
            "original_connector_name": details.get("original_connector_name"),
            "healing_steps": details.get("healing_steps"),
            "job_name": job_name,
            "incident_id": incident_id,
            "attempt_no": attempt_no,
            "healing_step": healing_step,
            "has_next_step": 1 if has_next_step else 0,
            "severity": severity,
            "details": details,
        },
    )
