from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

from self_healthy_kafka.storage.common import json_value

logger = logging.getLogger(__name__)


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
        log_details = _connector_log_details(
            details=details,
            severity=severity,
            task_id=task_id,
            scn=scn,
            commit_scn=commit_scn,
        )
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
