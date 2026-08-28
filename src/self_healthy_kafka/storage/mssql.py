from __future__ import annotations

from datetime import datetime
from typing import Any

from self_healthy_kafka.domain.healing import ConnectorJob
from self_healthy_kafka.storage.common import MssqlConnection
from self_healthy_kafka.storage.connector_repository import MssqlConnectorRepository
from self_healthy_kafka.storage.log_repository import MssqlConnectorLogRepository


class HealingRepository:
    """Microsoft SQL Server facade for connector state and healing audit logs."""

    def __init__(self, connection_string: str, timeout_seconds: int = 5):
        self._connection_string = connection_string
        self._timeout_seconds = timeout_seconds
        self._connector_repo = MssqlConnectorRepository(self._get_conn)
        self._log_repo = MssqlConnectorLogRepository(self._get_conn)

    def _get_conn(self):
        return MssqlConnection(
            self._connection_string,
            timeout_seconds=self._timeout_seconds,
        )

    @property
    def _connectors(self) -> MssqlConnectorRepository:
        repository = getattr(self, "_connector_repo", None)
        return repository or MssqlConnectorRepository(self._get_conn)

    @property
    def _logs(self) -> MssqlConnectorLogRepository:
        repository = getattr(self, "_log_repo", None)
        return repository or MssqlConnectorLogRepository(self._get_conn)

    def list_connectors(self) -> list[ConnectorJob]:
        return self._connectors.list_connectors()

    def get_connector(
        self,
        connector_name: str,
        *,
        include_runtime_config: bool = False,
    ) -> ConnectorJob | None:
        return self._connectors.get_connector(
            connector_name,
            include_runtime_config=include_runtime_config,
        )

    def get_connector_by_id(
        self,
        connector_id: Any,
        *,
        include_runtime_config: bool = False,
    ) -> ConnectorJob | None:
        return self._connectors.get_connector_by_id(
            connector_id,
            include_runtime_config=include_runtime_config,
        )

    def update_queue_fields(self, queue_id: Any, **fields: Any) -> None:
        self._connectors.update_queue_fields(queue_id, **fields)

    def enqueue_connector(
        self,
        *,
        root_connector_name: str,
        current_connector_name: str,
        connector_class: str | None,
        healing_mode: str,
    ) -> ConnectorJob:
        return self._connectors.enqueue_connector(
            root_connector_name=root_connector_name,
            current_connector_name=current_connector_name,
            connector_class=connector_class,
            healing_mode=healing_mode,
        )

    def start_processing(self, queue_id: Any) -> None:
        self._connectors.start_processing(queue_id)

    def wait_for_next_attempt(self, queue_id: Any, next_attempt_at: datetime) -> None:
        self._connectors.wait_for_next_attempt(queue_id, next_attempt_at)

    def complete(self, queue_id: Any, outcome: str) -> None:
        self._connectors.complete(queue_id, outcome)

    def ensure_active_incident(self, connector_id: Any) -> str:
        return self._connectors.ensure_active_incident(connector_id)

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
    ) -> None:
        self._logs.record_connector_log(
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
            task_id=task_id,
            scn=scn,
            commit_scn=commit_scn,
            details=details,
        )

    def close(self) -> None:
        return None
