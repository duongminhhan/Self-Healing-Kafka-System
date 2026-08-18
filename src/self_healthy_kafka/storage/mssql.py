from __future__ import annotations

from typing import Any

from self_healthy_kafka.domain.healing import ConnectorJob
from self_healthy_kafka.storage.common import MssqlConnection
from self_healthy_kafka.storage.connector_repository import MssqlConnectorRepository
from self_healthy_kafka.storage.log_repository import MssqlConnectorLogRepository
from self_healthy_kafka.storage.metrics_repository import (
    MssqlOperationalMetricsRepository,
)
from self_healthy_kafka.storage.topic_repository import MssqlTopicLagRepository


class HealingRepository:
    """
    Facade for Microsoft SQL Server-backed healing storage.

    The public methods are kept stable for callers, while connector state,
    connector logs, and topic idle state live in focused repository modules.
    """

    def __init__(self, connection_string: str, timeout_seconds: int = 5):
        self._connection_string = connection_string
        self._timeout_seconds = timeout_seconds
        self._connector_repo = MssqlConnectorRepository(self._get_conn)
        self._log_repo = MssqlConnectorLogRepository(self._get_conn)
        self._metrics_repo = MssqlOperationalMetricsRepository(self._get_conn)
        self._topic_repo = MssqlTopicLagRepository(self._get_conn)

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

    @property
    def _topics(self) -> MssqlTopicLagRepository:
        repository = getattr(self, "_topic_repo", None)
        return repository or MssqlTopicLagRepository(self._get_conn)

    @property
    def _metrics(self) -> MssqlOperationalMetricsRepository:
        repository = getattr(self, "_metrics_repo", None)
        return repository or MssqlOperationalMetricsRepository(self._get_conn)

    def list_connectors(self) -> list[ConnectorJob]:
        return self._connectors.list_connectors()

    def list_connector_activity(self) -> list[dict[str, Any]]:
        return self._connectors.list_connector_activity()

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

    def update_connector_fields(self, connector_id: int, **fields: Any) -> None:
        self._connectors.update_connector_fields(connector_id, **fields)

    def ensure_active_incident(self, connector_id: int) -> str:
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

    def get_operational_metrics(
        self,
        window_minutes: int = 15,
    ) -> dict[str, list[dict[str, Any]] | None]:
        return self._metrics.load(window_minutes)

    def list_monitored_topics(self, default_threshold_seconds: int) -> list[dict[str, Any]]:
        return self._topics.list_monitored_topics(default_threshold_seconds)

    def list_topic_lag_metrics(self) -> list[dict[str, Any]]:
        rows = self._metrics.load(metric_group="topic_lag")["topic_lag"]
        return rows or []

    def update_topic_lag_state(
        self,
        *,
        topic_lag_job_id: str,
        last_end_offset: int,
        last_message_at,
        is_over_threshold: bool,
    ) -> None:
        self._topics.update_topic_lag_state(
            topic_lag_job_id=topic_lag_job_id,
            last_end_offset=last_end_offset,
            last_message_at=last_message_at,
            is_over_threshold=is_over_threshold,
        )

    def record_topic_lag_log(self, **fields: Any) -> None:
        self._topics.record_topic_lag_log(**fields)

    def reset_topic_lag_after_connector_recreate(
        self,
        *,
        connector_id: str,
        old_connector_name: str,
        new_connector_name: str,
    ) -> int:
        return self._topics.reset_after_connector_recreate(
            connector_id=connector_id,
            old_connector_name=old_connector_name,
            new_connector_name=new_connector_name,
        )

    def close(self) -> None:
        return None
