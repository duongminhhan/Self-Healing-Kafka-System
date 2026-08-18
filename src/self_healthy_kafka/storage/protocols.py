from __future__ import annotations

from typing import Any, Protocol

from self_healthy_kafka.domain.healing import ConnectorJob


class HealingStore(Protocol):
    def list_connectors(self) -> list[ConnectorJob]: ...
    def get_connector(
        self,
        connector_name: str,
        *,
        include_runtime_config: bool = False,
    ) -> ConnectorJob | None: ...
    def get_connector_by_id(
        self,
        connector_id: Any,
        *,
        include_runtime_config: bool = False,
    ) -> ConnectorJob | None: ...
    def update_connector_fields(self, connector_id: Any, **fields: Any) -> None: ...
    def ensure_active_incident(self, connector_id: Any) -> str: ...
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
    ) -> None: ...
    def get_operational_metrics(
        self,
        window_minutes: int = 15,
    ) -> dict[str, list[dict[str, Any]] | None]: ...
    def reset_topic_lag_after_connector_recreate(
        self,
        *,
        connector_id: str,
        old_connector_name: str,
        new_connector_name: str,
    ) -> int: ...
