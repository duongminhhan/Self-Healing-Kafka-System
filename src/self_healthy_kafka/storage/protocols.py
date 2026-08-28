from __future__ import annotations

from datetime import datetime
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
    def update_queue_fields(self, queue_id: Any, **fields: Any) -> None: ...
    def enqueue_connector(
        self,
        *,
        root_connector_name: str,
        current_connector_name: str,
        connector_class: str | None,
        healing_mode: str,
    ) -> ConnectorJob: ...
    def start_processing(self, queue_id: Any) -> None: ...
    def wait_for_next_attempt(self, queue_id: Any, next_attempt_at: datetime) -> None: ...
    def complete(self, queue_id: Any, outcome: str) -> None: ...
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
