from __future__ import annotations

import logging
from typing import Optional

from self_healthy_kafka.connect.client import KafkaConnectClient
from self_healthy_kafka.domain.models import ConnectorState, HealthResult, HealthStatus

logger = logging.getLogger(__name__)


class HealthChecker:
    """
    Polls GET /connectors/{name}/status and classifies the connector based on:
    - connector.state
    - tasks[].state  (source connectors often fail at task level)
    """

    def __init__(self, client: KafkaConnectClient):
        self._client = client

    def check(self, connector_name: str) -> Optional[HealthResult]:
        try:
            status = self._client.get_status(connector_name)
        except Exception as e:
            logger.error(f"[{connector_name}] cannot reach Kafka Connect: {e}")
            return None

        if status is None:
            logger.info(
                f"[{connector_name}] connector not registered yet - waiting",
                extra={
                    "event": "connector_not_created",
                    "connector_name": connector_name,
                },
            )
            return HealthResult(
                connector_name=connector_name,
                status=HealthStatus.NOT_CREATED,
                reason="Connector not found (404) - waiting for initial creation",
            )

        return self._evaluate(status)

    def _evaluate(self, status) -> HealthResult:
        name = status.name

        if status.state == ConnectorState.FAILED:
            logger.warning(f"[{name}] FAILED - {status.trace}")
            return HealthResult(
                connector_name=name,
                status=HealthStatus.UNHEALTHY,
                reason="connector.state == FAILED",
                trace=status.trace,
            )

        if status.state in (ConnectorState.PAUSED, ConnectorState.UNASSIGNED):
            logger.warning(
                f"[{name}] {status.state.value} - alert only, no auto-restart",
                extra={
                    "event": "connector_alert_only",
                    "connector_name": name,
                    "connector_state": status.state.value,
                },
            )
            return HealthResult(
                connector_name=name,
                status=HealthStatus.HEALTHY,
                reason=f"connector.state == {status.state.value} (alert only)",
            )

        if status.state == ConnectorState.STOPPED:
            logger.info(
                f"[{name}] STOPPED - recovery depends on active healing context",
                extra={
                    "event": "connector_stopped",
                    "connector_name": name,
                    "connector_state": status.state.value,
                },
            )
            return HealthResult(
                connector_name=name,
                status=HealthStatus.STOPPED,
                reason="connector.state == STOPPED",
            )

        if status.state == ConnectorState.RUNNING:
            failed_tasks = [t for t in status.tasks if t.state != "RUNNING"]
            if failed_tasks:
                failed_ids = [t.id for t in failed_tasks]
                failed_trace = next((t.trace for t in failed_tasks if t.trace), None)
                logger.warning(
                    f"[{name}] connector RUNNING but tasks {failed_ids} FAILED"
                )
                return HealthResult(
                    connector_name=name,
                    status=HealthStatus.UNHEALTHY,
                    reason=f"tasks {failed_ids} FAILED while connector RUNNING",
                    failed_task_ids=failed_ids,
                    trace=failed_trace,
                )

        logger.debug(f"[{name}] HEALTHY - all tasks RUNNING")
        return HealthResult(
            connector_name=name,
            status=HealthStatus.HEALTHY,
            reason="All tasks RUNNING",
        )
