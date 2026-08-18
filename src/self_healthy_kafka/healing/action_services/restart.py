from __future__ import annotations

from typing import Any

from self_healthy_kafka.connect.client import KafkaConnectClient
from self_healthy_kafka.domain.healing import HealingStep
from self_healthy_kafka.domain.models import HealthResult
from self_healthy_kafka.healing.helpers import failure_details, failure_message
from self_healthy_kafka.healing.phases import EventType
from self_healthy_kafka.storage.protocols import HealingStore


class RestartExecutor:
    def __init__(
        self,
        *,
        client: KafkaConnectClient,
        db: HealingStore,
        post_restart_wait_seconds: int,
        failure_confirm_checks: int,
        max_failed_count: int,
    ):
        self._client = client
        self._db = db
        self._post_restart_wait_seconds = post_restart_wait_seconds
        self._task_failed_count = failure_confirm_checks + 1
        self._max_failed_count = max_failed_count

    def restart_failed_tasks(
        self,
        job,
        result: HealthResult,
        incident_id: str,
    ) -> None:
        task_ids = result.failed_task_ids or list(
            job.get("last_failed_task_ids") or []
        )
        failed_count = self._clamp(int(job.get("failed_count") or 0) + 1)
        self._client.restart_connector(job["connector_name"], only_failed=True)
        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=job["connector_name"],
            job_name=job.get("job_name"),
            incident_id=incident_id,
            event_type=EventType.TASK_RESTART,
            attempt_no=int(job.get("task_restart_count") or 0) + 1,
            healing_step=HealingStep.RESTART,
            has_next_step=True,
            severity="WARNING",
            message=failure_message(result),
            details=failure_details(
                result,
                action="Restarted failed connector tasks",
                task_ids=task_ids,
                failed_count=failed_count,
                post_restart_wait_seconds=self._post_restart_wait_seconds,
                kafka_connect_endpoint=(
                    f"/connectors/{job['connector_name']}/restart"
                    "?includeTasks=true&onlyFailed=true"
                ),
            ),
        )
        fields: dict[str, Any] = {
            "failed_count": failed_count,
            "last_failed_at": result.checked_at,
        }
        if failed_count == self._task_failed_count:
            fields.update(failed_task=True, failed_connector=False)
        if failed_count >= self._max_failed_count:
            fields.update(failed_task=True, failed_connector=True)
        self._db.update_connector_fields(job["id"], **fields)

    def restart_connector(
        self,
        job,
        result: HealthResult,
        incident_id: str,
    ) -> None:
        failed_count = self._clamp(int(job.get("failed_count") or 0) + 1)
        if job.get("failed_task"):
            failed_count = self._max_failed_count
        self._client.restart_connector(job["connector_name"], only_failed=False)
        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=job["connector_name"],
            job_name=job.get("job_name"),
            incident_id=incident_id,
            event_type=EventType.CONNECTOR_RESTART,
            attempt_no=int(job.get("connector_restart_count") or 0) + 1,
            healing_step=HealingStep.RESTART,
            has_next_step=True,
            severity="WARNING",
            message=failure_message(result),
            details=failure_details(
                result,
                action="Restarted full connector",
                failed_count=failed_count,
                post_restart_wait_seconds=self._post_restart_wait_seconds,
            ),
        )
        fields: dict[str, Any] = {
            "failed_count": failed_count,
            "failed_connector": True,
            "last_failed_at": result.checked_at,
        }
        if failed_count >= self._max_failed_count:
            fields.update(failed_task=True, failed_connector=True)
        self._db.update_connector_fields(job["id"], **fields)

    def _clamp(self, value: Any) -> int:
        return max(0, min(int(value or 0), self._max_failed_count))

