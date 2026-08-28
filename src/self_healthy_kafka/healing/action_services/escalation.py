from __future__ import annotations

from typing import Any

from self_healthy_kafka.domain.healing import HealingStep
from self_healthy_kafka.domain.models import HealthResult
from self_healthy_kafka.healing.helpers import (
    failure_details,
    failure_message,
    incident_id,
)
from self_healthy_kafka.healing.phases import EventType
from self_healthy_kafka.storage.protocols import HealingStore


class EscalationExecutor:
    def __init__(
        self,
        *,
        db: HealingStore,
        failure_confirm_checks: int,
        max_failed_count: int,
    ):
        self._db = db
        self._task_failed_count = failure_confirm_checks + 1
        self._max_failed_count = max_failed_count

    def escalate(self, job, result: HealthResult) -> None:
        active_incident = incident_id(job) or self._db.ensure_active_incident(job["id"])
        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=job["connector_name"],
            job_name=job.get("job_name"),
            incident_id=active_incident,
            event_type=EventType.HEALING_ESCALATED,
            healing_step=HealingStep.RECREATE_WITHOUT_OFFSET,
            has_next_step=False,
            severity="CRITICAL",
            message=failure_message(result),
            details=failure_details(
                result,
                action="Automated healing exhausted",
                phase=job.get("current_phase"),
            ),
        )
        self._db.complete(job["id"], "ESCALATED")

    def level_limit_reached(
        self,
        job,
        result: HealthResult,
        incident_id_value: str,
        *,
        required_level: int,
        attempted_event: str,
    ) -> None:
        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=job["connector_name"],
            job_name=job.get("job_name"),
            incident_id=incident_id_value,
            event_type=EventType.HEALING_LEVEL_LIMIT_REACHED,
            healing_step=required_level,
            has_next_step=False,
            severity="CRITICAL",
            message=(
                f"Connector level {job.get('level')} does not allow "
                f"{attempted_event}; manual configuration required"
            ),
            details=failure_details(
                result,
                required_level=required_level,
                connector_level=job.get("level"),
                attempted_event=attempted_event,
            ),
        )
        self._db.complete(job["id"], "ESCALATED")

    def _clamp(self, value: Any) -> int:
        return max(0, min(int(value or 0), self._max_failed_count))
