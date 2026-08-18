from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

import httpx

from self_healthy_kafka.connect.client import KafkaConnectClient
from self_healthy_kafka.domain.healing import HealingStep
from self_healthy_kafka.domain.models import HealthResult
from self_healthy_kafka.healing.action_services.escalation import EscalationExecutor
from self_healthy_kafka.healing.action_services.restart import RestartExecutor
from self_healthy_kafka.healing.config_resolution import template_for_storage
from self_healthy_kafka.healing.helpers import (
    failure_details,
    failure_message,
)
from self_healthy_kafka.healing.helpers import (
    incident_id as active_incident_id,
)
from self_healthy_kafka.healing.offset_recovery import (
    build_replacement_connector,
    extract_last_scn,
    has_offsets,
    is_versioned_connector_name,
    recreate_action,
)
from self_healthy_kafka.healing.phases import EventType
from self_healthy_kafka.storage.mssql import HealingRepository

DEFAULT_MAX_FAILED_COUNT = 7
JobLike = Mapping[str, Any]
logger = logging.getLogger(__name__)


def clamp_failed_count(value: Any, maximum: int = DEFAULT_MAX_FAILED_COUNT) -> int:
    return max(0, min(int(value or 0), maximum))


class HealingActions:
    """Executes Kafka Connect recovery actions and persists their audit logs."""

    def __init__(
        self,
        *,
        client: KafkaConnectClient,
        db: HealingRepository,
        post_restart_wait_seconds: int,
        recreate_verify_wait_seconds: int,
        recreate_keep_base_connector: bool = False,
        failure_confirm_checks: int = 3,
        max_failed_count: int = DEFAULT_MAX_FAILED_COUNT,
    ):
        self._client = client
        self._db = db
        self._post_restart_wait_seconds = post_restart_wait_seconds
        self._recreate_verify_wait_seconds = recreate_verify_wait_seconds
        self._recreate_keep_base_connector = recreate_keep_base_connector
        self._task_failed_count = failure_confirm_checks + 1
        self._max_failed_count = max_failed_count
        self._restart = RestartExecutor(
            client=client,
            db=db,
            post_restart_wait_seconds=post_restart_wait_seconds,
            failure_confirm_checks=failure_confirm_checks,
            max_failed_count=max_failed_count,
        )
        self._escalation = EscalationExecutor(
            db=db,
            failure_confirm_checks=failure_confirm_checks,
            max_failed_count=max_failed_count,
        )

    def restart_failed_tasks(
        self,
        job: JobLike,
        result: HealthResult,
        incident_id: str,
    ) -> None:
        self._restart.restart_failed_tasks(job, result, incident_id)

    def restart_connector(
        self,
        job: JobLike,
        result: HealthResult,
        incident_id: str,
    ) -> None:
        self._restart.restart_connector(job, result, incident_id)

    def recreate_with_offset(
        self,
        job: JobLike,
        result: HealthResult,
        incident_id: str,
    ) -> bool:
        old_name = job["connector_name"]
        offsets = self._client.get_offsets(old_name)
        scn, commit_scn = extract_last_scn(offsets)

        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=old_name,
            job_name=job.get("job_name"),
            incident_id=incident_id,
            event_type=EventType.OFFSET_CAPTURED,
            attempt_no=int(job.get("recreate_with_offset_count") or 0) + 1,
            healing_step=HealingStep.RECREATE_WITH_OFFSET,
            has_next_step=True,
            severity="WARNING",
            message="Captured connector offsets before recreate",
            scn=scn,
            commit_scn=commit_scn,
            details={"offsets": offsets},
        )

        if not has_offsets(offsets):
            self._log_recreate_with_offset_failure(
                job,
                incident_id,
                "No connector offsets captured; cannot safely recreate with offset",
                offsets=offsets,
            )
            return False
        assert offsets is not None

        try:
            connector_name, config = self.replacement_connector(
                job,
                scn=scn,
                preserve_schema_history=True,
            )
        except ValueError as exc:
            self._log_recreate_with_offset_failure(
                job,
                incident_id,
                str(exc),
                old_connector_name=old_name,
            )
            return False

        if not self._client.stop_connector(old_name):
            self._log_recreate_with_offset_failure(
                job,
                incident_id,
                "Failed to stop old connector before creating replacement",
                old_connector_name=old_name,
                new_connector_name=connector_name,
            )
            return False

        try:
            self._client.create_connector(
                connector_name,
                config,
                initial_state="STOPPED",
            )
        except Exception as exc:
            if _is_timeout(exc):
                self._mark_recreate_with_offset_created_connector(
                    job,
                    connector_name=connector_name,
                    config=config,
                    result=result,
                    scn=scn,
                    commit_scn=commit_scn,
                )
                self._db.record_connector_log(
                    connector_id=job["id"],
                    connector_name=connector_name,
                    job_name=job.get("job_name"),
                    incident_id=incident_id,
                    event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT,
                    attempt_no=int(job.get("recreate_with_offset_timeout_count") or 0) + 1,
                    healing_step=HealingStep.RECREATE_WITH_OFFSET,
                    has_next_step=True,
                    severity="WARNING",
                    message=(
                        "Timed out while creating replacement connector; "
                        "will wait and retry recreate with offset once"
                    ),
                    details={
                        "original_connector_name": self._original_connector_name(job),
                        "old_connector_name": old_name,
                        "new_connector_name": connector_name,
                        "retry_after_seconds": self._recreate_verify_wait_seconds,
                        "error": str(exc),
                        "offsets": offsets,
                        "config_template": self._config_template_for_storage(job, config),
                    },
                )
                return False
            self._log_recreate_with_offset_failure(
                job,
                incident_id,
                str(exc),
                old_connector_name=old_name,
                new_connector_name=connector_name,
            )
            return False

        if not self._client.patch_offsets(connector_name, offsets):
            self._mark_recreate_with_offset_created_connector(
                job,
                connector_name=connector_name,
                config=config,
                result=result,
                scn=scn,
                commit_scn=commit_scn,
            )
            self._db.record_connector_log(
                connector_id=job["id"],
                connector_name=connector_name,
                job_name=job.get("job_name"),
                incident_id=incident_id,
                event_type=EventType.OFFSET_PATCH_FAILED,
                attempt_no=int(job.get("recreate_with_offset_count") or 0) + 1,
                healing_step=HealingStep.RECREATE_WITH_OFFSET,
                has_next_step=True,
                severity="ERROR",
                message="Failed to patch offsets onto stopped replacement connector",
                details={
                    "original_connector_name": self._original_connector_name(job),
                    "old_connector_name": old_name,
                    "new_connector_name": connector_name,
                },
            )
            self._log_recreate_with_offset_failure(
                job,
                incident_id,
                "Failed to patch offsets onto stopped replacement connector",
                connector_name=connector_name,
                old_connector_name=old_name,
                new_connector_name=connector_name,
            )
            return False

        if not self._client.resume_connector(connector_name):
            self._mark_recreate_with_offset_created_connector(
                job,
                connector_name=connector_name,
                config=config,
                result=result,
                scn=scn,
                commit_scn=commit_scn,
            )
            self._db.record_connector_log(
                connector_id=job["id"],
                connector_name=connector_name,
                job_name=job.get("job_name"),
                incident_id=incident_id,
                event_type=EventType.CONNECTOR_RESUME_FAILED,
                attempt_no=int(job.get("recreate_with_offset_count") or 0) + 1,
                healing_step=HealingStep.RECREATE_WITH_OFFSET,
                has_next_step=True,
                severity="ERROR",
                message="Failed to resume replacement connector after offset patch",
                details={
                    "original_connector_name": self._original_connector_name(job),
                    "old_connector_name": old_name,
                    "new_connector_name": connector_name,
                },
            )
            self._log_recreate_with_offset_failure(
                job,
                incident_id,
                "Failed to resume replacement connector after offset patch",
                connector_name=connector_name,
                old_connector_name=old_name,
                new_connector_name=connector_name,
            )
            return False

        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=connector_name,
            job_name=job.get("job_name"),
            incident_id=incident_id,
            event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET,
            attempt_no=int(job.get("recreate_with_offset_count") or 0) + 1,
            healing_step=HealingStep.RECREATE_WITH_OFFSET,
            has_next_step=True,
            severity="WARNING",
            message=failure_message(result),
            scn=scn,
            commit_scn=commit_scn,
            details=failure_details(
                result,
                action=recreate_action(old_name, connector_name, "with offset restore"),
                old_connector_name=old_name,
                new_connector_name=connector_name,
                original_connector_name=self._original_connector_name(job),
                schema_history_topic=config.get("schema.history.internal.kafka.topic"),
                preserve_schema_history=True,
                recreate_verify_wait_seconds=self._recreate_verify_wait_seconds,
            ),
        )
        self._db.update_connector_fields(
            job["id"],
            connector_name=connector_name,
            config_template=self._config_template_for_storage(job, config),
            last_scn=scn,
            last_commit_scn=commit_scn,
            last_failed_at=result.checked_at,
        )
        self._delete_failed_versioned_connector(job, old_name, connector_name)
        self._reset_topic_lag_after_successful_recreate(
            job,
            old_connector_name=old_name,
            new_connector_name=connector_name,
        )
        return True

    def retry_timed_out_recreate_with_offset(
        self,
        job: JobLike,
        result: HealthResult,
        incident_id: str,
    ) -> bool:
        connector_name = job["connector_name"]
        details = job.get("latest_event_details") or {}
        offsets = details.get("offsets")
        config = (
            details.get("config")
            or job.get("active_config")
            or job.get("config_template")
            or {}
        )
        attempt_no = int(job.get("recreate_with_offset_timeout_count") or 0) + 1
        if not isinstance(offsets, dict):
            self._log_recreate_with_offset_failure(
                job,
                incident_id,
                "Saved offsets are missing from recreate retry state",
                connector_name=connector_name,
            )
            return False

        try:
            connector_exists = self._client.connector_exists(connector_name)
            if connector_exists:
                if not self._client.stop_connector(connector_name):
                    self._log_recreate_with_offset_failure(
                        job,
                        incident_id,
                        "Existing replacement connector could not be stopped before offset patch",
                        connector_name=connector_name,
                        old_connector_name=details.get("old_connector_name"),
                        new_connector_name=connector_name,
                    )
                    return False
            else:
                self._client.create_connector(
                    connector_name,
                    config,
                    initial_state="STOPPED",
                )
        except Exception as exc:
            if _is_timeout(exc):
                self._db.record_connector_log(
                    connector_id=job["id"],
                    connector_name=connector_name,
                    job_name=job.get("job_name"),
                    incident_id=incident_id,
                    event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT,
                    attempt_no=attempt_no,
                    healing_step=HealingStep.RECREATE_WITH_OFFSET,
                    has_next_step=False,
                    severity="ERROR",
                    message="Timed out again while retrying replacement connector creation",
                    details={
                        "original_connector_name": self._original_connector_name(job),
                        "old_connector_name": details.get("old_connector_name"),
                        "new_connector_name": connector_name,
                        "error": str(exc),
                    },
                )
                return False
            raise

        if not self._client.patch_offsets(connector_name, offsets):
            self._db.record_connector_log(
                connector_id=job["id"],
                connector_name=connector_name,
                job_name=job.get("job_name"),
                incident_id=incident_id,
                event_type=EventType.OFFSET_PATCH_FAILED,
                attempt_no=attempt_no,
                healing_step=HealingStep.RECREATE_WITH_OFFSET,
                has_next_step=True,
                severity="ERROR",
                message="Failed to patch offsets after retrying replacement connector create",
                details={
                    "original_connector_name": self._original_connector_name(job),
                    "old_connector_name": details.get("old_connector_name"),
                    "new_connector_name": connector_name,
                },
            )
            return False

        if not self._client.resume_connector(connector_name):
            self._db.record_connector_log(
                connector_id=job["id"],
                connector_name=connector_name,
                job_name=job.get("job_name"),
                incident_id=incident_id,
                event_type=EventType.CONNECTOR_RESUME_FAILED,
                attempt_no=attempt_no,
                healing_step=HealingStep.RECREATE_WITH_OFFSET,
                has_next_step=True,
                severity="ERROR",
                message="Failed to resume replacement connector after retry",
                details={
                    "original_connector_name": self._original_connector_name(job),
                    "old_connector_name": details.get("old_connector_name"),
                    "new_connector_name": connector_name,
                },
            )
            return False

        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=connector_name,
            job_name=job.get("job_name"),
            incident_id=incident_id,
            event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET,
            attempt_no=int(job.get("recreate_with_offset_count") or 0) + 1,
            healing_step=HealingStep.RECREATE_WITH_OFFSET,
            has_next_step=True,
            severity="INFO",
            message=failure_message(result),
            details=failure_details(
                result,
                action=f"Retried replacement connector {connector_name} with offset restore",
                old_connector_name=details.get("old_connector_name"),
                new_connector_name=connector_name,
                original_connector_name=self._original_connector_name(job),
                recreate_verify_wait_seconds=self._recreate_verify_wait_seconds,
            ),
        )
        return True

    def recreate_without_offset(self, job: JobLike, result: HealthResult) -> bool:
        incident_id = active_incident_id(job) or self._db.ensure_active_incident(job["id"])
        old_name = job["connector_name"]
        connector_name, config = self.replacement_connector(
            job,
            scn=None,
            preserve_schema_history=False,
        )
        config.pop("snapshot.scn", None)

        if not self._client.stop_connector(old_name):
            self._db.record_connector_log(
                connector_id=job["id"],
                connector_name=old_name,
                job_name=job.get("job_name"),
                incident_id=incident_id,
                event_type=EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED,
                attempt_no=int(job.get("recreate_without_offset_count") or 0) + 1,
                healing_step=HealingStep.RECREATE_WITHOUT_OFFSET,
                has_next_step=False,
                severity="ERROR",
                message="Failed to stop old connector before creating replacement",
                details={
                    "original_connector_name": self._original_connector_name(job),
                    "old_connector_name": old_name,
                    "new_connector_name": connector_name,
                },
            )
            return False

        try:
            self._client.create_connector(connector_name, config)
        except Exception as exc:
            self._db.record_connector_log(
                connector_id=job["id"],
                connector_name=old_name,
                job_name=job.get("job_name"),
                incident_id=incident_id,
                event_type=EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED,
                attempt_no=int(job.get("recreate_without_offset_count") or 0) + 1,
                healing_step=HealingStep.RECREATE_WITHOUT_OFFSET,
                has_next_step=False,
                severity="ERROR",
                message=str(exc),
                details={
                    "original_connector_name": self._original_connector_name(job),
                    "old_connector_name": old_name,
                    "new_connector_name": connector_name,
                },
            )
            return False

        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=connector_name,
            job_name=job.get("job_name"),
            incident_id=incident_id,
            event_type=EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET,
            attempt_no=int(job.get("recreate_without_offset_count") or 0) + 1,
            healing_step=HealingStep.RECREATE_WITHOUT_OFFSET,
            has_next_step=True,
            severity="WARNING",
            message=failure_message(result),
            details=failure_details(
                result,
                action=recreate_action(old_name, connector_name, "without offset restore"),
                old_connector_name=old_name,
                new_connector_name=connector_name,
                original_connector_name=self._original_connector_name(job),
                schema_history_topic=config.get("schema.history.internal.kafka.topic"),
                data_loss_possible=True,
                recreate_verify_wait_seconds=self._recreate_verify_wait_seconds,
            ),
        )
        self._db.update_connector_fields(
            job["id"],
            connector_name=connector_name,
            config_template=self._config_template_for_storage(job, config),
            last_failed_at=result.checked_at,
        )
        self._delete_failed_versioned_connector(job, old_name, connector_name)
        self._reset_topic_lag_after_successful_recreate(
            job,
            old_connector_name=old_name,
            new_connector_name=connector_name,
        )
        return True

    def escalate(self, job: JobLike, result: HealthResult) -> None:
        self._escalation.escalate(job, result)

    def level_limit_reached(
        self,
        job: JobLike,
        result: HealthResult,
        incident_id: str,
        *,
        required_level: int,
        attempted_event: str,
    ) -> None:
        self._escalation.level_limit_reached(
            job,
            result,
            incident_id,
            required_level=required_level,
            attempted_event=attempted_event,
        )

    def replacement_connector(
        self,
        job: JobLike,
        scn: str | None,
        *,
        preserve_schema_history: bool,
    ) -> tuple[str, dict[str, Any]]:
        return build_replacement_connector(
            job,
            scn=scn,
            preserve_schema_history=preserve_schema_history,
        )

    def _mark_recreate_with_offset_created_connector(
        self,
        job: JobLike,
        *,
        connector_name: str,
        config: dict[str, Any],
        result: HealthResult,
        scn: str | None,
        commit_scn: str | None,
    ) -> None:
        self._db.update_connector_fields(
            job["id"],
            connector_name=connector_name,
            config_template=self._config_template_for_storage(job, config),
            last_scn=scn,
            last_commit_scn=commit_scn,
            last_failed_at=result.checked_at,
        )

    def _log_recreate_with_offset_failure(
        self,
        job: JobLike,
        incident_id: str,
        message: str,
        connector_name: str | None = None,
        **details: Any,
    ) -> None:
        details.setdefault(
            "original_connector_name",
            self._original_connector_name(job),
        )
        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=connector_name or job["connector_name"],
            job_name=job.get("job_name"),
            incident_id=incident_id,
            event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_FAILED,
            attempt_no=int(job.get("recreate_with_offset_count") or 0) + 1,
            healing_step=HealingStep.RECREATE_WITH_OFFSET,
            has_next_step=True,
            severity="ERROR",
            message=message,
            details=details,
        )

    @staticmethod
    def _config_template_for_storage(
        job: JobLike,
        config: dict[str, Any],
    ) -> dict[str, Any]:
        return template_for_storage(config, job.get("config_template") or {})

    def _reset_topic_lag_after_successful_recreate(
        self,
        job: JobLike,
        *,
        old_connector_name: str,
        new_connector_name: str,
    ) -> None:
        self._db.reset_topic_lag_after_connector_recreate(
            connector_id=job["id"],
            old_connector_name=old_connector_name,
            new_connector_name=new_connector_name,
        )

    @staticmethod
    def _original_connector_name(job: JobLike) -> str:
        details = job.get("latest_event_details") or {}
        return str(
            details.get("original_connector_name")
            or details.get("old_connector_name")
            or job["connector_name"]
        )

    def _delete_failed_versioned_connector(
        self,
        job: JobLike,
        old_connector_name: str,
        new_connector_name: str,
    ) -> None:
        if old_connector_name == new_connector_name:
            return
        if not is_versioned_connector_name(old_connector_name):
            return
        if (
            self._recreate_keep_base_connector
            and old_connector_name == self._original_connector_name(job)
        ):
            return
        try:
            self._client.delete_connector(old_connector_name)
        except Exception as exc:
            logger.warning(
                "[%s] failed to delete failed versioned connector %s: %s",
                new_connector_name,
                old_connector_name,
                exc,
                extra={
                    "event": "failed_versioned_connector_delete_failed",
                    "connector_name": new_connector_name,
                    "old_connector_name": old_connector_name,
                },
            )


def _is_timeout(exc: Exception) -> bool:
    return isinstance(exc, (TimeoutError, httpx.TimeoutException)) or "timed out" in str(exc).lower()
