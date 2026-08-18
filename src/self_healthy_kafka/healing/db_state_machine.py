from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from self_healthy_kafka.connect.client import KafkaConnectClient
from self_healthy_kafka.domain.healing import (
    ConnectorJob,
    HealingStep,
    RecoveryAction,
    RecoveryPolicy,
)
from self_healthy_kafka.domain.models import HealthResult, HealthStatus
from self_healthy_kafka.healing.actions import (
    HealingActions,
    clamp_failed_count,
)
from self_healthy_kafka.healing.helpers import (
    failure_details,
    failure_message,
    latest_event_at,
    level,
    recovery_details,
    recovery_message,
    utc_now,
)
from self_healthy_kafka.healing.helpers import (
    incident_id as active_incident_id,
)
from self_healthy_kafka.healing.offset_recovery import extract_last_scn
from self_healthy_kafka.healing.phases import EventType
from self_healthy_kafka.healing.policy import (
    RECREATE_WITH_OFFSET_FAILURE_EVENTS,
    RECREATE_WITH_OFFSET_RETRY_EVENTS,
    WAIT_AFTER_RECREATE_EVENTS,
    WAIT_AFTER_RESTART_EVENTS,
    TransitionPolicy,
)
from self_healthy_kafka.health.checker import HealthChecker
from self_healthy_kafka.storage.mssql import HealingRepository

logger = logging.getLogger(__name__)

JobLike = ConnectorJob | dict[str, Any]

HEALING_MANAGED_STOPPED_EVENTS = (
    RECREATE_WITH_OFFSET_FAILURE_EVENTS
    | RECREATE_WITH_OFFSET_RETRY_EVENTS
    | {EventType.CONNECTOR_RECREATE_WITH_OFFSET}
)


@dataclass(frozen=True)
class ConnectorProcessingOutcome:
    processed: bool
    requires_followup: bool = False


class ConnectorStateMachine:
    """Orchestrates connector healing from DB rows and healing log history."""

    def __init__(
        self,
        *,
        client: KafkaConnectClient,
        checker: HealthChecker,
        db: HealingRepository,
        failure_confirm_checks: int,
        task_restart_max_attempts: int,
        connector_restart_max_attempts: int,
        post_restart_wait_seconds: int,
        recovery_healthy_confirm_seconds: int,
        recreate_verify_wait_seconds: int,
        scn_poll_interval_seconds: int,
        recreate_keep_base_connector: bool = False,
    ):
        self._client = client
        self._checker = checker
        self._db = db
        self._policy = RecoveryPolicy(
            failure_confirm_checks=failure_confirm_checks,
            task_restart_max_attempts=task_restart_max_attempts,
            connector_restart_max_attempts=min(connector_restart_max_attempts, 1),
        )
        self._transitions = TransitionPolicy(self._policy)
        self._failure_confirm_checks = self._policy.failure_confirm_checks
        self._post_restart_wait_seconds = post_restart_wait_seconds
        self._recovery_healthy_confirm_seconds = recovery_healthy_confirm_seconds
        self._recreate_verify_wait_seconds = recreate_verify_wait_seconds
        self._scn_poll_interval_seconds = scn_poll_interval_seconds
        self._recreate_keep_base_connector = recreate_keep_base_connector
        self._last_scn_fetch_at: dict[str, datetime] = {}
        self._healthy_since: dict[str, datetime] = {}
        self._connector_locks: dict[str, threading.Lock] = {}
        self._connector_locks_guard = threading.Lock()
        self._actions = HealingActions(
            client=client,
            db=db,
            post_restart_wait_seconds=post_restart_wait_seconds,
            recreate_verify_wait_seconds=recreate_verify_wait_seconds,
            recreate_keep_base_connector=recreate_keep_base_connector,
            failure_confirm_checks=self._policy.failure_confirm_checks,
            max_failed_count=self._policy.max_failed_count,
        )

    @property
    def db(self) -> HealingRepository:
        return self._db

    @property
    def client(self) -> KafkaConnectClient:
        return self._client

    def tick(self) -> list[str]:
        jobs = self._db.list_connectors()
        if not jobs:
            logger.warning(
                "no active connector jobs found in DB",
                extra={"event": "connector_jobs_empty"},
            )
            return []

        followup_connectors: list[str] = []
        for job in jobs:
            if self._client.status_circuit_open is True:
                logger.warning(
                    "Kafka Connect status circuit open; stopping reconciliation cycle",
                    extra={"event": "kafka_connect_circuit_open"},
                )
                break
            outcome = self._process_job_safely(job)
            if not outcome.requires_followup:
                continue
            refreshed = self._db.get_connector_by_id(job["id"])
            if (
                refreshed
                and refreshed.active_incident_id is not None
                and refreshed.failed_count > 0
            ):
                followup_connectors.append(refreshed.connector_name)
            if self._client.status_circuit_open is True:
                break
        return followup_connectors

    def process_connector(
        self,
        connector_name: str,
        failure_confirmed: bool = False,
    ) -> str | None:
        """
        Verify and process one connector after an external alert.

        Returns the current connector name while it has an active recovery
        state and should receive a targeted follow-up check. Recreate actions
        can change the connector name, so callers must use the returned value.
        """
        job = self._db.get_connector(connector_name)
        if not job:
            logger.warning(
                "Grafana alert does not map to an active connector",
                extra={
                    "event": "grafana_webhook_connector_not_found",
                    "connector_name": connector_name,
                },
            )
            return None
        processed = self._process_job_safely(
            job,
            failure_confirmed=failure_confirmed,
        )
        if not processed.processed:
            return connector_name
        if not processed.requires_followup:
            return None
        refreshed = self._db.get_connector_by_id(job["id"])
        if refreshed and int(refreshed.get("failed_count") or 0) > 0:
            return str(refreshed["connector_name"])
        return None

    def _process_job_safely(
        self,
        job: JobLike,
        *,
        failure_confirmed: bool = False,
    ) -> ConnectorProcessingOutcome:
        job = ConnectorJob.from_mapping(job)
        connector_name = job.connector_name or "unknown"
        lock = self._connector_lock(connector_name)
        if not lock.acquire(blocking=False):
            logger.info(
                "Connector check already in progress",
                extra={
                    "event": "connector_check_coalesced",
                    "connector_name": connector_name,
                },
            )
            return ConnectorProcessingOutcome(processed=False)
        try:
            requires_followup = self._process_job(
                job,
                failure_confirmed=failure_confirmed,
            )
        except Exception as exc:
            logger.exception(
                "[%s] state machine error", connector_name,
                extra={
                    "event": "connector_state_machine_error",
                    "connector_name": connector_name,
                },
            )
            self._db.record_connector_log(
                connector_id=job.get("id"),
                connector_name=connector_name,
                job_name=job.get("job_name"),
                incident_id=active_incident_id(job),
                event_type=EventType.STATE_MACHINE_ERROR,
                healing_step=None,
                has_next_step=True,
                severity="ERROR",
                message=str(exc),
                details={"phase": job.get("current_phase")},
            )
            return ConnectorProcessingOutcome(
                processed=True,
                requires_followup=job.failed_count > 0,
            )
        finally:
            lock.release()
        return ConnectorProcessingOutcome(
            processed=True,
            requires_followup=requires_followup,
        )

    def _connector_lock(self, connector_name: str) -> threading.Lock:
        with self._connector_locks_guard:
            return self._connector_locks.setdefault(connector_name, threading.Lock())

    def _process_job(
        self,
        job: JobLike,
        *,
        failure_confirmed: bool = False,
    ) -> bool:
        job = ConnectorJob.from_mapping(job)
        connector_name = job.connector_name
        wait_until = self._action_wait_until(job)
        if wait_until and utc_now() < wait_until:
            logger.debug(
                "[%s] waiting before next healing action", connector_name,
                extra={
                    "event": "connector_healing_waiting",
                    "connector_name": connector_name,
                    "next_action_after": wait_until.isoformat(),
                },
            )
            return job.failed_count > 0

        result = self._checker.check(connector_name)
        if result is None:
            self._handle_kafka_connect_unreachable(job)
            return job.failed_count > 0

        if (
            result.status == HealthStatus.NOT_CREATED
            and job.get("latest_event_type") in RECREATE_WITH_OFFSET_RETRY_EVENTS
        ):
            self._run_recreate_with_offset_retry_or_escalate(job, result)
            return True

        if result.status == HealthStatus.NOT_CREATED:
            self._handle_not_created(job, result)
            return job.failed_count > 0

        if result.status == HealthStatus.STOPPED:
            if self._is_healing_managed_stopped(job):
                self._handle_unhealthy(job, result)
            else:
                logger.info(
                    "[%s] connector is administratively stopped; skipping healing",
                    connector_name,
                    extra={
                        "event": "connector_admin_stopped",
                        "connector_name": connector_name,
                    },
                )
            return self._is_healing_managed_stopped(job)

        if result.is_healthy:
            self._handle_healthy(job, result)
            return job.failed_count > 0

        if failure_confirmed:
            job = self._mark_failure_confirmed_from_webhook(job, result)
        self._handle_unhealthy(job, result)
        return True

    def _mark_failure_confirmed_from_webhook(
        self,
        job: ConnectorJob,
        result: HealthResult,
    ) -> ConnectorJob:
        failed_count = max(job.failed_count, self._failure_confirm_checks)
        if failed_count == job.failed_count:
            return job
        self._db.update_connector_fields(
            job.id,
            failed_count=failed_count,
            last_failed_at=result.checked_at,
        )
        return job.copy(failed_count=failed_count)

    @staticmethod
    def _is_healing_managed_stopped(job: ConnectorJob) -> bool:
        return (
            job.active_incident_id is not None
            and job.latest_has_next_step is True
            and job.latest_event_type in HEALING_MANAGED_STOPPED_EVENTS
        )

    def _handle_kafka_connect_unreachable(self, job: JobLike) -> None:
        self._healthy_since.pop(str(job["id"]), None)
        self._db.update_connector_fields(
            job["id"],
            last_failed_at=utc_now(),
        )

    def _handle_not_created(self, job: JobLike, result: HealthResult) -> None:
        self._healthy_since.pop(str(job["id"]), None)
        self._db.update_connector_fields(
            job["id"],
            last_failed_at=result.checked_at,
        )

    def _handle_healthy(self, job: JobLike, result: HealthResult) -> None:
        latest_event = job.get("latest_event_type")

        if latest_event in WAIT_AFTER_RESTART_EVENTS | WAIT_AFTER_RECREATE_EVENTS:
            self._handle_post_action_healthy(job, result)
            return

        if int(job.get("failed_count") or 0):
            self._record_recovered(job, result)
            return

        self._refresh_connector_scn(job, result=result)

    def _handle_post_action_healthy(
        self,
        job: JobLike,
        result: HealthResult,
    ) -> None:
        action_at = latest_event_at(job)
        connector_id = str(job["id"])
        stable_started_at = self._healthy_since.get(connector_id)
        if action_at and stable_started_at and stable_started_at <= action_at:
            stable_started_at = None

        if stable_started_at is None:
            self._healthy_since[connector_id] = result.checked_at
            return

        wait_until = stable_started_at + timedelta(
            seconds=self._recovery_healthy_confirm_seconds
        )
        if utc_now() < wait_until:
            return

        self._record_recovered(job, result)

    def _record_recovered(self, job: JobLike, result: HealthResult) -> None:
        details = recovery_details(job, result)
        if details["healing_steps"] == "NO_AUTOMATED_STEP":
            self._db.update_connector_fields(
                job["id"],
                failed_count=0,
                failed_connector=False,
                failed_task=False,
            )
            self._healthy_since.pop(str(job["id"]), None)
            return
        self._delete_superseded_connector_if_needed(job, details)
        self._db.record_connector_log(
            connector_id=job["id"],
            connector_name=job["connector_name"],
            job_name=job.get("job_name"),
            incident_id=active_incident_id(job),
            event_type=EventType.HEALING_RECOVERED,
            healing_step=None,
            has_next_step=False,
            severity="INFO",
            message=recovery_message(details),
            details=details,
        )
        self._db.update_connector_fields(
            job["id"],
            failed_count=0,
            failed_connector=False,
            failed_task=False,
        )
        self._healthy_since.pop(str(job["id"]), None)

    def _delete_superseded_connector_if_needed(
        self,
        job: JobLike,
        details: dict[str, Any],
    ) -> None:
        current_name = str(job.get("connector_name") or "")
        if not current_name:
            return
        old_names = {
            str(name)
            for name in (
                details.get("original_connector_name"),
                details.get("old_connector_name"),
            )
            if name and str(name) != current_name
        }
        if self._recreate_keep_base_connector:
            original_name = str(details.get("original_connector_name") or "")
            old_names.discard(original_name)
        for old_name in sorted(old_names):
            try:
                self._client.delete_connector(old_name)
            except Exception as exc:
                logger.warning(
                    "[%s] failed to delete superseded connector %s: %s",
                    current_name,
                    old_name,
                    exc,
                    extra={
                        "event": "superseded_connector_delete_failed",
                        "connector_name": current_name,
                        "old_connector_name": old_name,
                    },
                )

    def _handle_unhealthy(self, job: JobLike, result: HealthResult) -> None:
        job = ConnectorJob.from_mapping(job)
        self._healthy_since.pop(str(job.id), None)

        decision = self._transitions.decide_unhealthy(
            job,
            has_failed_tasks=bool(result.failed_task_ids or job.failed_task),
        )

        if decision.action == RecoveryAction.STOP:
            fields: dict[str, Any] = {
                "is_active": False,
                "last_failed_at": result.checked_at,
            }
            if job.failed_count >= self._policy.max_failed_count:
                fields["failed_count"] = self._policy.max_failed_count
            self._db.update_connector_fields(job["id"], **fields)
            return

        incident_id = self._db.ensure_active_incident(job["id"])
        failed_count = clamp_failed_count(
            job.failed_count,
            self._policy.max_failed_count,
        )

        if decision.action == RecoveryAction.DEBOUNCE:
            self._db.update_connector_fields(
                job["id"],
                failed_count=clamp_failed_count(
                    failed_count + 1,
                    self._policy.max_failed_count,
                ),
                last_failed_at=result.checked_at,
            )
            return

        if failed_count == self._failure_confirm_checks:
            self._db.record_connector_log(
                connector_id=job["id"],
                connector_name=job["connector_name"],
                job_name=job.get("job_name"),
                incident_id=incident_id,
                event_type=EventType.HEALTH_FAILED_CONFIRMED,
                healing_step=HealingStep.DETECT,
                has_next_step=level(job) >= 2,
                severity="WARNING",
                message=(
                    "Connector failure confirmed; starting healing: "
                    f"{failure_message(result)}"
                ),
                details=failure_details(
                    result,
                    failed_task_ids=result.failed_task_ids,
                    failed_count=failed_count,
                ),
            )

        if decision.action == RecoveryAction.RESTART_TASKS:
            if not self._level_allows(
                job,
                result,
                incident_id,
                decision.required_level,
                decision.attempted_event,
            ):
                return
            self._refresh_connector_scn(job, result=result, force=True)
            self._actions.restart_failed_tasks(job, result, incident_id)
            return

        if decision.action == RecoveryAction.RESTART_CONNECTOR:
            if not self._level_allows(
                job,
                result,
                incident_id,
                decision.required_level,
                decision.attempted_event,
            ):
                return
            self._refresh_connector_scn(job, result=result, force=True)
            self._actions.restart_connector(job, result, incident_id)
            return

        if decision.action == RecoveryAction.RECREATE_WITH_OFFSET:
            if not self._level_allows(
                job,
                result,
                incident_id,
                decision.required_level,
                decision.attempted_event,
            ):
                return
            job = self._load_runtime_config(job)
            self._actions.recreate_with_offset(job, result, incident_id)
            return

        if decision.action == RecoveryAction.RETRY_RECREATE_WITH_OFFSET:
            job = self._load_runtime_config(job)
            if self._actions.retry_timed_out_recreate_with_offset(
                job,
                result,
                incident_id,
            ):
                return
            self._actions.escalate(job, result)
            return

        if decision.action == RecoveryAction.RECREATE_WITHOUT_OFFSET:
            if not self._level_allows(
                job,
                result,
                incident_id,
                decision.required_level,
                decision.attempted_event,
            ):
                return
            job = self._load_runtime_config(job)
            if self._actions.recreate_without_offset(job, result):
                return
            self._actions.escalate(job, result)
            return

        self._actions.escalate(job, result)

    def _load_runtime_config(self, job: ConnectorJob) -> ConnectorJob:
        if job.active_config is not None:
            return job
        refreshed = self._db.get_connector_by_id(
            job.id,
            include_runtime_config=True,
        )
        if refreshed is None:
            raise RuntimeError(
                f"Connector {job.connector_name} disappeared before recreate"
            )
        return ConnectorJob.from_mapping(refreshed)

    def _run_recreate_with_offset_retry_or_escalate(
        self,
        job: JobLike,
        result: HealthResult,
    ) -> None:
        job = self._load_runtime_config(ConnectorJob.from_mapping(job))
        incident_id = active_incident_id(job) or self._db.ensure_active_incident(job["id"])
        if int(job.get("recreate_with_offset_timeout_count") or 0) <= 1:
            if self._actions.retry_timed_out_recreate_with_offset(job, result, incident_id):
                return
        self._actions.escalate(job, result)

    def _level_allows(
        self,
        job: JobLike,
        result: HealthResult,
        incident_id: str,
        required_level: HealingStep | None,
        attempted_event: str | None,
    ) -> bool:
        if required_level is None or attempted_event is None:
            raise ValueError("Recovery decision is missing level or event metadata")
        if level(job) >= required_level:
            return True
        self._actions.level_limit_reached(
            job,
            result,
            incident_id,
            required_level=required_level,
            attempted_event=attempted_event,
        )
        return False

    def _action_wait_until(self, job: JobLike) -> datetime | None:
        latest_event = job.get("latest_event_type")
        latest_at = latest_event_at(job)
        if latest_at is None:
            return None
        if latest_event in WAIT_AFTER_RESTART_EVENTS:
            return latest_at + timedelta(seconds=self._post_restart_wait_seconds)
        if latest_event in WAIT_AFTER_RECREATE_EVENTS:
            return latest_at + timedelta(seconds=self._recreate_verify_wait_seconds)
        if latest_event in RECREATE_WITH_OFFSET_RETRY_EVENTS:
            return latest_at + timedelta(seconds=self._recreate_verify_wait_seconds)
        return None

    def _refresh_connector_scn(
        self,
        job: JobLike,
        *,
        result: HealthResult | None = None,
        force: bool = False,
    ) -> None:
        connector_id = str(job["id"])
        now = utc_now()
        if not force:
            last_fetch_at = self._last_scn_fetch_at.get(connector_id)
            if (
                last_fetch_at
                and now - last_fetch_at < timedelta(seconds=self._scn_poll_interval_seconds)
            ):
                return
        offsets = self._client.get_offsets(job["connector_name"])
        self._last_scn_fetch_at[connector_id] = now
        scn, commit_scn = extract_last_scn(offsets)
        if not scn and not commit_scn:
            return
        fields: dict[str, Any] = {}
        if scn:
            fields["last_scn"] = scn
        if commit_scn:
            fields["last_commit_scn"] = commit_scn
        self._db.update_connector_fields(job["id"], **fields)
