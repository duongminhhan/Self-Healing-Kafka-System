from __future__ import annotations

from self_healthy_kafka.domain.healing import (
    ConnectorJob,
    HealingStep,
    RecoveryAction,
    RecoveryDecision,
    RecoveryPolicy,
)
from self_healthy_kafka.healing.phases import EventType

WAIT_AFTER_RESTART_EVENTS = {
    EventType.TASK_RESTART,
    EventType.CONNECTOR_RESTART,
}

WAIT_AFTER_RECREATE_EVENTS = {
    EventType.CONNECTOR_RECREATE_WITH_OFFSET,
    EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET,
}

RECREATE_WITH_OFFSET_FAILURE_EVENTS = {
    EventType.CONNECTOR_RECREATE_WITH_OFFSET_FAILED,
    EventType.OFFSET_CAPTURE_FAILED,
    EventType.OFFSET_PATCH_FAILED,
    EventType.CONNECTOR_RESUME_FAILED,
}

RECREATE_WITH_OFFSET_RETRY_EVENTS = {
    EventType.CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT,
}

TERMINAL_EVENTS = {
    EventType.HEALING_ESCALATED,
    EventType.HEALING_LEVEL_LIMIT_REACHED,
}


class TransitionPolicy:
    """Pure decision table for unhealthy connector transitions."""

    def __init__(self, policy: RecoveryPolicy):
        self.policy = policy

    def decide_unhealthy(
        self,
        raw_job,
        *,
        has_failed_tasks: bool,
    ) -> RecoveryDecision:
        job = ConnectorJob.from_mapping(raw_job)
        event = job.latest_event_type

        if event == EventType.CONNECTOR_RECREATE_WITH_OFFSET:
            return self._recreate_without_offset_or_escalate(job)
        if event in RECREATE_WITH_OFFSET_RETRY_EVENTS:
            if (
                job.recreate_with_offset_timeout_count
                < self.policy.recreate_with_offset_timeout_max_attempts
            ):
                return RecoveryDecision(
                    RecoveryAction.RETRY_RECREATE_WITH_OFFSET,
                    HealingStep.RECREATE_WITH_OFFSET,
                    EventType.CONNECTOR_RECREATE_WITH_OFFSET,
                )
            return self._recreate_without_offset_or_escalate(job)
        if event in RECREATE_WITH_OFFSET_FAILURE_EVENTS:
            return self._recreate_without_offset_or_escalate(job)
        if event in {
            EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET,
            EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED,
        }:
            return RecoveryDecision(RecoveryAction.ESCALATE)
        if event in TERMINAL_EVENTS and job.latest_has_next_step is False:
            return RecoveryDecision(RecoveryAction.STOP)
        if job.failed_count < self.policy.failure_confirm_checks:
            return RecoveryDecision(RecoveryAction.DEBOUNCE)
        if has_failed_tasks and (
            job.task_restart_count < self.policy.task_restart_max_attempts
        ):
            return RecoveryDecision(
                RecoveryAction.RESTART_TASKS,
                HealingStep.RESTART,
                EventType.TASK_RESTART,
            )
        if self._should_restart_connector(job):
            return RecoveryDecision(
                RecoveryAction.RESTART_CONNECTOR,
                HealingStep.RESTART,
                EventType.CONNECTOR_RESTART,
            )
        if job.recreate_with_offset_count == 0:
            return RecoveryDecision(
                RecoveryAction.RECREATE_WITH_OFFSET,
                HealingStep.RECREATE_WITH_OFFSET,
                EventType.CONNECTOR_RECREATE_WITH_OFFSET,
            )
        return self._recreate_without_offset_or_escalate(job)

    def _should_restart_connector(self, job: ConnectorJob) -> bool:
        if job.connector_restart_count >= self.policy.connector_restart_max_attempts:
            return False
        if job.failed_task:
            return (
                job.task_restart_count >= self.policy.task_restart_max_attempts
                and job.failed_count
                >= (
                    self.policy.failure_confirm_checks
                    + self.policy.task_restart_max_attempts
                )
            )
        return True

    def _recreate_without_offset_or_escalate(
        self,
        job: ConnectorJob,
    ) -> RecoveryDecision:
        if (
            job.recreate_without_offset_count
            < self.policy.recreate_without_offset_max_attempts
        ):
            return RecoveryDecision(
                RecoveryAction.RECREATE_WITHOUT_OFFSET,
                HealingStep.RECREATE_WITHOUT_OFFSET,
                EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET,
            )
        return RecoveryDecision(RecoveryAction.ESCALATE)
