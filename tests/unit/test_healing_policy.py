from self_healthy_kafka.domain.healing import (
    ConnectorJob,
    RecoveryAction,
    RecoveryPolicy,
)
from self_healthy_kafka.healing.phases import EventType
from self_healthy_kafka.healing.policy import TransitionPolicy


def _job(**overrides):
    values = {
        "id": "connector-id",
        "connector_name": "CDC",
        "level": 4,
        "failed_count": 3,
    }
    values.update(overrides)
    return ConnectorJob.from_mapping(values)


def _policy():
    return TransitionPolicy(
        RecoveryPolicy(
            failure_confirm_checks=3,
            task_restart_max_attempts=3,
            connector_restart_max_attempts=1,
        )
    )


def test_connector_job_keeps_typed_fields_and_unknown_database_columns():
    job = ConnectorJob.from_mapping(
        {
            "id": "connector-id",
            "connector_name": "CDC",
            "failed_count": 2,
            "created_at": "2026-06-12T00:00:00Z",
        }
    )

    assert job.connector_name == "CDC"
    assert job.failed_count == 2
    assert job["created_at"] == "2026-06-12T00:00:00Z"
    assert job.copy(failed_count=3).failed_count == 3


def test_policy_debounces_before_failure_confirmation():
    decision = _policy().decide_unhealthy(
        _job(failed_count=2),
        has_failed_tasks=True,
    )

    assert decision.action == RecoveryAction.DEBOUNCE


def test_policy_escalates_from_task_to_connector_restart():
    decision = _policy().decide_unhealthy(
        _job(
            failed_count=6,
            failed_task=True,
            task_restart_count=3,
        ),
        has_failed_tasks=True,
    )

    assert decision.action == RecoveryAction.RESTART_CONNECTOR


def test_policy_routes_failed_offset_recreate_to_fresh_recreate():
    decision = _policy().decide_unhealthy(
        _job(
            failed_count=7,
            latest_event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_FAILED,
            recreate_with_offset_count=1,
        ),
        has_failed_tasks=True,
    )

    assert decision.action == RecoveryAction.RECREATE_WITHOUT_OFFSET


def test_policy_routes_exhausted_offset_create_timeouts_to_fresh_recreate():
    decision = _policy().decide_unhealthy(
        _job(
            failed_count=7,
            latest_event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT,
            recreate_with_offset_timeout_count=2,
        ),
        has_failed_tasks=True,
    )

    assert decision.action == RecoveryAction.RECREATE_WITHOUT_OFFSET


def test_policy_stops_terminal_incident_without_repeating_actions():
    decision = _policy().decide_unhealthy(
        _job(
            latest_event_type=EventType.HEALING_ESCALATED,
            latest_has_next_step=False,
        ),
        has_failed_tasks=True,
    )

    assert decision.action == RecoveryAction.STOP

