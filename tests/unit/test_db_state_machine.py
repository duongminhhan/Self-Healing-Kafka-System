from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from scripts.seed_healing_samples import ORACLE, HistoryStore
from self_healthy_kafka.domain.healing import ConnectorJob
from self_healthy_kafka.domain.models import HealthResult, HealthStatus
from self_healthy_kafka.healing.db_state_machine import ConnectorStateMachine
from self_healthy_kafka.healing.phases import EventType


def _machine(db=None, checker=None, client=None):
    return ConnectorStateMachine(
        client=client or MagicMock(),
        checker=checker or MagicMock(),
        db=db or MagicMock(),
        failure_confirm_checks=3,
        task_restart_max_attempts=3,
        connector_restart_max_attempts=1,
        post_restart_wait_seconds=30,
        recovery_healthy_confirm_seconds=60,
        recreate_verify_wait_seconds=30,
        scn_poll_interval_seconds=60,
    )


def _job(**overrides):
    job = {
        "id": "queue-id",
        "active_incident_id": "queue-id",
        "connector_name": "conn-a",
        "root_connector_name": "conn-a",
        "queue_status": "PENDING",
        "healing_mode": "RECOVERY",
        "level": 4,
        "failed_count": 0,
        "task_restart_count": 0,
        "connector_restart_count": 0,
        "recreate_with_offset_count": 0,
        "recreate_without_offset_count": 0,
        "latest_event_type": None,
        "latest_event_details": {},
        "latest_has_next_step": True,
        "active_config": None,
    }
    job.update(overrides)
    return job


def _healthy(name="conn-a"):
    return HealthResult(
        connector_name=name,
        status=HealthStatus.HEALTHY,
        reason="running",
        checked_at=datetime.now(timezone.utc),
    )


def _unhealthy(name="conn-a", task_ids=None):
    return HealthResult(
        connector_name=name,
        status=HealthStatus.UNHEALTHY,
        reason="failed",
        failed_task_ids=task_ids or [],
        checked_at=datetime.now(timezone.utc),
    )


def test_tick_discovers_all_connectors_but_only_enqueues_unhealthy():
    db = MagicMock()
    db.list_connectors.return_value = []
    db.get_connector.return_value = None
    checker = MagicMock()
    checker.check.side_effect = [_healthy("healthy"), _unhealthy("failed", [0])]
    client = MagicMock()
    client.list_connectors.return_value = ["healthy", "failed"]
    client.status_circuit_open = False
    client.get_config.return_value = {
        "connector.class": "io.debezium.connector.oracle.OracleConnector"
    }

    assert _machine(db=db, checker=checker, client=client).tick() == []

    db.enqueue_connector.assert_called_once_with(
        root_connector_name="failed",
        current_connector_name="failed",
        connector_class="io.debezium.connector.oracle.OracleConnector",
        healing_mode="RECOVERY",
    )


def test_discovery_does_not_enqueue_connector_with_open_queue():
    db = MagicMock()
    db.get_connector.return_value = _job()
    client = MagicMock()
    client.list_connectors.return_value = ["conn-a"]
    client.status_circuit_open = False

    _machine(db=db, client=client)._discover_failed_connectors()

    db.enqueue_connector.assert_not_called()


def test_non_oracle_connector_uses_restart_only_mode():
    db = MagicMock()
    checker = MagicMock()
    checker.check.return_value = _unhealthy()
    client = MagicMock()
    client.get_config.return_value = {"connector.class": "FileStreamSource"}

    _machine(db=db, checker=checker, client=client)._enqueue_failed_connector("conn-a")

    assert db.enqueue_connector.call_args.kwargs["healing_mode"] == "RESTART_ONLY"


def test_runtime_config_is_curlled_only_for_recovery_action():
    db = MagicMock()
    client = MagicMock()
    client.get_config.return_value = {"connector.class": "X"}
    machine = _machine(db=db, client=client)

    loaded = machine._load_runtime_config(ConnectorJob.from_mapping(_job()))

    assert loaded.active_config == {"connector.class": "X"}
    client.get_config.assert_called_once_with("conn-a")


def test_unhealthy_observation_is_logged_before_task_restart():
    db = MagicMock()
    db.ensure_active_incident.return_value = "queue-id"
    client = MagicMock()
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(_job(failed_count=3, failure_confirmed=True), _unhealthy(task_ids=[0]))

    assert [call.kwargs["event_type"] for call in db.record_connector_log.call_args_list] == [
        EventType.HEALTH_FAILURE_OBSERVED,
        EventType.TASK_RESTART,
    ]
    client.restart_connector.assert_called_once_with("conn-a", only_failed=True)


def test_recovered_queue_is_completed_after_stability_confirmation():
    db = MagicMock()
    machine = _machine(db=db)
    machine._healthy_since["queue-id"] = datetime(2026, 1, 1, tzinfo=timezone.utc)
    job = _job(
        failed_count=4,
        task_restart_count=1,
        latest_event_type=EventType.TASK_RESTART,
        latest_event_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )

    machine._handle_healthy(job, _healthy())

    assert db.record_connector_log.call_args.kwargs["event_type"] == EventType.HEALING_RECOVERED
    db.complete.assert_called_once_with("queue-id", "RECOVERED")


def test_healthy_before_first_poll_completes_without_inventing_action():
    db = MagicMock()
    checker = MagicMock()
    checker.check.return_value = _healthy()
    client = MagicMock()

    outcome = _machine(db=db, checker=checker, client=client)._process_job_safely(_job())

    assert outcome.processed
    assert not outcome.requires_followup
    db.start_processing.assert_called_once_with("queue-id")
    db.complete.assert_called_once_with("queue-id", "RECOVERED")
    db.record_connector_log.assert_not_called()
    db.wait_for_next_attempt.assert_not_called()
    client.restart_connector.assert_not_called()
    client.delete_connector.assert_not_called()


def test_confirmed_webhook_persists_confirmation_before_action():
    db = MagicMock()
    checker = MagicMock()
    checker.check.return_value = _unhealthy(task_ids=[0])
    machine = _machine(db=db, checker=checker)

    machine._process_job(_job(), failure_confirmed=True)

    events = [call.kwargs["event_type"] for call in db.record_connector_log.call_args_list]
    assert events == [EventType.HEALTH_FAILURE_OBSERVED,
                      EventType.HEALTH_FAILED_CONFIRMED, EventType.TASK_RESTART]
    assert db.record_connector_log.call_args_list[0].kwargs["attempt_no"] == 1


@pytest.mark.parametrize("webhook", [False, True])
def test_confirmation_survives_new_machine_and_repeated_delivery(webhook):
    store = HistoryStore(999, "confirmation", ORACLE, datetime(2020, 1, 1, tzinfo=timezone.utc))
    queue_id = store.queue["QueueId"]
    for _ in range(4):
        # New state machine models loss of all process-local confirmation state.
        checker = MagicMock()
        checker.check.return_value = _unhealthy(task_ids=[0])
        machine = _machine(db=store, checker=checker)
        machine._process_job(store.get_connector_by_id(queue_id), failure_confirmed=webhook)
        store.now += timedelta(minutes=2)
    assert sum(row["EventType"] == EventType.HEALTH_FAILED_CONFIRMED for row in store.logs) == 1
    assert store.get_connector_by_id(queue_id).failure_confirmed is True
    assert store.get_connector_by_id(queue_id).failed_count == 4


def test_confirmation_audit_failure_prevents_kafka_action_and_can_retry():
    db = MagicMock()
    checker = MagicMock()
    checker.check.return_value = _unhealthy(task_ids=[0])
    client = MagicMock()
    machine = _machine(db=db, checker=checker, client=client)

    def fail_confirmation(**kwargs):
        if kwargs["event_type"] == EventType.HEALTH_FAILED_CONFIRMED:
            raise RuntimeError("audit unavailable")

    db.record_connector_log.side_effect = fail_confirmation
    with pytest.raises(RuntimeError, match="audit unavailable"):
        machine._process_job(_job(), failure_confirmed=True)
    client.restart_connector.assert_not_called()
    db.record_connector_log.side_effect = None
    machine._process_job(_job(failed_count=1), failure_confirmed=True)
    client.restart_connector.assert_called_once()


def test_spontaneous_completion_failure_retries_without_recovery_action_log():
    db = MagicMock()
    machine = _machine(db=db)
    db.complete.side_effect = RuntimeError("commit failed")
    with pytest.raises(RuntimeError, match="commit failed"):
        machine._handle_healthy(_job(failed_count=1), _healthy())
    db.complete.side_effect = None
    machine._handle_healthy(_job(failed_count=1), _healthy())
    assert db.complete.call_count == 2
    db.record_connector_log.assert_not_called()


@pytest.mark.parametrize("missing", [False, True])
def test_recreation_retry_cannot_bypass_restart_only_mode(missing):
    db = MagicMock()
    machine = _machine(db=db)
    machine._actions = MagicMock()
    job = _job(healing_mode="RESTART_ONLY", level=4, failed_count=5,
               failure_confirmed=True, latest_event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT,
               recreate_with_offset_timeout_count=1)
    if missing:
        machine._run_recreate_with_offset_retry_or_escalate(job, _unhealthy())
    else:
        machine._handle_unhealthy(job, _unhealthy())
    machine._actions.retry_timed_out_recreate_with_offset.assert_not_called()
    machine._actions.level_limit_reached.assert_called_once()


def test_action_audit_failure_is_not_reported_as_recovery():
    db = MagicMock()
    machine = _machine(db=db)

    def fail_action(**kwargs):
        if kwargs["event_type"] == EventType.TASK_RESTART:
            raise RuntimeError("action audit failed after Kafka request")

    db.record_connector_log.side_effect = fail_action
    machine._handle_unhealthy(_job(failed_count=3, failure_confirmed=True), _unhealthy(task_ids=[0]))
    machine.client.restart_connector.assert_called_once()
    db.complete.assert_called_once_with("queue-id", "ESCALATED")
    assert db.record_connector_log.call_args.kwargs["details"]["outcome_uncertain"] is True


def test_quarantine_prevents_replay_after_audit_outage():
    db = MagicMock()
    checker = MagicMock()
    checker.check.return_value = _unhealthy(task_ids=[0])
    machine = _machine(db=db, checker=checker)
    job = _job(failed_count=3, failure_confirmed=True)

    def unavailable(**kwargs):
        if kwargs["event_type"] != EventType.HEALTH_FAILURE_OBSERVED:
            raise RuntimeError("audit unavailable")

    db.record_connector_log.side_effect = unavailable
    machine._process_job_safely(job)
    machine._process_job_safely(job)
    machine.client.restart_connector.assert_called_once()
    db.record_connector_log.side_effect = None
    machine._process_job_safely(job)
    machine.client.restart_connector.assert_called_once()
    db.complete.assert_called_once_with("queue-id", "ESCALATED")


def test_restored_quarantine_event_finishes_escalation_without_health_action():
    db = MagicMock()
    checker = MagicMock()
    machine = _machine(db=db, checker=checker)
    assert machine._process_job(_job(latest_event_type=EventType.HEALING_ESCALATED)) is False
    checker.check.assert_not_called()
    db.complete.assert_called_once_with("queue-id", "ESCALATED")


def test_escalated_queue_does_not_write_duplicate_observation():
    db = MagicMock()
    job = _job(
        latest_event_type=EventType.HEALING_ESCALATED,
        latest_has_next_step=False,
        failed_count=7,
        task_restart_count=3,
        connector_restart_count=1,
        recreate_with_offset_count=1,
        recreate_without_offset_count=1,
    )

    _machine(db=db)._handle_unhealthy(job, _unhealthy())

    db.record_connector_log.assert_not_called()
    db.complete.assert_called_once_with("queue-id", "ESCALATED")


def test_queue_wait_is_persisted_after_action():
    db = MagicMock()
    db.get_connector_by_id.return_value = _job(
        failed_count=4,
        latest_event_type=EventType.TASK_RESTART,
        latest_event_at=datetime.now(timezone.utc),
    )
    checker = MagicMock()
    checker.check.return_value = _unhealthy(task_ids=[0])
    client = MagicMock()
    machine = _machine(db=db, checker=checker, client=client)

    machine._process_job_safely(_job(failed_count=3))

    db.start_processing.assert_called_once_with("queue-id")
    db.wait_for_next_attempt.assert_called_once()
