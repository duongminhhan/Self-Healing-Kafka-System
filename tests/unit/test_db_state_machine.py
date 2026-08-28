from datetime import datetime, timezone
from unittest.mock import MagicMock

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

    machine._handle_unhealthy(_job(failed_count=3), _unhealthy(task_ids=[0]))

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
