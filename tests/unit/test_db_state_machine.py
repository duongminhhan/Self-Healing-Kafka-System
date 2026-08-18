from datetime import datetime, timezone
from unittest.mock import MagicMock, call

from self_healthy_kafka.domain.healing import ConnectorJob
from self_healthy_kafka.domain.models import HealthResult, HealthStatus
from self_healthy_kafka.healing.db_state_machine import ConnectorStateMachine
from self_healthy_kafka.healing.phases import EventType


def _machine(db=None, checker=None, client=None, recreate_keep_base_connector=False):
    return ConnectorStateMachine(
        client=client or MagicMock(),
        checker=checker or MagicMock(),
        db=db or MagicMock(),
        failure_confirm_checks=3,
        task_restart_max_attempts=3,
        connector_restart_max_attempts=3,
        post_restart_wait_seconds=30,
        recovery_healthy_confirm_seconds=60,
        recreate_verify_wait_seconds=30,
        scn_poll_interval_seconds=60,
        recreate_keep_base_connector=recreate_keep_base_connector,
    )


def _job(**overrides):
    base = {
        "id": 1,
        "job_name": "job-a",
        "connector_name": "conA",
        "level": 4,
        "current_phase": "HEALTHY",
        "latest_event_type": None,
        "latest_event_at": None,
        "latest_event_details": {},
        "latest_has_next_step": None,
        "failed_count": 0,
        "task_restart_count": 0,
        "connector_restart_count": 0,
        "recreate_with_offset_count": 0,
        "recreate_without_offset_count": 0,
        "failed_task": False,
        "failed_connector": False,
        "last_failed_task_ids": [],
        "active_incident_id": None,
        "active_config": {"connector.class": "X"},
        "config_template": {"connector.class": "X"},
    }
    base.update(overrides)
    return base


def _unhealthy(task_ids=None):
    return HealthResult(
        connector_name="conA",
        status=HealthStatus.UNHEALTHY,
        reason="failed",
        failed_task_ids=task_ids or [],
        checked_at=datetime.now(timezone.utc),
    )


def _healthy():
    return HealthResult(
        connector_name="conA",
        status=HealthStatus.HEALTHY,
        reason="ok",
        checked_at=datetime.now(timezone.utc),
    )


def _stopped():
    return HealthResult(
        connector_name="conA",
        status=HealthStatus.STOPPED,
        reason="connector.state == STOPPED",
        checked_at=datetime.now(timezone.utc),
    )


def _unhealthy_with_trace(task_ids=None):
    return HealthResult(
        connector_name="conA",
        status=HealthStatus.UNHEALTHY,
        reason="tasks [0] FAILED while connector RUNNING",
        failed_task_ids=task_ids or [],
        trace="Caused by: io.debezium.connector.oracle.logminer.parser.DmlParserException",
        checked_at=datetime.now(timezone.utc),
    )


def test_tick_does_not_reload_steady_healthy_connector():
    db = MagicMock()
    db.list_connectors.return_value = [_job()]
    checker = MagicMock()
    checker.check.return_value = _healthy()
    client = MagicMock()
    client.get_offsets.return_value = None
    machine = _machine(db=db, checker=checker, client=client)

    assert machine.tick() == []

    db.get_connector_by_id.assert_not_called()


def test_runtime_config_is_loaded_only_for_recreate():
    db = MagicMock()
    db.get_connector_by_id.return_value = _job(
        active_config={"connector.class": "X"},
    )
    machine = _machine(db=db)

    loaded = machine._load_runtime_config(
        ConnectorJob.from_mapping(
            _job(active_config=None, config_template={})
        )
    )

    assert loaded.active_config == {"connector.class": "X"}
    db.get_connector_by_id.assert_called_once_with(
        1,
        include_runtime_config=True,
    )


def test_admin_stopped_connector_does_not_start_healing():
    db = MagicMock()
    checker = MagicMock()
    checker.check.return_value = _stopped()
    machine = _machine(db=db, checker=checker)

    machine._process_job(_job())

    db.ensure_active_incident.assert_not_called()
    db.record_connector_log.assert_not_called()
    db.update_connector_fields.assert_not_called()


def test_stopped_replacement_continues_active_recreate_recovery():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    checker = MagicMock()
    checker.check.return_value = _stopped()
    client = MagicMock()
    machine = _machine(db=db, checker=checker, client=client)

    machine._process_job(
        _job(
            connector_name="conA.001",
            failed_count=7,
            active_incident_id="incident-1",
            latest_event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_FAILED,
            latest_has_next_step=True,
            recreate_with_offset_count=1,
        )
    )

    client.create_connector.assert_called_once()
    assert db.record_connector_log.call_args.kwargs["event_type"] == (
        EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET
    )


def test_debounce_first_three_failures_before_restart():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    machine = _machine(db=db)

    machine._handle_unhealthy(_job(failed_count=2), _unhealthy(task_ids=[0]))

    db.update_connector_fields.assert_called_once()
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["failed_count"] == 3
    assert "failed_task" not in fields
    assert "failed_connector" not in fields
    db.record_connector_log.assert_not_called()


def test_fourth_failure_restarts_failed_task_and_logs_warning():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    client = MagicMock()
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(_job(failed_count=3), _unhealthy(task_ids=[0]))

    client.restart_connector.assert_called_once_with("conA", only_failed=True)
    assert db.record_connector_log.call_args.kwargs["event_type"] == "TASK_RESTART"
    assert db.record_connector_log.call_args.kwargs["attempt_no"] == 1
    assert db.record_connector_log.call_args.kwargs["details"]["task_ids"] == [0]
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["failed_count"] == 4
    assert fields["failed_task"] is True


def test_failure_log_message_prefers_kafka_connect_trace():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    machine = _machine(db=db)

    machine._handle_unhealthy(_job(failed_count=3), _unhealthy_with_trace(task_ids=[0]))

    log_args = db.record_connector_log.call_args.kwargs
    assert log_args["event_type"] == "TASK_RESTART"
    assert log_args["message"].startswith("Caused by: io.debezium")
    assert log_args["details"]["reason"] == "tasks [0] FAILED while connector RUNNING"
    assert log_args["details"]["trace"] == log_args["message"]


def test_level_limit_disables_connector_to_stop_healing_loop():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    client = MagicMock()
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(
        _job(level=1, failed_count=3),
        _unhealthy(task_ids=[0]),
    )

    client.restart_connector.assert_not_called()
    assert db.record_connector_log.call_args.kwargs["event_type"] == (
        "HEALING_LEVEL_LIMIT_REACHED"
    )
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["is_active"] is False
    assert fields["failed_count"] == 3
    assert "failed_task" not in fields
    assert "failed_connector" not in fields
    assert fields["last_failed_at"]


def test_level_two_limit_at_failed_count_seven_marks_connector_failed():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    client = MagicMock()
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(
        _job(
            level=2,
            failed_count=7,
            task_restart_count=3,
            connector_restart_count=1,
            failed_task=True,
        ),
        _unhealthy(task_ids=[0]),
    )

    client.delete_connector.assert_not_called()
    assert db.record_connector_log.call_args.kwargs["event_type"] == (
        "HEALING_LEVEL_LIMIT_REACHED"
    )
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["is_active"] is False
    assert fields["failed_count"] == 7
    assert fields["failed_task"] is True
    assert fields["failed_connector"] is True


def test_after_three_task_restarts_uses_connector_restart():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    client = MagicMock()
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(
        _job(failed_count=6, task_restart_count=3, failed_task=True),
        _unhealthy(task_ids=[0]),
    )

    client.restart_connector.assert_called_once_with("conA", only_failed=False)
    assert db.record_connector_log.call_args.kwargs["event_type"] == "CONNECTOR_RESTART"
    assert db.record_connector_log.call_args.kwargs["attempt_no"] == 1
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["failed_count"] == 7
    assert fields["failed_task"] is True
    assert fields["failed_connector"] is True


def test_healthy_after_restart_wait_starts_confirmation_before_resetting_counts():
    db = MagicMock()
    machine = _machine(db=db)

    machine._handle_healthy(
        _job(
            latest_event_type=EventType.TASK_RESTART,
            latest_event_at=datetime(2026, 6, 3, 0, 0, tzinfo=timezone.utc),
            active_incident_id="incident-1",
            failed_count=4,
            task_restart_count=1,
            failed_task=True,
        ),
        _healthy(),
    )

    db.record_connector_log.assert_not_called()
    assert machine._healthy_since["1"]
    db.update_connector_fields.assert_not_called()


def test_second_healthy_after_stable_window_resets_counts():
    db = MagicMock()
    machine = _machine(db=db)
    machine._healthy_since["1"] = datetime(
        2026, 6, 3, 0, 1, tzinfo=timezone.utc
    )

    machine._handle_healthy(
        _job(
            latest_event_type=EventType.TASK_RESTART,
            latest_event_at=datetime(2026, 6, 3, 0, 0, tzinfo=timezone.utc),
            active_incident_id="incident-1",
            failed_count=4,
            task_restart_count=1,
            failed_task=True,
        ),
        _healthy(),
    )

    assert db.record_connector_log.call_args.kwargs["event_type"] == "HEALING_RECOVERED"
    log_args = db.record_connector_log.call_args.kwargs
    assert log_args["details"]["original_connector_name"] == "conA"
    assert log_args["details"]["final_connector_name"] == "conA"
    assert log_args["details"]["healing_steps"] == (
        "Đã khởi động lại task 1 lần; "
        "hệ thống phục hồi tại bước khởi động lại task."
    )
    assert "Original connector: conA" in log_args["message"]
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["failed_count"] == 0
    assert fields["failed_task"] is False
    assert fields["failed_connector"] is False


def test_healthy_without_automated_step_resets_without_recovered_log():
    db = MagicMock()
    machine = _machine(db=db)

    machine._handle_healthy(
        _job(
            failed_count=2,
            failed_task=False,
            failed_connector=False,
        ),
        _healthy(),
    )

    db.record_connector_log.assert_not_called()
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["failed_count"] == 0
    assert fields["failed_task"] is False
    assert fields["failed_connector"] is False
    assert "last_healthy_at" not in fields


def test_healthy_after_recreate_starts_stability_confirmation():
    db = MagicMock()
    machine = _machine(db=db)

    machine._handle_healthy(
        _job(
            connector_name="conA.002",
            latest_event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET,
            latest_event_at=datetime(2026, 6, 3, 0, 0, tzinfo=timezone.utc),
            latest_event_details={
                "old_connector_name": "conA.001",
                "new_connector_name": "conA.002",
            },
            active_incident_id="incident-1",
            failed_count=7,
            task_restart_count=3,
            connector_restart_count=1,
            recreate_with_offset_count=1,
            failed_task=True,
        ),
        _healthy(),
    )

    db.record_connector_log.assert_not_called()
    assert machine._healthy_since["1"]
    db.update_connector_fields.assert_not_called()


def test_recovered_after_recreate_records_meaningful_steps():
    db = MagicMock()
    client = MagicMock()
    machine = _machine(db=db, client=client)
    machine._healthy_since["1"] = datetime(
        2026, 6, 3, 0, 1, tzinfo=timezone.utc
    )

    machine._handle_healthy(
        _job(
            connector_name="conA.003",
            latest_event_type=EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET,
            latest_event_at=datetime(2026, 6, 3, 0, 0, tzinfo=timezone.utc),
            latest_event_details={
                "previous_event": EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET,
                "original_connector_name": "conA.001",
                "old_connector_name": "conA.002",
                "new_connector_name": "conA.003",
            },
            active_incident_id="incident-1",
            failed_count=7,
            task_restart_count=3,
            connector_restart_count=1,
            recreate_with_offset_count=1,
            recreate_without_offset_count=1,
            failed_task=True,
        ),
        _healthy(),
    )

    log_args = db.record_connector_log.call_args.kwargs
    assert log_args["event_type"] == "HEALING_RECOVERED"
    assert log_args["connector_name"] == "conA.003"
    assert log_args["details"]["original_connector_name"] == "conA.001"
    assert log_args["details"]["old_connector_name"] == "conA.002"
    assert log_args["details"]["final_connector_name"] == "conA.003"
    assert log_args["details"]["healing_steps"] == (
        "Đã trải qua khởi động lại task 3 lần, khởi động lại connector 1 lần, "
        "tạo lại connector với offset cũ 1 lần; cuối cùng tạo connector mới "
        "conA.003 không sử dụng offset cũ và đã hoạt động ổn định."
    )
    assert "current connector: conA.003" in log_args["message"]
    assert client.delete_connector.call_args_list == [
        call("conA.001"),
        call("conA.002"),
    ]


def test_recovered_after_recreate_keep_base_preserves_versioned_original_connector():
    db = MagicMock()
    client = MagicMock()
    machine = _machine(db=db, client=client, recreate_keep_base_connector=True)
    machine._healthy_since["1"] = datetime(
        2026, 6, 3, 0, 1, tzinfo=timezone.utc
    )

    machine._handle_healthy(
        _job(
            connector_name="conA.003",
            latest_event_type=EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET,
            latest_event_at=datetime(2026, 6, 3, 0, 0, tzinfo=timezone.utc),
            latest_event_details={
                "original_connector_name": "conA.001",
                "old_connector_name": "conA.002",
                "new_connector_name": "conA.003",
            },
            active_incident_id="incident-1",
            failed_count=7,
            task_restart_count=3,
            connector_restart_count=1,
            recreate_with_offset_count=1,
            recreate_without_offset_count=1,
            failed_task=True,
        ),
        _healthy(),
    )

    client.delete_connector.assert_called_once_with("conA.002")


def test_recovered_after_recreate_can_keep_unversioned_base_connector():
    db = MagicMock()
    client = MagicMock()
    machine = _machine(db=db, client=client, recreate_keep_base_connector=True)
    machine._healthy_since["1"] = datetime(
        2026, 6, 3, 0, 1, tzinfo=timezone.utc
    )

    machine._handle_healthy(
        _job(
            connector_name="conA.002",
            latest_event_type=EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET,
            latest_event_at=datetime(2026, 6, 3, 0, 0, tzinfo=timezone.utc),
            latest_event_details={
                "original_connector_name": "conA",
                "old_connector_name": "conA.001",
                "new_connector_name": "conA.002",
            },
            active_incident_id="incident-1",
            failed_count=7,
            task_restart_count=3,
            connector_restart_count=1,
            recreate_with_offset_count=1,
            recreate_without_offset_count=1,
            failed_task=True,
        ),
        _healthy(),
    )

    client.delete_connector.assert_called_once_with("conA.001")


def test_unhealthy_after_restart_wait_continues_restart_logic():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    client = MagicMock()
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(
        _job(
            latest_event_type=EventType.TASK_RESTART,
            failed_count=4,
            task_restart_count=1,
            failed_task=True,
        ),
        _unhealthy(task_ids=[0]),
    )

    client.restart_connector.assert_called_once_with("conA", only_failed=True)
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["failed_count"] == 5
    assert "failed_task" not in fields


def test_recreate_with_offset_failure_moves_to_without_offset():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    client = MagicMock()
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(
        _job(
            latest_event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_FAILED,
            failed_count=7,
            task_restart_count=3,
            connector_restart_count=1,
            recreate_with_offset_count=1,
        ),
        _unhealthy(task_ids=[0]),
    )

    client.create_connector.assert_called_once()
    assert db.record_connector_log.call_args.kwargs["event_type"] == (
        "CONNECTOR_RECREATE_WITHOUT_OFFSET"
    )


def test_escalated_connector_does_not_repeat_escalation_action():
    db = MagicMock()
    client = MagicMock()
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(
        _job(
            latest_event_type=EventType.HEALING_ESCALATED,
            latest_has_next_step=False,
            failed_count=9,
            task_restart_count=3,
            connector_restart_count=1,
            recreate_with_offset_count=1,
            recreate_without_offset_count=1,
        ),
        _unhealthy(task_ids=[0]),
    )

    client.restart_connector.assert_not_called()
    client.delete_connector.assert_not_called()
    db.record_connector_log.assert_not_called()
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["is_active"] is False
    assert fields["failed_count"] == 7
    assert fields["last_failed_at"]


def test_healthy_running_connector_refreshes_scn_on_interval():
    db = MagicMock()
    client = MagicMock()
    client.get_offsets.return_value = {
        "offsets": [{"offset": {"scn": "123", "commit_scn": "124:1:abc"}}]
    }
    machine = _machine(db=db, client=client)

    machine._handle_healthy(_job(id="connector-id"), _healthy())

    client.get_offsets.assert_called_once_with("conA")
    assert db.update_connector_fields.call_args_list[0].kwargs["last_scn"] == "124"
    assert db.update_connector_fields.call_args_list[0].kwargs["last_commit_scn"] == "124:1:abc"


def test_new_incident_does_not_reuse_closed_incident_attempt_counts():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-new"
    client = MagicMock()
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(
            _job(
                failed_count=3,
                failed_task=True,
                task_restart_count=0,
                active_incident_id=None,
            ),
        _unhealthy(task_ids=[0]),
    )

    assert db.record_connector_log.call_args.kwargs["event_type"] == "TASK_RESTART"
    assert db.record_connector_log.call_args.kwargs["attempt_no"] == 1


def test_webhook_firing_verifies_status_and_skips_poll_debounce():
    db = MagicMock()
    job = _job(failed_count=0)
    db.get_connector.return_value = job
    db.get_connector_by_id.return_value = _job(failed_count=4)
    db.ensure_active_incident.return_value = "incident-1"
    checker = MagicMock()
    checker.check.return_value = _unhealthy(task_ids=[0])
    client = MagicMock()
    machine = _machine(db=db, checker=checker, client=client)

    followup_name = machine.process_connector("conA", failure_confirmed=True)

    checker.check.assert_called_once_with("conA")
    client.restart_connector.assert_called_once_with("conA", only_failed=True)
    assert db.update_connector_fields.call_args_list[0].kwargs["failed_count"] == 3
    assert followup_name == "conA"


def test_webhook_firing_does_not_confirm_when_connector_is_healthy():
    db = MagicMock()
    job = _job(failed_count=0)
    db.get_connector.return_value = job
    db.get_connector_by_id.return_value = _job(failed_count=0)
    checker = MagicMock()
    checker.check.return_value = _healthy()
    client = MagicMock()
    machine = _machine(db=db, checker=checker, client=client)

    followup_name = machine.process_connector("conA", failure_confirmed=True)

    checker.check.assert_called_once_with("conA")
    client.restart_connector.assert_not_called()
    failed_count_updates = [
        call.kwargs.get("failed_count")
        for call in db.update_connector_fields.call_args_list
        if "failed_count" in call.kwargs
    ]
    assert failed_count_updates == []
    assert followup_name is None


def test_webhook_unknown_connector_is_ignored():
    db = MagicMock()
    db.get_connector.return_value = None
    machine = _machine(db=db)

    followup_name = machine.process_connector("missing", failure_confirmed=True)

    assert followup_name is None


def test_recreate_with_offset_timeout_retries_step_three_once():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    client = MagicMock()
    client.connector_exists.return_value = False
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(
        _job(
            connector_name="conA.002",
            latest_event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT,
            recreate_with_offset_timeout_count=1,
            latest_event_details={
                "offsets": {"offsets": [{"offset": {"scn": "123"}}]},
                "config": {"connector.class": "X", "name": "conA.002"},
            },
            failed_count=7,
        ),
        HealthResult(
            connector_name="conA",
            status=HealthStatus.NOT_CREATED,
            reason="not created",
            checked_at=datetime.now(timezone.utc),
        ),
    )

    client.create_connector.assert_called_once_with(
        "conA.002",
        {"connector.class": "X", "name": "conA.002"},
        initial_state="STOPPED",
    )


def test_second_recreate_with_offset_timeout_moves_to_new_without_offset_version():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    client = MagicMock()
    client.stop_connector.return_value = True
    machine = _machine(db=db, client=client)

    machine._handle_unhealthy(
        _job(
            connector_name="conA.002",
            latest_event_type=EventType.CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT,
            recreate_with_offset_timeout_count=2,
            latest_event_details={
                "original_connector_name": "conA.001",
                "old_connector_name": "conA.001",
                "new_connector_name": "conA.002",
            },
            failed_count=7,
        ),
        HealthResult(
            connector_name="conA",
            status=HealthStatus.NOT_CREATED,
            reason="not created",
            checked_at=datetime.now(timezone.utc),
        ),
    )

    client.create_connector.assert_called_once()
    assert client.create_connector.call_args.args[0] == "conA.003"
    assert client.create_connector.call_args.args[1]["name"] == "conA.003"
    assert (
        client.create_connector.call_args.args[1][
            "schema.history.internal.kafka.topic"
        ]
        == "schema-history.conA.003"
    )
    assert db.record_connector_log.call_args.kwargs["event_type"] == (
        "CONNECTOR_RECREATE_WITHOUT_OFFSET"
    )
