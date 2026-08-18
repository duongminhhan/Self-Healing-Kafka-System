from datetime import datetime, timezone
from unittest.mock import MagicMock

import httpx

from self_healthy_kafka.domain.models import HealthResult, HealthStatus
from self_healthy_kafka.healing.actions import HealingActions


def _actions(db=None, client=None, *, keep_base=False):
    return HealingActions(
        client=client or MagicMock(),
        db=db or MagicMock(),
        post_restart_wait_seconds=30,
        recreate_verify_wait_seconds=30,
        recreate_keep_base_connector=keep_base,
    )


def _job(**overrides):
    base = {
        "id": 1,
        "job_name": "job-a",
        "connector_name": "conA",
        "level": 4,
        "failed_count": 0,
        "task_restart_count": 0,
        "connector_restart_count": 0,
        "recreate_with_offset_count": 0,
        "recreate_without_offset_count": 0,
        "failed_task": False,
        "failed_connector": False,
        "last_failed_task_ids": [],
        "active_incident_id": None,
        "active_config": {
            "connector.class": "X",
            "schema.history.internal.kafka.topic": "schema-history.conA",
        },
        "config_template": {
            "connector.class": "X",
            "schema.history.internal.kafka.topic": "schema-history.conA",
        },
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


def test_restart_failed_tasks_records_task_restart_action():
    db = MagicMock()
    client = MagicMock()
    actions = _actions(db=db, client=client)

    actions.restart_failed_tasks(
        _job(failed_count=3),
        _unhealthy(task_ids=[0]),
        "incident-1",
    )

    client.restart_connector.assert_called_once_with("conA", only_failed=True)
    assert db.record_connector_log.call_args.kwargs["event_type"] == "TASK_RESTART"
    assert db.record_connector_log.call_args.kwargs["attempt_no"] == 1
    assert db.record_connector_log.call_args.kwargs["details"]["task_ids"] == [0]
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["failed_count"] == 4
    assert fields["failed_task"] is True
    assert fields["failed_connector"] is False


def test_recreate_with_offset_preserves_schema_history_and_patches_offsets():
    db = MagicMock()
    db.get_connector_credential.return_value = "jdbc:oracle:thin:@host/service;secret"
    db.get_kafka_server.return_value = "kafka-1:9092,kafka-2:9092"
    client = MagicMock()
    client.get_offsets.return_value = {
        "offsets": [
            {
                "partition": {"server": "oracle_cdc"},
                "offset": {"scn": "123", "commit_scn": "124:1:abc"},
            }
        ]
    }
    client.patch_offsets.return_value = True
    client.resume_connector.return_value = True
    client.stop_connector.return_value = True
    actions = _actions(db=db, client=client)

    actions.recreate_with_offset(
        _job(
            connector_name="conA.001",
            active_config={
                "connector.class": "X",
                "name": "conA.001",
                "schema.history.internal.kafka.topic": "schema-history.conA.001",
                "schema.history.internal.kafka.bootstrap.servers": (
                    "kafka-1:9092,kafka-2:9092"
                ),
                "database.url": "jdbc:oracle:thin:@host/service",
                "database.password": "secret",
            },
            config_id="318",
            config_template={
                "connector.class": "X",
                "name": "conA.001",
                "schema.history.internal.kafka.topic": "schema-history.conA.001",
                "database.url": "{url}",
                "database.password": "{pwd}",
                "schema.history.internal.kafka.bootstrap.servers": "{kafka_server}",
            },
            task_restart_count=3,
            connector_restart_count=1,
        ),
        _unhealthy(task_ids=[0]),
        "incident-1",
    )

    client.stop_connector.assert_called_once_with("conA.001")
    client.delete_connector.assert_called_once_with("conA.001")
    client.create_connector.assert_called_once()
    assert client.create_connector.call_args.args[0] == "conA.002"
    assert client.create_connector.call_args.args[1]["name"] == "conA.002"
    assert client.create_connector.call_args.kwargs["initial_state"] == "STOPPED"
    client.patch_offsets.assert_called_once_with("conA.002", client.get_offsets.return_value)
    client.resume_connector.assert_called_once_with("conA.002")
    method_order = [call[0] for call in client.method_calls]
    assert method_order.index("stop_connector") < method_order.index("create_connector")
    assert method_order.index("create_connector") < method_order.index("patch_offsets")
    assert method_order.index("patch_offsets") < method_order.index("resume_connector")
    assert (
        client.create_connector.call_args.args[1]["schema.history.internal.kafka.topic"]
        == "schema-history.conA.001"
    )
    assert client.create_connector.call_args.args[1]["database.url"] == (
        "jdbc:oracle:thin:@host/service"
    )
    assert client.create_connector.call_args.args[1]["database.password"] == "secret"
    assert (
        client.create_connector.call_args.args[1][
            "schema.history.internal.kafka.bootstrap.servers"
        ]
        == "kafka-1:9092,kafka-2:9092"
    )
    recreate_log = [
        call.kwargs
        for call in db.record_connector_log.call_args_list
        if call.kwargs["event_type"] == "CONNECTOR_RECREATE_WITH_OFFSET"
    ][0]
    assert recreate_log["details"]["schema_history_topic"] == "schema-history.conA.001"
    assert recreate_log["details"]["preserve_schema_history"] is True
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["connector_name"] == "conA.002"
    assert fields["last_scn"] == "124"
    assert fields["config_template"]["database.url"] == "{url}"
    assert fields["config_template"]["database.password"] == "{pwd}"
    db.reset_topic_lag_after_connector_recreate.assert_called_once_with(
        connector_id=1,
        old_connector_name="conA.001",
        new_connector_name="conA.002",
    )


def test_recreate_with_offset_keeps_unversioned_base_connector():
    db = MagicMock()
    client = MagicMock()
    client.get_offsets.return_value = {
        "offsets": [
            {
                "partition": {"server": "oracle_cdc"},
                "offset": {"scn": "123"},
            }
        ]
    }
    client.patch_offsets.return_value = True
    client.resume_connector.return_value = True
    client.stop_connector.return_value = True
    actions = _actions(db=db, client=client)

    actions.recreate_with_offset(
        _job(connector_name="conA"),
        _unhealthy(task_ids=[0]),
        "incident-1",
    )

    client.stop_connector.assert_called_once_with("conA")
    client.create_connector.assert_called_once()
    assert client.create_connector.call_args.args[0] == "conA.001"
    client.delete_connector.assert_not_called()


def test_recreate_with_offset_keep_base_preserves_versioned_original_connector():
    db = MagicMock()
    client = MagicMock()
    client.get_offsets.return_value = {
        "offsets": [{"partition": {"server": "oracle_cdc"}, "offset": {"scn": "123"}}]
    }
    client.patch_offsets.return_value = True
    client.resume_connector.return_value = True
    client.stop_connector.return_value = True
    actions = _actions(db=db, client=client, keep_base=True)

    actions.recreate_with_offset(
        _job(connector_name="conA.002"),
        _unhealthy(task_ids=[0]),
        "incident-1",
    )

    assert client.create_connector.call_args.args[0] == "conA.003"
    client.delete_connector.assert_not_called()


def test_recreate_with_offset_failure_does_not_resume_when_patch_fails():
    db = MagicMock()
    client = MagicMock()
    client.get_offsets.return_value = {
        "offsets": [
            {
                "partition": {"server": "oracle_cdc"},
                "offset": {"scn": "123"},
            }
        ]
    }
    client.patch_offsets.return_value = False
    client.stop_connector.return_value = True
    actions = _actions(db=db, client=client)

    ok = actions.recreate_with_offset(
        _job(connector_name="conA.001"),
        _unhealthy(task_ids=[0]),
        "incident-1",
    )

    assert ok is False
    client.stop_connector.assert_called_once_with("conA.001")
    client.delete_connector.assert_not_called()
    client.create_connector.assert_called_once()
    assert client.create_connector.call_args.kwargs["initial_state"] == "STOPPED"
    client.patch_offsets.assert_called_once()
    client.resume_connector.assert_not_called()
    assert db.update_connector_fields.call_args.kwargs["connector_name"] == "conA.002"
    assert [
        call.kwargs["event_type"] for call in db.record_connector_log.call_args_list
    ][-1] == "CONNECTOR_RECREATE_WITH_OFFSET_FAILED"


def test_recreate_with_offset_create_timeout_logs_retryable_timeout():
    db = MagicMock()
    client = MagicMock()
    client.get_offsets.return_value = {
        "offsets": [{"partition": {"server": "oracle_cdc"}, "offset": {"scn": "123"}}]
    }
    client.create_connector.side_effect = httpx.TimeoutException("timed out")
    client.stop_connector.return_value = True
    actions = _actions(db=db, client=client)

    ok = actions.recreate_with_offset(
        _job(connector_name="conA.001"),
        _unhealthy(task_ids=[0]),
        "incident-1",
    )

    assert ok is False
    client.stop_connector.assert_called_once_with("conA.001")
    client.delete_connector.assert_not_called()
    assert db.record_connector_log.call_args.kwargs["event_type"] == (
        "CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT"
    )
    assert db.record_connector_log.call_args.kwargs["attempt_no"] == 1
    assert db.update_connector_fields.call_args.kwargs["connector_name"] == "conA.002"


def test_timed_out_recreate_reuses_existing_connector_before_offset_patch():
    db = MagicMock()
    client = MagicMock()
    client.connector_exists.return_value = True
    client.stop_connector.return_value = True
    client.patch_offsets.return_value = True
    client.resume_connector.return_value = True
    actions = _actions(db=db, client=client)
    offsets = {"offsets": [{"partition": {"server": "oracle_cdc"}, "offset": {"scn": "123"}}]}

    ok = actions.retry_timed_out_recreate_with_offset(
        _job(
            connector_name="conA.002",
            latest_event_details={
                "original_connector_name": "conA.001",
                "old_connector_name": "conA.001",
                "new_connector_name": "conA.002",
                "offsets": offsets,
            },
        ),
        _unhealthy(task_ids=[0]),
        "incident-1",
    )

    assert ok is True
    client.create_connector.assert_not_called()
    client.stop_connector.assert_called_once_with("conA.002")
    client.patch_offsets.assert_called_once_with("conA.002", offsets)
    client.resume_connector.assert_called_once_with("conA.002")


def test_recreate_without_offset_increments_version_again():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    client = MagicMock()
    client.stop_connector.return_value = True
    actions = _actions(db=db, client=client)

    actions.recreate_without_offset(
        _job(
            connector_name="conA.002",
            active_config=None,
            config_template={
                "name": "conA.001",
                "config": {"connector.class": "X", "name": "conA.001"},
            },
        ),
        _unhealthy(task_ids=[0]),
    )

    client.stop_connector.assert_called_once_with("conA.002")
    client.delete_connector.assert_called_once_with("conA.002")
    client.create_connector.assert_called_once()
    assert client.create_connector.call_args.args[0] == "conA.003"
    assert client.create_connector.call_args.args[1]["connector.class"] == "X"
    assert client.create_connector.call_args.args[1]["name"] == "conA.003"
    assert (
        client.create_connector.call_args.args[1]["schema.history.internal.kafka.topic"]
        == "schema-history.conA.003"
    )
    assert "config" not in client.create_connector.call_args.args[1]
    assert db.update_connector_fields.call_args.kwargs["connector_name"] == "conA.003"
    db.reset_topic_lag_after_connector_recreate.assert_called_once_with(
        connector_id=1,
        old_connector_name="conA.002",
        new_connector_name="conA.003",
    )


def test_escalate_records_exhausted_healing_action():
    db = MagicMock()
    db.ensure_active_incident.return_value = "incident-1"
    actions = _actions(db=db)

    actions.escalate(_job(), _unhealthy())

    assert db.record_connector_log.call_args.kwargs["event_type"] == "HEALING_ESCALATED"
    fields = db.update_connector_fields.call_args.kwargs
    assert fields["is_active"] is False
    assert fields["failed_count"] == 7
    assert fields["failed_task"] is True
    assert fields["failed_connector"] is True
    assert fields["last_failed_at"]
