from unittest.mock import MagicMock, call

from self_healthy_kafka import main as main_module
from self_healthy_kafka.main import run_health_checks, sync_connector_metrics


def test_run_health_checks_delegates_to_db_state_machine():
    state_machine = MagicMock()
    state_machine.tick.return_value = []

    run_health_checks(state_machine)

    state_machine.tick.assert_called_once_with()


def test_run_health_checks_schedules_recovery_followups():
    state_machine = MagicMock()
    state_machine.tick.return_value = ["CDC.001", "CDC1.002"]
    schedule_followup = MagicMock()

    run_health_checks(state_machine, schedule_followup)

    assert schedule_followup.call_args_list == [
        call("CDC.001"),
        call("CDC1.002"),
    ]


def test_sync_connector_metrics_syncs_db_backed_metrics(monkeypatch):
    state_machine = MagicMock()
    state_machine.db.list_connector_activity.return_value = [
        {"connector_name": "conn-x", "is_active": True}
    ]
    state_machine.db.get_operational_metrics.return_value = {
        "healing_attempt": [
            {
                "connector_name": "conn-x",
                "event_type": "HEALTH_FAILED_CONFIRMED",
                "healing_step": 0,
                "total_attempts": 1,
            }
        ],
        "healing_recovered": [
            {
                "connector_name": "conn-x",
                "last_healing_recovered_timestamp_seconds": 1780000000,
            }
        ],
        "healing_escalated": [],
        "topic_lag_event": [
            {
                "connector_name": "conn-x",
                "topic_name": "topic-a",
                "event_status": "RECOVERED",
                "condition": "topic_idle",
                "total_events": 1,
            }
        ],
    }
    sync_activity = MagicMock()
    sync_attempts = MagicMock()
    sync_recovered = MagicMock()
    sync_topic_events = MagicMock()
    monkeypatch.setattr(main_module, "sync_connector_activity", sync_activity)
    monkeypatch.setattr(main_module, "sync_healing_attempt_metrics", sync_attempts)
    monkeypatch.setattr(main_module, "sync_healing_recovered_metrics", sync_recovered)
    monkeypatch.setattr(
        main_module,
        "sync_topic_lag_event_metrics",
        sync_topic_events,
    )

    sync_connector_metrics(state_machine)

    sync_activity.assert_called_once_with([{"connector_name": "conn-x", "is_active": True}])
    sync_attempts.assert_called_once_with(
        [
            {
                "connector_name": "conn-x",
                "event_type": "HEALTH_FAILED_CONFIRMED",
                "healing_step": 0,
                "total_attempts": 1,
            }
        ]
    )
    sync_recovered.assert_called_once_with(
        [
            {
                "connector_name": "conn-x",
                "last_healing_recovered_timestamp_seconds": 1780000000,
            }
        ]
    )
    state_machine.db.get_operational_metrics.assert_called_once_with(window_minutes=15)
    sync_topic_events.assert_called_once_with(
        [
            {
                "connector_name": "conn-x",
                "topic_name": "topic-a",
                "event_status": "RECOVERED",
                "condition": "topic_idle",
                "total_events": 1,
            }
        ]
    )


def test_sync_connector_metrics_preserves_group_when_its_query_failed(monkeypatch):
    state_machine = MagicMock()
    state_machine.db.list_connector_activity.return_value = []
    state_machine.db.get_operational_metrics.return_value = {
        "healing_attempt": None,
        "healing_recovered": [],
        "healing_escalated": [],
        "topic_lag_event": [],
    }
    sync_attempts = MagicMock()
    sync_recovered = MagicMock()
    sync_escalated = MagicMock()
    sync_topic_events = MagicMock()
    monkeypatch.setattr(main_module, "sync_connector_activity", MagicMock())
    monkeypatch.setattr(main_module, "sync_healing_attempt_metrics", sync_attempts)
    monkeypatch.setattr(main_module, "sync_healing_recovered_metrics", sync_recovered)
    monkeypatch.setattr(main_module, "sync_healing_escalated_metrics", sync_escalated)
    monkeypatch.setattr(
        main_module,
        "sync_topic_lag_event_metrics",
        sync_topic_events,
    )

    sync_connector_metrics(state_machine)

    sync_attempts.assert_not_called()
    sync_recovered.assert_called_once_with([])
    sync_escalated.assert_called_once_with([])
    sync_topic_events.assert_called_once_with([])
