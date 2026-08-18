import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from self_healthy_kafka.health.topic_idle import DbTopicIdleProbe


def _probe(db):
    return DbTopicIdleProbe(
        bootstrap_servers=["kafka:29092"],
        db=db,
        default_idle_threshold_seconds=60,
        event_capture_lag_threshold_ms=60000,
    )


def _probe_with_event_capture(db):
    return DbTopicIdleProbe(
        bootstrap_servers=["kafka:29092"],
        db=db,
        default_idle_threshold_seconds=60,
        event_capture_lag_threshold_ms=60000,
        topic_idle_enabled=True,
        event_capture_enabled=True,
    )


def _item(**overrides):
    now = datetime.now(timezone.utc)
    base = {
        "topic_lag_job_id": "topic-job-id",
        "connector_id": "connector-id",
        "job_name": "job-a",
        "connector_name": "conn-a",
        "topic_name": "topic-a",
        "idle_threshold_seconds": 60,
        "last_end_offset": 10,
        "last_message_at": (now - timedelta(seconds=61)).isoformat(),
        "is_over_threshold": False,
    }
    base.update(overrides)
    return base


def test_db_topic_idle_logs_only_first_idle_breach(caplog):
    db = MagicMock()
    probe = _probe(db)
    item = _item()
    now = datetime.fromisoformat(item["last_message_at"]) + timedelta(seconds=61)

    with caplog.at_level(logging.DEBUG):
        probe._evaluate_topic_idle(item, total=10, now=now)
        probe._evaluate_topic_idle({**item, "is_over_threshold": True}, total=10, now=now)

    db.record_topic_lag_log.assert_called_once()
    args = db.record_topic_lag_log.call_args.kwargs
    assert args["event_status"] == "OVER_THRESHOLD"
    assert args["details"]["milliseconds_since_last_event"] == 61000
    assert args["details"]["lag_seconds"] == 61
    assert args["details"]["lag_condition"] == "topic_idle"
    assert sum(
        1
        for record in caplog.records
        if getattr(record, "event", None) == "TOPIC_IDLE_WARNING"
    ) == 1
    assert any(
        getattr(record, "event", None) == "topic_lag_state"
        and getattr(record, "over_threshold", None) == 1
        for record in caplog.records
    )


def test_db_topic_idle_resets_state_when_offset_increases(caplog):
    db = MagicMock()
    probe = _probe(db)
    now = datetime.now(timezone.utc)
    latest_message_at = now

    with caplog.at_level(logging.DEBUG):
        probe._evaluate_topic_idle(
            _item(last_message_at=(now - timedelta(seconds=200)).isoformat()),
            total=11,
            now=now,
            latest_message_at=latest_message_at,
        )

    db.record_topic_lag_log.assert_not_called()
    state_args = db.update_topic_lag_state.call_args.kwargs
    assert state_args["last_end_offset"] == 11
    assert state_args["is_over_threshold"] is False
    assert any(
        getattr(record, "event", None) == "topic_lag_state"
        and getattr(record, "over_threshold", None) == 0
        for record in caplog.records
    )


def test_topic_idle_marks_over_threshold_when_discovered_message_is_already_old():
    db = MagicMock()
    probe = _probe(db)
    previous_message_at = datetime(2026, 6, 11, 1, 0, tzinfo=timezone.utc)
    latest_message_at = previous_message_at + timedelta(minutes=1)
    poll_time = latest_message_at + timedelta(seconds=90)

    probe._evaluate_topic_idle(
        _item(
            last_end_offset=10,
            last_message_at=previous_message_at.isoformat(),
            is_over_threshold=False,
        ),
        total=11,
        now=poll_time,
        latest_message_at=latest_message_at,
    )

    state_args = db.update_topic_lag_state.call_args.kwargs
    assert state_args["last_end_offset"] == 11
    assert state_args["last_message_at"] == latest_message_at
    assert state_args["is_over_threshold"] is True
    log_args = db.record_topic_lag_log.call_args.kwargs
    assert log_args["event_status"] == "OVER_THRESHOLD"
    assert log_args["details"]["lag_seconds"] == 90


def test_topic_idle_records_recovered_then_over_threshold_after_downtime():
    db = MagicMock()
    probe = _probe(db)
    previous_message_at = datetime(2026, 6, 11, 1, 0, tzinfo=timezone.utc)
    latest_message_at = previous_message_at + timedelta(minutes=1)
    poll_time = latest_message_at + timedelta(seconds=90)

    probe._evaluate_topic_idle(
        _item(
            last_end_offset=10,
            last_message_at=previous_message_at.isoformat(),
            is_over_threshold=True,
        ),
        total=11,
        now=poll_time,
        latest_message_at=latest_message_at,
    )

    state_args = db.update_topic_lag_state.call_args.kwargs
    assert state_args["last_end_offset"] == 11
    assert state_args["last_message_at"] == latest_message_at
    assert state_args["is_over_threshold"] is True
    statuses = [
        call.kwargs["event_status"]
        for call in db.record_topic_lag_log.call_args_list
    ]
    assert statuses == ["RECOVERED", "OVER_THRESHOLD"]
    recovered_details = db.record_topic_lag_log.call_args_list[0].kwargs["details"]
    over_threshold_details = db.record_topic_lag_log.call_args_list[1].kwargs["details"]
    assert recovered_details["lag_condition"] == "topic_lag"
    assert recovered_details["previous_message_at"] == previous_message_at.isoformat()
    assert recovered_details["current_message_at"] == latest_message_at.isoformat()
    assert over_threshold_details["lag_condition"] == "topic_idle"
    assert over_threshold_details["lag_seconds"] == 90


def test_topic_probe_records_messages_per_second(monkeypatch):
    db = MagicMock()
    probe = _probe(db)
    now = datetime.now(timezone.utc)
    record_state = MagicMock()
    monkeypatch.setattr(
        "self_healthy_kafka.health.topic_idle.record_topic_lag_state",
        record_state,
    )

    probe._evaluate_topic_idle(
        _item(
            last_end_offset=100,
            updated_at=(now - timedelta(seconds=10)).isoformat(),
            last_message_at=(now - timedelta(seconds=10)).isoformat(),
        ),
        total=120,
        now=now,
    )

    assert record_state.call_args.kwargs["messages_per_second"] == 2
    assert record_state.call_args.kwargs["records_lag"] == 0


def test_topic_probe_records_zero_records_lag_without_source_timestamp(monkeypatch):
    db = MagicMock()
    probe = _probe(db)
    now = datetime.now(timezone.utc)
    record_state = MagicMock()
    monkeypatch.setattr(
        "self_healthy_kafka.health.topic_idle.record_topic_lag_state",
        record_state,
    )

    probe._evaluate_topic_idle(
        _item(
            last_end_offset=100,
            updated_at=(now - timedelta(seconds=120)).isoformat(),
            last_message_at=(now - timedelta(seconds=120)).isoformat(),
        ),
        total=100,
        now=now,
    )

    assert record_state.call_args.kwargs["messages_per_second"] == 0
    assert record_state.call_args.kwargs["records_lag"] == 0


def test_topic_probe_records_source_lag_minutes(monkeypatch):
    db = MagicMock()
    probe = _probe(db)
    probe._reader = MagicMock()
    probe._latest_partition_record_info = MagicMock(
        return_value={"event_capture_lag_ms": 90000}
    )
    now = datetime.now(timezone.utc)
    record_state = MagicMock()
    monkeypatch.setattr(
        "self_healthy_kafka.health.topic_idle.record_topic_lag_state",
        record_state,
    )

    probe._evaluate_topic_idle(
        _item(
            last_end_offset=100,
            updated_at=(now - timedelta(seconds=120)).isoformat(),
            last_message_at=(now - timedelta(seconds=120)).isoformat(),
        ),
        total=100,
        now=now,
    )

    assert record_state.call_args.kwargs["messages_per_second"] == 0
    assert record_state.call_args.kwargs["records_lag"] == 1.5


def test_latest_topic_message_time_prefers_debezium_payload_timestamp():
    db = MagicMock()
    probe = _probe(db)
    payload_capture_ms = 1782984325326
    wrong_kafka_timestamp_ms = int(
        datetime(
            2026,
            7,
            4,
            17,
            25,
            25,
            386000,
            tzinfo=timezone(timedelta(hours=7)),
        ).timestamp()
        * 1000
    )
    record = SimpleNamespace(
        value=json.dumps(
            {
                "source": {"ts_ms": 1782984296000},
                "ts_ms": payload_capture_ms,
            }
        ).encode("utf-8"),
        timestamp=wrong_kafka_timestamp_ms,
    )
    latest_message_at = probe._message_at_from_records([record])

    assert latest_message_at == datetime.fromtimestamp(
        payload_capture_ms / 1000,
        tz=timezone.utc,
    )


def test_db_topic_idle_records_recovery_when_previous_state_was_over_threshold():
    db = MagicMock()
    probe = _probe(db)
    now = datetime.now(timezone.utc)
    previous_message_at = now - timedelta(seconds=120)
    latest_message_at = now - timedelta(seconds=5)

    probe._evaluate_topic_idle(
        _item(
            is_over_threshold=True,
            last_message_at=previous_message_at.isoformat(),
        ),
        total=11,
        now=now,
        latest_message_at=latest_message_at,
    )

    log_args = db.record_topic_lag_log.call_args.kwargs
    assert log_args["event_status"] == "RECOVERED"
    assert log_args["details"]["lag_seconds"] >= 60
    assert db.update_topic_lag_state.call_args.kwargs["is_over_threshold"] is False


def test_topic_idle_does_not_recover_when_offset_increase_has_no_record_timestamp():
    db = MagicMock()
    probe = _probe(db)
    now = datetime.now(timezone.utc)

    probe._evaluate_topic_idle(
        _item(
            is_over_threshold=True,
            last_message_at=(now - timedelta(seconds=120)).isoformat(),
        ),
        total=11,
        now=now,
        latest_message_at=None,
    )

    db.record_topic_lag_log.assert_not_called()
    db.update_topic_lag_state.assert_not_called()


def test_topic_idle_does_not_recover_with_record_older_than_last_observed_state():
    db = MagicMock()
    probe = _probe(db)
    previous_message_at = datetime(2026, 7, 2, 9, 0, tzinfo=timezone.utc)
    latest_message_at = datetime(2026, 7, 2, 9, 24, 56, tzinfo=timezone.utc)
    over_threshold_observed_at = datetime(2026, 7, 2, 10, 0, tzinfo=timezone.utc)
    poll_time = datetime(2026, 7, 2, 10, 30, tzinfo=timezone.utc)

    probe._evaluate_topic_idle(
        _item(
            is_over_threshold=True,
            last_message_at=previous_message_at.isoformat(),
            updated_at=over_threshold_observed_at.isoformat(),
        ),
        total=11,
        now=poll_time,
        latest_message_at=latest_message_at,
    )

    db.record_topic_lag_log.assert_not_called()
    state_args = db.update_topic_lag_state.call_args.kwargs
    assert state_args["last_end_offset"] == 11
    assert state_args["last_message_at"] == previous_message_at
    assert state_args["is_over_threshold"] is True


def test_recovery_duration_uses_kafka_message_timestamps():
    db = MagicMock()
    probe = _probe(db)
    previous_message_at = datetime(2026, 6, 11, 1, 0, tzinfo=timezone.utc)
    current_message_at = previous_message_at + timedelta(hours=2)
    poll_time = current_message_at + timedelta(seconds=17)

    probe._evaluate_topic_idle(
        _item(
            is_over_threshold=True,
            last_message_at=previous_message_at.isoformat(),
        ),
        total=11,
        now=poll_time,
        latest_message_at=current_message_at,
    )

    log_args = db.record_topic_lag_log.call_args.kwargs
    assert log_args["details"]["lag_seconds"] == 7200
    assert log_args["details"]["lag_time_source"] == "kafka_record_timestamp"
    assert log_args["details"]["previous_message_at"] == previous_message_at.isoformat()
    assert log_args["details"]["current_message_at"] == current_message_at.isoformat()
    assert (
        db.update_topic_lag_state.call_args.kwargs["last_message_at"]
        == current_message_at
    )


def test_tick_syncs_active_topic_labels_before_polling(monkeypatch):
    db = MagicMock()
    db.list_monitored_topics.return_value = [_item(is_over_threshold=True)]
    probe = _probe(db)
    probe._ensure_consumer = MagicMock()
    probe._end_offset_total = MagicMock(side_effect=RuntimeError("broker unavailable"))
    sync_active = MagicMock()
    sync_state = MagicMock()
    monkeypatch.setattr(
        "self_healthy_kafka.health.topic_idle.sync_active_topic_labels",
        sync_active,
    )
    monkeypatch.setattr(
        "self_healthy_kafka.health.topic_idle._sync_topic_lag_state_from_db",
        sync_state,
    )

    probe.tick()

    sync_active.assert_called_once_with(
        db.list_monitored_topics.return_value,
        ("topic_idle",),
    )
    sync_state.assert_called_once()


def test_event_capture_lag_can_mark_topic_over_threshold():
    db = MagicMock()
    probe = _probe_with_event_capture(db)
    probe._latest_partition_record_info = MagicMock(
        return_value={
            "event_capture_lag_ms": 90000,
            "partition": 0,
            "offset": 10,
        }
    )
    now = datetime.now(timezone.utc)

    probe._evaluate_topic_lag_state(
        _item(last_message_at=now.isoformat()),
        total=10,
        now=now,
    )

    log_args = db.record_topic_lag_log.call_args.kwargs
    assert log_args["event_status"] == "OVER_THRESHOLD"
    assert log_args["details"]["lag_condition"] == "event_capture_time"
    assert log_args["details"]["active_conditions"] == ["event_capture_time"]
    assert log_args["details"]["lag_seconds"] == 90
    db.update_topic_lag_state.assert_not_called()


def test_event_capture_lag_logs_only_on_transition():
    db = MagicMock()
    probe = _probe_with_event_capture(db)
    probe._latest_partition_record_info = MagicMock(
        return_value={"event_capture_lag_ms": 90000}
    )
    now = datetime.now(timezone.utc)
    item = _item(last_message_at=now.isoformat())

    probe._evaluate_topic_lag_state(item, total=10, now=now)
    probe._evaluate_topic_lag_state(
        {**item, "updated_at": now.isoformat()},
        total=10,
        now=now + timedelta(seconds=30),
    )

    db.record_topic_lag_log.assert_called_once()
    assert db.record_topic_lag_log.call_args.kwargs["event_status"] == "OVER_THRESHOLD"
    assert (
        db.record_topic_lag_log.call_args.kwargs["details"]["lag_condition"]
        == "event_capture_time"
    )


def test_event_capture_lag_does_not_log_again_while_topic_idle_is_already_over_threshold():
    db = MagicMock()
    probe = _probe_with_event_capture(db)
    probe._latest_partition_record_info = MagicMock(
        return_value={"event_capture_lag_ms": 90000}
    )
    now = datetime.now(timezone.utc)

    probe._evaluate_topic_lag_state(
        _item(
            is_over_threshold=True,
            last_message_at=(now - timedelta(seconds=120)).isoformat(),
        ),
        total=10,
        now=now,
    )

    db.record_topic_lag_log.assert_not_called()
    db.update_topic_lag_state.assert_not_called()


def test_event_capture_lag_does_not_clear_persisted_topic_idle_state_without_message():
    db = MagicMock()
    probe = _probe_with_event_capture(db)
    probe._latest_partition_record_info = MagicMock(
        return_value={"event_capture_lag_ms": 90000}
    )
    now = datetime.now(timezone.utc)

    probe._evaluate_topic_lag_state(
        _item(
            is_over_threshold=True,
            last_message_at=(now - timedelta(seconds=10)).isoformat(),
        ),
        total=10,
        now=now,
    )

    log_args = db.record_topic_lag_log.call_args.kwargs
    assert log_args["event_status"] == "OVER_THRESHOLD"
    assert log_args["details"]["lag_condition"] == "event_capture_time"
    db.update_topic_lag_state.assert_not_called()


def test_event_capture_lag_does_not_keep_topic_idle_state_over_threshold():
    db = MagicMock()
    probe = _probe_with_event_capture(db)
    probe._latest_partition_record_info = MagicMock(
        return_value={"event_capture_lag_ms": 90000}
    )
    previous_message_at = datetime(2026, 6, 11, 1, 0, tzinfo=timezone.utc)
    current_message_at = previous_message_at + timedelta(hours=2)
    poll_time = current_message_at + timedelta(seconds=10)

    probe._evaluate_topic_lag_state(
        _item(
            last_end_offset=10,
            last_message_at=previous_message_at.isoformat(),
            is_over_threshold=True,
        ),
        total=11,
        now=poll_time,
        latest_message_at=current_message_at,
    )

    state_args = db.update_topic_lag_state.call_args.kwargs
    assert state_args["last_end_offset"] == 11
    assert state_args["last_message_at"] == current_message_at
    assert state_args["is_over_threshold"] is False
    statuses = [
        call.kwargs["event_status"]
        for call in db.record_topic_lag_log.call_args_list
    ]
    assert statuses == ["RECOVERED", "OVER_THRESHOLD"]
    assert (
        db.record_topic_lag_log.call_args_list[1].kwargs["details"]["lag_condition"]
        == "event_capture_time"
    )


def test_topic_does_not_record_recovered_without_confirmed_new_message():
    db = MagicMock()
    probe = _probe_with_event_capture(db)
    probe._latest_partition_record_info = MagicMock(
        return_value={"event_capture_lag_ms": 1000}
    )
    now = datetime.now(timezone.utc)

    probe._evaluate_topic_lag_state(
        _item(
            is_over_threshold=True,
            last_message_at=(now - timedelta(seconds=10)).isoformat(),
        ),
        total=10,
        now=now,
    )

    db.record_topic_lag_log.assert_not_called()
    db.update_topic_lag_state.assert_not_called()


def test_topic_does_not_clear_lag_job_without_confirmed_new_message():
    db = MagicMock()
    probe = _probe(db)
    now = datetime.now(timezone.utc)

    probe._evaluate_topic_lag_state(
        _item(
            is_over_threshold=True,
            last_message_at=(now - timedelta(seconds=10)).isoformat(),
        ),
        total=10,
        now=now,
    )

    db.record_topic_lag_log.assert_not_called()
    db.update_topic_lag_state.assert_not_called()


def test_topic_does_not_repeat_recovered_when_db_state_remains_over_threshold():
    db = MagicMock()
    probe = _probe_with_event_capture(db)
    probe._latest_partition_record_info = MagicMock(
        return_value={"event_capture_lag_ms": 1000}
    )
    now = datetime.now(timezone.utc)
    item = _item(
        is_over_threshold=True,
        last_message_at=(now - timedelta(seconds=10)).isoformat(),
        updated_at=now.isoformat(),
    )

    probe._evaluate_topic_lag_state(item, total=10, now=now)
    probe._evaluate_topic_lag_state(item, total=10, now=now + timedelta(seconds=5))

    db.record_topic_lag_log.assert_not_called()
