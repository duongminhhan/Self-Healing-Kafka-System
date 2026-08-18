import json
from uuid import UUID

from self_healthy_kafka.storage.mssql import HealingRepository


class _Cursor:
    rowcount = 1

    def __init__(self, results=None):
        self.results = list(results or [])
        self.current = []
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        self.current = self.results.pop(0) if self.results else []

    def fetchone(self):
        return self.current[0] if self.current else None

    def fetchall(self):
        return self.current


class _Connection:
    def __init__(self, results=None):
        self.cursor_obj = _Cursor(results=results)
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.commit()
        return False

    def cursor(self, *_, **__):
        return self.cursor_obj

    def commit(self):
        self.committed = True


def test_update_connector_fields_rejects_unknown_field():
    repo = object.__new__(HealingRepository)

    try:
        repo.update_connector_fields(1, unknown_field="x")
    except ValueError as exc:
        assert "unknown_field" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_list_connector_activity_exposes_debezium_source_server():
    conn = _Connection(
        results=[
            [
                {
                    "connector_name": "CDC.006",
                    "is_active": True,
                    "source_server": "CDC",
                }
            ]
        ]
    )
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    rows = repo.list_connector_activity()
    sql, _params = conn.cursor_obj.executed[-1]

    assert sql == (
        "EXEC dbo.spGetConnectorContext "
        "@ActiveOnly = 0, @IncludeRuntimeConfig = 0, "
        "@IncludeHealingState = 0"
    )
    assert rows == [
        {
            "connector_name": "CDC.006",
            "is_active": True,
            "source_server": "CDC",
        }
    ]


def test_record_connector_log_inserts_healing_log_row():
    conn = _Connection()
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    repo.record_connector_log(
        connector_name="conn-x",
        event_type="TASK_RESTART",
        message="Restarted failed task 0",
        severity="WARNING",
        task_id=0,
        details={"reason": "FAILED"},
    )

    sql, params = conn.cursor_obj.executed[-1]
    assert sql.startswith("EXEC dbo.spInsertConnectorHealingLog")
    assert params[2] == "conn-x"
    assert params[3] == "TASK_RESTART"
    assert params[8] == "Restarted failed task 0"
    details = json.loads(params[9])
    assert details["severity"] == "WARNING"
    assert details["task_id"] == 0
    assert conn.committed is True


def test_update_connector_fields_updates_only_physical_columns():
    conn = _Connection()
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    repo.update_connector_fields(
        "connector-id",
        failed_count=4,
        failed_task=True,
    )

    sql, params = conn.cursor_obj.executed[-1]
    assert sql == "EXEC dbo.spUpdateConnector @ConnectorId = ?, @Fields = ?"
    assert params[0] == "connector-id"
    assert json.loads(params[1]) == {
        "failed_count": 4,
        "failed_task": True,
    }


def test_update_connector_fields_accepts_config_id_and_pretty_config_json():
    conn = _Connection()
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    repo.update_connector_fields(
        "connector-id",
        config_id="318",
        config_template={"database.password": "{pwd}"},
    )

    _sql, params = conn.cursor_obj.executed[-1]
    payload = params[1]
    assert "\n" in payload
    assert json.loads(payload) == {
        "config_id": "318",
        "config_template": {"database.password": "{pwd}"},
    }


def test_list_connectors_derives_healing_state_from_logs():
    conn = _Connection(
        results=[
            [
                {
                    "id": "connector-id",
                    "connector_name": "conn-x",
                    "failed_count": 7,
                    "config_template": {"connector.class": "X"},
                    "active_incident_id": "incident-1",
                    "latest_event_type": "CONNECTOR_RESTART",
                    "latest_attempt_no": 1,
                    "latest_has_next_step": True,
                    "latest_message": "restart",
                    "latest_event_details": {
                        "task_ids": [0],
                        "checked_at": "2026-06-03T00:00:00+00:00",
                    },
                    "latest_event_at": "2026-06-03T00:00:01+00:00",
                    "task_restart_count": 3,
                    "connector_restart_count": 1,
                }
            ],
        ]
    )
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    row = repo.list_connectors()[0]

    assert row["active_incident_id"] == "incident-1"
    assert row["current_phase"] == "CONNECTOR_RESTARTING"
    assert row["task_restart_count"] == 3
    assert row["connector_restart_count"] == 1
    assert row["last_failed_task_ids"] == [0]
    assert row["latest_event_details"]["task_ids"] == [0]


def test_list_connectors_does_not_load_runtime_config():
    conn = _Connection(
        results=[
            [
                {
                    "id": "connector-id",
                    "connector_name": "conn-x",
                    "failed_count": 0,
                    "config_id": "318",
                    "config_template": None,
                    "credential": None,
                    "kafka_server": None,
                }
            ]
        ]
    )
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    row = repo.list_connectors()[0]

    assert row["config_template"] == {}
    assert row["active_config"] is None
    assert [item[0] for item in conn.cursor_obj.executed] == [
        "EXEC dbo.spGetConnectorContext @IncludeRuntimeConfig = 0"
    ]


def test_get_connector_by_id_resolves_runtime_config_on_demand():
    conn = _Connection(
        results=[
            [
                {
                    "id": "connector-id",
                    "connector_name": "conn-x",
                    "failed_count": 7,
                    "config_id": "318",
                    "config_template": {
                        "database.url": "{url}",
                        "database.password": "{pwd}",
                        "schema.history.internal.kafka.bootstrap.servers": ("{kafka_server}"),
                    },
                    "credential": "jdbc:oracle:thin:@host/service;secret",
                    "kafka_server": "kafka-1:9092,kafka-2:9092",
                }
            ]
        ]
    )
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    row = repo.get_connector_by_id(
        "connector-id",
        include_runtime_config=True,
    )

    assert row is not None
    assert row["config_template"]["database.url"] == "{url}"
    assert row["active_config"]["database.url"] == "jdbc:oracle:thin:@host/service"
    assert row["active_config"]["database.password"] == "secret"
    assert (
        row["active_config"]["schema.history.internal.kafka.bootstrap.servers"]
        == "kafka-1:9092,kafka-2:9092"
    )
    assert row.get("credential") is None
    assert row.get("kafka_server") is None
    assert conn.cursor_obj.executed[-1][0] == (
        "EXEC dbo.spGetConnectorContext @ConnectorId = ?, @IncludeRuntimeConfig = 1"
    )


def test_list_connectors_ignores_closed_incident_when_failure_counter_restarted():
    conn = _Connection(
        results=[
            [
                {
                    "id": "connector-id",
                    "connector_name": "conn-x",
                    "failed_count": 3,
                    "config_template": {"connector.class": "X"},
                    "active_incident_id": None,
                }
            ]
        ]
    )
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    row = repo.list_connectors()[0]

    assert row["active_incident_id"] is None
    assert row["task_restart_count"] == 0


def test_ensure_active_incident_reuses_only_open_incident():
    open_conn = _Connection(results=[[{"active_incident_id": "incident-1"}]])
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: open_conn

    assert repo.ensure_active_incident("connector-id") == "incident-1"

    closed_conn = _Connection(results=[[{"active_incident_id": None}]])
    repo._get_conn = lambda: closed_conn

    new_incident_id = repo.ensure_active_incident("connector-id")
    assert new_incident_id != "incident-1"
    UUID(new_incident_id)


def test_list_monitored_topics_uses_topic_lag_jobs():
    conn = _Connection(
        results=[
            [
                {
                    "topic_lag_job_id": "topic-job-id",
                    "connector_id": "connector-id",
                    "job_name": "job-a",
                    "connector_name": "conn-x",
                    "topic_name": "topic-a",
                    "is_over_threshold": True,
                    "last_end_offset": 42,
                    "last_message_at": "2026-05-29T00:00:00+00:00",
                    "updated_at": "2026-05-29T00:01:00+00:00",
                }
            ]
        ]
    )
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    topics = repo.list_monitored_topics(default_threshold_seconds=60)
    sql, _params = conn.cursor_obj.executed[-1]

    assert sql == "EXEC dbo.spGetMonitoredTopics"
    assert topics[0]["topic_lag_job_id"] == "topic-job-id"
    assert topics[0]["connector_name"] == "conn-x"
    assert topics[0]["topic_name"] == "topic-a"
    assert topics[0]["last_end_offset"] == 42
    assert topics[0]["is_over_threshold"] is True
    assert topics[0]["updated_at"] == "2026-05-29T00:01:00+00:00"
    assert topics[0]["idle_threshold_seconds"] == 60


def test_update_topic_lag_state_updates_job_state():
    conn = _Connection()
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    repo.update_topic_lag_state(
        topic_lag_job_id="topic-job-id",
        last_end_offset=42,
        last_message_at="2026-05-29T00:00:00+00:00",
        is_over_threshold=True,
    )

    sql, params = conn.cursor_obj.executed[-1]
    assert sql.startswith("EXEC dbo.spUpdateTopicLagState")
    assert params == (
        "topic-job-id",
        42,
        "2026-05-29T00:00:00+00:00",
        True,
    )
    assert conn.committed is True


def test_list_topic_lag_metrics_exposes_last_message_timestamp():
    conn = _Connection(
        results=[
            [
                {
                    "metric_group": "topic_lag",
                    "connector_name": "conn-x",
                    "topic_name": "topic-a",
                    "is_over_threshold": True,
                    "last_message_timestamp_seconds": 1781229000,
                }
            ]
        ]
    )
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    metrics = repo.list_topic_lag_metrics()
    sql, _params = conn.cursor_obj.executed[-1]

    assert sql == "EXEC dbo.spGetTopicLagStateMetrics"
    assert _params is None
    assert metrics == [
        {
            "connector_name": "conn-x",
            "topic_name": "topic-a",
            "is_over_threshold": True,
            "last_message_timestamp_seconds": 1781229000,
        }
    ]


def test_operational_metrics_use_focused_stored_procedures():
    conn = _Connection(
        results=[
            [
                {
                    "connector_name": "conn-x",
                    "base_connector_name": "conn",
                    "event_type": "HEALTH_FAILED_CONFIRMED",
                    "healing_step": 1,
                    "total_attempts": 2,
                }
            ],
            [
                {
                    "connector_name": "conn-x",
                    "last_healing_recovered_timestamp_seconds": 1780000000,
                }
            ],
            [],
            [
                {
                    "connector_name": "conn-x",
                    "topic_name": "topic-a",
                    "event_status": "RECOVERED",
                    "condition": "topic_idle",
                    "total_events": 1,
                }
            ],
        ]
    )
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    metrics = repo.get_operational_metrics(window_minutes=30)

    assert conn.cursor_obj.executed == [
        ("EXEC dbo.spGetHealingAttemptMetrics", None),
        ("EXEC dbo.spGetHealingRecoveredMetrics @WindowMinutes = ?", (30,)),
        ("EXEC dbo.spGetHealingEscalatedMetrics @WindowMinutes = ?", (30,)),
        ("EXEC dbo.spGetTopicLagEventMetrics @WindowMinutes = ?", (30,)),
    ]
    assert metrics["healing_attempt"][0]["total_attempts"] == 2
    assert metrics["healing_attempt"][0]["base_connector_name"] == "conn"
    assert metrics["healing_recovered"][0]["last_healing_recovered_timestamp_seconds"] == 1780000000
    assert metrics["healing_escalated"] == []
    assert metrics["topic_lag_event"][0]["event_status"] == "RECOVERED"
    assert metrics["topic_lag"] == []


def test_operational_metrics_isolate_a_failed_metric_group():
    class FailingCursor(_Cursor):
        def execute(self, sql, params=None):
            if "spGetHealingRecoveredMetrics" in sql:
                self.executed.append((sql, params))
                raise RuntimeError("recovered metric query failed")
            super().execute(sql, params)

    conn = _Connection(
        results=[
            [],
            [],
            [],
        ]
    )
    conn.cursor_obj = FailingCursor(results=conn.cursor_obj.results)
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    metrics = repo.get_operational_metrics(window_minutes=15)

    assert metrics["healing_attempt"] == []
    assert metrics["healing_recovered"] is None
    assert metrics["healing_escalated"] == []
    assert metrics["topic_lag_event"] == []
    assert [sql for sql, _params in conn.cursor_obj.executed] == [
        "EXEC dbo.spGetHealingAttemptMetrics",
        "EXEC dbo.spGetHealingRecoveredMetrics @WindowMinutes = ?",
        "EXEC dbo.spGetHealingEscalatedMetrics @WindowMinutes = ?",
        "EXEC dbo.spGetTopicLagEventMetrics @WindowMinutes = ?",
    ]


def test_record_topic_lag_log_inserts_topic_lag_log_row():
    conn = _Connection()
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    repo.record_topic_lag_log(
        topic_lag_job_id="topic-job-id",
        connector_id="connector-id",
        job_name="job-a",
        connector_name="conn-x",
        topic_name="topic-a",
        event_status="OVER_THRESHOLD",
        message="Topic topic-a has no new messages",
        end_offset=42,
        details={
            "lag_condition": "topic_idle",
            "milliseconds_since_last_event": 60000,
        },
    )

    sql, params = conn.cursor_obj.executed[-1]
    assert sql.startswith("EXEC dbo.spInsertTopicLagLog")
    assert params[0] == "topic-job-id"
    assert params[4] == "topic-a"
    assert params[6] == "OVER_THRESHOLD"
    details = json.loads(params[8])
    assert details["lag_condition"] == "topic_idle"
    assert details["milliseconds_since_last_event"] == 60000


def test_reset_topic_lag_after_connector_recreate_supersedes_active_lag_state():
    conn = _Connection(
        results=[
            [
                {
                    "topic_lag_job_id": "topic-job-id",
                    "job_name": "job-a",
                    "topic_name": "topic-a",
                    "last_end_offset": 42,
                }
            ]
        ]
    )
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: conn

    count = repo.reset_topic_lag_after_connector_recreate(
        connector_id="connector-id",
        old_connector_name="conn-a",
        new_connector_name="conn-a.001",
    )

    assert count == 1
    select_sql, select_params = conn.cursor_obj.executed[0]
    assert select_sql == ("EXEC dbo.spGetMonitoredTopics @ConnectorId = ?, @OverThresholdOnly = 1")
    assert select_params == ("connector-id",)

    insert_sql, insert_params = conn.cursor_obj.executed[1]
    assert insert_sql.startswith("EXEC dbo.spInsertTopicLagLog")
    assert insert_params[1] == "connector-id"
    assert insert_params[3] == "conn-a"
    assert insert_params[6] == "SUPERSEDED"
    details = json.loads(insert_params[8])
    assert details["old_connector_name"] == "conn-a"
    assert details["new_connector_name"] == "conn-a.001"

    reset_sql, reset_params = conn.cursor_obj.executed[2]
    assert reset_sql == (
        "EXEC dbo.spUpdateTopicLagState @ConnectorId = ?, @ResetConnectorTopics = 1"
    )
    assert reset_params == ("connector-id",)
    assert conn.committed is True
