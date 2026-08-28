import json
from uuid import UUID

import pytest

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
            self.committed = True
        return False

    def cursor(self, *_, **__):
        return self.cursor_obj


def _repository(connection):
    repo = object.__new__(HealingRepository)
    repo._get_conn = lambda: connection
    return repo


def test_update_connector_fields_rejects_unknown_field():
    with pytest.raises(ValueError, match="unknown_field"):
        _repository(_Connection()).update_connector_fields(1, unknown_field="x")


def test_update_connector_fields_serializes_only_physical_columns():
    connection = _Connection()
    repo = _repository(connection)

    repo.update_connector_fields("connector-id", failed_count=4, failed_task=True)

    sql, params = connection.cursor_obj.executed[-1]
    assert sql == "EXEC dbo.spUpdateConnector @ConnectorId = ?, @Fields = ?"
    assert params[0] == "connector-id"
    assert json.loads(params[1]) == {"failed_count": 4, "failed_task": True}


def test_record_connector_log_persists_audit_details():
    connection = _Connection()
    repo = _repository(connection)

    repo.record_connector_log(
        connector_name="conn-x",
        event_type="TASK_RESTART",
        message="Restarted failed task 0",
        severity="WARNING",
        task_id=0,
        details={"reason": "FAILED"},
    )

    sql, params = connection.cursor_obj.executed[-1]
    assert sql.startswith("EXEC dbo.spInsertConnectorHealingLog")
    assert params[2:4] == ("conn-x", "TASK_RESTART")
    assert json.loads(params[9]) == {
        "reason": "FAILED",
        "severity": "WARNING",
        "task_id": 0,
    }
    assert connection.committed is True


def test_list_connectors_derives_healing_state_from_context():
    connection = _Connection(results=[[{
        "id": "connector-id",
        "connector_name": "conn-x",
        "failed_count": 7,
        "config_template": {"connector.class": "X"},
        "active_incident_id": "incident-1",
        "latest_event_type": "CONNECTOR_RESTART",
        "latest_attempt_no": 1,
        "latest_has_next_step": True,
        "latest_message": "restart",
        "latest_event_details": {"task_ids": [0]},
        "latest_event_at": "2026-06-03T00:00:01+00:00",
        "task_restart_count": 3,
        "connector_restart_count": 1,
    }]])

    row = _repository(connection).list_connectors()[0]

    assert row["active_incident_id"] == "incident-1"
    assert row["current_phase"] == "CONNECTOR_RESTARTING"
    assert row["task_restart_count"] == 3
    assert row["last_failed_task_ids"] == [0]


def test_get_connector_by_id_resolves_runtime_config_on_demand():
    connection = _Connection(results=[[{
        "id": "connector-id",
        "connector_name": "conn-x",
        "failed_count": 7,
        "config_id": "318",
        "config_template": {
            "database.url": "{url}",
            "database.password": "{pwd}",
            "schema.history.internal.kafka.bootstrap.servers": "{kafka_server}",
        },
        "credential": "jdbc:oracle:thin:@host/service;secret",
        "kafka_server": "kafka-1:9092,kafka-2:9092",
    }]])

    row = _repository(connection).get_connector_by_id(
        "connector-id",
        include_runtime_config=True,
    )

    assert row["active_config"]["database.password"] == "secret"
    assert row["active_config"]["database.url"] == "jdbc:oracle:thin:@host/service"
    assert row["active_config"]["schema.history.internal.kafka.bootstrap.servers"] == (
        "kafka-1:9092,kafka-2:9092"
    )


def test_ensure_active_incident_reuses_open_incident_or_creates_uuid():
    repo = _repository(_Connection(results=[[{"active_incident_id": "incident-1"}]]))
    assert repo.ensure_active_incident("connector-id") == "incident-1"

    repo._get_conn = lambda: _Connection(results=[[{"active_incident_id": None}]])
    UUID(repo.ensure_active_incident("connector-id"))
