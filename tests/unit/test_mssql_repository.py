import json

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


def test_update_queue_fields_rejects_unknown_field():
    with pytest.raises(ValueError, match="unknown_field"):
        _repository(_Connection()).update_queue_fields(1, unknown_field="x")


def test_update_queue_fields_serializes_queue_columns():
    connection = _Connection()
    repo = _repository(connection)
    repo.update_queue_fields(
        "queue-id",
        queue_status="WAITING",
        current_connector_name="conn-x.001",
    )

    sql, params = connection.cursor_obj.executed[-1]
    assert sql == "EXEC dbo.spUpdateConnectorHealingQueue @QueueId = ?, @Fields = ?"
    assert params[0] == "queue-id"
    assert json.loads(params[1]) == {
        "queue_status": "WAITING",
        "current_connector_name": "conn-x.001",
    }


def test_record_connector_log_persists_audit_details_for_queue():
    connection = _Connection()
    repo = _repository(connection)
    repo.record_connector_log(
        connector_id="queue-id",
        connector_name="conn-x",
        event_type="TASK_RESTART",
        message="Restarted failed task 0",
        severity="WARNING",
        task_id=0,
        details={"reason": "FAILED"},
    )

    sql, params = connection.cursor_obj.executed[-1]
    assert sql.startswith("EXEC dbo.spInsertConnectorHealingLog")
    assert params[0] == "queue-id"
    assert params[1:3] == ("conn-x", "TASK_RESTART")
    assert json.loads(params[7]) == {
        "reason": "FAILED",
        "severity": "WARNING",
        "task_id": 0,
    }
    assert connection.committed is True


def test_list_connectors_reads_only_due_open_queue_items():
    connection = _Connection(results=[[{
        "id": "queue-id",
        "connector_name": "conn-x",
        "root_connector_name": "conn-x",
        "queue_status": "PENDING",
        "healing_mode": "RESTART_ONLY",
        "failed_count": 1,
        "latest_event_type": "HEALTH_FAILURE_OBSERVED",
        "latest_event_details": {"task_ids": [0]},
        "task_restart_count": 0,
        "connector_restart_count": 0,
    }]])

    row = _repository(connection).list_connectors()[0]

    sql, params = connection.cursor_obj.executed[-1]
    assert "spGetConnectorHealingQueue" in sql
    assert params[-1] is True
    assert row["active_incident_id"] == "queue-id"
    assert row["last_failed_task_ids"] == [0]
    assert row["active_config"] is None


def test_enqueue_connector_uses_queue_procedure():
    connection = _Connection(results=[[{
        "id": "queue-id",
        "connector_name": "conn-x",
        "root_connector_name": "conn-x",
        "queue_status": "PENDING",
        "healing_mode": "RECOVERY",
    }]])

    row = _repository(connection).enqueue_connector(
        root_connector_name="conn-x",
        current_connector_name="conn-x",
        connector_class="io.debezium.connector.oracle.OracleConnector",
        healing_mode="RECOVERY",
    )

    sql, params = connection.cursor_obj.executed[-1]
    assert "spEnqueueConnectorHealing" in sql
    assert params[-1] == "RECOVERY"
    assert row["connector_name"] == "conn-x"
