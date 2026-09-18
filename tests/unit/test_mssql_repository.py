import json

import pytest

from self_healthy_kafka.storage.mssql import HealingRepository
from self_healthy_kafka.semantic.fact_source import IncidentFactSource, incident_fact_source
from self_healthy_kafka.semantic.tsql import compile_incident_query
from self_healthy_kafka.webhook.analytics import parse_plan


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


def test_search_healing_logs_uses_fixed_parameterized_retrieval_procedure():
    connection = _Connection(results=[[{"id": "log-1", "message": "ORA-01291"}]])

    rows = _repository(connection).search_healing_logs_for_chat(
        question="TOPO-A ORA-01291", limit=20
    )

    sql, params = connection.cursor_obj.executed[-1]
    assert sql == "EXEC dbo.spSearchConnectorHealingLogs @SearchText = ?, @Limit = ?"
    assert params == ("TOPO-A ORA-01291", 20)
    assert rows == [{"id": "log-1", "message": "ORA-01291"}]


def test_incident_facts_use_fixed_read_only_procedure_and_bound_parameters():
    connection = _Connection(results=[[{"incident_id": "incident-1"}]])

    rows = _repository(connection).list_incident_facts_for_chat(
        from_at="2026-09-03T00:00:00+07:00", to_at="2026-09-04T00:00:00+07:00",
        event_type="HEALTH_FAILED_CONFIRMED", final_outcome="OPEN",
        connector_name="TOPO-A", error_code="ORA-01013", limit=20,
    )

    sql, params = connection.cursor_obj.executed[-1]
    assert sql.startswith("EXEC dbo.spGetConnectorIncidentFacts")
    assert "ORA-01013" not in sql
    assert params == (
        "2026-09-03T00:00:00+07:00", "2026-09-04T00:00:00+07:00",
        "HEALTH_FAILED_CONFIRMED", "OPEN", "TOPO-A", "ORA-01013", 20,
    )
    assert rows == [{"incident_id": "incident-1"}]


def test_compiled_incident_query_executes_only_the_approved_parameterized_cte():
    connection = _Connection(results=[[{"root_connector_name": "orders", "incident_count": 3}]])
    query = compile_incident_query(parse_plan({
        "dataset": "connector_incidents",
        "metrics": [{"name": "failure_count", "aggregation": "count_distinct_incident"}],
        "group_by": ["job_name"],
        "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
        "order_by": [{"field": "failure_count", "direction": "desc"}],
        "limit": 3,
    }), from_at=None, to_at=None, row_limit=501)

    rows = _repository(connection).execute_compiled_incident_query(
        statement=query.statement, parameters=query.parameter_values
    )

    sql, parameters = connection.cursor_obj.executed[-1]
    assert sql == query.statement
    assert parameters == query.parameter_values
    assert connection.timeout == 10
    assert "ConnectorHealingLogs" not in sql
    assert "EXEC " not in sql.upper()
    assert rows == [{"root_connector_name": "orders", "incident_count": 3}]


def test_compiled_incident_query_rejects_non_select_input_at_repository_boundary():
    with pytest.raises(ValueError, match="approved read-only"):
        _repository(_Connection()).execute_compiled_incident_query(
            statement="DELETE FROM [dbo].[ConnectorHealingLogs]", parameters=()
        )


def test_compiled_incident_query_rejects_an_unapproved_dbt_source_at_repository_boundary():
    source = IncidentFactSource(
        key="dbt",
        qualified_name="[analytics].[vSemanticConnectorIncidentFacts]; DROP TABLE x;--",
        evidence_source="vSemanticConnectorIncidentFacts",
    )
    with pytest.raises(ValueError, match="source is not approved"):
        _repository(_Connection()).execute_compiled_incident_query(
            statement="SELECT 1", parameters=(), fact_source=source
        )


def test_compiled_incident_query_accepts_the_allowlisted_dbt_compatibility_view():
    connection = _Connection(results=[[{"root_connector_name": "orders", "incident_count": 3}]])
    source = incident_fact_source(mode="dbt", dbt_schema="analytics")
    query = compile_incident_query(parse_plan({
        "dataset": "connector_incidents",
        "metrics": [{"name": "failure_count", "aggregation": "count_distinct_incident"}],
        "group_by": ["job_name"],
        "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
        "order_by": [{"field": "failure_count", "direction": "desc"}],
        "limit": 3,
    }), from_at=None, to_at=None, row_limit=501, fact_source=source)

    rows = _repository(connection).execute_compiled_incident_query(
        statement=query.statement, parameters=query.parameter_values, fact_source=source
    )

    assert rows == [{"root_connector_name": "orders", "incident_count": 3}]
    assert "[analytics].[vSemanticConnectorIncidentFacts]" in connection.cursor_obj.executed[-1][0]


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
