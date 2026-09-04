"""Offline compiler tests: these do not measure Qwen model accuracy."""

import copy
import sqlite3

import pytest

from notebooks.evaluation.fixtures import EXPECTED, VARIANTS, create_duration_fixture
from notebooks.shared.analytics import Snapshot
from notebooks.shared.semantic_plan import PlanError, compile_plan


def run(snapshot, plan):
    compiled = compile_plan(plan, snapshot)
    return snapshot.execute(compiled.sql, compiled.parameters)


@pytest.fixture
def snapshot(tmp_path):
    path = tmp_path / "plan.db"
    create_duration_fixture(path, "normal")
    with sqlite3.connect(path) as connection:
        connection.executescript("""
            ALTER TABLE ConnectorHealingQueue ADD CurrentConnectorName TEXT;
            UPDATE ConnectorHealingQueue SET CurrentConnectorName=RootConnectorName || '-v2';
            CREATE TABLE ConnectorHealingLogs(Id INTEGER PRIMARY KEY,QueueId INTEGER,Severity TEXT,AttemptNo INTEGER,CreatedAt TEXT);
            INSERT INTO ConnectorHealingLogs VALUES
                (1,1,'INFO',NULL,'2026-01-01T00:01:00Z'),
                (2,1,'WARNING',1,'2026-01-01T00:02:00Z'),
                (3,2,'ERROR',1,'2026-01-01T00:03:00Z');
        """)
    return Snapshot(path)


def query(**values):
    return {
        "kind": "query",
        "entity": "incidents",
        "dimensions": ["root"],
        "metrics": ["incident_count"],
        "order_by": [{"field": "root", "direction": "asc"}],
        **values,
    }


def test_join_grain_and_zero_logs(snapshot):
    result = run(snapshot, query(metrics=["incident_count", "log_count"]))
    assert result["rows"] == [
        {"root": "alpha", "incident_count": 1, "log_count": 2},
        {"root": "beta", "incident_count": 1, "log_count": 1},
        {"root": "gamma", "incident_count": 1, "log_count": 0},
    ]


def test_orphan_event_is_not_silently_dropped(snapshot):
    with sqlite3.connect(snapshot.path) as connection:
        connection.execute(
            "INSERT INTO ConnectorHealingLogs VALUES(100,999,'ORPHAN',NULL,'2026-01-01T00:00:00Z')"
        )
    current = Snapshot(snapshot.path)
    result = run(
        current,
        query(
            entity="events",
            dimensions=["severity"],
            metrics=["log_count"],
            filters=[{"field": "severity", "op": "eq", "value": "ORPHAN"}],
            order_by=[],
        ),
    )
    assert result["rows"] == [{"severity": "ORPHAN", "log_count": 1}]
    result = run(
        current,
        query(
            entity="events",
            metrics=["incident_count", "log_count"],
            filters=[{"field": "severity", "op": "eq", "value": "ORPHAN"}],
        ),
    )
    assert result["rows"] == [{"root": None, "incident_count": 0, "log_count": 1}]


def test_nullable_text_primary_key_rejected(tmp_path):
    path = tmp_path / "nullable.db"
    with sqlite3.connect(path) as connection:
        connection.execute(
            "CREATE TABLE ConnectorHealingQueue(QueueId TEXT PRIMARY KEY, RootConnectorName TEXT)"
        )
        connection.execute("INSERT INTO ConnectorHealingQueue VALUES(NULL,'alpha')")
    with pytest.raises(PlanError, match="non-null identity"):
        compile_plan(query(), Snapshot(path))


def test_composable_having_population_mean(snapshot):
    result = run(
        snapshot,
        query(
            metrics=["log_count"],
            having=[{"metric": "log_count", "op": "lt", "compare_to": "population_mean"}],
        ),
    )
    assert result["rows"] == [{"root": "gamma", "log_count": 0}]


def test_row_projection_null(snapshot):
    result = run(
        snapshot,
        query(
            entity="events",
            dimensions=["event_id", "attempt"],
            metrics=[],
            filters=[{"field": "attempt", "op": "is_null"}],
            order_by=[{"field": "event_id", "direction": "asc"}],
        ),
    )
    assert result["rows"] == [{"event_id": 1, "attempt": None}]


def test_bind_parameters_and_injection(snapshot):
    value = "alpha' OR 1=1 --"
    compiled = compile_plan(
        query(filters=[{"field": "root", "op": "eq", "value": value}]), snapshot
    )
    assert value not in compiled.sql
    assert value in compiled.parameters.values()
    assert snapshot.execute(compiled.sql, compiled.parameters)["rows"] == []


@pytest.mark.parametrize("variant", VARIANTS)
def test_duration_quality(tmp_path, variant):
    path = tmp_path / "duration.db"
    create_duration_fixture(path, variant)
    plan = query(
        dimensions=[],
        metrics=[
            "avg_duration_minutes",
            "matched_count",
            "valid_duration_count",
            "excluded_duration_count",
        ],
        success_only=True,
        order_by=[],
    )
    result = run(Snapshot(path), plan)
    assert [tuple(row.values()) for row in result["rows"]] == EXPECTED[variant]


def test_timezone_equivalence_metamorphic(snapshot):
    first = query(filters=[{"field": "received_at", "op": "gte", "value": "2026-01-01T00:00:00Z"}])
    second = copy.deepcopy(first)
    second["filters"][0]["value"] = "2026-01-01T07:00:00+07:00"
    assert run(snapshot, first)["rows"] == run(snapshot, second)["rows"]


def test_latest_status_is_root_not_current_name(snapshot):
    rows = run(snapshot, query(latest_status=True))["rows"]
    assert rows[0] == {"root": "alpha", "incident_count": 1, "latest_queue_status": "COMPLETED"}


@pytest.mark.parametrize(
    "updates",
    [
        {"sql": "SELECT 1"},
        {"metrics": ["avg_duration_minutes"]},
        {"dimensions": ["Message"]},
        {"dimensions": ["severity"]},
        {"entity": "events", "metrics": ["avg_duration_minutes"]},
        {"latest_status": True, "dimensions": ["current_connector"]},
        {"success_only": True, "filters": [{"field": "outcome", "op": "eq", "value": "FAILED"}]},
        {"filters": [{"field": "received_at", "op": "gte", "value": "2026-01-01"}]},
        {"filters": [{"field": "received_at", "op": "gte", "value": "bad"}]},
        {"limit": 100000},
        {"having": [{"metric": "incident_count", "op": "gt", "value": 10**1000}]},
        {"having": [{"metric": "incident_count", "op": "gt", "value": True}]},
        {"filters": [{"field": "received_at", "op": "gte", "value": "0001-01-01T00:00:00+23:00"}]},
    ],
)
def test_invalid_plan_fails_closed(snapshot, updates):
    with pytest.raises(PlanError):
        compile_plan(query(**updates), snapshot)


def test_log_duplication_does_not_change_incident_count(snapshot):
    plan = query(metrics=["incident_count", "log_count"])
    before = run(snapshot, plan)["rows"]
    with sqlite3.connect(snapshot.path) as connection:
        connection.execute(
            "INSERT INTO ConnectorHealingLogs SELECT 10,QueueId,Severity,AttemptNo,CreatedAt FROM ConnectorHealingLogs WHERE Id=1"
        )
    after = run(Snapshot(snapshot.path), plan)["rows"]
    assert [r["incident_count"] for r in before] == [r["incident_count"] for r in after]
    assert after[0]["log_count"] == before[0]["log_count"] + 1


class PlansClient:
    def __init__(self, *plans):
        self.plans = iter(plans)
        self.calls = []

    def chat_completion(self, **kwargs):
        import json
        from types import SimpleNamespace as NS

        self.calls.append(kwargs)
        return NS(
            choices=[NS(finish_reason="stop", message=NS(content=json.dumps(next(self.plans))))]
        )


def test_strict_repairs_plan_without_executing_raw_sql(snapshot):
    from notebooks.shared.semantic_workflow import SemanticWorkflow

    client = PlansClient({"kind": "sql", "sql": "DELETE FROM ConnectorHealingQueue"}, query())
    flow = SemanticWorkflow(snapshot, client, model_id="test", mode="strict")
    assert len(flow.query("Count incidents per root")["rows"]) == 3
    assert flow.metrics["sql_attempts"] == 1
    assert flow.metrics["sql_api_calls"] == 2
    assert flow.metrics["policy_rejections"] == 1
    assert flow.compiled.parameters


def test_strict_prompt_contains_only_approved_profiles(snapshot):
    import json

    from notebooks.shared.semantic_workflow import SemanticWorkflow

    client = PlansClient(query())
    SemanticWorkflow(snapshot, client, model_id="test", mode="strict").query("Count incidents")
    payload = json.loads(client.calls[0]["messages"][1]["content"])
    profiles = payload["categorical_profiles"]
    assert "ConnectorHealingLogs.Severity" in profiles
    assert not profiles["ConnectorHealingLogs.Severity"]["is_allowed_value_constraint"]
    assert all(not key.endswith((".Message", ".Details", ".ConnectorName")) for key in profiles)


def test_nested_plan_rejected_safely(snapshot):
    nested = []
    for _ in range(1200):
        nested = [nested]
    with pytest.raises(PlanError):
        compile_plan(query(filters=nested), snapshot)


def test_strict_budget_and_stale_state(snapshot):
    from notebooks.shared.analytics import QueryError
    from notebooks.shared.semantic_workflow import SemanticWorkflow

    client = PlansClient(query(), {}, {}, {})
    flow = SemanticWorkflow(snapshot, client, model_id="test", mode="strict")
    flow.query("First")
    with pytest.raises(QueryError, match="exhausted"):
        flow.query("New question")
    assert flow.result is None and flow.compiled is None
    assert flow.metrics["sql_api_calls"] == 3


def test_shadow_budget_separate_metrics(snapshot):
    from notebooks.shared.semantic_workflow import SemanticWorkflow

    client = PlansClient(
        {
            "kind": "sql",
            "sql": "SELECT COUNT(*) AS n FROM ConnectorHealingQueue",
            "interpretation": "Count incidents",
        },
        query(),
    )
    flow = SemanticWorkflow(snapshot, client, model_id="test", mode="shadow")
    result = flow.query("Count incidents")
    assert result["rows"] == [{"n": 3}]
    assert flow.shadow["status"] == "completed"
    assert flow.metrics["sql_api_calls"] == 1
    assert flow.metrics["shadow"]["sql_api_calls"] == 1
    assert flow.metrics["total_sql_api_calls"] == 2


@pytest.mark.parametrize("status", [401, 402, 403, 429])
@pytest.mark.parametrize("mode", ["strict", "shadow"])
def test_service_block_stops_correction_and_response(snapshot, status, mode):
    from types import SimpleNamespace

    from notebooks.shared.analytics import QueryError
    from notebooks.shared.semantic_workflow import SemanticWorkflow

    class ServiceError(Exception):
        response = SimpleNamespace(status_code=status)

    class BlockedClient(PlansClient):
        def chat_completion(self, **kwargs):
            if mode == "shadow" and not self.calls:
                return super().chat_completion(**kwargs)
            self.calls.append(kwargs)
            raise ServiceError("sensitive upstream body must not be exposed")

    client = BlockedClient(
        {
            "kind": "sql",
            "sql": "SELECT COUNT(*) AS n FROM ConnectorHealingQueue",
            "interpretation": "Count incidents",
        }
    )
    flow = SemanticWorkflow(snapshot, client, model_id="test", mode=mode)
    with pytest.raises(QueryError):
        flow.query("Count incidents")
    calls = len(client.calls)
    assert calls == (2 if mode == "shadow" else 1)
    assert flow.service_block["http_status"] == status
    assert "sensitive upstream" not in str(flow.trace)
    with pytest.raises(QueryError, match="not attempted"):
        flow.respond()
    assert len(client.calls) == calls
