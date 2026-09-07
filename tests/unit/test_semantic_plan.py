"""Offline compiler tests: these do not measure Qwen model accuracy."""

import copy
import sqlite3

import pytest

from notebooks.evaluation.fixtures import EXPECTED, VARIANTS, create_duration_fixture
from notebooks.shared.analytics import Snapshot
from notebooks.shared.semantic_plan import PlanError, catalog, compile_plan
from notebooks.shared.semantic_workflow import default_business_plan


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
            ALTER TABLE ConnectorHealingLogs ADD EventType TEXT;
            UPDATE ConnectorHealingLogs SET EventType=CASE Id
                WHEN 1 THEN 'HEALTH_FAILED_CONFIRMED'
                WHEN 2 THEN 'TASK_RESTART'
                ELSE 'CONNECTOR_RESTART' END;
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


def test_documented_action_metrics_count_only_their_event_types(snapshot):
    result = run(snapshot, query(
        entity="events", dimensions=[],
        metrics=["confirmed_failure_count", "task_restart_count", "connector_restart_count"],
        order_by=[],
    ))
    assert result["rows"] == [{
        "confirmed_failure_count": 1, "task_restart_count": 1, "connector_restart_count": 1,
    }]
    result = run(snapshot, query(
        dimensions=[], metrics=["recovery_count", "escalation_count", "recovery_rate_percent"], order_by=[],
    ))
    assert result["rows"] == [{"recovery_count": 2, "escalation_count": 1, "recovery_rate_percent": 66.67}]
    assert catalog(snapshot)["time_basis"]["ingestion"].startswith("Unavailable")


def test_business_defaults_are_compiled_plans_not_sample_question_answers(snapshot):
    totals = default_business_plan("Ngày 5 tháng 9 có bao nhiêu incident và healing log?")
    assert totals["kind"] == "independent"
    assert run(snapshot, totals)["rows"] == [{"incident_count": 3, "log_count": 3}]
    plain_totals = default_business_plan(
        "Ngày 5 tháng 9 có bao nhiêu sự cố và nhật ký healing?"
    )
    assert plain_totals == totals
    ranking = default_business_plan("Connector nào hay lỗi nhất?")
    assert ranking["metrics"] == ["confirmed_failure_count"]
    assert run(snapshot, ranking)["rows"][0]["confirmed_failure_count"] == 1
    rate = default_business_plan("Tỷ lệ phục hồi là bao nhiêu?")
    assert run(snapshot, rate)["rows"] == [{"recovery_rate_percent": 66.67}]


def test_plain_vietnamese_day_counts_populations_independently(snapshot, monkeypatch):
    from notebooks.shared.semantic_workflow import SemanticWorkflow

    with sqlite3.connect(snapshot.path) as connection:
        connection.executemany(
            "UPDATE ConnectorHealingQueue SET ReceivedAt=? WHERE QueueId=?",
            [
                ("2026-09-04T17:00:00Z", 1),
                ("2026-09-04T16:59:59Z", 2),
                ("2026-09-05T16:59:59Z", 3),
            ],
        )
        connection.executemany(
            "UPDATE ConnectorHealingLogs SET CreatedAt=? WHERE Id=?",
            [
                ("2026-09-04T16:59:59Z", 1),
                ("2026-09-04T17:00:00Z", 2),
                ("2026-09-05T17:00:00Z", 3),
            ],
        )
    monkeypatch.setattr(
        "notebooks.shared.analytics.utc_now", lambda: "2026-09-07T00:00:00+00:00"
    )
    client = PlansClient({"unexpected": "response"})
    flow = SemanticWorkflow(Snapshot(snapshot.path), client, model_id="test", mode="strict")

    result = flow.query("Ngày 5 tháng 9 có bao nhiêu incident và healing log?")

    assert result["rows"] == [{"incident_count": 2, "log_count": 1}]
    assert " CROSS JOIN " in result["sql"]
    assert client.calls == []
    assert result["evidence_context"]["query_scope"]["metrics"]["incident_count"] == {
        "meaning": "Count unique incident IDs in selected population",
        "label_vi": "incident",
        "unit": "incident",
        "provenance": "database_aggregate",
    }
    assert [
        item["time_basis"]["field"]
        for item in result["evidence_context"]["query_scope"]["populations"]
    ] == ["received_at", "event_at"]
    answer = flow.respond()
    assert answer["source"] == "verified_table_fallback"
    assert answer["reason"] == "missing_claims"
    assert answer["text"].startswith("Ngày 5/9/2026, có 2 incident và 1 healing log")
    assert "compiler_enforced" not in answer["text"]
    assert answer["diagnostics_text"]


def test_independent_populations_preserve_counts_and_time_basis(snapshot):
    from notebooks.qwen.output_schema import contract_error

    plan = {"kind": "independent", "queries": [
        {"kind": "query", "entity": "incidents", "dimensions": [],
         "metrics": ["incident_count"]},
        {"kind": "query", "entity": "events", "dimensions": [],
         "metrics": ["log_count"], "filters": [
             {"field": "event_at", "op": "gte", "value": "2026-01-01T00:02:00Z"}]},
    ]}
    assert contract_error(plan, "strict_planning") is None
    assert run(snapshot, plan)["rows"] == [{"incident_count": 3, "log_count": 2}]
    with sqlite3.connect(snapshot.path) as connection:
        connection.execute("INSERT INTO ConnectorHealingLogs VALUES(99,999,'INFO',NULL,'2026-01-02T00:00:00Z',NULL)")
    assert run(Snapshot(snapshot.path), plan)["rows"] == [{"incident_count": 3, "log_count": 3}]
    plan["queries"][1]["filters"][0]["value"] = "2030-01-01T00:00:00Z"
    assert run(Snapshot(snapshot.path), plan)["rows"] == [{"incident_count": 3, "log_count": 0}]


@pytest.mark.parametrize("extra", [{"dimensions": ["root"]}, {"having": []}, {"kind": "independent"}])
def test_independent_rejects_non_scalar_or_recursive_populations(snapshot, extra):
    child = {"kind": "query", "entity": "incidents", "dimensions": [], "metrics": ["incident_count"]}
    with pytest.raises(PlanError):
        run(snapshot, {"kind": "independent", "queries": [child, {**child, **extra}]})


def test_orphan_event_is_not_silently_dropped(snapshot):
    with sqlite3.connect(snapshot.path) as connection:
        connection.execute(
            "INSERT INTO ConnectorHealingLogs VALUES(100,999,'ORPHAN',NULL,'2026-01-01T00:00:00Z',NULL)"
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
            "INSERT INTO ConnectorHealingLogs SELECT 10,QueueId,Severity,AttemptNo,CreatedAt,EventType FROM ConnectorHealingLogs WHERE Id=1"
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
