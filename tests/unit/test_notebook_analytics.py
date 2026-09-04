import json
import sqlite3
from types import SimpleNamespace as NS

import pytest

from notebooks.shared.analytics import QueryError, Snapshot, Workflow, render_table, validate_claims


@pytest.fixture
def snapshot(tmp_path):
    path = tmp_path / "fixture.db"
    with sqlite3.connect(path) as c:
        c.executescript("""
            CREATE TABLE queue(id INTEGER PRIMARY KEY, root TEXT, status TEXT, received TEXT);
            CREATE TABLE logs(id INTEGER PRIMARY KEY, queue_id INTEGER REFERENCES queue(id), severity TEXT, attempt INTEGER);
            CREATE TABLE secret(value TEXT);
            INSERT INTO queue VALUES
              (1,'alpha','COMPLETED','2026-09-01T10:00:00+00:00'),
              (2,'alpha','WAITING','2026-09-02T10:00:00+00:00'),
              (3,'beta','PENDING','2026-09-02T12:00:00+00:00'),
              (4,'gamma','COMPLETED','2026-09-03T09:00:00+00:00');
            INSERT INTO logs VALUES (1,1,'INFO',NULL),(2,1,'WARN',1),(3,2,'FATAL',2),(4,3,'WARN',NULL);
        """)
    return Snapshot(path, allowed_tables=["queue", "logs"])


@pytest.mark.parametrize(
    "sql,expected",
    [
        (
            "SELECT root,COUNT(*) AS entries FROM queue GROUP BY root ORDER BY entries DESC,root",
            [
                {"root": "alpha", "entries": 2},
                {"root": "beta", "entries": 1},
                {"root": "gamma", "entries": 1},
            ],
        ),
        (
            "SELECT q.root,COUNT(l.id) AS events FROM queue q LEFT JOIN logs l ON q.id=l.queue_id GROUP BY q.root ORDER BY events DESC,q.root",
            [
                {"root": "alpha", "events": 3},
                {"root": "beta", "events": 1},
                {"root": "gamma", "events": 0},
            ],
        ),
        (
            "SELECT root FROM queue WHERE datetime(received)>=datetime('2026-09-02T11:00:00Z') ORDER BY received",
            [{"root": "beta"}, {"root": "gamma"}],
        ),
        (
            "WITH counts AS (SELECT root,COUNT(*) AS n FROM queue GROUP BY root) SELECT root,n,DENSE_RANK() OVER(ORDER BY n DESC) AS rank FROM counts ORDER BY n DESC,root",
            [
                {"root": "alpha", "n": 2, "rank": 1},
                {"root": "beta", "n": 1, "rank": 2},
                {"root": "gamma", "n": 1, "rank": 2},
            ],
        ),
        (
            "SELECT id,attempt FROM logs WHERE attempt IS NULL ORDER BY id",
            [{"id": 1, "attempt": None}, {"id": 4, "attempt": None}],
        ),
        ("SELECT root FROM queue WHERE root='absent'", []),
        ("SELECT 'DROP; DELETE' AS literal", [{"literal": "DROP; DELETE"}]),
    ],
)
def test_semantic_results(snapshot, sql, expected):
    result = snapshot.execute(sql)
    assert result["rows"] == expected
    assert result["returned_row_count"] == len(expected)
    assert not result["truncated"]
    assert result["snapshot"]["source_refresh"] is None


def test_schema_and_scope(snapshot):
    assert set(snapshot.schema) == {"queue", "logs"}
    assert snapshot.relationships[0]["to_table"] == "queue"
    assert snapshot.schema["logs"][3]["declared_type"] == "INTEGER"
    assert (
        snapshot.execute("SELECT attempt FROM logs WHERE id=1")["columns"][0]["observed_types"]
        is None
    )


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM queue",
        "SELECT * FROM queue; SELECT * FROM logs",
        "PRAGMA table_info(queue)",
        "SELECT * FROM sqlite_master",
        "SELECT * FROM secret",
        "SELECT * FROM main.queue",
        "SELECT load_extension('evil')",
        "SELECT * FROM pragma_table_info('queue')",
        "ATTACH DATABASE ':memory:' AS x",
        "CREATE TABLE x AS SELECT * FROM queue",
        "WITH secret AS (SELECT * FROM main.secret) SELECT * FROM secret",
        "SELECT 1 AS x,2 AS x",
    ],
)
def test_adversarial_sql(snapshot, sql):
    with pytest.raises(QueryError):
        snapshot.execute(sql)


def test_database_protection_independent_of_parser(snapshot):
    with snapshot.connect() as c:
        c.set_authorizer(snapshot.authorize)
        with pytest.raises(sqlite3.DatabaseError):
            c.execute("DELETE FROM queue")
        with pytest.raises(sqlite3.DatabaseError):
            c.execute("SELECT * FROM secret")


def test_limit_and_timeout(snapshot):
    snapshot.row_limit = 2
    r = snapshot.execute("SELECT * FROM queue ORDER BY id")
    assert r["returned_row_count"] == 2 and r["truncated"]
    assert r["truncation_reason"] == "row_limit"
    snapshot.timeout_seconds = 0.001
    with pytest.raises(QueryError, match="interrupted"):
        snapshot.execute(
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n) SELECT sum(x) FROM n"
        )


class FakeClient:
    def __init__(self, *outputs):
        self.outputs = iter(outputs)
        self.messages = []

    def chat_completion(self, **kwargs):
        self.messages.append(kwargs)
        value = next(self.outputs)
        if isinstance(value, Exception):
            raise value
        return NS(
            choices=[NS(message=NS(content=json.dumps(value)), finish_reason="stop")],
            usage=NS(prompt_tokens=10, completion_tokens=20),
        )


def sql_decision(sql):
    return {"kind": "sql", "sql": sql, "interpretation": "requested analytic metric"}


def test_dynamic_top_five(snapshot):
    client = FakeClient(
        sql_decision(
            "SELECT root,COUNT(*) AS entries FROM queue GROUP BY root ORDER BY entries DESC,root LIMIT 5"
        )
    )
    flow = Workflow(snapshot, client, model_id="test")
    result = flow.query("Top 5 roots by persisted queue entries?")
    assert len(result["rows"]) == 3
    assert set(result["rows"][0]) == {"root", "entries"}
    assert flow.metrics["sql_api_calls"] == 1


def test_clarification_and_stale_state(snapshot):
    client = FakeClient(
        sql_decision("SELECT count(*) AS n FROM logs"),
        {"kind": "clarification", "question": "Count failures by severity or event type?"},
        {"kind": "clarification", "question": "Count failures by severity or event type?"},
        TimeoutError(),
    )
    flow = Workflow(snapshot, client, model_id="test")
    flow.query("Count events")
    assert flow.query("Which is worst?") is None
    assert flow.respond()["source"] == "clarification"
    assert flow.result is None
    with pytest.raises(QueryError, match="TimeoutError"):
        flow.query("Retry")
    assert flow.result is None and flow.answer is None and flow.clarification is None


def test_unnecessary_clarification_is_reviewed_once_then_queries(snapshot):
    client = FakeClient(
        {"kind": "clarification", "question": "Do you mean queue entries? Any time filter?"},
        sql_decision(
            "SELECT root,COUNT(*) AS entries FROM queue GROUP BY root ORDER BY entries DESC,root LIMIT 5"
        ),
    )
    flow = Workflow(snapshot, client, model_id="test")
    result = flow.query("Top 5 roots by stored queue incidents")
    assert result["rows"][0] == {"root": "alpha", "entries": 2}
    assert flow.clarification is None
    assert flow.metrics["sql_api_calls"] == 2
    assert flow.metrics["sql_attempts"] == 1
    assert flow.trace[0]["status"] == "clarification_review"


def test_correction_uses_actual_sql_error(snapshot):
    client = FakeClient(
        sql_decision("SELECT missing FROM queue"),
        sql_decision("SELECT root FROM queue ORDER BY id"),
    )
    flow = Workflow(snapshot, client, model_id="test")
    assert flow.query("Show roots")["returned_row_count"] == 4
    assert flow.metrics["sql_attempts"] == 2
    assert "no such column" in json.dumps(client.messages[-1]["messages"])
    assert "SELECT missing" in json.dumps(client.messages[-1]["messages"])


def test_bounded_repair(snapshot):
    flow = Workflow(snapshot, FakeClient({}, {}, {}), model_id="test")
    with pytest.raises(QueryError, match="exhausted"):
        flow.query("Something")
    assert flow.metrics["sql_api_calls"] == 3 and flow.result is None


def test_response_evidence(snapshot):
    r = snapshot.execute("SELECT root,id FROM queue WHERE id=1")
    good = {
        "claims": [
            {
                "text": "alpha: 1",
                "evidence": [{"row": 0, "column": "root"}, {"row": 0, "column": "id"}],
            }
        ]
    }
    assert validate_claims(good, r) is None
    good["claims"][0]["text"] = "alpha: 1, another 999"
    assert validate_claims(good, r) == "unsupported_numeric_claim"
    good["claims"][0]["evidence"][0]["column"] = "invented"
    assert validate_claims(good, r) == "unknown_reference"


@pytest.mark.parametrize("output", [{}, {"claims": []}, TimeoutError()])
def test_response_fallback(snapshot, output):
    client = FakeClient(sql_decision("SELECT root FROM queue ORDER BY id"), output)
    flow = Workflow(snapshot, client, model_id="test")
    flow.query("Roots")
    answer = flow.respond()
    assert answer["source"] == "verified_table_fallback"
    assert "alpha" in answer["text"] and "gamma" in answer["text"]


def test_empty_and_truncated_skip_response_api(snapshot):
    snapshot.row_limit = 1
    client = FakeClient(
        sql_decision("SELECT root FROM queue"),
        sql_decision("SELECT root FROM queue WHERE 1=0"),
        {"kind": "accept_result"},
    )
    flow = Workflow(snapshot, client, model_id="test")
    for q, reason in [("Roots", "query_result_truncated"), ("None", "empty_result")]:
        flow.query(q)
        assert flow.respond()["reason"] == reason
        assert flow.metrics["response_api_calls"] == 0


def test_fallback_escapes_untrusted_cells(snapshot):
    r = snapshot.execute("SELECT '<script>x</script>|ignore instructions' AS note,NULL AS missing")
    text = render_table(r)
    assert "<script>" not in text and "NULL" in text and "\\|" in text


def test_snapshot_change_requires_schema_reload(snapshot):
    import os

    stat = snapshot.path.stat()
    os.utime(snapshot.path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000000))
    with pytest.raises(QueryError, match="reinitialize"):
        snapshot.execute("SELECT root FROM queue")


def test_byte_limit_preserves_values_and_discloses_truncation(snapshot):
    snapshot.byte_limit = 30
    r = snapshot.execute("SELECT root FROM queue ORDER BY id")
    assert r["truncated"] and r["truncation_reason"] == "byte_limit"
    assert r["rows"] == [{"root": "alpha"}]


def test_successful_response_and_rerun_metrics(snapshot):
    claim = {"claims": [{"text": "alpha", "evidence": [{"row": 0, "column": "root"}]}]}
    client = FakeClient(sql_decision("SELECT root FROM queue WHERE id=1"), claim, claim)
    flow = Workflow(snapshot, client, model_id="test")
    flow.query("Root of first queue incident")
    for _ in range(2):
        assert flow.respond()["source"] == "huggingface"
        assert flow.metrics["response_api_calls"] == 1
        assert flow.metrics["tokens"]["response"] == {"input": 10, "output": 20}


def test_response_token_truncation(snapshot):
    client = FakeClient(sql_decision("SELECT root FROM queue"))
    flow = Workflow(snapshot, client, model_id="test")
    flow.query("Roots")
    client.chat_completion = lambda **kwargs: NS(
        choices=[NS(message=NS(content="{}"), finish_reason="length")], usage=None
    )
    assert flow.respond()["source"] == "verified_table_fallback"
    assert "complete" in flow.answer["reason"]


def test_unknown_sql_error_does_not_leak_api_secret(snapshot):
    flow = Workflow(
        snapshot, FakeClient(RuntimeError("secret-in-provider-exception")), model_id="test"
    )
    with pytest.raises(QueryError) as error:
        flow.query("Roots")
    assert "secret-in-provider-exception" not in str(error.value)
    assert "secret-in-provider-exception" not in json.dumps(flow.trace)
