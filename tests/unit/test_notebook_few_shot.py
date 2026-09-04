import json
import sqlite3

from notebooks.shared.analytics import SQL_INSTRUCTIONS, Snapshot, Workflow, validate_claims
from notebooks.shared.few_shot import RESPONSE_EXAMPLES, SQL_EXAMPLES, few_shot_messages


def test_sql_examples_execute_with_independent_expected_rows(tmp_path):
    path = tmp_path / "demo.db"
    with sqlite3.connect(path) as c:
        c.executescript("""
          CREATE TABLE demo_incidents(id INTEGER PRIMARY KEY,service TEXT,received TEXT,state TEXT);
          CREATE TABLE demo_events(id INTEGER PRIMARY KEY,incident_id INTEGER,severity TEXT,attempt INTEGER);
          INSERT INTO demo_incidents VALUES
            (1,'a','2026-01-01T00:00:00Z','COMPLETED'),
            (2,'a','2026-02-01T00:00:00Z','WAITING'),
            (3,'b','2026-01-10T00:00:00Z','PENDING'),
            (4,'c','2026-01-10T00:00:00Z','COMPLETED');
          INSERT INTO demo_events VALUES (1,1,'WARN',1),(2,1,'WARN',NULL),
            (3,2,'WARN',1),(4,3,'INFO',NULL);
        """)
    snapshot = Snapshot(path)
    expected = [
        [{"service": "a", "incident_count": 2}, {"service": "b", "incident_count": 1}],
        [
            {"service": "a", "warning_events": 2},
            {"service": "b", "warning_events": 0},
            {"service": "c", "warning_events": 0},
        ],
        [
            {"service": "a", "incident_count": 2, "latest_queue_state": "WAITING"},
            {"service": "b", "incident_count": 1, "latest_queue_state": "PENDING"},
            {"service": "c", "incident_count": 1, "latest_queue_state": "COMPLETED"},
        ],
    ]
    for (_, decision), rows in zip(SQL_EXAMPLES[:3], expected, strict=True):
        assert snapshot.execute(decision["sql"])["rows"] == rows
    assert SQL_EXAMPLES[3][1]["kind"] == "clarification"
    assert snapshot.execute(SQL_EXAMPLES[4][1]["sql"])["rows"] == [{"service": "a", "incidents": 2}]


def test_response_examples_satisfy_generic_evidence_contract():
    for request, answer in RESPONSE_EXAMPLES:
        assert validate_claims(answer, request["verified_result"]) is None


def test_message_order_and_isolation():
    for stage, count in [("sql", len(SQL_EXAMPLES)), ("response", len(RESPONSE_EXAMPLES))]:
        messages = few_shot_messages(stage)
        assert len(messages) == 2 * count
        assert [m["role"] for m in messages] == ["user", "assistant"] * count
        for message in messages:
            assert isinstance(json.loads(message["content"]), dict)
        messages[0]["content"] = "mutated"
        assert few_shot_messages(stage)[0]["content"] != "mutated"


def test_optional_filters_are_defaults_not_clarification_requirements():
    assert "No time range: use all available rows in the snapshot" in SQL_INSTRUCTIONS
    assert "No status/severity filter: do not add a filter" in SQL_INSTRUCTIONS
    assert "Missing optional filters are NOT blockers" in SQL_INSTRUCTIONS
    question, decision = SQL_EXAMPLES[0]
    assert "thời gian" not in question and "trạng thái" not in question
    assert decision["kind"] == "sql"
    assert "WHERE" not in decision["sql"]
    assert "toàn bộ snapshot" in decision["interpretation"]


def test_ambiguous_metric_example_does_not_request_optional_filters():
    clarification = SQL_EXAMPLES[3][1]
    assert clarification["kind"] == "clarification"
    assert "thời gian" not in clarification["question"]
    assert "trạng thái" not in clarification["question"]


def test_enabled_and_disabled_preserve_actual_question(tmp_path):
    from types import SimpleNamespace as NS

    path = tmp_path / "real.db"
    with sqlite3.connect(path) as c:
        c.execute("CREATE TABLE real_table(value INTEGER)")
        c.execute("INSERT INTO real_table VALUES (42)")

    class Client:
        def chat_completion(self, **kwargs):
            self.messages = kwargs["messages"]
            return NS(
                choices=[
                    NS(
                        message=NS(
                            content=json.dumps(
                                {
                                    "kind": "sql",
                                    "sql": "SELECT value FROM real_table",
                                    "interpretation": "stored value",
                                }
                            )
                        ),
                        finish_reason="stop",
                    )
                ],
                usage=None,
            )

    for enabled in [True, False]:
        client = Client()
        flow = Workflow(Snapshot(path), client, model_id="test", few_shot=enabled)
        assert flow.query("Show the real value")["rows"] == [{"value": 42}]
        assert len(client.messages) == (2 + 2 * len(SQL_EXAMPLES) if enabled else 2)
        assert json.loads(client.messages[-1]["content"])["question"] == "Show the real value"
        assert set(json.loads(client.messages[-1]["content"])["context"]["tables"]) == {
            "real_table"
        }
        assert flow.metrics["sql_api_calls"] == 1
