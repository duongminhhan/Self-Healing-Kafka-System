"""Offline Gemini transport/ordered notebook tests; never use a real API key."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

import httpx
import nbformat
import pytest
from google.genai import errors, types

import notebooks.gemini.adapter as gemini
import notebooks.shared.analytics as analytics
from notebooks.evaluation.evaluate import CASES, matches
from notebooks.evaluation.fixtures import EXPECTED, VARIANTS, create_duration_fixture

ROOT = Path(__file__).resolve().parents[2]


def reply(value=None, *, finish="STOP", block=None):
    data = {
        "model_version": "gemini-3.5-flash",
        "usage_metadata": {
            "prompt_token_count": 100,
            "candidates_token_count": 20,
            "thoughts_token_count": 30,
            "total_token_count": 150,
        },
        "candidates": [
            {
                "finish_reason": finish,
                "content": {
                    "role": "model",
                    "parts": [
                        {"text": "private reasoning", "thought": True},
                        {
                            "text": value
                            if isinstance(value, str)
                            else json.dumps(value or {"kind": "accept_result"})
                        },
                    ],
                },
            }
        ],
    }
    if block:
        data["prompt_feedback"] = {"block_reason": block}
        data["candidates"] = []
    return types.GenerateContentResponse(**data)


class FakeSDK:
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.calls = []
        self.models = self

    def generate_content(self, **kwargs):
        self.calls.append(kwargs)
        value = next(self.responses)
        if isinstance(value, Exception):
            raise value
        return value

    def close(self):
        pass


def adapter(*responses):
    return gemini.GeminiAdapter(api_key="unit-test-not-a-key", sdk_client=FakeSDK(*responses))


MESSAGES = [
    {"role": "system", "content": "instructions"},
    {"role": "user", "content": "question"},
    {"role": "assistant", "content": "example"},
    {"role": "user", "content": "actual question"},
]


def test_normalization_and_json_schema():
    client = adapter(reply())
    output = client.complete_stage(MESSAGES, "sql", 4096)
    request = client.client.calls[0]
    assert request["model"] == gemini.DEFAULT_MODEL
    assert [c.role for c in request["contents"]] == ["user", "model", "user"]
    assert request["config"].system_instruction == "instructions"
    assert request["config"].response_json_schema == gemini.SQL_SCHEMA
    assert request["config"].thinking_config.thinking_level.value == "LOW"
    assert request["config"].max_output_tokens == 4096
    assert output.choices[0].finish_reason == "stop"
    assert "private reasoning" not in output.choices[0].message.content
    assert output.usage.thinking_tokens == 30
    assert output.usage.total_tokens == 150
    assert output.output_error is None


def test_sdk_timeout_and_no_implicit_retries(monkeypatch):
    recorded = {}
    monkeypatch.setattr(gemini.genai, "Client", lambda **kw: recorded.update(kw) or FakeSDK())
    gemini.GeminiAdapter(api_key="unit-test-not-a-key", timeout_seconds=12.5)
    assert recorded["http_options"].timeout == 12500
    assert recorded["http_options"].retry_options.attempts == 1
    assert recorded["http_options"].base_url == "https://generativelanguage.googleapis.com"
    assert recorded["vertexai"] is False


@pytest.mark.parametrize(
    "response,needle",
    [
        (reply("not json"), "invalid JSON"),
        (reply("[]"), "invalid JSON"),
        (reply('{"kind":', finish="MAX_TOKENS"), "budget exhausted"),
        (reply(finish="SAFETY"), "blocked"),
        (reply(block="SAFETY"), "blocked"),
        (reply(finish="RECITATION"), "incomplete"),
        (types.GenerateContentResponse(), "incomplete"),
    ],
)
def test_output_failures(response, needle):
    output = adapter(response).complete_stage(MESSAGES, "sql", 4096)
    assert needle in output.output_error


@pytest.mark.parametrize(
    "error,category,status",
    [
        (httpx.ReadTimeout("secret must not be logged"), "timeout", None),
        (
            errors.APIError(429, {"error": {"message": "private quota detail"}}),
            "quota_or_rate_limit",
            429,
        ),
        (errors.APIError(402, {"error": {"message": "billing private detail"}}), "billing", 402),
        (
            errors.APIError(403, {"error": {"message": "permission private detail"}}),
            "authentication_or_permission",
            403,
        ),
        (errors.APIError(404, {"error": {"message": "not found"}}), "model_unavailable", 404),
        (errors.APIError(400, {"error": {"message": "invalid parameter"}}), "invalid_request", 400),
        (
            errors.APIError(503, {"error": {"message": "private detail"}}),
            "service_unavailable",
            503,
        ),
    ],
)
def test_service_errors_are_safe_and_not_retried(error, category, status):
    client = adapter(error)
    with pytest.raises(gemini.GeminiServiceError) as caught:
        client.complete_stage(MESSAGES, "sql", 4096)
    assert caught.value.category == category
    assert caught.value.response.status_code == status
    assert "private" not in str(caught.value) and "secret" not in str(caught.value)
    assert len(client.client.calls) == 1


def test_missing_key_and_wrong_id():
    with pytest.raises(ValueError, match="GEMINI_API_KEY"):
        gemini.GeminiAdapter(api_key="")
    with pytest.raises(ValueError, match="Official model ID"):
        gemini.GeminiAdapter(api_key="test", model_id="gemini-flash-3.5")


def test_quota_help_link_does_not_mean_billing_failure():
    client = adapter(
        errors.APIError(
            429, {"error": {"message": "Quota exceeded; check plan and billing details"}}
        )
    )
    with pytest.raises(gemini.GeminiServiceError) as caught:
        client.complete_stage(MESSAGES, "sql", 4096)
    assert caught.value.category == "quota_or_rate_limit"


@pytest.fixture
def db(tmp_path):
    path = tmp_path / "snapshot.db"
    create_duration_fixture(path, "normal")
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.executescript("""
            CREATE TABLE ConnectorHealingLogs (
                Id TEXT, QueueId INTEGER, ConnectorName TEXT, Severity TEXT,
                AttemptNo INTEGER, Message TEXT, Details TEXT);
            INSERT INTO ConnectorHealingLogs VALUES
                ('l1',1,'alpha','WARN',NULL,'raw-secret','raw-secret'),
                ('l2',1,'alpha','INFO',1,'raw-secret','raw-secret'),
                ('l3',2,'beta','INFO',1,'raw-secret','raw-secret');
        """)
    return path


def flow_for(db, *responses):
    return analytics.Workflow(
        analytics.Snapshot(db, blocked_columns=gemini.RAW_LOG_COLUMNS),
        adapter(*responses),
        model_id=gemini.DEFAULT_MODEL,
        provider="google",
    )


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT Message AS renamed FROM ConnectorHealingLogs",
        "SELECT substr(Details,1,2) AS snippet FROM ConnectorHealingLogs",
        "SELECT COUNT(*) AS n FROM ConnectorHealingLogs WHERE Message='raw-secret'",
        "SELECT * FROM ConnectorHealingLogs",
    ],
)
def test_raw_log_fields_blocked_even_through_aliases(db, sql):
    snapshot = analytics.Snapshot(db, blocked_columns=gemini.RAW_LOG_COLUMNS)
    assert "raw-secret" not in json.dumps(snapshot.context())
    assert "Message" not in {c["name"] for c in snapshot.schema["ConnectorHealingLogs"]}
    with pytest.raises(analytics.QueryError):
        snapshot.execute(sql)
    # Default HF behavior is unchanged.
    assert analytics.Snapshot(db).execute("SELECT Message FROM ConnectorHealingLogs")["rows"]


@pytest.mark.parametrize("index", range(len(CASES)))
def test_dynamic_sql_shared_executor(db, index):
    question, sql = CASES[index]
    flow = flow_for(
        db,
        reply({"kind": "sql", "sql": sql, "interpretation": "gold query"}),
        reply({"kind": "accept_result"}),
    )
    result = flow.query(question)
    with closing(sqlite3.connect(db)) as conn:
        expected = conn.execute(sql).fetchall()
    assert matches(result, expected)
    assert flow.metrics["sql_api_calls"] <= 3
    assert "raw-secret" not in str(flow.client.client.calls)


@pytest.mark.parametrize("variant", VARIANTS)
def test_duration_fixtures_through_adapter(tmp_path, variant):
    path = tmp_path / "fixture.db"
    create_duration_fixture(path, variant)
    question, sql = CASES[-1]
    flow = flow_for(path, reply({"kind": "sql", "sql": sql, "interpretation": "duration"}), reply())
    assert matches(flow.query(question), EXPECTED[variant])


def test_clarification_is_preserved(db):
    clarification = {"kind": "clarification", "question": "Bạn muốn so sánh metric nào?"}
    flow = flow_for(db, reply(clarification), reply(clarification))
    assert flow.query("Connector nào tệ nhất?") is None
    assert flow.respond()["source"] == "clarification"
    assert flow.metrics["sql_api_calls"] == 2


def test_correction_budget_and_tokens(db):
    flow = flow_for(db, *[reply("broken") for _ in range(3)])
    with pytest.raises(analytics.QueryError, match="exhausted"):
        flow.query("Đếm incident")
    assert flow.metrics["sql_api_calls"] == 3
    assert flow.metrics["sql_attempts"] == 0
    assert flow.metrics["tokens"]["sql"]["thinking_tokens"] == 90
    assert flow.result is None


def test_quota_is_not_a_sql_error(db):
    flow = flow_for(db, errors.APIError(429, {"error": {"message": "quota"}}))
    with pytest.raises(analytics.QueryError, match="quota_or_rate_limit"):
        flow.query("Đếm incident")
    assert flow.metrics["sql_api_calls"] == 1
    assert flow.trace[-1]["status"] == "api_error"
    assert flow.trace[-1]["http_status"] == 429


def test_response_timeout_preserves_verified_facts(db):
    flow = flow_for(
        db,
        reply(
            {
                "kind": "sql",
                "sql": "SELECT COUNT(*) AS n FROM ConnectorHealingQueue",
                "interpretation": "count",
            }
        ),
        httpx.ReadTimeout("private"),
    )
    flow.query("Đếm incident")
    answer = flow.respond()
    assert answer["source"] == "verified_table_fallback"
    assert "timeout" in answer["reason"]
    assert "private" not in str(answer)
    assert flow.metrics["response_service_error"]["category"] == "timeout"


def test_fixture_setup_uses_existing_tables_only(tmp_path, monkeypatch):
    path = tmp_path / "one-table.db"
    create_duration_fixture(path, "normal")
    monkeypatch.setattr(gemini.GeminiAdapter, "from_env", lambda: adapter())
    monkeypatch.delenv("GEMINI_SQL_ALLOWED_TABLES", raising=False)
    flow = gemini.make_gemini_workflow(path)
    assert flow.snapshot.allowed_tables == {"ConnectorHealingQueue"}


@pytest.mark.parametrize("backend", ["hf", "gemini"])
def test_evaluator_missing_key_is_not_a_model_pass(db, monkeypatch, capsys, backend):
    import notebooks.evaluation.evaluate as evaluator

    monkeypatch.setattr(evaluator, "load_dotenv", lambda *args, **kwargs: None)
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setattr("sys.argv", ["eval", "--backend", backend, "--live", "--snapshot", str(db)])
    assert evaluator.main() == 0  # Local reference checks pass, not model accuracy.
    output = capsys.readouterr().out
    assert "LIVE SKIPPED" in output
    assert '"model_accuracy": null' in output
    assert '"final_execution_accuracy"' not in output


def test_response_evidence_fallback(db):
    flow = flow_for(
        db,
        reply(
            {
                "kind": "sql",
                "sql": "SELECT COUNT(*) AS n FROM ConnectorHealingQueue",
                "interpretation": "count",
            }
        ),
        reply({"claims": [{"text": "Có 999 incident", "evidence": [{"row": 0, "column": "n"}]}]}),
    )
    flow.query("Đếm incident")
    answer = flow.respond()
    assert answer["source"] == "verified_table_fallback"
    assert "999" not in answer["text"]
    assert flow.client.client.calls[-1]["config"].response_json_schema == gemini.RESPONSE_SCHEMA


@pytest.mark.parametrize("provider_cwd", [False, True])
def test_ordered_gemini_notebook_no_refresh(db, monkeypatch, capsys, provider_cwd):
    import google.genai

    monkeypatch.chdir(ROOT / "notebooks" / "gemini" if provider_cwd else ROOT)

    nb = nbformat.read(
        ROOT / "notebooks" / "gemini" / "text_to_sql_self_healthy_kafka_gemini.ipynb", as_version=4
    )
    nbformat.validate(nb)
    before = db.read_bytes()
    monkeypatch.setenv("GEMINI_API_KEY", "unit-test-not-a-key")
    monkeypatch.setenv("GEMINI_MODEL_ID", gemini.DEFAULT_MODEL)
    monkeypatch.setenv("BENCHMARK_SQLITE_PATH", str(db))
    sql = "SELECT COUNT(*) AS n FROM ConnectorHealingQueue"
    response = {"claims": [{"text": "Có 3 incident.", "evidence": [{"row": 0, "column": "n"}]}]}
    sdk = FakeSDK(
        *[
            r
            for _ in range(2)
            for r in [
                reply({"kind": "sql", "sql": sql, "interpretation": "count"}),
                reply(response),
            ]
        ]
    )
    monkeypatch.setattr(google.genai, "Client", lambda **kwargs: sdk)
    scope = {}
    for _ in range(2):
        for index, cell in enumerate(nb.cells):
            if cell.cell_type == "code":
                assert "huggingface_hub" not in cell.source
                assert "pyodbc" not in cell.source and "ollama" not in cell.source.lower()
                exec(compile(cell.source, f"gemini-cell-{index}", "exec"), scope)
        assert scope["final_answer"]["source"] == "gemini"
        assert scope["workflow"].metrics["response_api_calls"] == 1
    assert db.read_bytes() == before
    assert "unit-test-not-a-key" not in capsys.readouterr().out
    assert all(not cell.get("outputs") for cell in nb.cells)
