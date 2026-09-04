import json

import httpx
import pytest

from notebooks.nemotron.adapter import CloudError, NemotronClient


def payload(content, **extra):
    return {
        "model": "nemotron-3-ultra",
        "done": True,
        "done_reason": "stop",
        "message": {"content": content},
        **extra,
    }


@pytest.fixture
def client_factory(monkeypatch):
    monkeypatch.setenv("OLLAMA_API_KEY", "synthetic-test-secret")
    monkeypatch.setenv("NEMOTRON_MODEL_ID", "nemotron-3-ultra")
    monkeypatch.setenv("NEMOTRON_THINKING", "true")
    clients = []

    def create(handler):
        client = NemotronClient(transport=httpx.MockTransport(handler))
        clients.append(client)
        return client

    yield create
    for client in clients:
        client.close()


def test_official_request_and_usage(client_factory):
    def handler(request):
        assert str(request.url) == "https://ollama.com/api/chat"
        data = json.loads(request.content)
        assert "format" not in data
        assert data["options"]["num_predict"] == 4096
        assert "synthetic-test-secret" not in request.content.decode()
        return httpx.Response(200, json=payload('{"kind":"accept_result"}', prompt_eval_count=0))

    result = client_factory(handler).complete_stage([], "sql", 4096)
    assert result.output_error is None
    assert result.reported_usage == {"input": 0, "output": None, "cached_tokens": None}


@pytest.mark.parametrize(
    "body,error",
    [
        (payload("not json"), "invalid_json"),
        (payload("[]"), "invalid_schema"),
        (payload('{"kind":"sql","sql":"SELECT 1"}'), "invalid_schema"),
        (payload(""), "empty_content"),
        (payload("", message={"thinking": "private reasoning"}), "thinking_only"),
        (payload('{"kind":"accept_result"}', done_reason="length"), "output_budget_exhausted"),
        (payload("", done_reason="safety"), "safety_block"),
    ],
)
def test_invalid_outputs(client_factory, body, error):
    client = client_factory(lambda request: httpx.Response(200, json=body))
    result = client.complete_stage([], "sql", 100)
    assert result.output_error == error
    assert "private reasoning" not in repr(result)


@pytest.mark.parametrize(
    "status,category",
    [
        (401, "authentication"),
        (402, "billing"),
        (403, "permission"),
        (404, "model_unavailable"),
        (429, "quota_or_rate_limit"),
        (400, "unsupported_or_invalid_request"),
    ],
)
def test_service_errors_no_retry(client_factory, status, category):
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(status, text="secret raw response")

    with pytest.raises(CloudError) as error:
        client_factory(handler).complete_stage([], "sql", 100)
    assert error.value.category == category
    assert "secret" not in str(error.value)
    assert len(calls) == 1


def test_timeout(client_factory):
    def handler(request):
        raise httpx.ReadTimeout("private details", request=request)

    with pytest.raises(CloudError, match="timeout"):
        client_factory(handler).complete_stage([], "sql", 100)


def test_model_preflight(client_factory):
    client = client_factory(
        lambda request: httpx.Response(200, json={"models": [{"name": "nemotron-3-ultra"}]})
    )
    assert client.check_model()["model_list_verified"]


def test_missing_key(monkeypatch):
    monkeypatch.delenv("OLLAMA_API_KEY", raising=False)
    with pytest.raises(ValueError, match="Set OLLAMA_API_KEY"):
        NemotronClient()


def test_stage_thinking_override(client_factory, monkeypatch):
    monkeypatch.setenv("NEMOTRON_SQL_THINKING", "true")
    monkeypatch.setenv("NEMOTRON_RESPONSE_THINKING", "false")
    sent = []

    def handler(request):
        sent.append(json.loads(request.content)["think"])
        return httpx.Response(200, json=payload('{"kind":"accept_result"}'))

    client = client_factory(handler)
    client.complete_stage([], "sql", 100)
    client.complete_stage([], "response", 100)
    assert sent == [True, False]


@pytest.mark.parametrize("provider_cwd", [False, True])
def test_notebook_ordered_cells(tmp_path, monkeypatch, capsys, provider_cwd):
    from pathlib import Path

    import nbformat

    import notebooks.nemotron.adapter as adapter
    from notebooks.evaluation.fixtures import create_duration_fixture

    root = Path(__file__).resolve().parents[2]
    path = root / "notebooks/nemotron/text_to_sql_self_healthy_kafka_nemotron.ipynb"
    nb = nbformat.read(path, as_version=4)
    nbformat.validate(nb)
    db = tmp_path / "test.db"
    create_duration_fixture(db, "normal")
    monkeypatch.chdir(path.parent if provider_cwd else root)
    monkeypatch.setenv("BENCHMARK_SQLITE_PATH", str(db))
    monkeypatch.setenv("OLLAMA_API_KEY", "synthetic-test-secret")
    calls = []

    def handler(request):
        if request.url.path == "/api/tags":
            return httpx.Response(200, json={"models": [{"name": "nemotron-3-ultra"}]})
        calls.append(request)
        if len(calls) == 1:
            content = {
                "kind": "sql",
                "sql": "SELECT COUNT(*) AS n FROM ConnectorHealingQueue",
                "interpretation": "Count queue entries",
            }
        else:
            content = {
                "claims": [
                    {"text": "Có 3 queue trong snapshot.", "evidence": [{"row": 0, "column": "n"}]}
                ]
            }
        return httpx.Response(200, json=payload(json.dumps(content)))

    original = adapter.NemotronClient
    monkeypatch.setattr(
        adapter, "NemotronClient", lambda **kwargs: original(transport=httpx.MockTransport(handler))
    )
    namespace = {}
    code_cells = [c for c in nb.cells if c.cell_type == "code"]
    try:
        for cell in code_cells:
            assert not cell.outputs
            exec(compile(cell.source, str(path), "exec"), namespace)
        assert namespace["final_answer"]["source"] == "ollama_cloud"
        assert namespace["workflow"].metrics["sql_api_calls"] == 1
        exec(code_cells[-1].source, namespace)
        assert len(calls) == 3  # SQL once; response twice
        namespace["question"] = "changed"
        with pytest.raises(RuntimeError, match="Question"):
            exec(code_cells[-1].source, namespace)
        assert namespace["final_answer"] is None
        assert "synthetic-test-secret" not in capsys.readouterr().out
    finally:
        if namespace.get("workflow"):
            namespace["workflow"].client.close()


def test_budget_and_unknown_usage(tmp_path, client_factory):
    from notebooks.evaluation.fixtures import create_duration_fixture
    from notebooks.shared.analytics import QueryError, Snapshot, Workflow

    path = tmp_path / "test.db"
    create_duration_fixture(path, "normal")
    calls = []

    def handler(request):
        calls.append(request)
        return httpx.Response(200, json=payload("broken"))

    flow = Workflow(
        Snapshot(path), client_factory(handler), model_id="nemotron-3-ultra", max_attempts=10
    )
    with pytest.raises(QueryError):
        flow.query("Count queues")
    assert len(calls) == 3
    assert flow.result is None
    assert flow.metrics["tokens"]["sql"]["input"] is None


@pytest.mark.parametrize(
    "variant", ["normal", "no_success", "missing", "malformed", "offsets", "negative", "zero"]
)
def test_duration_with_cloud_transport(tmp_path, monkeypatch, variant):
    from notebooks.evaluation.evaluate import CASES, matches
    from notebooks.evaluation.fixtures import EXPECTED, create_duration_fixture
    from notebooks.nemotron.adapter import make_nemotron_workflow

    monkeypatch.setenv("OLLAMA_API_KEY", "synthetic-test-secret")
    db = tmp_path / "fixture.db"
    create_duration_fixture(db, variant)
    count = 0

    def handler(request):
        nonlocal count
        count += 1
        decision = (
            {
                "kind": "sql",
                "sql": CASES[-1][1],
                "interpretation": "Average successful duration, exclude invalid values, retain zero",
            }
            if count == 1
            else {"kind": "accept_result"}
        )
        return httpx.Response(200, json=payload(json.dumps(decision)))

    flow = make_nemotron_workflow(db, transport=httpx.MockTransport(handler))
    try:
        result = flow.query(CASES[-1][0])
        assert matches(result, EXPECTED[variant])
        assert count <= 3
    finally:
        flow.client.close()


def test_privacy_boundary(tmp_path, monkeypatch):
    import sqlite3

    from notebooks.evaluation.fixtures import create_duration_fixture
    from notebooks.nemotron.adapter import make_nemotron_workflow
    from notebooks.shared.analytics import QueryError

    monkeypatch.setenv("OLLAMA_API_KEY", "synthetic-test-secret")
    db = tmp_path / "fixture.db"
    create_duration_fixture(db, "normal")
    with sqlite3.connect(db) as conn:
        conn.executescript(
            "CREATE TABLE ConnectorHealingLogs(Id TEXT, QueueId INTEGER, Message TEXT, Details TEXT); INSERT INTO ConnectorHealingLogs VALUES ('l1',1,'PRIVATE-LOG','PRIVATE-DETAIL'); CREATE TABLE secret(value TEXT);"
        )
    sent = []

    def handler(request):
        sent.append(request.content.decode())
        return httpx.Response(
            200,
            json=payload(
                json.dumps(
                    {
                        "kind": "sql",
                        "sql": "SELECT COUNT(*) AS n FROM ConnectorHealingLogs",
                        "interpretation": "Count events",
                    }
                )
            ),
        )

    flow = make_nemotron_workflow(db, transport=httpx.MockTransport(handler))
    try:
        for sql in [
            "SELECT Message AS harmless FROM ConnectorHealingLogs",
            "SELECT substr(Details,1,3) FROM ConnectorHealingLogs",
            "SELECT COUNT(*) FROM ConnectorHealingLogs WHERE Message='x'",
            "SELECT * FROM secret",
        ]:
            with pytest.raises(QueryError):
                flow.snapshot.execute(sql)
        flow.query("Count healing events")
        assert not any(
            secret in body
            for body in sent
            for secret in ["PRIVATE-LOG", "PRIVATE-DETAIL", "synthetic-test-secret"]
        )
        assert "Message" not in {c["name"] for c in flow.snapshot.schema["ConnectorHealingLogs"]}
    finally:
        flow.client.close()


@pytest.mark.parametrize("preflight_status", [200, 429])
def test_evaluator_backend(tmp_path, monkeypatch, capsys, preflight_status):
    import notebooks.evaluation.evaluate as evaluator
    import notebooks.nemotron.adapter as adapter
    from notebooks.evaluation.fixtures import create_duration_fixture

    db = tmp_path / "fixture.db"
    create_duration_fixture(db, "normal")
    monkeypatch.setenv("OLLAMA_API_KEY", "synthetic-test-secret")
    monkeypatch.setattr(
        "sys.argv",
        ["evaluate", "--backend", "nemotron", "--live", "--snapshot", str(db), "--case", "1"],
    )
    calls = []

    def handler(request):
        if request.url.path == "/api/tags":
            return httpx.Response(preflight_status, json={"models": [{"name": "nemotron-3-ultra"}]})
        calls.append(request)
        content = (
            {"kind": "sql", "sql": evaluator.CASES[0][1], "interpretation": "Count incidents"}
            if len(calls) == 1
            else {"claims": []}
        )
        return httpx.Response(200, json=payload(json.dumps(content)))

    original = adapter.NemotronClient
    monkeypatch.setattr(
        adapter, "NemotronClient", lambda **kwargs: original(transport=httpx.MockTransport(handler))
    )
    status = evaluator.main()
    output = capsys.readouterr().out
    assert "synthetic-test-secret" not in output
    if preflight_status == 429:
        assert status == 1 and not calls
        assert '"not_run_cases": 1' in output
        assert '"preflight_error": "quota_or_rate_limit"' in output
    else:
        assert status == 0 and len(calls) == 2
        assert '"final_execution_accuracy": 1.0' in output
        assert '"fallback_frequency": 1.0' in output
