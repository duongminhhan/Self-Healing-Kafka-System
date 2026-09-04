"""Fresh-kernel ordered execution with synthetic MSSQL rows and mocked HF transport."""

import ast
import json
from pathlib import Path
from types import SimpleNamespace as NS

import nbformat
import pytest

from notebooks.shared.analytics import Snapshot

ROOT = Path(__file__).resolve().parents[2]
NOTEBOOK = ROOT / "notebooks" / "qwen" / "text_to_sql_self_healthy_kafka.ipynb"


def test_notebook_format_and_no_fixed_contract():
    nb = nbformat.read(NOTEBOOK, as_version=4)
    nbformat.validate(nb)
    for i, c in enumerate(nb.cells):
        if c.cell_type == "code" and i != 0:
            compile(c.source, f"cell-{i}", "exec")
    source = "\n".join(c.source for c in nb.cells)
    assert "canonicalize_top_connector_facts" not in source
    assert "ollama" not in source.lower()
    question = next(
        n
        for n in ast.parse(nb.cells[19].source).body
        if isinstance(n, ast.Assign)
        and any(isinstance(t, ast.Name) and t.id == "question" for t in n.targets)
    )
    # The user is expected to edit this question without breaking the test suite.
    assert isinstance(ast.literal_eval(question.value), str)
    assert ast.literal_eval(question.value).strip()


@pytest.mark.parametrize("empty_source", [False, True])
@pytest.mark.parametrize("provider_cwd", [False, True])
def test_ordered_cells_and_idempotent_refresh(
    tmp_path, monkeypatch, capsys, empty_source, provider_cwd
):
    import huggingface_hub
    import pyodbc

    nb = nbformat.read(NOTEBOOK, as_version=4)
    monkeypatch.chdir(NOTEBOOK.parent if provider_cwd else ROOT)
    db = tmp_path / "snapshot.db"
    monkeypatch.setenv("HF_TOKEN", "unit-test-placeholder")
    monkeypatch.setenv("BENCHMARK_MSSQL_CONNECTION_STRING", "mock-only")
    monkeypatch.setenv("BENCHMARK_SQLITE_PATH", str(db))
    monkeypatch.setenv("HF_PROVIDER", "auto")
    monkeypatch.setattr(pyodbc, "drivers", lambda: ["ODBC Driver 17 for SQL Server"])
    queue = [
        (
            "q1",
            "alpha",
            "alpha",
            None,
            "RESTART_ONLY",
            "COMPLETED",
            None,
            "2026-09-01",
            None,
            None,
            None,
        ),
        (
            "q2",
            "alpha",
            "alpha",
            None,
            "RESTART_ONLY",
            "WAITING",
            None,
            "2026-09-02",
            None,
            None,
            None,
        ),
        (
            "q3",
            "beta",
            "beta",
            None,
            "RESTART_ONLY",
            "PENDING",
            None,
            "2026-09-03",
            None,
            None,
            None,
        ),
    ]
    logs = [
        ("l1", "q1", "alpha", "START", None, None, "INFO", "[REDACTED]", "[REDACTED]", "2026-09-01")
    ]
    if empty_source:
        queue, logs = [], []

    class Source:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def cursor(self):
            return self

        def execute(self, sql):
            assert sql.lstrip().startswith("SELECT")
            self.rows = logs if "[dbo].[ConnectorHealingLogs]" in sql else queue

        def fetchall(self):
            return self.rows

    monkeypatch.setattr(pyodbc, "connect", lambda *a, **k: Source())
    sql = "SELECT RootConnectorName AS connector, COUNT(QueueId) AS entries FROM ConnectorHealingQueue GROUP BY RootConnectorName ORDER BY entries DESC,connector LIMIT 5"
    response = {
        "claims": [
            {
                "text": "alpha có 2 incident.",
                "evidence": [{"row": 0, "column": "connector"}, {"row": 0, "column": "entries"}],
            },
            {
                "text": "beta có 1 incident.",
                "evidence": [{"row": 1, "column": "connector"}, {"row": 1, "column": "entries"}],
            },
        ]
    }

    class HF:
        def __init__(self, **kwargs):
            self.calls = 0

        def chat_completion(self, **kwargs):
            self.calls += 1
            value = (
                {"kind": "sql", "sql": sql, "interpretation": "persisted incidents per root"}
                if "generate analytical SQLite" in kwargs["messages"][0]["content"]
                else response
            )
            return NS(
                choices=[NS(message=NS(content=json.dumps(value)), finish_reason="stop")],
                usage=NS(prompt_tokens=10, completion_tokens=20),
            )

    monkeypatch.setattr(huggingface_hub, "InferenceClient", HF)
    scope = {}
    for iteration in range(2):
        # Include both real SQLite loading cells, against synthetic read-only source results.
        for i, c in enumerate(nb.cells):
            if c.cell_type == "code" and i != 0:
                exec(compile(c.source, f"cell-{i}", "exec"), scope)
        expected = (
            []
            if empty_source
            else [
                {"connector": "alpha", "entries": 2},
                {"connector": "beta", "entries": 1},
            ]
        )
        assert scope["verified_result"]["rows"] == expected
        assert scope["final_answer"]["source"] == (
            "verified_table_fallback" if empty_source else "huggingface"
        )
        assert scope["snapshot_refresh_metadata"]["tables"]["ConnectorHealingLogs"]["rows"] == len(
            logs
        )
        assert Snapshot(db).execute("SELECT COUNT(*) AS n FROM ConnectorHealingLogs")["rows"] == [
            {"n": len(logs)}
        ]
        scope["engine"].dispose()
    assert "unit-test-placeholder" not in capsys.readouterr().out
