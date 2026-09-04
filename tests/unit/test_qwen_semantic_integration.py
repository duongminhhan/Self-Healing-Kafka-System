"""Offline notebook execution deliberately skips every refresh/install cell."""

import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from notebooks.evaluation.evaluate_qwen_semantics import evaluate
from notebooks.evaluation.fixtures import create_duration_fixture
from notebooks.evaluation.semantic_cases import GOLD, HOLDOUT
from notebooks.qwen.adapter import HFServiceError
from notebooks.shared.analytics import Snapshot

ROOT = Path(__file__).resolve().parents[2]


class Responses:
    def __init__(self, values):
        self.values = iter(values)
        self.calls = []

    def chat_completion(self, **kwargs):
        self.calls.append(kwargs)
        value = next(self.values)
        if isinstance(value, Exception):
            raise value
        return NS(choices=[NS(finish_reason="stop", message=NS(content=json.dumps(value)))])


@pytest.fixture
def snapshot_file(tmp_path):
    path = tmp_path / "fixture.db"
    create_duration_fixture(path, "normal")
    return path


def test_offline_evaluation_never_claims_model_accuracy(snapshot_file):
    report = evaluate(Snapshot(snapshot_file), cases=[GOLD[0], GOLD[6], HOLDOUT[-1]])
    assert report["model_accuracy"] is None
    assert report["offline_compiler_passed"]
    assert report["records"][-1]["compiler_match"] is None
    assert all(not r["model_executed"] for r in report["records"])


def test_billing_stops_remaining_modes_and_cases(snapshot_file):
    client = Responses([HFServiceError("billing", 402)])
    report = evaluate(Snapshot(snapshot_file), client=client, cases=[GOLD[0], GOLD[6]])
    assert report["service_blocked"]
    assert len(client.calls) == 1
    assert len(report["records"]) == 6
    assert sum(r["status"] == "not_run_service_block" for r in report["records"]) == 5
    assert report["summaries"]["legacy"]["final_execution_accuracy"] == 0
    assert report["summaries"]["strict"]["final_execution_accuracy"] is None


@pytest.mark.parametrize("mode", ["legacy", "shadow", "strict"])
@pytest.mark.parametrize("cwd", ["root", "notebook"])
def test_notebook_cells_in_order_without_refresh(snapshot_file, monkeypatch, capsys, mode, cwd):
    import notebooks.qwen.adapter as adapter

    nb = json.loads(
        (ROOT / "notebooks/qwen/text_to_sql_self_healthy_kafka.ipynb").read_text(encoding="utf-8")
    )
    raw_sql = {
        "kind": "sql",
        "sql": "SELECT QueueStatus,COUNT(*) n FROM ConnectorHealingQueue GROUP BY QueueStatus ORDER BY QueueStatus",
        "interpretation": "Số incident theo trạng thái; toàn bộ snapshot",
    }
    semantic = {
        "kind": "query",
        "entity": "incidents",
        "dimensions": ["queue_status"],
        "metrics": ["incident_count"],
        "order_by": [{"field": "queue_status", "direction": "asc"}],
    }
    values = {"legacy": [raw_sql], "strict": [semantic], "shadow": [raw_sql, semantic]}[mode]
    # A malformed response must use deterministic evidence fallback, not crash.
    client = Responses(values + [{"unsupported_response": "not grounded"}])
    monkeypatch.setattr(adapter, "QwenClient", lambda **kwargs: client)
    monkeypatch.setenv("HF_TOKEN", "offline-test-only")
    monkeypatch.setenv("HF_MODEL_ID", "Qwen/test")
    monkeypatch.setenv("HF_PROVIDER", "auto")
    monkeypatch.setenv("QWEN_SEMANTIC_MODE", mode)
    monkeypatch.setenv("BENCHMARK_SQLITE_PATH", str(snapshot_file))
    monkeypatch.delenv("HF_SQL_ALLOWED_TABLES", raising=False)
    monkeypatch.chdir(ROOT if cwd == "root" else ROOT / "notebooks/qwen")
    state = {"os": os, "Path": Path, "REPO_ROOT": ROOT}
    before = hashlib.sha256(snapshot_file.read_bytes()).hexdigest()
    for index in [10, 12, 17, 19, 20]:
        source = "".join(nb["cells"][index]["source"])
        assert "pyodbc.connect" not in source and "local_poc_connection_string()" not in source
        exec(compile(source, f"qwen-cell-{index}", "exec"), state)
    assert state["workflow"].mode == mode
    assert state["verified_result"]["returned_row_count"] > 0
    assert state["final_answer"]["source"] == "verified_table_fallback"
    assert len(client.calls) == (3 if mode == "shadow" else 2)
    assert hashlib.sha256(snapshot_file.read_bytes()).hexdigest() == before
    output = capsys.readouterr().out
    assert "Response source:" in output and "Step B" in output
    assert "offline-test-only" not in output


def test_default_mode_is_legacy(snapshot_file, monkeypatch):
    nb = json.loads(
        (ROOT / "notebooks/qwen/text_to_sql_self_healthy_kafka.ipynb").read_text(encoding="utf-8")
    )
    from notebooks.shared.semantic_workflow import SemanticWorkflow

    monkeypatch.delenv("QWEN_SEMANTIC_MODE", raising=False)
    monkeypatch.setenv("BENCHMARK_SQLITE_PATH", str(snapshot_file))
    monkeypatch.delenv("HF_SQL_ALLOWED_TABLES", raising=False)
    state = {
        "os": os,
        "Path": Path,
        "REPO_ROOT": ROOT,
        "Snapshot": Snapshot,
        "SemanticWorkflow": SemanticWorkflow,
        "hf_client": None,
        "hf_model_id": "Qwen/test",
        "hf_provider": "auto",
    }
    exec("".join(nb["cells"][17]["source"]), state)
    assert state["workflow"].mode == "legacy"


def test_notebook_through_real_hf_sdk_mock_http(snapshot_file, monkeypatch, capsys):
    import httpx
    import huggingface_hub.inference._client as sdk

    nb = json.loads(
        (ROOT / "notebooks/qwen/text_to_sql_self_healthy_kafka.ipynb").read_text(encoding="utf-8")
    )
    requests = []

    def handler(request):
        body = json.loads(request.content)
        requests.append(body)
        if len(requests) == 1:
            content = {
                "kind": "query",
                "entity": "incidents",
                "dimensions": ["queue_status"],
                "metrics": ["incident_count"],
                "order_by": [{"field": "queue_status", "direction": "asc"}],
            }
        else:
            evidence = json.loads(body["messages"][-1]["content"])["verified_result"]
            content = {
                "claims": [
                    {
                        "text": f"Trạng thái hàng đợi {row['queue_status']} có {row['incident_count']} incident trong snapshot.",
                        "evidence": [{"row": i, "column": c} for c in row],
                    }
                    for i, row in enumerate(evidence["rows"])
                ]
            }
        return httpx.Response(
            200,
            json={
                "id": "test",
                "object": "chat.completion",
                "created": 1,
                "model": "Qwen/test",
                "choices": [
                    {
                        "index": 0,
                        "finish_reason": "stop",
                        "message": {"role": "assistant", "content": json.dumps(content)},
                    }
                ],
                "usage": {"prompt_tokens": 100, "completion_tokens": 30, "total_tokens": 130},
            },
        )

    monkeypatch.setenv("HF_TOKEN", "hf_offline_test_only")
    monkeypatch.setenv("HF_MODEL_ID", "Qwen/test")
    monkeypatch.setenv("HF_PROVIDER", "auto")
    monkeypatch.setenv("HF_STRUCTURED_OUTPUT", "auto")
    monkeypatch.setenv("QWEN_SEMANTIC_MODE", "strict")
    monkeypatch.setenv("BENCHMARK_SQLITE_PATH", str(snapshot_file))
    monkeypatch.delenv("HF_SQL_ALLOWED_TABLES", raising=False)
    state = {"os": os, "Path": Path, "REPO_ROOT": ROOT}
    with httpx.Client(transport=httpx.MockTransport(handler)) as session:
        monkeypatch.setattr(sdk, "get_session", lambda: session)
        for index in [10, 12, 17, 19, 20]:
            exec(
                compile("".join(nb["cells"][index]["source"]), f"qwen-cell-{index}", "exec"), state
            )
    assert len(requests) == 2
    assert all(r["response_format"]["type"] == "json_schema" for r in requests)
    assert state["final_answer"]["source"] == "huggingface"
    assert state["workflow"].metrics["tokens"]["sql"] == {"input": 100, "output": 30}
    assert state["workflow"].metrics["tokens"]["response"] == {"input": 100, "output": 30}
    assert "hf_offline_test_only" not in capsys.readouterr().out
