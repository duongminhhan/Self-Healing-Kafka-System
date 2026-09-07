"""Exercise the actual adapter and workflow contract, not prevalidated dict mocks."""

import json
from types import SimpleNamespace as NS

import httpx
import pytest
from jsonschema import Draft202012Validator

from notebooks.evaluation.fixtures import create_duration_fixture
from notebooks.qwen.adapter import QwenClient
from notebooks.qwen.output_schema import SCHEMAS
from notebooks.shared.analytics import QueryError, Snapshot
from notebooks.shared.semantic_workflow import SemanticWorkflow

SQL = {
    "kind": "sql",
    "sql": "SELECT COUNT(*) n FROM ConnectorHealingQueue",
    "interpretation": "All incidents",
}
PLAN = {"kind": "query", "entity": "incidents", "dimensions": [], "metrics": ["incident_count"]}


class Transport:
    def __init__(self, values):
        self.values = iter(values)
        self.calls = []

    def __call__(self, **kwargs):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def chat_completion(self, **kwargs):
        self.calls.append(kwargs)
        value = next(self.values)
        if isinstance(value, Exception):
            raise value
        reason, value = value if isinstance(value, tuple) else ("stop", value)
        return NS(
            choices=[
                NS(finish_reason=reason, message=NS(content=json.dumps(value), thinking="PRIVATE"))
            ],
            usage=NS(prompt_tokens=15, completion_tokens=20),
        )


def flow_for(tmp_path, values, **kwargs):
    path = tmp_path / "fixture.db"
    create_duration_fixture(path, "normal")
    transport = Transport(values)
    client = QwenClient(
        model="Qwen/test", provider="auto", api_key="test-only", client_factory=transport
    )
    flow = SemanticWorkflow(Snapshot(path), client, model_id="Qwen/test", **kwargs)
    return flow, transport


@pytest.mark.parametrize(
    "mode,value,detail",
    [
        ("legacy", {"kind": "sql", "sql": SQL["sql"]}, "Missing required fields: interpretation"),
        ("legacy", PLAN, "kind must be one of: sql, clarification"),
        ("strict", SQL, "kind must be one of: query, clarification"),
        ("legacy", {"kind": "accept_result"}, "kind must be one of: sql, clarification"),
        ("legacy", {**SQL, "interpretation": 123}, "Invalid interpretation: type"),
        ("strict", {**PLAN, "entity": "incidents|events"}, "expected one of incidents, events"),
    ],
)
def test_reject_then_repair(tmp_path, mode, value, detail):
    valid = PLAN if mode == "strict" else SQL
    flow, transport = flow_for(tmp_path, [value, valid], mode=mode)
    assert flow.query("Count all incidents")["rows"]
    assert flow.metrics["sql_attempts"] == 1
    assert flow.metrics["sql_api_calls"] == 2
    assert detail in str(flow.trace)
    assert detail in json.dumps(transport.calls[1]["messages"])
    for call in transport.calls:
        schema = call["response_format"]["json_schema"]["schema"]
        assert Draft202012Validator(schema).is_valid(valid)
        assert not Draft202012Validator(schema).is_valid(value)
    assert [c["max_tokens"] for c in transport.calls] == [2048, 2048]


def test_review_contract_only_after_verified_candidate(tmp_path):
    empty = {**SQL, "sql": "SELECT QueueStatus FROM ConnectorHealingQueue WHERE 1=0"}
    flow, transport = flow_for(tmp_path, [empty, {"kind": "accept_result"}])
    assert flow.query("Queues matching an empty scope")["rows"] == []
    assert [r["contract"] for r in flow.metrics["calls"]] == ["legacy_generation", "legacy_review"]
    assert (
        transport.calls[1]["response_format"]["json_schema"]["schema"] == SCHEMAS["legacy_review"]
    )
    assert flow.metrics["result_reviews"] == 1


def test_review_accept_restores_interpretation_of_pending_result(tmp_path):
    duration = (
        "AVG(CASE WHEN (julianday(CompletedAt)-julianday(ReceivedAt))*1440 >= 0 "
        "THEN (julianday(CompletedAt)-julianday(ReceivedAt))*1440 END) AS minutes"
    )
    original = {
        "kind": "sql",
        "sql": (
            f"SELECT {duration}, COUNT(*) AS matched_count FROM ConnectorHealingQueue "
            "WHERE QueueId='not-an-incident'"
        ),
        "interpretation": "Average for the explicitly requested empty incident scope",
    }
    rejected_replacement = {
        "kind": "sql",
        "sql": (
            f"SELECT {duration}, 999 AS matched_count FROM ConnectorHealingQueue "
            "WHERE QueueStatus='COMPLETED' AND FinalOutcome='RECOVERED'"
        ),
        "interpretation": "Different scope with an invalid diagnostic count",
    }
    flow, transport = flow_for(
        tmp_path, [original, rejected_replacement, {"kind": "accept_result"}]
    )

    result = flow.query("Average duration for incident not-an-incident")

    assert result["sql"] == original["sql"]
    assert result["rows"] == [{"minutes": None, "matched_count": 0}]
    assert flow.interpretation == original["interpretation"]
    assert any(
        entry["status"] == "rejected"
        and "matched_count=999 contradicts diagnostic 2" in entry["error"]
        for entry in flow.trace
    )
    assert flow.trace[-1]["status"] == "result_confirmed"
    assert flow.metrics["sql_api_calls"] == len(transport.calls) == 3
    assert flow.metrics["sql_attempts"] == 2
    assert [r["contract"] for r in flow.metrics["calls"]] == [
        "legacy_generation",
        "legacy_review",
        "legacy_review",
    ]


@pytest.mark.parametrize("mode", ["legacy", "strict"])
def test_truncated_complete_json_not_used_and_budget_grows(tmp_path, mode):
    value = PLAN if mode == "strict" else SQL
    flow, transport = flow_for(tmp_path, [("length", value), value], mode=mode)
    assert flow.query("Count incidents")["rows"]
    assert [c["max_tokens"] for c in transport.calls] == [2048, 4096]
    assert flow.metrics["sql_attempts"] == 1
    assert flow.sql_max_tokens == 2048
    assert flow.metrics["calls"][0]["finish_reason"] == "length"
    assert flow.metrics["calls"][0]["token_usage"] == {"input": 15, "output": 20}
    assert "PRIVATE" not in json.dumps(flow.metrics)


@pytest.mark.parametrize(
    "initial,ceiling,expected", [(512, 1500, [512, 1024, 1500]), (700, 700, [700] * 3)]
)
def test_exhaustion_and_explicit_limits(tmp_path, initial, ceiling, expected):
    flow, transport = flow_for(
        tmp_path, [("length", SQL)] * 3, sql_max_tokens=initial, sql_token_ceiling=ceiling
    )
    with pytest.raises(QueryError, match="exhausted"):
        flow.query("Count incidents")
    assert [c["max_tokens"] for c in transport.calls] == expected
    assert flow.metrics["sql_attempts"] == 0
    with pytest.raises(QueryError, match="no current evidence"):
        flow.respond()
    assert len(transport.calls) == 3
    assert flow.metrics["response_api_calls"] == 0


def test_provider_cap_and_reset(tmp_path):
    flow, transport = flow_for(
        tmp_path,
        [("length", SQL), SQL, SQL],
        sql_max_tokens=600,
        model_output_token_limit=900,
        response_max_tokens=900,
    )
    flow.query("Count incidents")
    flow.query("Count incidents again")
    assert [c["max_tokens"] for c in transport.calls] == [600, 900, 600]
    with pytest.raises(ValueError, match="ceiling"):
        SemanticWorkflow(
            flow.snapshot,
            flow.client,
            model_id="Qwen/test",
            sql_max_tokens=1000,
            model_output_token_limit=900,
            response_max_tokens=900,
        )


def test_response_budget_above_provider_cap_fails_before_inference(tmp_path):
    flow, transport = flow_for(tmp_path, [])
    with pytest.raises(ValueError, match="HF_RESPONSE_MAX_TOKENS"):
        SemanticWorkflow(
            flow.snapshot,
            flow.client,
            model_id="Qwen/test",
            sql_max_tokens=600,
            model_output_token_limit=900,
            response_max_tokens=901,
        )
    assert transport.calls == []


@pytest.mark.parametrize("response_budget", [700, 900])
def test_valid_response_budget_is_sent_unchanged(tmp_path, response_budget):
    response = {
        "claims": [
            {"text": "Có 3 incident trong snapshot.", "evidence": [{"row": 0, "column": "n"}]}
        ]
    }
    flow, transport = flow_for(
        tmp_path,
        [SQL, response],
        sql_max_tokens=600,
        model_output_token_limit=900,
        response_max_tokens=response_budget,
    )
    flow.query("Count incidents")
    answer = flow.respond()

    assert answer["source"] == "huggingface"
    assert [c["max_tokens"] for c in transport.calls] == [600, response_budget]
    assert flow.metrics["calls"][-1]["requested_output_budget"] == response_budget
    assert flow.response_max_tokens == response_budget


def test_response_without_api_call_preserves_prior_call_diagnostics(tmp_path):
    response = {
        "claims": [
            {"text": "Có 3 incident trong snapshot.", "evidence": [{"row": 0, "column": "n"}]}
        ]
    }
    flow, transport = flow_for(tmp_path, [SQL, response])
    flow.query("Count incidents")
    assert flow.respond()["source"] == "huggingface"
    prior_calls = json.loads(json.dumps(flow.metrics["calls"]))
    assert prior_calls[-1]["response_source"] == "huggingface"
    assert prior_calls[-1]["fallback_reason"] is None

    # A later response can use empty verified evidence without calling the model.
    flow.result = flow.snapshot.execute("SELECT QueueId FROM ConnectorHealingQueue WHERE 1=0")
    answer = flow.respond()

    assert answer["source"] == "verified_table_fallback"
    assert answer["reason"] == "empty_result"
    assert flow.metrics["response_api_calls"] == 0
    assert len(transport.calls) == 2
    assert flow.metrics["fallback_reason"] == "empty_result"
    assert flow.metrics["calls"] == prior_calls


def test_shadow_uses_distinct_contracts_and_remaining_budget(tmp_path):
    flow, transport = flow_for(tmp_path, [SQL, ("length", PLAN), PLAN], mode="shadow")
    flow.query("Count incidents")
    assert flow.metrics["total_sql_api_calls"] == 3
    assert [c["response_format"]["json_schema"]["name"] for c in transport.calls] == [
        "qwen_legacy_generation",
        "qwen_strict_planning",
        "qwen_strict_planning",
    ]
    assert [c["max_tokens"] for c in transport.calls] == [2048, 2048, 4096]


def test_format_rejection_spends_one_of_three_calls(tmp_path):
    error = httpx.HTTPStatusError(
        "json_schema unsupported",
        request=httpx.Request("POST", "https://example.test"),
        response=httpx.Response(400),
    )
    flow, transport = flow_for(tmp_path, [error, ("length", SQL), SQL])
    assert flow.query("Count incidents")["rows"]
    assert len(transport.calls) == 3
    assert "response_format" in transport.calls[0]
    assert all("response_format" not in c for c in transport.calls[1:])
    assert [c["max_tokens"] for c in transport.calls] == [2048, 2048, 4096]


@pytest.mark.parametrize("status", [401, 402, 403, 429])
def test_service_failure_no_correction_or_response(tmp_path, status):
    error = httpx.HTTPStatusError(
        "PRIVATE",
        request=httpx.Request("POST", "https://example.test"),
        response=httpx.Response(status),
    )
    flow, transport = flow_for(tmp_path, [error])
    with pytest.raises(QueryError):
        flow.query("Count incidents")
    with pytest.raises(QueryError):
        flow.respond()
    assert len(transport.calls) == 1
    assert flow.metrics["sql_attempts"] == 0
    assert "PRIVATE" not in json.dumps(flow.metrics)


@pytest.mark.parametrize("mode", ["legacy", "strict", "shadow"])
def test_timeout_is_one_call(tmp_path, mode):
    flow, transport = flow_for(tmp_path, [httpx.ReadTimeout("PRIVATE")], mode=mode)
    with pytest.raises(QueryError):
        flow.query("Count incidents")
    assert len(transport.calls) == 1
    assert flow.metrics["calls"][0]["service_error"]["category"] == "timeout"
    assert flow.result is None


def test_conditional_schema_matches_local_branch_validation():
    from notebooks.qwen.output_schema import contract_error

    samples = [
        SQL,
        PLAN,
        {"kind": "clarification", "question": "Which denominator?"},
        {"kind": "accept_result"},
        {"kind": "sql", "sql": "SELECT 1"},
        {**SQL, "question": "PRIVATE"},
        {**PLAN, "entity": "incidents|events"},
        {"kind": "clarification", "question": "Which?", "sql": "SELECT 1"},
    ]
    for contract, schema in SCHEMAS.items():
        Draft202012Validator.check_schema(schema)
        for sample in samples:
            assert Draft202012Validator(schema).is_valid(sample) == (
                contract_error(sample, contract) is None
            )


def test_model_values_never_leak_in_schema_errors():
    from notebooks.qwen.output_schema import contract_error

    for value in [
        {**SQL, "PRIVATE": "PRIVATE"},
        {**PLAN, "entity": "PRIVATE"},
        {**SQL, "interpretation": {"PRIVATE": "PRIVATE"}},
    ]:
        error = contract_error(
            value, "strict_planning" if value.get("kind") == "query" else "legacy_generation"
        )
        assert error and "PRIVATE" not in error
