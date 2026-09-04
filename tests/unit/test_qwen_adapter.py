from types import SimpleNamespace as NS

import httpx
import pytest

from notebooks.qwen.adapter import HFServiceError, QwenClient


class FakeClient:
    def __init__(self, *, result=None, error=None):
        self.result, self.error = result, error
        self.calls = []
        self.configs = []

    def __call__(self, **kwargs):
        self.configs.append(kwargs)
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def chat_completion(self, **kwargs):
        self.calls.append(kwargs)
        if self.error:
            raise self.error
        return self.result


def client(fake):
    return QwenClient(
        model="Qwen/test",
        provider="auto",
        api_key="test-only",
        sql_timeout=11,
        response_timeout=23,
        client_factory=fake,
    )


@pytest.mark.parametrize(
    "content,reason,error",
    [
        (
            '{"kind":"query","entity":"incidents","dimensions":["root"],"metrics":["incident_count"]}',
            "stop",
            None,
        ),
        ('```json\n{"kind":"clarification","question":"Which metric?"}\n```', "stop", None),
        ('{"kind":"query"}', "stop", "invalid_output_schema"),
        (
            '{"kind":"query","entity":"incidents","dimensions":[],"metrics":[],"limit":NaN}',
            "stop",
            "invalid_json",
        ),
        (None, "stop", "empty_content"),
        ("", "length", "output_truncated"),
        (None, "content_filter", "output_blocked"),
        ("not JSON", "stop", "invalid_json"),
        ("[]", "stop", "invalid_json_object"),
        ("x" * 16001, "stop", "output_size_exceeded"),
    ],
)
def test_output_contract(content, reason, error):
    fake = FakeClient(
        result=NS(
            choices=[
                NS(
                    finish_reason=reason,
                    message=NS(content=content, thinking="Never use this as final answer"),
                )
            ]
        )
    )
    result = client(fake).complete_stage([], "sql", 512)
    assert result.output_error == error
    assert result.reported_usage == {"input": None, "output": None}
    assert len(fake.calls) == 1


def test_stage_timeout_and_budget():
    fake = FakeClient(result=NS(choices=[NS(finish_reason="stop", message=NS(content="{}"))]))
    adapter = client(fake)
    adapter.complete_stage([], "sql", 700)
    adapter.complete_stage([], "response", 900)
    assert [c["timeout"] for c in fake.configs] == [11, 23]
    assert [c["max_tokens"] for c in fake.calls] == [700, 900]
    assert all(c["provider"] == "auto" for c in fake.configs)


@pytest.mark.parametrize(
    "status,category",
    [
        (401, "authentication"),
        (403, "authentication"),
        (402, "billing"),
        (429, "quota"),
        (400, "unsupported_model_provider_or_parameters"),
        (503, "service_unavailable"),
    ],
)
def test_service_errors_no_retry_or_sensitive_body(status, category):
    error = httpx.HTTPStatusError(
        "secret upstream body",
        request=httpx.Request("POST", "https://example.test"),
        response=httpx.Response(status),
    )
    fake = FakeClient(error=error)
    with pytest.raises(HFServiceError) as caught:
        client(fake).complete_stage([], "sql", 512)
    assert caught.value.category == category
    assert "secret" not in str(caught.value)
    assert len(fake.calls) == 1


def test_timeout_no_retry():
    fake = FakeClient(error=httpx.ReadTimeout("sensitive endpoint"))
    with pytest.raises(HFServiceError) as caught:
        client(fake).complete_stage([], "response", 512)
    assert caught.value.category == "timeout"
    assert len(fake.calls) == 1


def test_format_incompatibility_uses_next_budgeted_call_not_hidden_retry():
    fake = FakeClient(
        error=httpx.HTTPStatusError(
            "response_format json_schema is not supported",
            request=httpx.Request("POST", "https://example.test"),
            response=httpx.Response(400),
        )
    )
    adapter = client(fake)
    first = adapter.complete_stage([], "sql", 512)
    assert first.output_error == "structured_output_unsupported_use_local_json"
    assert len(fake.calls) == 1
    fake.error = None
    fake.result = NS(
        choices=[
            NS(
                finish_reason="stop",
                message=NS(content='{"kind":"clarification","question":"Which metric?"}'),
            )
        ]
    )
    second = adapter.complete_stage([], "sql", 512)
    assert second.output_error is None
    assert "response_format" in fake.calls[0] and "response_format" not in fake.calls[1]
    assert all(config["provider"] == "auto" for config in fake.configs)
    assert second.metadata["format_status"] == "local_validation_only"


@pytest.mark.parametrize("status", [402, 429, 503])
def test_installed_sdk_sends_one_http_post_without_retry(monkeypatch, status):
    import huggingface_hub.inference._client as sdk

    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, json={"error": "test service failure"})

    with httpx.Client(transport=httpx.MockTransport(handler)) as session:
        monkeypatch.setattr(sdk, "get_session", lambda: session)
        adapter = QwenClient(model="Qwen/test", provider="auto", api_key="hf_offline_test_only")
        with pytest.raises(HFServiceError):
            adapter.complete_stage([{"role": "user", "content": "Count"}], "sql", 512)
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].url.host == "router.huggingface.co"


def test_wire_schema_never_authorizes_raw_sql_in_strict(tmp_path):
    from notebooks.evaluation.fixtures import create_duration_fixture
    from notebooks.shared.analytics import QueryError, Snapshot
    from notebooks.shared.semantic_workflow import SemanticWorkflow

    path = tmp_path / "fixture.db"
    create_duration_fixture(path, "normal")
    fake = FakeClient(
        result=NS(
            choices=[
                NS(finish_reason="stop", message=NS(content='{"kind":"sql","sql":"SELECT 1"}'))
            ]
        )
    )
    flow = SemanticWorkflow(Snapshot(path), client(fake), model_id="Qwen/test", mode="strict")
    with pytest.raises(QueryError, match="exhausted"):
        flow.query("Count incidents")
    assert flow.result is None
    assert flow.metrics["sql_attempts"] == 0
    assert len(fake.calls) == 3
