from __future__ import annotations

from datetime import datetime, timezone

from self_healthy_kafka.config import ChatApiConfig, OllamaChatConfig
from self_healthy_kafka.webhook.chat_api import ChatReadApi
from self_healthy_kafka.webhook.ollama_chat import OllamaChatService


class _Response:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _OllamaClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    def post(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return _Response(self.responses.pop(0))


def _read_api() -> ChatReadApi:
    return ChatReadApi(
        ChatApiConfig(
            enabled=True,
            path_prefix="/api/v1",
            token="test-token",
            default_limit=10,
            max_limit=100,
        ),
        queue_lookup=lambda _queue_id, _connector_name: [],
        healing_logs=lambda **_kwargs: [],
    )


def test_chat_routes_failure_ranking_to_sql_without_hardcoded_connectors():
    requested = []
    client = _OllamaClient(
        [
            {"message": {"role": "assistant", "content": "not used"}},
        ]
    )

    def failure_ranking(*, from_at, to_at, limit):
        requested.append((from_at, to_at, limit))
        return [
            {
                "root_connector_name": "TOPO-A",
                "failure_incident_count": 7,
                "last_failure_at": datetime(2026, 8, 28, tzinfo=timezone.utc),
            }
        ]

    service = OllamaChatService(
        OllamaChatConfig(
            enabled=True,
            base_url="http://127.0.0.1:11434",
            model="tool-model",
            request_timeout_seconds=10,
            max_tool_rounds=3,
            think=False,
            max_tokens=256,
        ),
        read_api=_read_api(),
        failure_ranking=failure_ranking,
        client=client,
    )

    result = service.ask("liệt kê top connector chết nhiều nhất")

    assert requested == [(None, None, 10)]
    assert result["answer"] == (
        "Top connector có nhiều incident lỗi nhất:\n"
        "1. TOPO-A: 7 incident lỗi (đang mở: 0)."
    )
    assert result["sources"][0]["tool"] == "get_connector_failure_ranking"
    assert result["sources"][0]["result"]["items"][0]["failure_incident_count"] == 7
    assert client.requests == []


def test_chat_scopes_today_failure_ranking_to_vietnam_day():
    requested = []
    service = OllamaChatService(
        OllamaChatConfig(
            enabled=True,
            base_url="http://127.0.0.1:11434",
            model="tool-model",
            request_timeout_seconds=10,
            max_tool_rounds=3,
            think=False,
            max_tokens=256,
        ),
        read_api=_read_api(),
        failure_ranking=lambda **kwargs: requested.append(kwargs) or [],
        client=_OllamaClient([]),
    )

    result = service.ask("hôm nay top connector lỗi nhiều nhất")

    assert result["sources"][0]["tool"] == "get_connector_failure_ranking"
    assert requested[0]["from_at"].hour == 0
    assert requested[0]["from_at"].minute == 0
    assert requested[0]["to_at"] >= requested[0]["from_at"]
