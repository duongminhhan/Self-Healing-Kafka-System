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


def _config() -> OllamaChatConfig:
    return OllamaChatConfig(
        enabled=True,
        base_url="http://127.0.0.1:11434",
        model="grounded-model",
        request_timeout_seconds=10,
        think=False,
        max_tokens=256,
        context_log_limit=20,
    )


def test_chat_uses_one_generic_db_retrieval_then_passes_redacted_context_to_llm():
    retrieved_questions = []

    def search_logs(question, limit):
        retrieved_questions.append((question, limit))
        return [{
            "id": "log-1",
            "connector_name": "TOPO-A",
            "event_type": "TASK_FAILED",
            "message": "ORA-01291 missing log file",
            "details": '{"database.password":"hidden","task_id":0}',
            "created_at": datetime(2026, 9, 3, tzinfo=timezone.utc),
        }]

    api = ChatReadApi(
        ChatApiConfig(True, "/api/v1", "test-token", 10, 100),
        queue_lookup=lambda _queue_id, _connector_name: [],
        healing_logs=lambda **_kwargs: [],
        log_search=search_logs,
    )
    client = _OllamaClient([{"message": {"role": "assistant", "content": "Lỗi archive [log-1]."}}])
    service = OllamaChatService(_config(), read_api=api, client=client)

    result = service.ask("TOPO-A task 0 bị lỗi gì?")

    assert retrieved_questions == [("TOPO-A task 0 bị lỗi gì?", 20)]
    assert result["answer"] == "Bằng chứng: [log-1]\nQuan sát: Lỗi archive [log-1]."
    assert result["sources"][0]["count"] == 1
    assert result["sources"][0]["items"][0]["details"]["database.password"] == "[REDACTED]"
    request = client.requests[0][1]["json"]
    assert "tools" not in request
    assert request["messages"][-1] == {
        "role": "assistant",
        "content": "Bằng chứng: [log-1]\nQuan sát: ",
    }
    context = request["messages"][1]["content"]
    assert "TOPO-A task 0 bị lỗi gì?" in context
    assert '"database.password": "[REDACTED]"' in context
    assert '"task_id": 0' in context


def test_chat_retrieves_before_every_question_even_when_no_logs_match():
    requested = []
    api = ChatReadApi(
        ChatApiConfig(True, "/api/v1", "test-token", 10, 100),
        queue_lookup=lambda _queue_id, _connector_name: [],
        healing_logs=lambda **_kwargs: [],
        log_search=lambda question, limit: requested.append((question, limit)) or [],
    )
    client = _OllamaClient([{"message": {"role": "assistant", "content": "Không có log phù hợp."}}])

    result = OllamaChatService(_config(), read_api=api, client=client).ask("vì sao connector chậm?")

    assert requested == [("vì sao connector chậm?", 20)]
    assert result["sources"] == [{
        "source": "ConnectorHealingLogs",
        "query": "vì sao connector chậm?",
        "count": 0,
        "items": [],
    }]


def test_chat_prefixes_retrieved_log_ids_when_the_model_omits_them():
    api = ChatReadApi(
        ChatApiConfig(True, "/api/v1", "test-token", 10, 100),
        queue_lookup=lambda _queue_id, _connector_name: [],
        healing_logs=lambda **_kwargs: [],
        log_search=lambda _question, _limit: [{"id": "log-1", "message": "failed"}],
    )
    client = _OllamaClient([{"message": {"role": "assistant", "content": "Không có trích dẫn."}}])

    result = OllamaChatService(_config(), read_api=api, client=client).ask("lỗi gì?")

    assert result["answer"] == "Bằng chứng: [log-1]\nQuan sát: Không có trích dẫn."
