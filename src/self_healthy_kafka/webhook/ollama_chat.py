from __future__ import annotations

import json
from typing import Any

import httpx

from self_healthy_kafka.config import OllamaChatConfig
from self_healthy_kafka.webhook.chat_api import ChatReadApi

SYSTEM_PROMPT = """Bạn là trợ lý vận hành Kafka Connect. BẮT BUỘC trả lời bằng tiếng Việt.
Chỉ được dùng dữ kiện xuất hiện trong JSON ConnectorHealingLogs được đưa ở tin nhắn
người dùng. Không dùng kiến thức bên ngoài, không giải thích nguyên nhân “thường là”,
không dự đoán, và không bịa connector, task, worker, lỗi, thời gian, số lượng hoặc kết
quả phục hồi. Không tiết lộ secret hay cấu hình.

Nếu có log: trả lời tối đa 150 từ theo đúng ba dòng:
Bằng chứng: [Id log liên quan]
Quan sát: các dữ kiện có trong log.
Kết luận: chỉ kết luận trực tiếp từ các dữ kiện đó.
Mỗi câu trả lời phải chứa ít nhất một Id log có trong JSON.
Nếu không có log phù hợp: chỉ trả lời "Không truy xuất được log Kafka Connect đã lưu
phù hợp với câu hỏi."""


class OllamaChatService:
    """One grounded pipeline: retrieve persisted logs, then ask the LLM with that context."""

    def __init__(
        self,
        config: OllamaChatConfig,
        *,
        read_api: ChatReadApi,
        client: httpx.Client | None = None,
        **_ignored: Any,
    ):
        self._config = config
        self._read_api = read_api
        self._client = client or httpx.Client()

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    @property
    def path(self) -> str:
        return f"{self._read_api.path_prefix.rstrip('/')}/chat"

    def validate(self) -> None:
        if not self._config.base_url.startswith(("http://", "https://")):
            raise ValueError("OLLAMA_BASE_URL must be an HTTP URL")
        if not self._config.model:
            raise ValueError("OLLAMA_MODEL is required when Ollama chat is enabled")
        if self._config.request_timeout_seconds <= 0:
            raise ValueError("OLLAMA_REQUEST_TIMEOUT_SECONDS must be positive")
        if self._config.max_tokens < 1:
            raise ValueError("OLLAMA_MAX_TOKENS must be positive")
        if not 1 <= self._config.context_log_limit <= self._read_api.max_limit:
            raise ValueError("OLLAMA_CONTEXT_LOG_LIMIT must be between 1 and CHAT_API_MAX_LIMIT")

    def ask(self, question: str) -> dict[str, Any]:
        question = question.strip()
        if not question:
            raise ValueError("question is required")
        if len(question) > 4_000:
            raise ValueError("question must not exceed 4000 characters")

        logs = self._read_api.retrieve_log_context(
            question, self._config.context_log_limit
        )
        source = {
            "source": "ConnectorHealingLogs",
            "query": question,
            "count": len(logs),
            "items": logs,
        }
        evidence_prefix = _evidence_prefix(logs)
        message = self._chat(
            [
                {"role": "system", "content": SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Câu hỏi gốc:\n{question}\n\n"
                        "Ngữ cảnh log đã truy xuất từ DB (JSON, đã redaction):\n"
                        f"{json.dumps(source, ensure_ascii=False, default=str)}"
                    ),
                },
                *(
                    [{"role": "assistant", "content": evidence_prefix}]
                    if evidence_prefix
                    else []
                ),
            ]
        )
        answer = _strip_thinking(str(message.get("content") or "")).strip()
        if not answer:
            raise RuntimeError("Ollama response did not contain answer content")
        if evidence_prefix:
            answer = f"{evidence_prefix}{answer}"
        if logs and not any(str(log["id"]) in answer for log in logs):
            raise RuntimeError("Ollama answer did not cite retrieved log evidence")
        return {"answer": answer, "sources": [source]}

    def _chat(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        response = self._client.post(
            f"{self._config.base_url.rstrip('/')}/api/chat",
            json={
                "model": self._config.model,
                "stream": False,
                "think": self._config.think,
                "messages": messages,
                "options": {"num_predict": self._config.max_tokens},
            },
            timeout=self._config.request_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        message = payload.get("message")
        if not isinstance(message, dict):
            raise RuntimeError("Ollama response did not contain a message")
        return message


def _evidence_prefix(logs: list[dict[str, Any]]) -> str:
    if not logs:
        return ""
    ids = ", ".join(f"[{log['id']}]" for log in logs[:3])
    return f"Bằng chứng: {ids}\nQuan sát: "


def _strip_thinking(content: str) -> str:
    return content.split("<think>", maxsplit=1)[0]
