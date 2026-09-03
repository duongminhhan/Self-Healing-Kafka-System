from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from self_healthy_kafka.config import OllamaChatConfig
from self_healthy_kafka.storage.common import json_safe
from self_healthy_kafka.webhook.chat_api import ChatReadApi

SYSTEM_PROMPT = (
    "You are a Kafka Connect operations assistant. Answer in Vietnamese. "
    "For operational facts, counts, incidents, connector failures, and healing history, "
    "you must call a tool first. Never invent values. Explain the metric briefly and "
    "state the time range when the tool result provides one. The failure ranking counts "
    "queue incidents grouped by RootConnectorName, not Kafka Connect REST status polls."
)

VIETNAM_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")


class OllamaChatService:
    """Turns natural-language questions into read-only, SQL-backed tool calls."""

    def __init__(
        self,
        config: OllamaChatConfig,
        *,
        read_api: ChatReadApi,
        failure_ranking: Callable[..., list[dict[str, Any]]],
        client: httpx.Client | None = None,
    ):
        self._config = config
        self._read_api = read_api
        self._failure_ranking = failure_ranking
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
        if self._config.max_tool_rounds < 1:
            raise ValueError("OLLAMA_MAX_TOOL_ROUNDS must be at least 1")
        if self._config.max_tokens < 1:
            raise ValueError("OLLAMA_MAX_TOKENS must be positive")

    def ask(self, question: str) -> dict[str, Any]:
        question = question.strip()
        if not question:
            raise ValueError("question is required")
        if len(question) > 4_000:
            raise ValueError("question must not exceed 4000 characters")

        direct_source = self._direct_source_for_question(question)
        if direct_source is not None:
            return {
                "answer": _authoritative_answer("", [direct_source]),
                "sources": [direct_source],
            }

        messages: list[dict[str, Any]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ]
        sources: list[dict[str, Any]] = []
        for _ in range(self._config.max_tool_rounds):
            message = self._chat(messages)
            tool_calls = message.get("tool_calls") or []
            if not tool_calls:
                return {
                    "answer": _authoritative_answer(
                        str(message.get("content") or ""),
                        sources,
                    ),
                    "sources": sources,
                }
            messages.append(message)
            for call in tool_calls:
                function = call.get("function") or {}
                name = str(function.get("name") or "")
                arguments = function.get("arguments") or {}
                if not isinstance(arguments, dict):
                    arguments = {}
                result = self._call_tool(name, arguments)
                sources.append({"tool": name, "result": result})
                messages.append(
                    {
                        "role": "tool",
                        "tool_name": name,
                        "content": json.dumps(json_safe(result), ensure_ascii=False),
                    }
                )
        raise RuntimeError("Ollama exceeded the configured tool-call limit")

    def _direct_source_for_question(self, question: str) -> dict[str, Any] | None:
        """Route routine operational questions to an authoritative read tool.

        Routing selects only the data source. Counts and states are still read
        from SQL Server, so no operational value is embedded in application code.
        """
        normalized = question.casefold()
        connector_name = _connector_name_from_question(question)
        if _is_failure_ranking_question(normalized):
            arguments: dict[str, Any] = {"limit": 10}
            arguments.update(_question_time_window(normalized))
            return {
                "tool": "get_connector_failure_ranking",
                "result": self._call_tool("get_connector_failure_ranking", arguments),
            }
        if any(token in normalized for token in ("leo thang", "escalat", "vượt cấp")):
            return {
                "tool": "list_incidents",
                "result": self._call_tool("list_incidents", {"status": "escalated"}),
            }
        if any(token in normalized for token in ("đã phục hồi", "đã khôi phục", "recovered", "completed")):
            return {
                "tool": "list_incidents",
                "result": self._call_tool("list_incidents", {"status": "completed"}),
            }
        if any(token in normalized for token in ("log", "lịch sử", "history", "bước xử lý")):
            arguments = {"connector_name": connector_name} if connector_name else {}
            return {
                "tool": "list_healing_logs",
                "result": self._call_tool("list_healing_logs", arguments),
            }
        if connector_name or any(
            token in normalized
            for token in ("đang lỗi", "chưa phục hồi", "chưa khôi phục", "cần xử lý", "hàng đợi", "queue", "pending")
        ):
            arguments = {"status": "open"}
            if connector_name:
                arguments["connector_name"] = connector_name
            return {"tool": "list_incidents", "result": self._call_tool("list_incidents", arguments)}
        return None

    def _chat(self, messages: list[dict[str, Any]]) -> dict[str, Any]:
        response = self._client.post(
            f"{self._config.base_url.rstrip('/')}/api/chat",
            json={
                "model": self._config.model,
                "stream": False,
                "think": self._config.think,
                "messages": messages,
                "tools": _tool_definitions(),
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

    def _call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if name == "get_connector_failure_ranking":
            from_at = _optional_datetime(arguments.get("from"), "from")
            to_at = _optional_datetime(arguments.get("to"), "to")
            if from_at and to_at and from_at > to_at:
                raise ValueError("from must be before to")
            limit = _bounded_limit(arguments.get("limit"), default=10, maximum=100)
            rows = self._failure_ranking(from_at=from_at, to_at=to_at, limit=limit)
            return {
                "metric": "failure_incident_count",
                "definition": "Number of ConnectorHealingQueue incidents grouped by RootConnectorName",
                "from": from_at.isoformat() if from_at else None,
                "to": to_at.isoformat() if to_at else None,
                "items": [json_safe(row) for row in rows],
            }
        if name == "list_incidents":
            return self._read_tool("/incidents", arguments)
        if name == "get_incident":
            queue_id = str(arguments.get("queue_id") or "").strip()
            if not queue_id:
                raise ValueError("queue_id is required")
            return self._read_tool(f"/incidents/{queue_id}", {})
        if name == "list_healing_logs":
            return self._read_tool("/healing-logs", arguments)
        raise ValueError(f"unsupported tool: {name}")

    def _read_tool(self, suffix: str, arguments: dict[str, Any]) -> dict[str, Any]:
        query = {
            key: [str(value)]
            for key, value in arguments.items()
            if value is not None
            and key in {"status", "connector_name", "queue_id", "from", "to", "limit"}
        }
        status, payload = self._read_api.handle_get(
            f"{self._read_api.path_prefix.rstrip('/')}{suffix}",
            query,
        )
        if status != 200:
            raise ValueError(str(payload.get("error") or "tool request failed"))
        return payload


def _optional_datetime(value: Any, name: str) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 timestamp with an offset") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{name} must be an ISO-8601 timestamp with an offset")
    return parsed


def _bounded_limit(value: Any, *, default: int, maximum: int) -> int:
    if value in (None, ""):
        return default
    try:
        limit = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("limit must be an integer") from exc
    if limit < 1:
        raise ValueError("limit must be positive")
    return min(limit, maximum)


def _is_failure_ranking_question(question: str) -> bool:
    ranking_words = ("top", "nhiều nhất", "cao nhất", "xếp hạng", "ranking")
    failure_words = ("chết", "lỗi", "fail", "failed", "incident", "sự cố")
    return any(word in question for word in ranking_words) and any(
        word in question for word in failure_words
    )


def _connector_name_from_question(question: str) -> str | None:
    match = re.search(r"\b[A-Za-z0-9]+(?:[-_][A-Za-z0-9]+)+(?:\.\d+)?\b", question)
    return match.group(0) if match else None


def _question_time_window(question: str) -> dict[str, str]:
    now = datetime.now(VIETNAM_TIMEZONE)
    if any(token in question for token in ("hôm nay", "today", "đầu ngày")):
        return {
            "from": datetime.combine(now.date(), time.min, tzinfo=VIETNAM_TIMEZONE).isoformat(),
            "to": now.isoformat(),
        }
    if any(token in question for token in ("24 giờ", "24h", "last 24")):
        return {"from": (now - timedelta(hours=24)).isoformat(), "to": now.isoformat()}
    return {}


def _tool_definitions() -> list[dict[str, Any]]:
    ranking = {
        "type": "function",
        "function": {
            "name": "get_connector_failure_ranking",
            "description": "Get exact connector failure incident ranking from SQL Server.",
            "parameters": {
                "type": "object",
                "properties": {
                    "from": {"type": "string", "description": "ISO-8601 timestamp with offset"},
                    "to": {"type": "string", "description": "ISO-8601 timestamp with offset"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 100},
                },
            },
        },
    }
    incidents = {
        "type": "function",
        "function": {
            "name": "list_incidents",
            "description": "List queue incidents and their latest healing state.",
            "parameters": {"type": "object", "properties": {"status": {"type": "string"}, "connector_name": {"type": "string"}, "limit": {"type": "integer"}}},
        },
    }
    incident = {
        "type": "function",
        "function": {
            "name": "get_incident",
            "description": "Get one queue incident with its healing logs.",
            "parameters": {"type": "object", "properties": {"queue_id": {"type": "string"}}, "required": ["queue_id"]},
        },
    }
    logs = {
        "type": "function",
        "function": {
            "name": "list_healing_logs",
            "description": "List healing logs, optionally filtered by connector or queue.",
            "parameters": {"type": "object", "properties": {"queue_id": {"type": "string"}, "connector_name": {"type": "string"}, "from": {"type": "string"}, "to": {"type": "string"}, "limit": {"type": "integer"}}},
        },
    }
    return [ranking, incidents, incident, logs]


def _authoritative_answer(model_answer: str, sources: list[dict[str, Any]]) -> str:
    """Keep the failure-ranking numbers tied to the SQL tool result, not model prose."""
    for source in reversed(sources):
        if source["tool"] != "get_connector_failure_ranking":
            continue
        result = source["result"]
        items = result["items"]
        if not items:
            return "Không có incident connector nào trong khoảng thời gian được yêu cầu."
        lines = ["Top connector có nhiều incident lỗi nhất:"]
        for index, item in enumerate(items, start=1):
            root = item.get("root_connector_name") or item.get("RootConnectorName")
            count = item.get("failure_incident_count") or item.get("FailureIncidentCount") or 0
            open_count = item.get("open_incident_count") or item.get("OpenIncidentCount") or 0
            lines.append(
                f"{index}. {root}: {count} incident lỗi (đang mở: {open_count})."
            )
        return "\n".join(lines)
    for source in reversed(sources):
        result = source["result"]
        items = result.get("items", [])
        if source["tool"] == "list_incidents":
            if not items:
                return "Không có incident phù hợp với điều kiện được yêu cầu."
            lines = [f"Có {result.get('count', len(items))} incident phù hợp:"]
            for index, item in enumerate(items, start=1):
                name = item.get("root_connector_name") or item.get("connector_name")
                outcome = item.get("final_outcome") or "chưa có"
                lines.append(f"{index}. {name}: {item.get('queue_status')} (kết quả: {outcome}).")
            return "\n".join(lines)
        if source["tool"] == "list_healing_logs":
            if not items:
                return "Không có healing log phù hợp với điều kiện được yêu cầu."
            lines = [f"Có {result.get('count', len(items))} healing log phù hợp:"]
            for index, item in enumerate(items, start=1):
                lines.append(
                    f"{index}. {item.get('connector_name')}: {item.get('event_type')} - "
                    f"{item.get('message')}"
                )
            return "\n".join(lines)
    return model_answer
