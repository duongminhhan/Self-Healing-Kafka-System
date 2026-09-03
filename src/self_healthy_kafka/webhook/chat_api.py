from __future__ import annotations

import hmac
import json
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from self_healthy_kafka.config import ChatApiConfig
from self_healthy_kafka.storage.common import json_safe

QUEUE_FIELDS = (
    "id",
    "active_incident_id",
    "root_connector_name",
    "connector_name",
    "connector_class",
    "healing_mode",
    "queue_status",
    "final_outcome",
    "received_at",
    "started_at",
    "completed_at",
    "next_attempt_at",
    "latest_event_type",
    "latest_event_at",
    "latest_message",
    "current_phase",
    "failed_count",
    "task_restart_count",
    "connector_restart_count",
    "recreate_with_offset_count",
    "recreate_with_offset_timeout_count",
    "recreate_without_offset_count",
)

LOG_FIELDS = (
    "id",
    "queue_id",
    "connector_name",
    "event_type",
    "attempt_no",
    "healing_step",
    "severity",
    "message",
    "details",
    "created_at",
)


class ChatReadApi:
    """Read-only tool surface for a chatbot; never accepts arbitrary SQL."""

    def __init__(
        self,
        config: ChatApiConfig,
        *,
        queue_lookup: Callable[[str | None, str | None], list[Any]],
        healing_logs: Callable[..., list[dict[str, Any]]],
    ):
        self._config = config
        self._queue_lookup = queue_lookup
        self._healing_logs = healing_logs

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    @property
    def path_prefix(self) -> str:
        return self._config.path_prefix

    def validate(self) -> None:
        if not self._config.path_prefix.startswith("/"):
            raise ValueError("CHAT_API_PATH_PREFIX must start with '/'")
        if not self._config.token:
            raise ValueError("CHAT_API_TOKEN is required when the chat API is enabled")
        if self._config.default_limit < 1 or self._config.max_limit < 1:
            raise ValueError("CHAT_API_DEFAULT_LIMIT and CHAT_API_MAX_LIMIT must be positive")
        if self._config.default_limit > self._config.max_limit:
            raise ValueError("CHAT_API_DEFAULT_LIMIT cannot exceed CHAT_API_MAX_LIMIT")

    def is_authorized(self, authorization: str) -> bool:
        if not authorization.startswith("Bearer "):
            return False
        return hmac.compare_digest(authorization[7:].strip(), self._config.token)

    def handle_get(self, path: str, query: Mapping[str, list[str]]) -> tuple[int, dict[str, Any]]:
        prefix = self._config.path_prefix.rstrip("/")
        if path == f"{prefix}/incidents":
            return 200, self._list_incidents(query)
        if path.startswith(f"{prefix}/incidents/"):
            queue_id = path.removeprefix(f"{prefix}/incidents/")
            return self._get_incident(queue_id)
        if path == f"{prefix}/healing-logs":
            return 200, self._list_logs(query)
        return 404, {"error": "not found"}

    def _list_incidents(self, query: Mapping[str, list[str]]) -> dict[str, Any]:
        status = _query_value(query, "status", "all").lower()
        if status not in {"all", "open", "completed", "escalated"}:
            raise ValueError("status must be all, open, completed, or escalated")
        limit = self._limit(query)
        connector_name = _query_value(query, "connector_name") or None
        rows = [_queue_json(item) for item in self._queue_lookup(None, connector_name)]
        if status == "open":
            rows = [item for item in rows if item["queue_status"] in {"PENDING", "PROCESSING", "WAITING"}]
        elif status != "all":
            rows = [item for item in rows if item["queue_status"] == status.upper()]
        return {"items": rows[:limit], "count": min(len(rows), limit)}

    def _get_incident(self, queue_id: str) -> tuple[int, dict[str, Any]]:
        if not queue_id:
            return 404, {"error": "not found"}
        rows = self._queue_lookup(queue_id, None)
        if not rows:
            return 404, {"error": "incident not found"}
        incident = _queue_json(rows[0])
        logs = self._healing_logs(
            queue_id=queue_id,
            connector_name=None,
            from_at=None,
            to_at=None,
            limit=self._config.max_limit,
        )
        incident["healing_logs"] = [_log_json(item) for item in logs]
        return 200, incident

    def _list_logs(self, query: Mapping[str, list[str]]) -> dict[str, Any]:
        from_at = _query_datetime(query, "from")
        to_at = _query_datetime(query, "to")
        if from_at and to_at and from_at > to_at:
            raise ValueError("from must be before to")
        rows = self._healing_logs(
            queue_id=_query_value(query, "queue_id") or None,
            connector_name=_query_value(query, "connector_name") or None,
            from_at=from_at,
            to_at=to_at,
            limit=self._limit(query),
        )
        return {"items": [_log_json(item) for item in rows], "count": len(rows)}

    def _limit(self, query: Mapping[str, list[str]]) -> int:
        raw = _query_value(query, "limit")
        if not raw:
            return self._config.default_limit
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError("limit must be an integer") from exc
        if value < 1:
            raise ValueError("limit must be positive")
        return min(value, self._config.max_limit)


def _queue_json(value: Any) -> dict[str, Any]:
    raw = value.to_dict() if hasattr(value, "to_dict") else dict(value)
    return {field: json_safe(raw.get(field)) for field in QUEUE_FIELDS}


def _log_json(value: Mapping[str, Any]) -> dict[str, Any]:
    return {field: _redact(json_safe(value.get(field))) for field in LOG_FIELDS}


def _redact(value: Any) -> Any:
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return value
        return _redact(decoded)
    if isinstance(value, list):
        return [_redact(item) for item in value]
    if isinstance(value, dict):
        return {
            key: "[REDACTED]" if _sensitive_key(key) else _redact(item)
            for key, item in value.items()
        }
    return value


def _sensitive_key(key: str) -> bool:
    normalized = key.lower()
    return any(
        token in normalized
        for token in ("password", "credential", "database.url", "bootstrap.servers", "token", "secret")
    )


def _query_value(query: Mapping[str, list[str]], key: str, default: str = "") -> str:
    values = query.get(key) or []
    return str(values[0]).strip() if values else default


def _query_datetime(query: Mapping[str, list[str]], key: str) -> datetime | None:
    value = _query_value(query, key)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{key} must be an ISO-8601 timestamp with an offset") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{key} must be an ISO-8601 timestamp with an offset")
    return parsed
