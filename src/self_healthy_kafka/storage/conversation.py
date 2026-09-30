"""Redis storage for bounded, verified conversation state."""

from __future__ import annotations

import json
from typing import Any, Protocol


class ConversationStoreError(RuntimeError):
    """Conversation state could not be read or written safely."""


class ConversationStore(Protocol):
    def get(self, conversation_id: str) -> dict[str, Any] | None: ...

    def set(self, conversation_id: str, value: dict[str, Any]) -> None: ...

    def delete(self, conversation_id: str) -> None: ...

    def close(self) -> None: ...


class RedisConversationStore:
    _MAX_PAYLOAD_BYTES = 64 * 1024

    def __init__(
        self,
        url: str,
        *,
        ttl_seconds: int,
        client: Any | None = None,
        key_prefix: str = "self-healthy-kafka:conversation",
    ) -> None:
        if client is None:
            from redis import Redis

            client = Redis.from_url(
                url,
                decode_responses=False,
                socket_connect_timeout=3,
                socket_timeout=3,
            )
        self._client = client
        self._ttl_seconds = ttl_seconds
        self._key_prefix = key_prefix

    def get(self, conversation_id: str) -> dict[str, Any] | None:
        try:
            raw = self._client.get(self._key(conversation_id))
        except Exception as exc:
            raise ConversationStoreError("conversation store is unavailable") from exc
        if raw is None:
            return None
        if isinstance(raw, str):
            raw = raw.encode("utf-8")
        if not isinstance(raw, bytes) or len(raw) > self._MAX_PAYLOAD_BYTES:
            raise ConversationStoreError("conversation state is invalid")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ConversationStoreError("conversation state is invalid") from exc
        if not isinstance(value, dict):
            raise ConversationStoreError("conversation state is invalid")
        return value

    def set(self, conversation_id: str, value: dict[str, Any]) -> None:
        try:
            payload = json.dumps(
                value,
                ensure_ascii=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ConversationStoreError("conversation state is invalid") from exc
        if len(payload) > self._MAX_PAYLOAD_BYTES:
            raise ConversationStoreError("conversation state is too large")
        try:
            self._client.setex(self._key(conversation_id), self._ttl_seconds, payload)
        except Exception as exc:
            raise ConversationStoreError("conversation store is unavailable") from exc

    def delete(self, conversation_id: str) -> None:
        try:
            self._client.delete(self._key(conversation_id))
        except Exception as exc:
            raise ConversationStoreError("conversation store is unavailable") from exc

    def close(self) -> None:
        close = getattr(self._client, "close", None)
        if callable(close):
            close()

    def _key(self, conversation_id: str) -> str:
        return f"{self._key_prefix}:{conversation_id}"
