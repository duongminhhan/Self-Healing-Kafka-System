from __future__ import annotations

import hashlib
import hmac
import threading
import time
from collections.abc import Mapping

from self_healthy_kafka.config import GrafanaWebhookConfig


class WebhookAuthenticator:
    def __init__(self, config: GrafanaWebhookConfig):
        self._config = config

    def verify(
        self,
        headers: Mapping[str, str],
        body: bytes,
        *,
        query_token: str = "",
    ) -> bool:
        mode = self._config.auth_mode.strip().lower()
        if mode == "bearer":
            return any(
                (
                    hmac.compare_digest(
                        headers.get("Authorization", ""),
                        f"Bearer {self._config.secret}",
                    ),
                    hmac.compare_digest(
                        headers.get("X-SELF-HEALTHY-KAFKA-Token", ""),
                        self._config.secret,
                    ),
                    hmac.compare_digest(query_token, self._config.secret),
                )
            )
        if mode != "hmac":
            return False
        timestamp = headers.get(self._config.timestamp_header, "")
        try:
            timestamp_value = int(timestamp)
        except (TypeError, ValueError):
            return False
        if abs(int(time.time()) - timestamp_value) > (
            self._config.timestamp_tolerance_seconds
        ):
            return False
        expected = hmac.new(
            self._config.secret.encode("utf-8"),
            timestamp.encode("utf-8") + b":" + body,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(
            headers.get(self._config.signature_header, ""),
            expected,
        )


class EventDeduplicator:
    def __init__(self, ttl_seconds: int):
        self._ttl_seconds = ttl_seconds
        self._claims: dict[str, float] = {}
        self._lock = threading.Lock()

    def claim(self, key: str) -> bool:
        now = time.monotonic()
        with self._lock:
            expired = [
                candidate
                for candidate, claimed_at in self._claims.items()
                if now - claimed_at >= self._ttl_seconds
            ]
            for candidate in expired:
                del self._claims[candidate]
            if key in self._claims:
                return False
            self._claims[key] = now
            return True

    def release(self, key: str) -> None:
        with self._lock:
            self._claims.pop(key, None)

