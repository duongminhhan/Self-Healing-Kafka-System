from __future__ import annotations

from datetime import datetime, timezone

import pytest

from self_healthy_kafka.config import ChatApiConfig
from self_healthy_kafka.webhook.chat_api import ChatReadApi


def _config(**overrides) -> ChatApiConfig:
    values = {
        "enabled": True,
        "path_prefix": "/api/v1",
        "token": "chat-test-token",
        "default_limit": 10,
        "max_limit": 50,
    }
    values.update(overrides)
    return ChatApiConfig(**values)


def _api():
    queues = [{
        "id": "queue-1",
        "active_incident_id": "queue-1",
        "root_connector_name": "TEST-TOPO-CLI-G042",
        "connector_name": "TOPO-CLI-G042.009",
        "connector_class": "io.debezium.connector.oracle.OracleConnector",
        "healing_mode": "RECOVERY",
        "queue_status": "COMPLETED",
        "final_outcome": "RECOVERED",
        "received_at": datetime(2026, 8, 11, tzinfo=timezone.utc),
        "latest_event_type": "HEALING_RECOVERED",
        "failed_count": 1,
    }]
    logs = [{
        "id": "log-1",
        "queue_id": "queue-1",
        "connector_name": "TOPO-CLI-G042.009",
        "event_type": "TASK_RESTART",
        "severity": "WARNING",
        "details": '{"database.password":"do-not-return","reason":"FAILED"}',
        "created_at": datetime(2026, 8, 11, tzinfo=timezone.utc),
    }]

    def queue_lookup(queue_id, connector_name):
        return [
            item
            for item in queues
            if (not queue_id or item["id"] == queue_id)
            and (not connector_name or item["connector_name"] == connector_name)
        ]

    return ChatReadApi(_config(), queue_lookup=queue_lookup, healing_logs=lambda **_: logs)


def test_chat_api_requires_exact_bearer_token():
    api = _api()

    assert api.is_authorized("Bearer chat-test-token") is True
    assert api.is_authorized("Bearer wrong") is False
    assert api.is_authorized("") is False


def test_chat_api_lists_completed_incidents():
    status, payload = _api().handle_get("/api/v1/incidents", {"status": ["completed"]})

    assert status == 200
    assert payload["count"] == 1
    assert payload["items"][0]["connector_name"] == "TOPO-CLI-G042.009"
    assert payload["items"][0]["queue_status"] == "COMPLETED"


def test_chat_api_redacts_sensitive_log_details():
    status, payload = _api().handle_get("/api/v1/healing-logs", {})

    assert status == 200
    assert payload["items"][0]["details"]["database.password"] == "[REDACTED]"
    assert payload["items"][0]["details"]["reason"] == "FAILED"


def test_chat_api_rejects_invalid_time_range():
    with pytest.raises(ValueError, match="ISO-8601"):
        _api().handle_get("/api/v1/healing-logs", {"from": ["2026-08-11"]})
