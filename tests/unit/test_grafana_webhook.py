from __future__ import annotations

import hashlib
import hmac
import http.client
import json
import threading
import time

from self_healthy_kafka.config import GrafanaWebhookConfig
from self_healthy_kafka.webhook.grafana import (
    GrafanaWebhookService,
    parse_grafana_alerts,
)


def _config(**overrides) -> GrafanaWebhookConfig:
    values = {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 0,
        "path": "/webhooks/grafana",
        "auth_mode": "bearer",
        "secret": "test-secret",
        "signature_header": "X-Grafana-Alerting-Signature",
        "timestamp_header": "X-Grafana-Alerting-Timestamp",
        "timestamp_tolerance_seconds": 300,
        "dedupe_ttl_seconds": 600,
        "queue_size": 10,
        "worker_count": 2,
        "recovery_followup_seconds": 1,
        "monitoring_inventory_refresh_seconds": 60,
    }
    values.update(overrides)
    return GrafanaWebhookConfig(**values)


def _payload(status: str = "firing") -> dict:
    return {
        "status": status,
        "alerts": [
            {
                "status": status,
                "labels": {
                    "alertname": "KafkaConnectorFailed",
                    "connector": "CDC",
                },
                "fingerprint": "abc123",
                "startsAt": "2026-06-10T00:00:00Z",
            }
        ],
    }


def test_parse_grafana_alert_accepts_connector_label():
    events = parse_grafana_alerts(_payload())

    assert len(events) == 1
    assert events[0].connector_name == "CDC"
    assert events[0].status == "firing"


def test_parse_grafana_alert_accepts_debezium_server_label():
    payload = _payload()
    payload["alerts"][0]["labels"] = {
        "alertname": "DebeziumDisconnected",
        "server": "CDC",
    }

    events = parse_grafana_alerts(payload)

    assert events[0].connector_name == "CDC"


def test_bearer_webhook_enqueues_connector_and_deduplicates():
    processed = threading.Event()
    calls = []

    def process(connector_name: str, confirmed: bool):
        calls.append((connector_name, confirmed))
        processed.set()
        return None

    service = GrafanaWebhookService(_config(), process)
    service.start()
    try:
        port = service._server.server_port
        body = json.dumps(_payload()).encode()
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
        connection.request(
            "POST",
            "/webhooks/grafana",
            body=body,
            headers={
                "Authorization": "Bearer test-secret",
                "Content-Type": "application/json",
            },
        )
        response = connection.getresponse()
        result = json.loads(response.read())

        assert response.status == 202
        assert result["accepted"] == 1
        assert processed.wait(2)
        assert calls == [("CDC", True)]

        duplicate = service.submit(_payload())
        assert duplicate["duplicate"] == 1
    finally:
        service.close()


def test_firing_webhook_followup_preserves_confirmed_failure():
    processed_twice = threading.Event()
    calls = []

    def process(connector_name: str, confirmed: bool):
        calls.append((connector_name, confirmed))
        if len(calls) == 2:
            processed_twice.set()
        return connector_name if len(calls) == 1 else None

    service = GrafanaWebhookService(_config(), process)
    service.start()
    try:
        assert service.submit(_payload())["accepted"] == 1
        assert processed_twice.wait(2)
        assert calls == [("CDC", True), ("CDC", True)]
    finally:
        service.close()


def test_queue_full_releases_dedupe_claim_for_retry():
    service = GrafanaWebhookService(
        _config(queue_size=1),
        lambda *_: None,
    )
    first = _payload()
    second = _payload()
    second["alerts"][0]["fingerprint"] = "second"
    second["alerts"][0]["startsAt"] = "2026-06-10T00:01:00Z"

    assert service.submit(first)["accepted"] == 1
    assert service.submit(second)["ignored"] == 1

    service._queue.get_nowait()
    service._queue.task_done()

    assert service.submit(second)["accepted"] == 1


def test_multiple_workers_process_different_connectors_in_parallel():
    both_started = threading.Barrier(3)
    release = threading.Event()
    completed = []

    def process(connector_name: str, confirmed: bool):
        both_started.wait(timeout=2)
        release.wait(timeout=2)
        completed.append((connector_name, confirmed))
        return None

    service = GrafanaWebhookService(_config(worker_count=2), process)
    service.start()
    try:
        payload = _payload()
        second_alert = dict(payload["alerts"][0])
        second_alert["labels"] = {
            "alertname": "KafkaConnectorFailed",
            "connector": "CDC.002",
        }
        second_alert["fingerprint"] = "def456"
        payload["alerts"].append(second_alert)

        assert service.submit(payload)["accepted"] == 2
        both_started.wait(timeout=2)
        release.set()

        deadline = time.time() + 2
        while len(completed) < 2 and time.time() < deadline:
            time.sleep(0.01)
        assert sorted(completed) == [("CDC", True), ("CDC.002", True)]
    finally:
        release.set()
        service.close()


def test_webhook_rejects_invalid_bearer_token():
    service = GrafanaWebhookService(_config(), lambda *_: None)
    service.start()
    try:
        port = service._server.server_port
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
        connection.request(
            "POST",
            "/webhooks/grafana",
            body=json.dumps(_payload()),
            headers={"Authorization": "Bearer wrong"},
        )
        response = connection.getresponse()
        response.read()

        assert response.status == 401
    finally:
        service.close()


def test_metrics_exposes_active_and_inactive_connectors():
    service = GrafanaWebhookService(
        _config(),
        lambda *_: None,
        connector_activity=lambda: [
            {
                "connector_name": "CDC",
                "is_active": True,
                "source_server": "oracle_cdc",
            },
            {
                "connector_name": 'CDC."old"',
                "is_active": False,
                "source_server": "oracle_cdc",
            },
        ],
        topic_lag_metrics=lambda: [
            {
                "connector_name": "CDC",
                "topic_name": 'CDC.C__CDCUSER."CUSTOMERS"',
                "last_message_timestamp_seconds": 1781229000.25,
                "is_over_threshold": True,
            },
            {
                "connector_name": "CDC.001",
                "topic_name": "CDC.C__CDCUSER.ORDERS",
                "last_message_timestamp_seconds": 1781229400,
                "is_over_threshold": False,
            },
        ],
    )
    service.start()
    try:
        port = service._server.server_port
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
        connection.request("GET", "/metrics")
        response = connection.getresponse()
        body = response.read().decode()

        assert response.status == 200
        assert response.getheader("Content-Type").startswith("text/plain")
        assert 'kc_shs_connector_active{connector_name="CDC"} 1' in body
        assert 'kc_shs_connector_active{connector_name="CDC.\\"old\\""} 0' in body
        assert (
            'kc_shs_connector_source_server{connector_name="CDC",'
            'source_server="oracle_cdc"} 1'
        ) in body
        assert (
            'kc_shs_connector_source_server{connector_name="CDC.\\"old\\"",'
            'source_server="oracle_cdc"} 0'
        ) in body
        assert (
            'kc_shs_topic_last_message_timestamp_seconds{connector_name="CDC",'
            'topic="CDC.C__CDCUSER.\\"CUSTOMERS\\""} 1781229000.250000'
        ) in body
        assert (
            'kc_shs_topic_last_message_timestamp_seconds{connector_name="CDC.001",'
            'topic="CDC.C__CDCUSER.ORDERS"} 1781229400.000000'
        ) in body
        assert (
            'kc_shs_topic_over_threshold{connector_name="CDC",'
            'topic="CDC.C__CDCUSER.\\"CUSTOMERS\\"",condition="db_state"} 1'
        ) in body
        assert (
            'kc_shs_topic_over_threshold{connector_name="CDC.001",'
            'topic="CDC.C__CDCUSER.ORDERS",condition="db_state"} 0'
        ) in body
        assert "kc_shs_topic_lagging" not in body
        assert "kc_shs_topic_idle_seconds" not in body
        assert "kc_shs_topic_lag_firing_count" not in body
    finally:
        service.close()


def test_bearer_mode_accepts_grafana_custom_token_header():
    service = GrafanaWebhookService(_config(), lambda *_: None)

    assert service.verify_request(
        {"X-SELF-HEALTHY-KAFKA-Token": "test-secret"},
        b"{}",
    ) is True


def test_bearer_mode_accepts_query_token():
    service = GrafanaWebhookService(_config(), lambda *_: None)

    assert service.verify_request(
        {},
        b"{}",
        query_token="test-secret",
    ) is True


def test_hmac_verification_includes_timestamp_and_raw_body():
    config = _config(auth_mode="hmac")
    service = GrafanaWebhookService(config, lambda *_: None)
    body = json.dumps(_payload(), separators=(",", ":")).encode()
    timestamp = str(int(time.time()))
    signature = hmac.new(
        config.secret.encode(),
        timestamp.encode() + b":" + body,
        hashlib.sha256,
    ).hexdigest()

    headers = {
        config.signature_header: signature,
        config.timestamp_header: timestamp,
    }

    assert service.verify_request(headers, body) is True
