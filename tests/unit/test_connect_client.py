import json
import logging
from unittest.mock import patch

import httpx

from self_healthy_kafka.connect.client import (
    KafkaConnectCircuitOpen,
    KafkaConnectClient,
)


def test_client_can_disable_tls_certificate_verification():
    with patch("self_healthy_kafka.connect.client.httpx.Client") as client_class:
        KafkaConnectClient(
            base_url="https://kc:8083",
            timeout=1.0,
            tls_verify=False,
        )

    client_class.assert_called_once_with(
        timeout=1.0,
        verify=False,
        headers={"Content-Type": "application/json"},
    )


def _client_with(handler):
    transport = httpx.MockTransport(handler)
    client = KafkaConnectClient(base_url="http://kc:8083", timeout=1.0)
    client._client = httpx.Client(transport=transport, headers={"Content-Type": "application/json"})
    return client


def test_get_offsets_returns_payload_on_200():
    payload = {"offsets": [{"partition": {"p": 0}, "offset": {"o": 10}}]}

    def handler(request):
        assert request.url.path == "/connectors/conn-x/offsets"
        return httpx.Response(200, json=payload)

    client = _client_with(handler)
    assert client.get_offsets("conn-x") == payload


def test_get_offsets_returns_none_on_404():
    def handler(request):
        return httpx.Response(404, json={"message": "not found"})

    client = _client_with(handler)
    assert client.get_offsets("conn-x") is None


def test_get_offsets_returns_none_on_405_older_cluster():
    def handler(request):
        return httpx.Response(405)

    client = _client_with(handler)
    assert client.get_offsets("conn-x") is None


def test_patch_offsets_returns_true_on_success():
    def handler(request):
        assert request.method == "PATCH"
        assert request.url.path == "/connectors/conn-x/offsets"
        return httpx.Response(200, json={"message": "ok"})

    client = _client_with(handler)
    assert client.patch_offsets("conn-x", {"offsets": []}) is True


def test_patch_offsets_returns_false_on_405():
    def handler(request):
        return httpx.Response(405)

    client = _client_with(handler)
    assert client.patch_offsets("conn-x", {"offsets": []}) is False


def test_stop_connector_treats_missing_connector_as_idempotent_success():
    def handler(request):
        assert request.url.path == "/connectors/conn-x/stop"
        return httpx.Response(404)

    client = _client_with(handler)

    assert client.stop_connector("conn-x") is True


def test_get_status_returns_none_on_404():
    def handler(request):
        return httpx.Response(404)

    client = _client_with(handler)
    assert client.get_status("ghost") is None


def test_get_status_parses_payload():
    def handler(request):
        return httpx.Response(200, json={
            "name": "src-a",
            "type": "source",
            "connector": {"state": "RUNNING", "worker_id": "w1"},
            "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "w1"}],
        })

    client = _client_with(handler)
    status = client.get_status("src-a")
    assert status.name == "src-a"
    assert status.state.value == "RUNNING"


def test_status_circuit_short_circuits_after_connection_error():
    requests = []

    def handler(request):
        requests.append(request)
        raise httpx.ConnectError("worker unavailable", request=request)

    client = _client_with(handler)

    try:
        client.get_status("src-a")
    except httpx.ConnectError:
        pass
    else:
        raise AssertionError("expected ConnectError")

    try:
        client.get_status("src-b")
    except KafkaConnectCircuitOpen:
        pass
    else:
        raise AssertionError("expected KafkaConnectCircuitOpen")

    assert len(requests) == 1


def test_restart_connector_uses_include_tasks_and_only_failed_flag():
    seen = []

    def handler(request):
        seen.append((request.url.path, dict(request.url.params)))
        return httpx.Response(202)

    client = _client_with(handler)
    client.restart_connector("conn-x")
    client.restart_connector("conn-x", only_failed=False)

    assert seen == [
        (
            "/connectors/conn-x/restart",
            {"includeTasks": "true", "onlyFailed": "true"},
        ),
        (
            "/connectors/conn-x/restart",
            {"includeTasks": "true", "onlyFailed": "false"},
        ),
    ]


def test_create_connector_can_request_initial_stopped_state():
    seen = {}
    config = {"connector.class": "X", "topic.prefix": "oracle_cdc"}

    def handler(request):
        seen["method"] = request.method
        seen["path"] = request.url.path
        seen["json"] = json.loads(request.read())
        return httpx.Response(201, json={"name": "conn-x", "config": config})

    client = _client_with(handler)
    result = client.create_connector("conn-x", config, initial_state="STOPPED")

    assert seen["method"] == "POST"
    assert seen["path"] == "/connectors"
    assert seen["json"] == {
        "name": "conn-x",
        "config": config,
        "initial_state": "STOPPED",
    }
    assert result == {"name": "conn-x", "config": config}


def test_kafka_connect_request_log_includes_latency_and_status(caplog):
    def handler(request):
        return httpx.Response(200, json=[])

    client = _client_with(handler)

    with caplog.at_level(logging.INFO):
        client.list_connectors()

    request_log = next(
        record for record in caplog.records
        if getattr(record, "event", None) == "kafka_connect_request"
    )
    assert request_log.method == "GET"
    assert request_log.path == "/connectors"
    assert request_log.status_code == 200
    assert request_log.outcome == "ok"
    assert request_log.latency_ms >= 0
