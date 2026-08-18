import hashlib
import hmac
import time

from self_healthy_kafka.config import GrafanaWebhookConfig
from self_healthy_kafka.webhook.metrics import render_prometheus_metrics
from self_healthy_kafka.webhook.security import EventDeduplicator, WebhookAuthenticator


def _config(**overrides):
    values = {
        "enabled": True,
        "host": "127.0.0.1",
        "port": 0,
        "path": "/webhooks/grafana",
        "auth_mode": "bearer",
        "secret": "secret",
        "signature_header": "X-Signature",
        "timestamp_header": "X-Timestamp",
        "timestamp_tolerance_seconds": 300,
        "dedupe_ttl_seconds": 60,
        "queue_size": 10,
        "worker_count": 2,
        "recovery_followup_seconds": 1,
    }
    values.update(overrides)
    return GrafanaWebhookConfig(**values)


def test_event_deduplicator_can_release_failed_queue_claim():
    dedupe = EventDeduplicator(60)

    assert dedupe.claim("event") is True
    assert dedupe.claim("event") is False
    dedupe.release("event")
    assert dedupe.claim("event") is True


def test_hmac_authenticator_checks_timestamp_and_body():
    config = _config(auth_mode="hmac")
    timestamp = str(int(time.time()))
    body = b'{"status":"firing"}'
    signature = hmac.new(
        config.secret.encode(),
        timestamp.encode() + b":" + body,
        hashlib.sha256,
    ).hexdigest()

    assert WebhookAuthenticator(config).verify(
        {
            config.timestamp_header: timestamp,
            config.signature_header: signature,
        },
        body,
    )


def test_metrics_renderer_escapes_labels_and_preserves_topic_mapping():
    rendered = render_prometheus_metrics(
        [{"connector_name": 'CDC"1', "source_server": "CDC", "is_active": True}],
        [{
            "connector_name": 'CDC"1',
            "topic_name": "CDC.CUSTOMERS",
            "last_message_timestamp_seconds": 123.5,
            "is_over_threshold": True,
        }],
        {
            "connectors": [{
                "connector_name": "CDC.002",
                "configured_connector_name": "CDC.001",
                "source_server": "CDC",
                "state": "RUNNING",
                "inventory_status": "REPLACEMENT",
                "configured": False,
                "present": True,
                "managed": True,
                "is_active": True,
            }],
            "topics": [{
                "connector_name": "CDC.002",
                "configured_connector_name": "",
                "source_server": "CDC",
                "topic_name": "CDC.ORDERS",
                "inventory_status": "UNCONFIGURED",
                "configured": False,
                "present": True,
            }],
        },
    ).decode()

    assert 'connector_name="CDC\\"1"' in rendered
    assert 'topic="CDC.CUSTOMERS"} 123.500000' in rendered
    assert (
        'kc_shs_topic_active{connector_name="CDC\\"1",'
        'topic="CDC.CUSTOMERS",condition="db_state"} 1'
    ) in rendered
    assert (
        'kc_shs_topic_over_threshold{connector_name="CDC\\"1",'
        'topic="CDC.CUSTOMERS",condition="db_state"} 1'
    ) in rendered
    assert (
        'kc_shs_connector_inventory{connector_name="CDC.002",'
        'configured_connector_name="CDC.001",source_server="CDC",'
        'state="RUNNING",inventory_status="REPLACEMENT",configured="false",'
        'present="true",managed="true",'
        'active="true"} 1'
    ) in rendered
    assert (
        'kc_shs_topic_inventory{connector_name="CDC.002",'
        'configured_connector_name="",source_server="CDC",topic="CDC.ORDERS",'
        'inventory_status="UNCONFIGURED",configured="false",present="true"} 1'
    ) in rendered
