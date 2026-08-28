import hashlib
import hmac
import time

from self_healthy_kafka.config import GrafanaWebhookConfig
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
