from self_healthy_kafka.monitoring.metrics import (
    CONNECTOR_FAILED,
    HEALING_ATTEMPTS_STORED_COUNT,
    sync_connector_activity,
    sync_healing_attempt_metrics,
)


def test_connector_failed_is_reconciled_from_database_failed_count():
    connector_name = "metrics-sync-test"

    sync_connector_activity([{
        "connector_name": connector_name,
        "source_server": "CDC",
        "is_active": True,
        "failed_count": 3,
    }])
    assert CONNECTOR_FAILED.labels(connector_name=connector_name)._value.get() == 1

    sync_connector_activity([{
        "connector_name": connector_name,
        "source_server": "CDC",
        "is_active": True,
        "failed_count": 0,
    }])
    assert CONNECTOR_FAILED.labels(connector_name=connector_name)._value.get() == 0


def test_stored_healing_attempt_metric_keeps_base_connector_label():
    sync_healing_attempt_metrics([
        {
            "connector_name": "conn-a.002",
            "base_connector_name": "conn-a",
            "event_type": "HEALTH_FAILED_CONFIRMED",
            "healing_step": 1,
            "total_attempts": 3,
        }
    ])

    assert HEALING_ATTEMPTS_STORED_COUNT.labels(
        connector_name="conn-a.002",
        base_connector_name="conn-a",
        event_type="HEALTH_FAILED_CONFIRMED",
        healing_step="1",
    )._value.get() == 3
