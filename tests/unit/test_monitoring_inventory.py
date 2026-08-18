from unittest.mock import MagicMock

from self_healthy_kafka.domain.models import ConnectorState
from self_healthy_kafka.monitoring.inventory import (
    MonitoringInventory,
    _reconcile_topics,
)
from tests.conftest import make_status


def test_inventory_checks_only_active_monitored_connectors():
    client = MagicMock()
    client.get_status.return_value = make_status(
        name="CDC.001",
        state=ConnectorState.RUNNING,
    )
    inventory = MonitoringInventory(
        client=client,
        bootstrap_servers=["kafka:29092"],
        connector_activity=lambda: [
            {
                "connector_name": "CDC.001",
                "source_server": "CDC",
                "is_active": True,
            },
            {
                "connector_name": "INACTIVE",
                "source_server": "INACTIVE",
                "is_active": False,
            }
        ],
        monitored_topics=lambda: [
            {
                "connector_name": "CDC.001",
                "topic_name": "CDC.APP.CUSTOMERS",
            }
        ],
        cache_seconds=0,
    )
    inventory._list_broker_topics = MagicMock(
        return_value={
            "CDC",
            "CDC.APP.CUSTOMERS",
            "CDC.APP.ORDERS",
            "INACTIVE.DATA",
        }
    )

    snapshot = inventory.snapshot()

    connector = next(
        row for row in snapshot["connectors"]
        if row["connector_name"] == "CDC.001"
    )
    assert connector["configured"] is True
    assert connector["managed"] is True
    assert connector["configured_connector_name"] == ""
    assert connector["inventory_status"] == "MATCHED"
    client.list_connectors.assert_not_called()
    client.get_status.assert_called_once_with("CDC.001")

    missing_config = next(
        row for row in snapshot["topics"]
        if row["topic_name"] == "CDC.APP.ORDERS"
    )
    assert missing_config["connector_name"] == "CDC.001"
    assert missing_config["configured"] is False
    assert missing_config["present"] is True
    assert missing_config["inventory_status"] == "UNCONFIGURED"
    assert not any(row["topic_name"] == "CDC" for row in snapshot["topics"])
    assert not any(row["topic_name"] == "INACTIVE.DATA" for row in snapshot["topics"])


def test_inventory_keeps_configured_connector_missing_from_connect():
    client = MagicMock()
    client.get_status.return_value = None
    inventory = MonitoringInventory(
        client=client,
        bootstrap_servers=["kafka:29092"],
        connector_activity=lambda: [
            {
                "connector_name": "CDC",
                "source_server": "CDC",
                "is_active": True,
            }
        ],
        monitored_topics=lambda: [],
        cache_seconds=0,
    )
    inventory._list_broker_topics = MagicMock(return_value=set())

    row = inventory.snapshot()["connectors"][0]

    assert row["connector_name"] == "CDC"
    assert row["configured"] is True
    assert row["present"] is False
    assert row["state"] == "MISSING"
    assert row["inventory_status"] == "MISSING"
    client.list_connectors.assert_not_called()
    client.get_status.assert_called_once_with("CDC")


def test_started_inventory_serves_cached_snapshot_without_live_io():
    client = MagicMock()
    client.get_status.return_value = make_status(
        name="CDC.001",
        state=ConnectorState.RUNNING,
    )
    connector_activity = MagicMock(return_value=[{
        "connector_name": "CDC.001",
        "source_server": "CDC",
        "is_active": True,
    }])
    monitored_topics = MagicMock(return_value=[])
    inventory = MonitoringInventory(
        client=client,
        bootstrap_servers=["kafka:29092"],
        connector_activity=connector_activity,
        monitored_topics=monitored_topics,
        cache_seconds=60,
    )
    inventory._list_broker_topics = MagicMock(return_value=set())

    inventory.start()
    try:
        connector_activity.reset_mock()
        monitored_topics.reset_mock()
        client.get_status.reset_mock()

        snapshot = inventory.snapshot()

        assert snapshot["connectors"][0]["connector_name"] == "CDC.001"
        connector_activity.assert_not_called()
        monitored_topics.assert_not_called()
        client.get_status.assert_not_called()
    finally:
        inventory.close()


def test_topic_reconciliation_indexes_nested_source_prefixes():
    rows = _reconcile_topics(
        connector_rows=[
            {
                "connector_name": "TOPO-CLI-G041.006",
                "source_server": "CDC.TOPO-CLI",
                "present": True,
                "managed": True,
            }
        ],
        configured_topics=[],
        broker_topics={
            "CDC.TOPO-CLI.TOPOVN.TRK_MASTER",
            "CDC.OTHER.TOPOVN.ITEM",
            "CDC.TOPO-CLI",
        },
    )

    assert rows == [
        {
            "connector_name": "TOPO-CLI-G041.006",
            "configured_connector_name": "",
            "source_server": "CDC.TOPO-CLI",
            "topic_name": "CDC.TOPO-CLI.TOPOVN.TRK_MASTER",
            "inventory_status": "UNCONFIGURED",
            "configured": False,
            "present": True,
        }
    ]
