from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from kafka import KafkaConsumer

from self_healthy_kafka.connect.client import KafkaConnectClient

logger = logging.getLogger(__name__)

INTERNAL_TOPIC_PREFIXES = ("__", "_connect", "_schemas")


class MonitoringInventory:
    """Reconciles configured resources with Kafka Connect and Kafka reality."""

    def __init__(
        self,
        *,
        client: KafkaConnectClient,
        bootstrap_servers: list[str],
        connector_activity: Callable[[], list[dict[str, Any]]],
        monitored_topics: Callable[[], list[dict[str, Any]]],
        topic_lag_metrics: Callable[[], list[dict[str, Any]]] | None = None,
        cache_seconds: int = 60,
    ):
        self._client = client
        self._bootstrap_servers = bootstrap_servers
        self._connector_activity = connector_activity
        self._monitored_topics = monitored_topics
        self._topic_lag_metrics = topic_lag_metrics or (lambda: [])
        self._cache_seconds = cache_seconds
        self._lock = threading.Lock()
        self._refresh_lock = threading.Lock()
        self._stopping = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = False
        self._consumer: KafkaConsumer | None = None
        self._cached_at = 0.0
        self._cached: dict[str, list[dict[str, Any]]] = {
            "connectors": [],
            "topics": [],
            "connector_activity": [],
            "topic_lag_metrics": [],
        }

    def snapshot(self) -> dict[str, list[dict[str, Any]]]:
        with self._lock:
            snapshot = self._cached
            initialized = self._cached_at > 0
            started = self._started
        if initialized or started:
            return snapshot
        self.refresh()
        with self._lock:
            return self._cached

    def start(self) -> None:
        with self._lock:
            self._started = True
        self.refresh()
        if self._thread is not None:
            return
        self._stopping.clear()
        self._thread = threading.Thread(
            target=self._refresh_loop,
            name="monitoring-inventory-refresh",
            daemon=True,
        )
        self._thread.start()

    def close(self) -> None:
        self._stopping.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
            self._thread = None
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None

    def refresh(self) -> None:
        if not self._refresh_lock.acquire(blocking=False):
            return
        started_at = time.monotonic()
        try:
            snapshot = self._load()
        except Exception:
            logger.exception(
                "Failed to refresh monitoring inventory",
                extra={"event": "monitoring_inventory_refresh_failed"},
            )
            return
        finally:
            self._refresh_lock.release()
        with self._lock:
            self._cached = snapshot
            self._cached_at = time.monotonic()
        logger.info(
            "monitoring inventory refreshed",
            extra={
                "event": "monitoring_inventory_refreshed",
                "duration_ms": round((time.monotonic() - started_at) * 1000, 2),
                "connector_count": len(snapshot["connectors"]),
                "topic_count": len(snapshot["topics"]),
            },
        )

    def _refresh_loop(self) -> None:
        while not self._stopping.wait(self._cache_seconds):
            self.refresh()

    def _load(self) -> dict[str, list[dict[str, Any]]]:
        configured = self._connector_activity()
        configured_topics = self._monitored_topics()
        configured_by_name = {
            str(row["connector_name"]): row for row in configured
        }
        connector_rows: list[dict[str, Any]] = []
        for connector_name, row in configured_by_name.items():
            if not row.get("is_active"):
                continue
            status = self._client.get_status(connector_name)
            if status is None:
                connector_rows.append(_missing_connector_row(connector_name, row))
                continue
            source_server = str(row.get("source_server") or connector_name)
            connector_rows.append(
                {
                    "connector_name": connector_name,
                    "configured_connector_name": "",
                    "source_server": source_server,
                    "state": status.state.value,
                    "inventory_status": "MATCHED",
                    "configured": True,
                    "present": True,
                    "managed": True,
                    "is_active": True,
                }
            )

        broker_topics = self._list_broker_topics()
        topic_rows = _reconcile_topics(
            connector_rows=connector_rows,
            configured_topics=configured_topics,
            broker_topics=broker_topics,
        )
        return {
            "connectors": sorted(
                connector_rows,
                key=lambda item: str(item["connector_name"]),
            ),
            "topics": sorted(
                topic_rows,
                key=lambda item: (
                    str(item["connector_name"]),
                    str(item["topic_name"]),
                ),
            ),
            "connector_activity": configured,
            "topic_lag_metrics": self._topic_lag_metrics(),
        }

    def _list_broker_topics(self) -> set[str]:
        if self._consumer is None:
            self._consumer = KafkaConsumer(
                bootstrap_servers=self._bootstrap_servers,
                group_id=None,
                enable_auto_commit=False,
                client_id="self-healthy-kafka-monitoring-inventory",
                api_version_auto_timeout_ms=10000,
                request_timeout_ms=15000,
            )
        return {
            topic
            for topic in self._consumer.topics()
            if not topic.startswith(INTERNAL_TOPIC_PREFIXES)
        }


def _missing_connector_row(
    connector_name: str,
    row: dict[str, Any],
) -> dict[str, Any]:
    return {
        "connector_name": connector_name,
        "configured_connector_name": connector_name,
        "source_server": str(row.get("source_server") or connector_name),
        "state": "MISSING",
        "inventory_status": "MISSING",
        "configured": True,
        "present": False,
        "managed": True,
        "is_active": True,
    }


def _reconcile_topics(
    *,
    connector_rows: list[dict[str, Any]],
    configured_topics: list[dict[str, Any]],
    broker_topics: set[str],
) -> list[dict[str, Any]]:
    rows: dict[tuple[str, str], dict[str, Any]] = {}
    connector_by_source = {
        str(item["source_server"]): str(item["connector_name"])
        for item in connector_rows
        if item.get("present") and item.get("managed")
    }
    for item in configured_topics:
        connector_name = str(item["connector_name"])
        topic_name = str(item["topic_name"])
        source_server = str(
            item.get("source_server")
            or _topic_source_server(topic_name)
        )
        physical_connector = connector_by_source.get(source_server, connector_name)
        key = (physical_connector, topic_name)
        rows[key] = {
            "connector_name": physical_connector,
            "configured_connector_name": connector_name,
            "source_server": source_server,
            "topic_name": topic_name,
            "inventory_status": (
                "MONITORED" if topic_name in broker_topics else "MISSING"
            ),
            "configured": True,
            "present": topic_name in broker_topics,
        }

    broker_topics_by_source = _index_broker_topics_by_source(
        broker_topics,
        connector_by_source,
    )
    for source_server, connector_name in connector_by_source.items():
        for topic_name in broker_topics_by_source[source_server]:
            key = (connector_name, topic_name)
            if key in rows:
                continue
            rows[key] = {
                "connector_name": connector_name,
                "configured_connector_name": "",
                "source_server": source_server,
                "topic_name": topic_name,
                "inventory_status": "UNCONFIGURED",
                "configured": False,
                "present": True,
            }
    return list(rows.values())


def _topic_source_server(topic_name: str) -> str:
    return topic_name.split(".", 1)[0]


def _index_broker_topics_by_source(
    broker_topics: set[str],
    connector_by_source: dict[str, str],
) -> dict[str, list[str]]:
    indexed: dict[str, list[str]] = {
        source_server: [] for source_server in connector_by_source
    }
    if not indexed:
        return indexed

    for topic_name in broker_topics:
        separator = topic_name.find(".")
        while separator >= 0:
            source_server = topic_name[:separator]
            if source_server in indexed:
                indexed[source_server].append(topic_name)
            separator = topic_name.find(".", separator + 1)
    return indexed
