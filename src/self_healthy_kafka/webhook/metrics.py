from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def render_prometheus_metrics(
    connector_rows: Iterable[Mapping[str, Any]],
    topic_rows: Iterable[Mapping[str, Any]],
    inventory: Mapping[str, Iterable[Mapping[str, Any]]] | None = None,
) -> bytes:
    lines = [
        "# HELP kc_shs_connector_active Whether SELF-HEALTHY-KAFKA monitoring and healing is enabled for the connector.",
        "# TYPE kc_shs_connector_active gauge",
        "# HELP kc_shs_connector_source_server Maps a physical connector name to its Debezium JMX server name.",
        "# TYPE kc_shs_connector_source_server gauge",
    ]
    for row in connector_rows:
        connector_name = prometheus_label(str(row["connector_name"]))
        value = 1 if row.get("is_active") else 0
        lines.append(
            f'kc_shs_connector_active{{connector_name="{connector_name}"}} {value}'
        )
        source_server = prometheus_label(
            str(row.get("source_server") or row["connector_name"])
        )
        lines.append(
            "kc_shs_connector_source_server"
            f'{{connector_name="{connector_name}",'
            f'source_server="{source_server}"}} {value}'
        )
    lines.extend(
        (
            "# HELP kc_shs_topic_last_message_timestamp_seconds Unix timestamp of the latest Kafka record observed for the topic.",
            "# TYPE kc_shs_topic_last_message_timestamp_seconds gauge",
            "# HELP kc_shs_topic_active Whether SELF-HEALTHY-KAFKA topic lag monitoring is enabled for the topic.",
            "# TYPE kc_shs_topic_active gauge",
            "# HELP kc_shs_topic_over_threshold Whether a monitored topic is over the configured lag threshold.",
            "# TYPE kc_shs_topic_over_threshold gauge",
        )
    )
    for row in topic_rows:
        connector_name = prometheus_label(str(row["connector_name"]))
        topic = prometheus_label(str(row["topic_name"]))
        timestamp = max(0.0, float(row.get("last_message_timestamp_seconds") or 0))
        lines.append(
            "kc_shs_topic_last_message_timestamp_seconds"
            f'{{connector_name="{connector_name}",topic="{topic}"}} '
            f"{timestamp:.6f}"
        )
        lines.append(
            "kc_shs_topic_active"
            f'{{connector_name="{connector_name}",topic="{topic}",'
            'condition="db_state"} 1'
        )
        over_threshold = 1 if row.get("is_over_threshold") else 0
        lines.append(
            "kc_shs_topic_over_threshold"
            f'{{connector_name="{connector_name}",topic="{topic}",'
            f'condition="db_state"}} {over_threshold}'
        )
    if inventory:
        lines.extend(_render_inventory(inventory))
    return ("\n".join(lines) + "\n").encode("utf-8")


def _render_inventory(
    inventory: Mapping[str, Iterable[Mapping[str, Any]]],
) -> list[str]:
    lines = [
        "# HELP kc_shs_connector_inventory Connector reconciliation between configuration and Kafka Connect.",
        "# TYPE kc_shs_connector_inventory gauge",
    ]
    for row in inventory.get("connectors", []):
        labels = _labels(
            connector_name=row["connector_name"],
            configured_connector_name=row.get("configured_connector_name", ""),
            source_server=row.get("source_server", ""),
            state=row.get("state", "UNKNOWN"),
            inventory_status=row.get("inventory_status", "UNKNOWN"),
            configured=_bool_label(row.get("configured")),
            present=_bool_label(row.get("present")),
            managed=_bool_label(row.get("managed")),
            active=_bool_label(row.get("is_active")),
        )
        lines.append(f"kc_shs_connector_inventory{{{labels}}} 1")
    lines.extend(
        (
            "# HELP kc_shs_topic_inventory Topic reconciliation between monitoring configuration and Kafka broker.",
            "# TYPE kc_shs_topic_inventory gauge",
        )
    )
    for row in inventory.get("topics", []):
        labels = _labels(
            connector_name=row["connector_name"],
            configured_connector_name=row.get("configured_connector_name", ""),
            source_server=row.get("source_server", ""),
            topic=row["topic_name"],
            inventory_status=row.get("inventory_status", "UNKNOWN"),
            configured=_bool_label(row.get("configured")),
            present=_bool_label(row.get("present")),
        )
        lines.append(f"kc_shs_topic_inventory{{{labels}}} 1")
    return lines


def _labels(**values: Any) -> str:
    return ",".join(
        f'{key}="{prometheus_label(str(value))}"'
        for key, value in values.items()
    )


def _bool_label(value: Any) -> str:
    return "true" if value else "false"


def prometheus_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')
