from __future__ import annotations

import logging
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server

from self_healthy_kafka.config import PrometheusMetricsConfig
from self_healthy_kafka.healing.phases import EventType

logger = logging.getLogger(__name__)

_server_started = False

APP_INFO = Gauge(
    "kc_shs_app_info",
    "self-healthy-kafka application info",
    ["service"],
)

TOPIC_LAG_SECONDS = Gauge(
    "kc_shs_topic_lag_seconds",
    "Current topic lag seconds detected by self-healthy-kafka",
    ["connector_name", "topic", "condition"],
)

TOPIC_OVER_THRESHOLD = Gauge(
    "kc_shs_topic_over_threshold",
    "Whether a monitored topic is over the configured lag threshold",
    ["connector_name", "topic", "condition"],
)

TOPIC_MESSAGES_PER_SECOND = Gauge(
    "kc_shs_topic_messages_per_second",
    "Current message throughput detected by self-healthy-kafka",
    ["connector_name", "topic", "condition"],
)

TOPIC_RECORDS_LAG = Gauge(
    "kc_shs_topic_records_lag",
    "Latest Debezium source event to Kafka capture lag in minutes",
    ["connector_name", "topic", "condition"],
)

TOPIC_ACTIVE = Gauge(
    "kc_shs_topic_active",
    "Whether self-healthy-kafka monitoring is enabled for the topic",
    ["connector_name", "topic", "condition"],
)

TOPIC_LAG_EVENTS_TOTAL = Counter(
    "kc_shs_topic_lag_events_total",
    "Total topic lag transition events emitted by self-healthy-kafka",
    ["connector_name", "topic", "event_status", "condition"],
)

TOPIC_LAG_EVENTS_STORED_COUNT = Gauge(
    "kc_shs_topic_lag_events_stored_count",
    "Topic lag transition events stored in MSSQL within the configured window",
    ["connector_name", "topic", "event_status", "condition"],
)

CONNECTOR_HEALTH_FAILED_TOTAL = Counter(
    "kc_shs_connector_health_failed_total",
    "Total confirmed connector health failures",
    ["connector_name"],
)

HEALING_ATTEMPTS_TOTAL = Counter(
    "kc_shs_healing_attempts_total",
    "Total automated healing attempts",
    ["connector_name", "event_type", "healing_step"],
)

HEALING_ATTEMPTS_STORED_COUNT = Gauge(
    "kc_shs_healing_attempts_stored_count",
    "Total connector healing events stored in MSSQL by event type",
    ["connector_name", "base_connector_name", "event_type", "healing_step"],
)

HEALING_RECOVERED_TOTAL = Counter(
    "kc_shs_healing_recovered_total",
    "Total successful connector recoveries",
    ["connector_name"],
)

CONNECTOR_LAST_HEALING_RECOVERED_TIMESTAMP = Gauge(
    "kc_shs_connector_last_healing_recovered_timestamp_seconds",
    "Unix timestamp of the latest HEALING_RECOVERED log stored in MSSQL",
    ["connector_name"],
)

CONNECTOR_LAST_HEALING_ESCALATED_TIMESTAMP = Gauge(
    "kc_shs_connector_last_healing_escalated_timestamp_seconds",
    "Unix timestamp of the latest terminal healing failure log stored in MSSQL",
    ["connector_name", "event_type"],
)

HEALING_ESCALATED_TOTAL = Counter(
    "kc_shs_healing_escalated_total",
    "Total connector healing escalations",
    ["connector_name", "event_type"],
)

CONNECTOR_FAILED = Gauge(
    "kc_shs_connector_failed",
    "Whether self-healthy-kafka currently considers a connector failed",
    ["connector_name"],
)

CONNECTOR_ACTIVE = Gauge(
    "kc_shs_connector_active",
    "Whether self-healthy-kafka monitoring and healing is enabled for the connector",
    ["connector_name", "source_server"],
)

_connector_activity_labels: set[tuple[str, str]] = set()
_connector_failed_labels: set[str] = set()
_topic_state_labels: set[tuple[str, str, str]] = set()
_topic_lag_event_labels: set[tuple[str, str, str, str]] = set()
_healing_attempt_labels: set[tuple[str, str, str, str]] = set()
_last_healing_recovered_labels: set[str] = set()
_last_healing_escalated_labels: set[tuple[str, str]] = set()


def start_metrics_server(config: PrometheusMetricsConfig) -> None:
    global _server_started
    if not config.enabled:
        return
    if _server_started:
        return
    start_http_server(config.port, addr=config.host)
    _server_started = True
    APP_INFO.labels(service="self-healthy-kafka").set(1)
    logger.info(
        "Prometheus metrics endpoint started",
        extra={
            "event": "prometheus_metrics_started",
            "host": config.host,
            "port": config.port,
            "path": "/metrics",
        },
    )


def record_topic_lag_event(
    *,
    connector_name: str | None,
    topic_name: str,
    event_status: str,
    details: dict[str, Any],
) -> None:
    connector = connector_name or "unknown"
    condition = str(details.get("lag_condition") or "topic_idle")
    lag_seconds = _float_or_zero(details.get("lag_seconds") or details.get("idle_seconds"))

    TOPIC_LAG_EVENTS_TOTAL.labels(
        connector_name=connector,
        topic=topic_name,
        event_status=event_status,
        condition=condition,
    ).inc()

    if event_status == "OVER_THRESHOLD":
        TOPIC_LAG_SECONDS.labels(
            connector_name=connector,
            topic=topic_name,
            condition=condition,
        ).set(lag_seconds)
        TOPIC_OVER_THRESHOLD.labels(
            connector_name=connector,
            topic=topic_name,
            condition=condition,
        ).set(1)
    elif event_status == "RECOVERED":
        TOPIC_LAG_SECONDS.labels(
            connector_name=connector,
            topic=topic_name,
            condition=condition,
        ).set(0)
        TOPIC_OVER_THRESHOLD.labels(
            connector_name=connector,
            topic=topic_name,
            condition=condition,
        ).set(0)


def record_topic_lag_state(
    *,
    connector_name: str | None,
    topic_name: str,
    over_threshold: bool,
    lag_seconds: float | int,
    messages_per_second: float | int = 0,
    records_lag: float | int | None = None,
    condition: str = "topic_idle",
) -> None:
    connector = connector_name or "unknown"
    labels = (connector, topic_name, condition)
    _topic_state_labels.add(labels)
    TOPIC_ACTIVE.labels(
        connector_name=connector,
        topic=topic_name,
        condition=condition,
    ).set(1)
    TOPIC_OVER_THRESHOLD.labels(
        connector_name=connector,
        topic=topic_name,
        condition=condition,
    ).set(1 if over_threshold else 0)
    TOPIC_LAG_SECONDS.labels(
        connector_name=connector,
        topic=topic_name,
        condition=condition,
    ).set(_float_or_zero(lag_seconds))
    TOPIC_MESSAGES_PER_SECOND.labels(
        connector_name=connector,
        topic=topic_name,
        condition=condition,
    ).set(_float_or_zero(messages_per_second))
    if records_lag is not None:
        TOPIC_RECORDS_LAG.labels(
            connector_name=connector,
            topic=topic_name,
            condition=condition,
        ).set(_float_or_zero(records_lag))


def sync_active_topic_labels(
    rows: list[dict[str, Any]],
    conditions: tuple[str, ...] = ("topic_idle",),
) -> None:
    active_labels = {
        (
            str(row.get("connector_name") or "unknown"),
            str(row.get("topic_name") or ""),
            condition,
        )
        for row in rows
        for condition in conditions
        if row.get("topic_name")
    }
    for connector_name, topic_name, condition in _topic_state_labels - active_labels:
        _remove_topic_label(connector_name, topic_name, condition)
    _topic_state_labels.intersection_update(active_labels)


def record_connector_healing_event(
    *,
    connector_name: str,
    event_type: str,
    healing_step: int | None,
) -> None:
    if event_type == EventType.HEALTH_FAILED_CONFIRMED:
        CONNECTOR_HEALTH_FAILED_TOTAL.labels(connector_name=connector_name).inc()
        CONNECTOR_FAILED.labels(connector_name=connector_name).set(1)
        return

    if event_type == EventType.HEALING_RECOVERED:
        HEALING_RECOVERED_TOTAL.labels(connector_name=connector_name).inc()
        CONNECTOR_FAILED.labels(connector_name=connector_name).set(0)
        return

    if event_type in {
        EventType.HEALING_ESCALATED,
        EventType.HEALING_LEVEL_LIMIT_REACHED,
        EventType.STATE_MACHINE_ERROR,
    }:
        HEALING_ESCALATED_TOTAL.labels(
            connector_name=connector_name,
            event_type=event_type,
        ).inc()
        CONNECTOR_FAILED.labels(connector_name=connector_name).set(1)
        return

    if event_type in {
        EventType.TASK_RESTART,
        EventType.CONNECTOR_RESTART,
        EventType.CONNECTOR_RECREATE_WITH_OFFSET,
        EventType.CONNECTOR_RECREATE_WITHOUT_OFFSET,
    }:
        HEALING_ATTEMPTS_TOTAL.labels(
            connector_name=connector_name,
            event_type=event_type,
            healing_step=str(healing_step or 0),
        ).inc()


def sync_connector_activity(rows: list[dict[str, Any]]) -> None:
    current_labels: set[tuple[str, str]] = set()
    current_failed_labels: set[str] = set()
    for row in rows:
        connector_name = str(row.get("connector_name") or "")
        if not connector_name:
            continue
        source_server = str(row.get("source_server") or connector_name)
        current_labels.add((connector_name, source_server))
        current_failed_labels.add(connector_name)
        CONNECTOR_ACTIVE.labels(
            connector_name=connector_name,
            source_server=source_server,
        ).set(1 if row.get("is_active") else 0)
        CONNECTOR_FAILED.labels(connector_name=connector_name).set(
            1 if int(row.get("failed_count") or 0) > 0 else 0
        )

    for connector_name, source_server in _connector_activity_labels - current_labels:
        CONNECTOR_ACTIVE.remove(connector_name, source_server)
    _connector_activity_labels.clear()
    _connector_activity_labels.update(current_labels)
    for connector_name in _connector_failed_labels - current_failed_labels:
        CONNECTOR_FAILED.remove(connector_name)
    _connector_failed_labels.clear()
    _connector_failed_labels.update(current_failed_labels)


def sync_healing_attempt_metrics(rows: list[dict[str, Any]]) -> None:
    current_labels: set[tuple[str, str, str, str]] = set()
    for row in rows:
        connector_name = str(row.get("connector_name") or "")
        base_connector_name = str(
            row.get("base_connector_name") or connector_name
        )
        event_type = str(row.get("event_type") or "")
        if not connector_name or not event_type:
            continue
        healing_step = str(row.get("healing_step") or 0)
        total_attempts = _float_or_zero(
            row.get("total_attempts") or row.get("attempt_count")
        )
        labels = (
            connector_name,
            base_connector_name,
            event_type,
            healing_step,
        )
        current_labels.add(labels)
        HEALING_ATTEMPTS_STORED_COUNT.labels(
            connector_name=connector_name,
            base_connector_name=base_connector_name,
            event_type=event_type,
            healing_step=healing_step,
        ).set(total_attempts)

    for connector_name, base_connector_name, event_type, healing_step in (
        _healing_attempt_labels - current_labels
    ):
        HEALING_ATTEMPTS_STORED_COUNT.remove(
            connector_name,
            base_connector_name,
            event_type,
            healing_step,
        )
    _healing_attempt_labels.clear()
    _healing_attempt_labels.update(current_labels)


def sync_topic_lag_event_metrics(rows: list[dict[str, Any]]) -> None:
    current_labels: set[tuple[str, str, str, str]] = set()
    for row in rows:
        connector_name = str(row.get("connector_name") or "")
        topic_name = str(row.get("topic_name") or row.get("topic") or "")
        event_status = str(row.get("event_status") or "")
        condition = str(row.get("condition") or "topic_idle")
        if not connector_name or not topic_name or not event_status:
            continue
        total_events = _float_or_zero(
            row.get("total_events") or row.get("event_count")
        )
        labels = (connector_name, topic_name, event_status, condition)
        current_labels.add(labels)
        TOPIC_LAG_EVENTS_STORED_COUNT.labels(
            connector_name=connector_name,
            topic=topic_name,
            event_status=event_status,
            condition=condition,
        ).set(total_events)

    for connector_name, topic_name, event_status, condition in (
        _topic_lag_event_labels - current_labels
    ):
        TOPIC_LAG_EVENTS_STORED_COUNT.remove(
            connector_name,
            topic_name,
            event_status,
            condition,
        )
    _topic_lag_event_labels.clear()
    _topic_lag_event_labels.update(current_labels)


def sync_healing_recovered_metrics(rows: list[dict[str, Any]]) -> None:
    current_labels: set[str] = set()
    for row in rows:
        connector_name = str(row.get("connector_name") or "")
        if not connector_name:
            continue
        timestamp = _float_or_zero(
            row.get("last_healing_recovered_timestamp_seconds")
        )
        if timestamp <= 0:
            continue
        current_labels.add(connector_name)
        CONNECTOR_LAST_HEALING_RECOVERED_TIMESTAMP.labels(
            connector_name=connector_name,
        ).set(timestamp)

    for connector_name in _last_healing_recovered_labels - current_labels:
        CONNECTOR_LAST_HEALING_RECOVERED_TIMESTAMP.remove(connector_name)
    _last_healing_recovered_labels.clear()
    _last_healing_recovered_labels.update(current_labels)


def sync_healing_escalated_metrics(rows: list[dict[str, Any]]) -> None:
    current_labels: set[tuple[str, str]] = set()
    for row in rows:
        connector_name = str(row.get("connector_name") or "")
        event_type = str(row.get("event_type") or "")
        if not connector_name or not event_type:
            continue
        timestamp = _float_or_zero(
            row.get("last_healing_escalated_timestamp_seconds")
        )
        if timestamp <= 0:
            continue
        labels = (connector_name, event_type)
        current_labels.add(labels)
        CONNECTOR_LAST_HEALING_ESCALATED_TIMESTAMP.labels(
            connector_name=connector_name,
            event_type=event_type,
        ).set(timestamp)

    for connector_name, event_type in _last_healing_escalated_labels - current_labels:
        CONNECTOR_LAST_HEALING_ESCALATED_TIMESTAMP.remove(connector_name, event_type)
    _last_healing_escalated_labels.clear()
    _last_healing_escalated_labels.update(current_labels)


def _float_or_zero(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _remove_topic_label(connector_name: str, topic_name: str, condition: str) -> None:
    for gauge in (
        TOPIC_ACTIVE,
        TOPIC_OVER_THRESHOLD,
        TOPIC_LAG_SECONDS,
        TOPIC_MESSAGES_PER_SECOND,
        TOPIC_RECORDS_LAG,
    ):
        try:
            gauge.remove(connector_name, topic_name, condition)
        except KeyError:
            pass
