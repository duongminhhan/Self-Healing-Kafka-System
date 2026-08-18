from __future__ import annotations

from self_healthy_kafka.config import cfg
from self_healthy_kafka.connect.client import KafkaConnectClient
from self_healthy_kafka.healing.db_state_machine import ConnectorStateMachine
from self_healthy_kafka.health.checker import HealthChecker
from self_healthy_kafka.health.topic_idle import DbTopicIdleProbe
from self_healthy_kafka.monitoring.inventory import MonitoringInventory
from self_healthy_kafka.storage.mssql import HealingRepository
from self_healthy_kafka.webhook.grafana import GrafanaWebhookService


def build_state_machine():
    kc_client = KafkaConnectClient(
        base_url=cfg.kafka_connect.base_url,
        timeout=cfg.kafka_connect.request_timeout,
        tls_verify=cfg.kafka_connect.tls_verify,
        circuit_breaker_cooldown_seconds=(
            cfg.kafka_connect.circuit_breaker_cooldown_seconds
        ),
    )
    db = HealingRepository(
        connection_string=cfg.mssql.connection_string,
        timeout_seconds=cfg.mssql.connection_timeout_seconds,
    )
    checker = HealthChecker(client=kc_client)

    state_machine = ConnectorStateMachine(
        client=kc_client,
        checker=checker,
        db=db,
        failure_confirm_checks=cfg.state_machine.failure_confirm_checks,
        task_restart_max_attempts=cfg.state_machine.task_restart_max_attempts,
        connector_restart_max_attempts=cfg.state_machine.connector_restart_max_attempts,
        post_restart_wait_seconds=cfg.state_machine.post_restart_wait_seconds,
        recovery_healthy_confirm_seconds=cfg.state_machine.recovery_healthy_confirm_seconds,
        recreate_verify_wait_seconds=cfg.state_machine.recreate_verify_wait_seconds,
        scn_poll_interval_seconds=cfg.state_machine.scn_poll_interval_seconds,
        recreate_keep_base_connector=cfg.state_machine.recreate_keep_base_connector,
    )
    return state_machine, kc_client, checker


def build_topic_idle_probe(db: HealingRepository) -> DbTopicIdleProbe:
    """
    Stand-alone factory — the probe doesn't depend on the state machine and
    main.py schedules it on a separate cadence. Returned even when topics
    is empty; the caller checks .enabled() before scheduling.
    """
    return DbTopicIdleProbe(
        bootstrap_servers=cfg.topic_idle.bootstrap_servers,
        db=db,
        default_idle_threshold_seconds=cfg.topic_idle.idle_threshold_seconds,
        topic_idle_enabled=cfg.topic_idle.topic_idle_enabled,
        event_capture_enabled=cfg.topic_idle.event_capture_enabled,
        event_capture_lag_threshold_ms=cfg.topic_idle.event_capture_lag_threshold_ms,
    )


def build_grafana_webhook(
    state_machine: ConnectorStateMachine,
) -> GrafanaWebhookService:
    inventory = MonitoringInventory(
        client=state_machine.client,
        bootstrap_servers=cfg.topic_idle.bootstrap_servers,
        connector_activity=state_machine.db.list_connector_activity,
        monitored_topics=lambda: state_machine.db.list_monitored_topics(
            cfg.topic_idle.idle_threshold_seconds
        ),
        topic_lag_metrics=state_machine.db.list_topic_lag_metrics,
        cache_seconds=cfg.grafana_webhook.monitoring_inventory_refresh_seconds,
    )
    return GrafanaWebhookService(
        config=cfg.grafana_webhook,
        process_connector=state_machine.process_connector,
        connector_activity=state_machine.db.list_connector_activity,
        topic_lag_metrics=state_machine.db.list_topic_lag_metrics,
        monitoring_inventory=inventory.snapshot,
        monitoring_start=inventory.start,
        monitoring_close=inventory.close,
    )
