from __future__ import annotations

from self_healthy_kafka.config import cfg
from self_healthy_kafka.connect.client import KafkaConnectClient
from self_healthy_kafka.healing.db_state_machine import ConnectorStateMachine
from self_healthy_kafka.health.checker import HealthChecker
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


def build_grafana_webhook(
    state_machine: ConnectorStateMachine,
) -> GrafanaWebhookService:
    return GrafanaWebhookService(
        config=cfg.grafana_webhook,
        process_connector=state_machine.process_connector,
        chat_api_config=cfg.chat_api,
        ollama_chat_config=cfg.ollama_chat,
        queue_lookup=lambda queue_id, connector_name: state_machine.db.list_queue_for_chat(
            queue_id=queue_id,
            connector_name=connector_name,
        ),
        healing_logs=state_machine.db.list_healing_logs,
        failure_ranking=state_machine.db.get_failure_ranking,
    )
