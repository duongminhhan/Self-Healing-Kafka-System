from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from collections.abc import Callable

from self_healthy_kafka.config import cfg
from self_healthy_kafka.container import (
    build_grafana_webhook,
    build_state_machine,
    build_topic_idle_probe,
)
from self_healthy_kafka.logging_config import setup_logging
from self_healthy_kafka.monitoring.metrics import (
    start_metrics_server,
    sync_connector_activity,
    sync_healing_attempt_metrics,
    sync_healing_escalated_metrics,
    sync_healing_recovered_metrics,
    sync_topic_lag_event_metrics,
)
from self_healthy_kafka.startup import (
    StartupError,
    log_effective_config,
    validate_environment,
    wrap_wiring,
)

logger = logging.getLogger("main")


class PeriodicWorker:
    def __init__(self, name: str, interval_seconds: int, action: Callable[[], None]):
        self._name = name
        self._interval = interval_seconds
        self._action = action
        self._stopping = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=name,
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def close(self) -> None:
        self._stopping.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stopping.wait(self._interval):
            started_at = time.monotonic()
            try:
                self._action()
            except Exception:
                logger.exception(
                    "%s scheduled execution failed",
                    self._name,
                    extra={"event": "scheduled_worker_failed", "worker": self._name},
                )
            elapsed = time.monotonic() - started_at
            skipped = int(elapsed // self._interval)
            logger.info(
                "%s scheduled execution finished",
                self._name,
                extra={
                    "event": "scheduled_worker_finished",
                    "worker": self._name,
                    "duration_ms": round(elapsed * 1000, 2),
                    "skipped_executions": skipped,
                },
            )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka Connect self-healing service",
    )
    parser.add_argument(
        "--health-check-once",
        action="store_true",
        help=(
            "Run one connector health check/healing pass, then exit. "
            "Useful when CONNECTOR_HEALTH_POLLING_ENABLED=false."
        ),
    )
    parser.add_argument(
        "--topic-lag-check-once",
        action="store_true",
        help=(
            "Run one topic lag check, then exit. If --source-server is provided, "
            "only topics mapped to that Debezium server/topic.prefix are checked."
        ),
    )
    parser.add_argument(
        "--source-server",
        help="Debezium source server/topic.prefix from a Grafana JMX alert.",
    )
    return parser.parse_args(argv)


def run_health_checks(state_machine, schedule_followup=None) -> None:
    started_at = time.monotonic()
    followup_connectors = state_machine.tick()
    logger.info(
        "connector reconciliation completed",
        extra={
            "event": "connector_reconciliation_completed",
            "duration_ms": round((time.monotonic() - started_at) * 1000, 2),
            "followup_count": len(followup_connectors),
        },
    )
    if schedule_followup is None:
        return
    for connector_name in followup_connectors:
        schedule_followup(connector_name)


def sync_connector_metrics(state_machine) -> None:
    started_at = time.monotonic()
    connector_rows = state_machine.db.list_connector_activity()
    sync_connector_activity(connector_rows)
    try:
        metrics = state_machine.db.get_operational_metrics(window_minutes=15)
        if metrics["healing_attempt"] is not None:
            sync_healing_attempt_metrics(metrics["healing_attempt"])
        if metrics["healing_recovered"] is not None:
            sync_healing_recovered_metrics(metrics["healing_recovered"])
        if metrics["healing_escalated"] is not None:
            sync_healing_escalated_metrics(metrics["healing_escalated"])
        if metrics["topic_lag_event"] is not None:
            sync_topic_lag_event_metrics(metrics["topic_lag_event"])
    except Exception as exc:
        logger.warning(
            "failed to sync DB-backed operational metrics: %s",
            exc,
            extra={"event": "db_metrics_sync_failed"},
        )
    finally:
        logger.info(
            "DB-backed metrics synchronization completed",
            extra={
                "event": "db_metrics_sync_completed",
                "duration_ms": round((time.monotonic() - started_at) * 1000, 2),
                "connector_count": len(connector_rows),
            },
        )


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    setup_logging()

    # Dump every resolved config value so an operator can answer
    # "what config is this process running with?" from one log line.
    log_effective_config()

    # Pre-flight: validate env vars, reach SQL Server, reach Kafka Connect.
    # Fail fast with a single readable line instead of crash-looping on
    # the same error a thousand times.
    try:
        validate_environment()
    except StartupError as e:
        logger.error(
            f"startup check failed [{e.code}]: {e}",
            extra={"event": "startup_failed", "error_code": e.code},
        )
        sys.exit(1)

    # Wiring smoke-test: build_state_machine() exercises every constructor.
    # A TypeError here means container.py is out of sync with a dependency.
    try:
        state_machine, kc_client, _checker = wrap_wiring(build_state_machine)
    except StartupError as e:
        logger.error(
            f"startup check failed [{e.code}]: {e}",
            extra={"event": "startup_failed", "error_code": e.code},
        )
        sys.exit(1)

    # Topic-idle probe (separate cadence from connector health). When
    # no topics are configured in DB, tick() returns without work.
    topic_probe = build_topic_idle_probe(state_machine.db)
    webhook_service = build_grafana_webhook(state_machine)
    try:
        start_metrics_server(cfg.prometheus_metrics)
    except Exception as exc:
        logger.error(
            "Prometheus metrics endpoint failed to start: %s",
            exc,
            extra={"event": "prometheus_metrics_start_failed"},
        )
        kc_client.close()
        topic_probe.close()
        sys.exit(1)
    sync_connector_metrics(state_machine)

    logger.info(
        "Kafka Source Connector Self-Healing started",
        extra={
            "event": "app_started",
            "kafka_connect": cfg.kafka_connect.base_url,
            "connector_source": "mssql.connectors",
            "connector_health_polling_enabled": cfg.polling.enabled,
            "reconciliation_interval": cfg.polling.interval_seconds,
            "grafana_webhook_enabled": cfg.grafana_webhook.enabled,
            "grafana_webhook_port": cfg.grafana_webhook.port,
            "grafana_webhook_path": cfg.grafana_webhook.path,
            "grafana_webhook_worker_count": cfg.grafana_webhook.worker_count,
            "topic_idle_enabled": topic_probe.enabled(),
            "topic_source": "mssql.topic_lag_jobs",
            "topic_idle_threshold_seconds": cfg.topic_idle.idle_threshold_seconds,
            "prometheus_metrics_enabled": cfg.prometheus_metrics.enabled,
            "prometheus_metrics_port": cfg.prometheus_metrics.port,
        },
    )

    if args.health_check_once:
        logger.info(
            "manual connector health check started",
            extra={
                "event": "manual_health_check_started",
                "connector_source": "mssql.connectors",
            },
        )
        try:
            run_health_checks(state_machine)
            logger.info(
                "manual connector health check finished",
                extra={"event": "manual_health_check_finished"},
            )
        finally:
            kc_client.close()
            topic_probe.close()
        return

    if args.topic_lag_check_once:
        logger.info(
            "manual topic lag check started",
            extra={
                "event": "manual_topic_lag_check_started",
                "source_server": args.source_server,
            },
        )
        try:
            topic_probe.tick(
                source_server=args.source_server,
                check_event_latency=True,
            )
            logger.info(
                "manual topic lag check finished",
                extra={"event": "manual_topic_lag_check_finished"},
            )
        finally:
            kc_client.close()
            topic_probe.close()
        return

    try:
        webhook_service.start()
    except Exception as exc:
        logger.error(
            "Grafana webhook receiver failed to start: %s",
            exc,
            extra={"event": "grafana_webhook_start_failed"},
        )
        kc_client.close()
        topic_probe.close()
        sys.exit(1)

    # Slow full-cluster reconciliation. Grafana webhook alerts are the primary
    # trigger; this catches missed deliveries and monitoring gaps.
    if cfg.polling.enabled:
        run_health_checks(
            state_machine,
            webhook_service.schedule_recovery_followup,
        )
    else:
        logger.warning(
            "connector reconciliation polling disabled",
            extra={
                "event": "connector_health_polling_disabled",
                "manual_command": "python -m self_healthy_kafka.main --health-check-once",
            },
        )

    if topic_probe.enabled():
        logger.info(
            "topic-idle probe configured",
            extra={
                "event": "topic_idle_probe_configured",
                "topic_source": "mssql.topic_lag_jobs",
                "topic_event_capture_lag_threshold_ms": (
                    cfg.topic_idle.event_capture_lag_threshold_ms
                ),
                "topic_idle_threshold_seconds": cfg.topic_idle.idle_threshold_seconds,
                "topic_idle_poll_interval_seconds": cfg.topic_idle.poll_interval_seconds,
                "kafka_bootstrap_servers": cfg.topic_idle.bootstrap_servers,
            },
        )
        # Seed immediately so idle alert state is available right away
        # instead of after one full interval.
        topic_probe.tick()
    workers: list[PeriodicWorker] = []
    if cfg.polling.enabled:
        workers.append(
            PeriodicWorker(
                "connector-reconciliation",
                cfg.polling.interval_seconds,
                lambda: run_health_checks(
                    state_machine,
                    webhook_service.schedule_recovery_followup,
                ),
            )
        )
    if topic_probe.enabled():
        workers.append(
            PeriodicWorker(
                "topic-lag-probe",
                cfg.topic_idle.poll_interval_seconds,
                topic_probe.tick,
            )
        )
    workers.append(
        PeriodicWorker(
            "db-metrics-sync",
            cfg.prometheus_metrics.sync_interval_seconds,
            lambda: sync_connector_metrics(state_machine),
        )
    )
    for worker in workers:
        worker.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        for worker in workers:
            worker.close()
        webhook_service.close()
        kc_client.close()
        topic_probe.close()


if __name__ == "__main__":
    main()
