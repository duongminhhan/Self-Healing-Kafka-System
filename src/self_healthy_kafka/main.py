from __future__ import annotations

import argparse
import logging
import sys
import threading
import time
from collections.abc import Callable

from self_healthy_kafka.config import cfg
from self_healthy_kafka.container import build_grafana_webhook, build_state_machine
from self_healthy_kafka.logging_config import setup_logging
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
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

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
            logger.info(
                "%s scheduled execution finished",
                self._name,
                extra={
                    "event": "scheduled_worker_finished",
                    "worker": self._name,
                    "duration_ms": round((time.monotonic() - started_at) * 1000, 2),
                },
            )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Kafka Connect self-healing service")
    parser.add_argument(
        "--health-check-once",
        action="store_true",
        help="Run one connector health check/healing pass, then exit.",
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
    if schedule_followup is not None:
        for connector_name in followup_connectors:
            schedule_followup(connector_name)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    setup_logging()
    log_effective_config()

    try:
        validate_environment()
        state_machine, kc_client, _checker = wrap_wiring(build_state_machine)
    except StartupError as exc:
        logger.error(
            "startup check failed [%s]: %s",
            exc.code,
            exc,
            extra={"event": "startup_failed", "error_code": exc.code},
        )
        sys.exit(1)

    webhook_service = build_grafana_webhook(state_machine)
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
        },
    )

    if args.health_check_once:
        try:
            run_health_checks(state_machine)
        finally:
            kc_client.close()
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
        sys.exit(1)

    workers: list[PeriodicWorker] = []
    if cfg.polling.enabled:
        run_health_checks(state_machine, webhook_service.schedule_recovery_followup)
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
    else:
        logger.warning(
            "connector reconciliation polling disabled",
            extra={
                "event": "connector_health_polling_disabled",
                "manual_command": "python -m self_healthy_kafka.main --health-check-once",
            },
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


if __name__ == "__main__":
    main()
