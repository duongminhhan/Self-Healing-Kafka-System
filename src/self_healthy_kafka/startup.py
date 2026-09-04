"""Pre-flight checks run before the poll loop starts."""

from __future__ import annotations

import logging
from dataclasses import asdict

import httpx

from self_healthy_kafka.config import cfg

logger = logging.getLogger(__name__)


class StartupError(Exception):
    """Raised when a pre-flight check fails fatally."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(message)


def _connection_target(connection_string: str) -> str:
    """Extract SQL Server and database names without exposing credentials."""
    fields = {}
    for item in connection_string.split(";"):
        key, separator, value = item.partition("=")
        if separator:
            fields[key.strip().lower()] = value.strip()
    return f"{fields.get('server', '?')}/{fields.get('database', '?')}"


def validate_environment() -> None:
    _check_mssql()
    if cfg.polling.enabled or cfg.grafana_webhook.enabled:
        _check_kafka_connect()
    else:
        logger.info(
            "Kafka Connect pre-flight skipped because only the read-only chat API is enabled",
            extra={"event": "startup_kc_check_skipped_chat_only"},
        )


def log_effective_config() -> None:
    logger.info(
        "effective configuration",
        extra={
            "event": "config_dump",
            "kafka_connect": asdict(cfg.kafka_connect),
            "polling": asdict(cfg.polling),
            "grafana_webhook": {
                "enabled": cfg.grafana_webhook.enabled,
                "host": cfg.grafana_webhook.host,
                "port": cfg.grafana_webhook.port,
                "path": cfg.grafana_webhook.path,
                "auth_mode": cfg.grafana_webhook.auth_mode,
                "signature_header": cfg.grafana_webhook.signature_header,
                "timestamp_header": cfg.grafana_webhook.timestamp_header,
                "timestamp_tolerance_seconds": (
                    cfg.grafana_webhook.timestamp_tolerance_seconds
                ),
                "dedupe_ttl_seconds": cfg.grafana_webhook.dedupe_ttl_seconds,
                "queue_size": cfg.grafana_webhook.queue_size,
                "worker_count": cfg.grafana_webhook.worker_count,
                "recovery_followup_seconds": (
                    cfg.grafana_webhook.recovery_followup_seconds
                ),
            },
            "state_machine": asdict(cfg.state_machine),
            "mssql": {
                "target": _connection_target(cfg.mssql.connection_string),
                "connection_timeout_seconds": (
                    cfg.mssql.connection_timeout_seconds
                ),
            },
            "logging_cfg": asdict(cfg.logging),
        },
    )


def wrap_wiring(builder):
    try:
        return builder()
    except TypeError as exc:
        raise StartupError(
            "WIRING_ERROR",
            f"Container wiring mismatch: {exc}. "
            "Check container.py against each step's __init__ signature.",
        ) from exc
    except Exception as exc:
        raise StartupError(
            "WIRING_ERROR",
            f"Failed to build state machine: {type(exc).__name__}: {exc}",
        ) from exc


def _check_mssql() -> None:
    import pyodbc

    target = _connection_target(cfg.mssql.connection_string)
    try:
        conn = pyodbc.connect(
            cfg.mssql.connection_string,
            timeout=cfg.mssql.connection_timeout_seconds,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        finally:
            conn.close()
    except pyodbc.Error as exc:
        message = str(exc).splitlines()[0] if str(exc) else "connection refused"
        raise StartupError(
            "DB_UNREACHABLE",
            f"SQL Server at {target} unreachable: {message}",
        ) from exc
    except Exception as exc:
        raise StartupError(
            "DB_UNREACHABLE",
            f"SQL Server at {target} check failed: {type(exc).__name__}: {exc}",
        ) from exc

    logger.info(
        "SQL Server reachable at %s",
        target,
        extra={"event": "startup_mssql_ok", "target": target},
    )


def _check_kafka_connect() -> None:
    url = cfg.kafka_connect.base_url
    try:
        response = httpx.get(
            url + "/",
            timeout=cfg.kafka_connect.request_timeout,
            verify=cfg.kafka_connect.tls_verify,
        )
        response.raise_for_status()
        info = (
            response.json()
            if response.headers.get("content-type", "").startswith(
                "application/json"
            )
            else {}
        )
    except httpx.RequestError as exc:
        raise StartupError(
            "KC_UNREACHABLE",
            f"Kafka Connect at {url} unreachable: "
            f"{type(exc).__name__}: {exc}",
        ) from exc
    except httpx.HTTPStatusError as exc:
        raise StartupError(
            "KC_UNREACHABLE",
            f"Kafka Connect at {url} returned HTTP "
            f"{exc.response.status_code}",
        ) from exc

    logger.info(
        "Kafka Connect reachable at %s (version=%s)",
        url,
        info.get("version", "unknown"),
        extra={
            "event": "startup_kc_ok",
            "url": url,
            "kc_version": info.get("version"),
        },
    )
