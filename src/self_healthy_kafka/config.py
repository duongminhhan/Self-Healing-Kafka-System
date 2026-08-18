"""
Application configuration.

Single source of truth: .env.example at the repo root.
- .env.example (committed) defines every tunable and its default value.
- env/<APP_ENV>.env (gitignored) carries environment-specific overrides.
- SELF_HEALTHY_KAFKA_ENV_FILE can point to an explicit env file.
- Shell env vars are also respected.

Precedence (highest wins):
    shell env > explicit env file > env/<APP_ENV>.env > .env.example

`_env(KEY, cast=int)` reads `os.environ[KEY]` and casts. If the key isn't
set anywhere (shell, env/<APP_ENV>.env, or .env.example), it raises KeyError at
import time — that forces every tunable to be documented in .env.example.

Adding a new tunable:
    1. Add a line to .env.example (KEY=default_value).
    2. Reference it from a dataclass field via `_env("KEY", cast=int)`.
    3. Override it in env/<APP_ENV>.env, an explicit env file, or the shell.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, TypeVar, overload

from dotenv import load_dotenv

# ─── Load defaults then overrides ────────────────────────────────────────────
# Run commands from the project directory. This also works for editable installs
# and systemd services that set WorkingDirectory to the project directory.
_project_root = Path.cwd()
_app_env = os.getenv("APP_ENV", "dev").strip().lower() or "dev"
_explicit_env_file = os.getenv("SELF_HEALTHY_KAFKA_ENV_FILE", "").strip()

if _explicit_env_file:
    load_dotenv(Path(_explicit_env_file).expanduser(), override=False)
else:
    load_dotenv(_project_root / "env" / f"{_app_env}.env", override=False)

load_dotenv(_project_root / ".env.example", override=False)


T = TypeVar("T")


@overload
def _env(key: str) -> str: ...


@overload
def _env(key: str, cast: Callable[[str], T]) -> T: ...


def _env(
    key: str,
    cast: Callable[[str], Any] = str,
) -> Any:
    """
    Read a required env var and cast it.

    Raises KeyError at import time if the key isn't set anywhere (shell,
    env/<APP_ENV>.env, or .env.example). That guarantees every tunable
    referenced by code is documented in .env.example.
    """
    raw = os.getenv(key)
    if raw is None:
        raise KeyError(
            f"Required env var {key!r} is not set. Add it to .env.example "
            "with a documented default value, or set it explicitly in "
            "env/<APP_ENV>.env / your shell environment."
        )
    return cast(raw)


def _env_csv(key: str) -> list[str]:
    """Comma-separated env var → list of non-empty trimmed strings."""
    raw: str = _env(key)
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_bool(key: str) -> bool:
    """Boolean env var accepting the usual true-ish values."""
    return _env(key, lambda v: v.lower() in ("1", "true", "yes", "on"))


# ─── Dataclasses ─────────────────────────────────────────────────────────────


@dataclass
class KafkaConnectConfig:
    base_url: str          = _env("KAFKA_CONNECT_URL")
    request_timeout: float = _env("KC_REQUEST_TIMEOUT", float)
    tls_verify: bool       = _env_bool("KAFKA_CONNECT_TLS_VERIFY")
    circuit_breaker_cooldown_seconds: int = _env(
        "KC_CIRCUIT_BREAKER_COOLDOWN_SECONDS",
        int,
    )


@dataclass
class PollingConfig:
    enabled: bool           = _env_bool("CONNECTOR_HEALTH_POLLING_ENABLED")
    interval_seconds: int   = _env("POLL_INTERVAL_SECONDS", int)


@dataclass
class GrafanaWebhookConfig:
    enabled: bool = _env_bool("GRAFANA_WEBHOOK_ENABLED")
    host: str = _env("GRAFANA_WEBHOOK_HOST")
    port: int = _env("GRAFANA_WEBHOOK_PORT", int)
    path: str = _env("GRAFANA_WEBHOOK_PATH")
    auth_mode: str = _env("GRAFANA_WEBHOOK_AUTH_MODE")
    secret: str = _env("GRAFANA_WEBHOOK_SECRET")
    signature_header: str = _env("GRAFANA_WEBHOOK_SIGNATURE_HEADER")
    timestamp_header: str = _env("GRAFANA_WEBHOOK_TIMESTAMP_HEADER")
    timestamp_tolerance_seconds: int = _env(
        "GRAFANA_WEBHOOK_TIMESTAMP_TOLERANCE_SECONDS",
        int,
    )
    dedupe_ttl_seconds: int = _env("GRAFANA_WEBHOOK_DEDUPE_TTL_SECONDS", int)
    queue_size: int = _env("GRAFANA_WEBHOOK_QUEUE_SIZE", int)
    worker_count: int = _env("GRAFANA_WEBHOOK_WORKER_COUNT", int)
    recovery_followup_seconds: int = _env(
        "GRAFANA_WEBHOOK_RECOVERY_FOLLOWUP_SECONDS",
        int,
    )
    monitoring_inventory_refresh_seconds: int = _env(
        "MONITORING_INVENTORY_REFRESH_SECONDS",
        int,
    )


@dataclass
class StateMachineConfig:
    failure_confirm_checks: int       = _env("FAILURE_CONFIRM_CHECKS", int)
    task_restart_max_attempts: int    = _env("TASK_RESTART_MAX_ATTEMPTS", int)
    connector_restart_max_attempts: int = _env("CONNECTOR_RESTART_MAX_ATTEMPTS", int)
    post_restart_wait_seconds: int    = _env("POST_RESTART_WAIT_SECONDS", int)
    recovery_healthy_confirm_seconds: int = _env("RECOVERY_HEALTHY_CONFIRM_SECONDS", int)
    recreate_verify_wait_seconds: int = _env("RECREATE_VERIFY_WAIT_SECONDS", int)
    scn_poll_interval_seconds: int = _env("SCN_POLL_INTERVAL_SECONDS", int)
    recreate_keep_base_connector: bool = _env_bool("RECREATE_KEEP_BASE_CONNECTOR")


@dataclass
class TopicIdleConfig:
    """
    Topic-idleness probe — detects "no new messages written to topic X for
    N seconds" by polling Kafka end-offsets directly (not via Connect).

    Topic names are loaded from SQL Server, not from env. These values are
    defaults for DB rows that do not override them.
    """
    bootstrap_servers: list[str] = field(default_factory=lambda: _env_csv("KAFKA_BOOTSTRAP_SERVERS"))
    topic_idle_enabled: bool = _env_bool("TOPIC_LAG_TOPIC_IDLE_ENABLED")
    event_capture_enabled: bool = _env_bool("TOPIC_LAG_EVENT_CAPTURE_ENABLED")
    idle_threshold_seconds: int  = _env("TOPIC_IDLE_THRESHOLD_SECONDS", int)
    poll_interval_seconds: int   = _env("TOPIC_IDLE_POLL_INTERVAL_SECONDS", int)
    event_capture_lag_threshold_ms: int = _env("TOPIC_EVENT_CAPTURE_LAG_THRESHOLD_MS", int)


@dataclass
class MssqlConfig:
    connection_string: str = _env("MSSQL_CONNECTION_STRING")
    connection_timeout_seconds: int = _env(
        "MSSQL_CONNECTION_TIMEOUT_SECONDS",
        int,
    )


@dataclass
class LoggingConfig:
    log_level: str        = _env("LOG_LEVEL")


@dataclass
class PrometheusMetricsConfig:
    enabled: bool = _env_bool("PROMETHEUS_METRICS_ENABLED")
    host: str = _env("PROMETHEUS_METRICS_HOST")
    port: int = _env("PROMETHEUS_METRICS_PORT", int)
    sync_interval_seconds: int = _env("METRICS_SYNC_INTERVAL_SECONDS", int)


@dataclass
class AppConfig:
    kafka_connect: KafkaConnectConfig = field(default_factory=KafkaConnectConfig)
    polling: PollingConfig            = field(default_factory=PollingConfig)
    grafana_webhook: GrafanaWebhookConfig = field(default_factory=GrafanaWebhookConfig)
    state_machine: StateMachineConfig = field(default_factory=StateMachineConfig)
    mssql: MssqlConfig                = field(default_factory=MssqlConfig)
    logging: LoggingConfig            = field(default_factory=LoggingConfig)
    topic_idle: TopicIdleConfig       = field(default_factory=TopicIdleConfig)
    prometheus_metrics: PrometheusMetricsConfig = field(
        default_factory=PrometheusMetricsConfig,
    )


cfg = AppConfig()
