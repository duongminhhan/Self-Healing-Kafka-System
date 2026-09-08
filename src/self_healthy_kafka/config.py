"""
Application configuration.

Single source of truth: env/<APP_ENV>.env.
- env/<APP_ENV>.env (gitignored) carries the complete runtime configuration.
- env/<APP_ENV>.env.example documents the required keys for that environment.
- SELF_HEALTHY_KAFKA_ENV_FILE can point to an explicit env file.
- Shell env vars are also respected.

Precedence (highest wins):
    shell env > explicit env file > env/<APP_ENV>.env

`_env(KEY, cast=int)` reads `os.environ[KEY]` and casts. If the key isn't
set anywhere (shell or the selected env file), it raises KeyError at import
time. This keeps missing deployment configuration explicit.

Adding a new tunable:
    1. Add the key to each env/<APP_ENV>.env.example template.
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
    env/<APP_ENV>.env). That guarantees deployment configuration is complete.
    """
    raw = os.getenv(key)
    if raw is None:
        raise KeyError(
            f"Required env var {key!r} is not set. Add it to the selected "
            "env/<APP_ENV>.env file or your shell environment."
        )
    return cast(raw)


def _env_bool(key: str) -> bool:
    """Boolean env var accepting the usual true-ish values."""
    return _env(key, lambda v: v.lower() in ("1", "true", "yes", "on"))


# ─── Dataclasses ─────────────────────────────────────────────────────────────


@dataclass
class KafkaConnectConfig:
    base_url: str = _env("KAFKA_CONNECT_URL")
    request_timeout: float = _env("KC_REQUEST_TIMEOUT", float)
    tls_verify: bool = _env_bool("KAFKA_CONNECT_TLS_VERIFY")
    circuit_breaker_cooldown_seconds: int = _env(
        "KC_CIRCUIT_BREAKER_COOLDOWN_SECONDS",
        int,
    )


@dataclass
class PollingConfig:
    enabled: bool = _env_bool("CONNECTOR_HEALTH_POLLING_ENABLED")
    interval_seconds: int = _env("POLL_INTERVAL_SECONDS", int)


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


@dataclass
class ChatApiConfig:
    enabled: bool = _env_bool("CHAT_API_ENABLED")
    path_prefix: str = _env("CHAT_API_PATH_PREFIX")
    token: str = _env("CHAT_API_TOKEN")
    default_limit: int = _env("CHAT_API_DEFAULT_LIMIT", int)
    max_limit: int = _env("CHAT_API_MAX_LIMIT", int)


@dataclass
class OllamaChatConfig:
    enabled: bool = _env_bool("OLLAMA_ENABLED")
    base_url: str = _env("OLLAMA_BASE_URL")
    model: str = _env("OLLAMA_MODEL")
    request_timeout_seconds: float = _env("OLLAMA_REQUEST_TIMEOUT_SECONDS", float)
    think: bool = _env_bool("OLLAMA_THINK")
    max_tokens: int = _env("OLLAMA_MAX_TOKENS", int)
    context_log_limit: int = _env("OLLAMA_CONTEXT_LOG_LIMIT", int)


@dataclass
class AnalyticsChatConfig:
    """Optional HF-backed semantic analytics, kept separate from the UI token."""

    enabled: bool = os.getenv("CHAT_ANALYTICS_ENABLED", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    timezone: str = os.getenv("CHAT_ANALYTICS_TIMEZONE", "UTC")
    hf_endpoint_url: str = os.getenv("HF_CHAT_ENDPOINT_URL", "")
    hf_token: str = os.getenv("HF_CHAT_TOKEN", "")
    hf_model_id: str = os.getenv("HF_CHAT_MODEL_ID", "")
    hf_request_timeout_seconds: float = float(os.getenv("HF_CHAT_REQUEST_TIMEOUT_SECONDS", "30"))


@dataclass
class RagConfig:
    """Opt-in Qdrant Cloud index used only for approved operational runbooks."""

    enabled: bool = os.getenv("RAG_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
    qdrant_url: str = os.getenv("QDRANT_URL", "")
    qdrant_api_key: str = os.getenv("QDRANT_API_KEY", "")
    collection: str = os.getenv("QDRANT_COLLECTION", "healing_runbooks_v1")
    embedding_model: str = os.getenv("QDRANT_EMBEDDING_MODEL", "")
    embedding_size: int = int(os.getenv("QDRANT_EMBEDDING_SIZE", "384"))
    search_mode: str = os.getenv("RAG_SEARCH_MODE", "dense").strip().lower()
    dense_vector_name: str = os.getenv("QDRANT_DENSE_VECTOR_NAME", "dense")
    sparse_vector_name: str = os.getenv("QDRANT_SPARSE_VECTOR_NAME", "sparse")
    dense_embedding_model: str = os.getenv("QDRANT_DENSE_EMBEDDING_MODEL", "")
    sparse_embedding_model: str = os.getenv("QDRANT_SPARSE_EMBEDDING_MODEL", "qdrant/bm25")
    top_k: int = int(os.getenv("RAG_TOP_K", "5"))
    request_timeout_seconds: float = float(os.getenv("RAG_REQUEST_TIMEOUT_SECONDS", "10"))
    max_retries: int = int(os.getenv("RAG_MAX_RETRIES", "1"))
    score_threshold: float = float(os.getenv("RAG_SCORE_THRESHOLD", "0.55"))
    dense_score_threshold: float | None = (
        float(os.environ["RAG_DENSE_SCORE_THRESHOLD"])
        if os.getenv("RAG_DENSE_SCORE_THRESHOLD", "").strip()
        else None
    )
    sparse_score_threshold: float | None = (
        float(os.environ["RAG_SPARSE_SCORE_THRESHOLD"])
        if os.getenv("RAG_SPARSE_SCORE_THRESHOLD", "").strip()
        else None
    )
    dense_candidate_limit: int = int(os.getenv("RAG_DENSE_CANDIDATE_LIMIT", "0"))
    sparse_candidate_limit: int = int(os.getenv("RAG_SPARSE_CANDIDATE_LIMIT", "0"))
    fusion_method: str = os.getenv("RAG_FUSION_METHOD", "rrf").strip().lower()
    fusion_limit: int = int(os.getenv("RAG_FUSION_LIMIT", "0"))
    hybrid_fallback_to_dense: bool = os.getenv("RAG_HYBRID_FALLBACK_TO_DENSE", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    max_chunk_chars: int = int(os.getenv("RAG_MAX_CHUNK_CHARS", "2400"))
    max_context_chars: int = int(os.getenv("RAG_MAX_CONTEXT_CHARS", "8000"))
    environment: str = os.getenv("RAG_ENVIRONMENT", "all")
    tenant_id: str = os.getenv("RAG_TENANT_ID", "default")
    diagnostics_enabled: bool = os.getenv("RAG_DIAGNOSTICS_ENABLED", "false").lower() in {
        "1",
        "true",
        "yes",
        "on",
    }

    @property
    def effective_dense_embedding_model(self) -> str:
        """Use the explicit hybrid model or the legacy dense model."""
        return self.dense_embedding_model.strip() or self.embedding_model.strip()

    @property
    def effective_dense_score_threshold(self) -> float:
        return (
            self.dense_score_threshold
            if self.dense_score_threshold is not None
            else self.score_threshold
        )

    @property
    def effective_dense_candidate_limit(self) -> int:
        return self.dense_candidate_limit or min(self.top_k * 3, 50)

    @property
    def effective_sparse_candidate_limit(self) -> int:
        return self.sparse_candidate_limit or min(self.top_k * 3, 50)

    @property
    def effective_fusion_limit(self) -> int:
        return self.fusion_limit or min(self.top_k * 3, 50)

    def validate(self) -> None:
        if not self.enabled:
            return
        missing = [
            name
            for name, value in (
                ("QDRANT_URL", self.qdrant_url),
                ("QDRANT_API_KEY", self.qdrant_api_key),
                ("QDRANT_EMBEDDING_MODEL", self.effective_dense_embedding_model),
            )
            if not value.strip()
        ]
        if missing:
            raise ValueError("RAG is enabled but configuration is missing: " + ", ".join(missing))
        if not self.qdrant_url.lower().startswith("https://"):
            raise ValueError("QDRANT_URL must use HTTPS when RAG is enabled")
        if not self.collection.strip():
            raise ValueError("QDRANT_COLLECTION must not be empty")
        if self.search_mode not in {"dense", "hybrid"}:
            raise ValueError("RAG_SEARCH_MODE must be dense or hybrid")
        if self.search_mode == "hybrid":
            hybrid_missing = [
                name
                for name, value in (
                    ("QDRANT_DENSE_VECTOR_NAME", self.dense_vector_name),
                    ("QDRANT_SPARSE_VECTOR_NAME", self.sparse_vector_name),
                    ("QDRANT_SPARSE_EMBEDDING_MODEL", self.sparse_embedding_model),
                )
                if not value.strip()
            ]
            if hybrid_missing:
                raise ValueError(
                    "Hybrid RAG configuration is missing: " + ", ".join(hybrid_missing)
                )
            if self.dense_vector_name == self.sparse_vector_name:
                raise ValueError("Dense and sparse vector names must be different")
            if self.fusion_method != "rrf":
                raise ValueError("RAG_FUSION_METHOD currently supports only rrf")
        if self.embedding_size < 1:
            raise ValueError("QDRANT_EMBEDDING_SIZE must be positive")
        if not 1 <= self.top_k <= 20:
            raise ValueError("RAG_TOP_K must be between 1 and 20")
        if not 0 <= self.score_threshold <= 1:
            raise ValueError("RAG_SCORE_THRESHOLD must be between 0 and 1")
        if not 0 <= self.effective_dense_score_threshold <= 1:
            raise ValueError("RAG_DENSE_SCORE_THRESHOLD must be between 0 and 1")
        if self.sparse_score_threshold is not None and self.sparse_score_threshold < 0:
            raise ValueError("RAG_SPARSE_SCORE_THRESHOLD must not be negative")
        for name, value in (
            ("RAG_DENSE_CANDIDATE_LIMIT", self.effective_dense_candidate_limit),
            ("RAG_SPARSE_CANDIDATE_LIMIT", self.effective_sparse_candidate_limit),
            ("RAG_FUSION_LIMIT", self.effective_fusion_limit),
        ):
            if not 1 <= value <= 100:
                raise ValueError(f"{name} must be between 1 and 100")
        if not 1 <= self.request_timeout_seconds <= 60:
            raise ValueError("RAG_REQUEST_TIMEOUT_SECONDS must be between 1 and 60")
        if not 0 <= self.max_retries <= 3:
            raise ValueError("RAG_MAX_RETRIES must be between 0 and 3")
        if not 400 <= self.max_chunk_chars <= 8_000:
            raise ValueError("RAG_MAX_CHUNK_CHARS must be between 400 and 8000")
        if not 800 <= self.max_context_chars <= 24_000:
            raise ValueError("RAG_MAX_CONTEXT_CHARS must be between 800 and 24000")
        if not self.environment.strip() or not self.tenant_id.strip():
            raise ValueError("RAG_ENVIRONMENT and RAG_TENANT_ID must not be empty")


@dataclass
class StateMachineConfig:
    failure_confirm_checks: int = _env("FAILURE_CONFIRM_CHECKS", int)
    task_restart_max_attempts: int = _env("TASK_RESTART_MAX_ATTEMPTS", int)
    connector_restart_max_attempts: int = _env("CONNECTOR_RESTART_MAX_ATTEMPTS", int)
    post_restart_wait_seconds: int = _env("POST_RESTART_WAIT_SECONDS", int)
    recovery_healthy_confirm_seconds: int = _env("RECOVERY_HEALTHY_CONFIRM_SECONDS", int)
    recreate_verify_wait_seconds: int = _env("RECREATE_VERIFY_WAIT_SECONDS", int)
    scn_poll_interval_seconds: int = _env("SCN_POLL_INTERVAL_SECONDS", int)
    recreate_keep_base_connector: bool = _env_bool("RECREATE_KEEP_BASE_CONNECTOR")


@dataclass
class MssqlConfig:
    connection_string: str = _env("MSSQL_CONNECTION_STRING")
    connection_timeout_seconds: int = _env(
        "MSSQL_CONNECTION_TIMEOUT_SECONDS",
        int,
    )


@dataclass
class LoggingConfig:
    log_level: str = _env("LOG_LEVEL")


@dataclass
class AppConfig:
    kafka_connect: KafkaConnectConfig = field(default_factory=KafkaConnectConfig)
    polling: PollingConfig = field(default_factory=PollingConfig)
    grafana_webhook: GrafanaWebhookConfig = field(default_factory=GrafanaWebhookConfig)
    chat_api: ChatApiConfig = field(default_factory=ChatApiConfig)
    ollama_chat: OllamaChatConfig = field(default_factory=OllamaChatConfig)
    analytics_chat: AnalyticsChatConfig = field(default_factory=AnalyticsChatConfig)
    rag: RagConfig = field(default_factory=RagConfig)
    state_machine: StateMachineConfig = field(default_factory=StateMachineConfig)
    mssql: MssqlConfig = field(default_factory=MssqlConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)


cfg = AppConfig()
