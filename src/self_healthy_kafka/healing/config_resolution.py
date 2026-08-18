from __future__ import annotations

from copy import deepcopy
from typing import Any

DATABASE_URL_KEY = "database.url"
DATABASE_USER_KEY = "database.user"
DATABASE_PASSWORD_KEY = "database.password"
KAFKA_SERVER_KEY = "schema.history.internal.kafka.bootstrap.servers"

DATABASE_URL_PLACEHOLDER = "{url}"
DATABASE_USER_PLACEHOLDER = "{user}"
DATABASE_PASSWORD_PLACEHOLDER = "{pwd}"
KAFKA_SERVER_PLACEHOLDER = "{kafka_server}"

_PLACEHOLDERS = {
    DATABASE_URL_KEY: DATABASE_URL_PLACEHOLDER,
    DATABASE_USER_KEY: DATABASE_USER_PLACEHOLDER,
    DATABASE_PASSWORD_KEY: DATABASE_PASSWORD_PLACEHOLDER,
    KAFKA_SERVER_KEY: KAFKA_SERVER_PLACEHOLDER,
}


def split_credential_value(value: Any) -> tuple[str, str]:
    raw = str(value or "")
    database_url, separator, password = raw.rpartition(";")
    if not separator or not database_url or not password:
        raise ValueError(
            "Connector credential must have '<database.url>;<database.password>' shape"
        )
    return database_url, password


def split_credential_value_with_user(value: Any) -> tuple[str, str, str]:
    raw = str(value or "")
    parts = raw.rsplit(";", 2)
    if len(parts) != 3 or not all(parts):
        raise ValueError(
            "Connector credential must have "
            "'<database.url>;<database.user>;<database.password>' shape "
            "when {user} is used"
        )
    return parts[0], parts[1], parts[2]


def resolve_connector_config(
    config_template: dict[str, Any],
    *,
    config_id: Any,
    load_credential,
    load_kafka_server,
) -> dict[str, Any]:
    resolved = deepcopy(config_template)
    config = _inner_config(resolved)
    if not isinstance(config, dict):
        return resolved

    if _needs_database_credential(config):
        if config_id is None or str(config_id).strip() == "":
            raise ValueError("ConfigId is required when database placeholders are used")
        credential = load_credential(str(config_id))
        if config.get(DATABASE_USER_KEY) == DATABASE_USER_PLACEHOLDER:
            database_url, database_user, password = split_credential_value_with_user(
                credential
            )
            _replace(
                config,
                DATABASE_USER_KEY,
                DATABASE_USER_PLACEHOLDER,
                database_user,
            )
        else:
            database_url, password = split_credential_value(credential)
        _replace(config, DATABASE_URL_KEY, DATABASE_URL_PLACEHOLDER, database_url)
        _replace(config, DATABASE_PASSWORD_KEY, DATABASE_PASSWORD_PLACEHOLDER, password)

    if config.get(KAFKA_SERVER_KEY) == KAFKA_SERVER_PLACEHOLDER:
        kafka_server = load_kafka_server()
        if kafka_server is None or str(kafka_server).strip() == "":
            raise ValueError("Connector runtime context returned an empty Kafka server")
        config[KAFKA_SERVER_KEY] = str(kafka_server)

    return resolved


def has_external_placeholders(config_template: dict[str, Any]) -> bool:
    config = _inner_config(config_template)
    return isinstance(config, dict) and (
        _needs_database_credential(config)
        or config.get(KAFKA_SERVER_KEY) == KAFKA_SERVER_PLACEHOLDER
    )


def template_for_storage(
    runtime_config: dict[str, Any],
    original_template: dict[str, Any],
) -> dict[str, Any]:
    stored = deepcopy(runtime_config)
    stored_config = _inner_config(stored)
    original_config = _inner_config(original_template)
    if not isinstance(stored_config, dict) or not isinstance(original_config, dict):
        return stored

    for key, placeholder in _PLACEHOLDERS.items():
        if original_config.get(key) == placeholder:
            stored_config[key] = placeholder
    return stored


def _inner_config(value: dict[str, Any]) -> Any:
    config = value.get("config")
    return config if isinstance(config, dict) else value


def _needs_database_credential(config: dict[str, Any]) -> bool:
    return (
        config.get(DATABASE_URL_KEY) == DATABASE_URL_PLACEHOLDER
        or config.get(DATABASE_USER_KEY) == DATABASE_USER_PLACEHOLDER
        or config.get(DATABASE_PASSWORD_KEY) == DATABASE_PASSWORD_PLACEHOLDER
    )


def _replace(config: dict[str, Any], key: str, placeholder: str, value: str) -> None:
    if config.get(key) == placeholder:
        config[key] = value
