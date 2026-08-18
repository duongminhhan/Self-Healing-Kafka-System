import pytest

from self_healthy_kafka.healing.config_resolution import (
    resolve_connector_config,
    split_credential_value,
    split_credential_value_with_user,
)


def test_split_credential_value_uses_last_semicolon():
    value = (
        "jdbc:sqlserver://host:1433;databaseName=ods;"
        "encrypt=true;TrustServerCertificate=true;Secret#1"
    )

    database_url, password = split_credential_value(value)

    assert database_url == (
        "jdbc:sqlserver://host:1433;databaseName=ods;"
        "encrypt=true;TrustServerCertificate=true"
    )
    assert password == "Secret#1"


def test_resolve_connector_config_replaces_database_and_kafka_placeholders():
    config = {
        "database.url": "{url}",
        "database.password": "{pwd}",
        "schema.history.internal.kafka.bootstrap.servers": "{kafka_server}",
        "name": "conn-x",
    }

    resolved = resolve_connector_config(
        config,
        config_id="318",
        load_credential=lambda config_id: "jdbc:oracle:thin:@host/service;Hdp789#Etl",
        load_kafka_server=lambda: "kafka-1:9092,kafka-2:9092",
    )

    assert resolved["database.url"] == "jdbc:oracle:thin:@host/service"
    assert resolved["database.password"] == "Hdp789#Etl"
    assert (
        resolved["schema.history.internal.kafka.bootstrap.servers"]
        == "kafka-1:9092,kafka-2:9092"
    )
    assert config["database.url"] == "{url}"


def test_split_credential_value_with_user_preserves_url_semicolons():
    value = (
        "jdbc:sqlserver://host:1433;databaseName=ods;encrypt=true;"
        "APP_BIGDATA;Secret#1"
    )

    database_url, database_user, password = split_credential_value_with_user(value)

    assert database_url == (
        "jdbc:sqlserver://host:1433;databaseName=ods;encrypt=true"
    )
    assert database_user == "APP_BIGDATA"
    assert password == "Secret#1"


def test_resolve_connector_config_replaces_database_user_placeholder():
    resolved = resolve_connector_config(
        {
            "database.url": "{url}",
            "database.user": "{user}",
            "database.password": "{pwd}",
        },
        config_id="318",
        load_credential=lambda config_id: (
            "jdbc:oracle:thin:@host/service;APP_BIGDATA;Hdp789#Etl"
        ),
        load_kafka_server=lambda: None,
    )

    assert resolved["database.url"] == "jdbc:oracle:thin:@host/service"
    assert resolved["database.user"] == "APP_BIGDATA"
    assert resolved["database.password"] == "Hdp789#Etl"


def test_resolve_connector_config_requires_config_id_for_database_placeholders():
    with pytest.raises(ValueError, match="ConfigId is required"):
        resolve_connector_config(
            {"database.url": "{url}", "database.password": "{pwd}"},
            config_id=None,
            load_credential=lambda config_id: None,
            load_kafka_server=lambda: None,
        )

