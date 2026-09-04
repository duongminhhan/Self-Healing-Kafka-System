import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
INIT_TABLE = ROOT / "sql" / "init-table"
STORED_PROCEDURES = ROOT / "sql" / "ingest_reference" / "stored-procedures"
CONNECTOR_SEEDS = ROOT / "sql" / "ingest_reference" / "connector"
TABLE_FILES = ("ConnectorHealingQueue.sql", "ConnectorHealingLogs.sql")
PROCEDURE_NAMES = {
    "spEnqueueConnectorHealing",
    "spGetConnectorHealingQueue",
    "spGetConnectorHealingLogs",
    "spSearchConnectorHealingLogs",
    "spGetConnectorIncidentFacts",
    "spGetConnectorFailureRanking",
    "spInsertConnectorHealingLog",
    "spUpdateConnectorHealingQueue",
}


def _schema() -> str:
    return "\n".join(
        (INIT_TABLE / file_name).read_text(encoding="utf-8")
        for file_name in TABLE_FILES
    )


def _procedures() -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(STORED_PROCEDURES.glob("sp*.sql"))
    )


def test_environment_files_only_define_healing_runtime_endpoints():
    removed_keys = (
        "KAFKA_BOOTSTRAP_SERVERS",
        "TOPIC_LAG_",
        "TOPIC_IDLE_",
        "TOPIC_EVENT_CAPTURE_",
        "PROMETHEUS_METRICS_",
        "METRICS_SYNC_INTERVAL_SECONDS",
        "MONITORING_INVENTORY_REFRESH_SECONDS",
    )
    for environment in ("dev", "uat", "prod"):
        source = (ROOT / "env" / f"{environment}.env.example").read_text(
            encoding="utf-8"
        )
        assert f"APP_ENV={environment}" in source
        assert "KAFKA_CONNECT_URL=" in source
        assert "MSSQL_CONNECTION_STRING=" in source
        assert "OLLAMA_ENABLED=" in source
        for key in removed_keys:
            assert key not in source


def test_python_package_exposes_direct_runtime_command():
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    run_script = (ROOT / "scripts" / "run.sh").read_text(encoding="utf-8")

    assert 'self-healthy-kafka = "self_healthy_kafka.main:main"' in project
    assert "python -m self_healthy_kafka.main" in run_script


def test_runtime_has_no_custom_metrics_or_topic_lag_modules():
    source = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (ROOT / "src" / "self_healthy_kafka").rglob("*.py")
    )
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    for forbidden in (
        "prometheus_client",
        "start_metrics_server",
        "sync_connector_metrics",
        "DbTopicIdleProbe",
        "KafkaConsumer",
        "topic_lag_metrics",
        "record_topic_lag",
    ):
        assert forbidden not in source
    for dependency in ("prometheus-client", "kafka-python-ng", "lz4"):
        assert dependency not in project


def test_mssql_schema_contains_only_healing_tables():
    schema = _schema()
    table_files = {path.name for path in INIT_TABLE.glob("*.sql")}

    assert table_files == set(TABLE_FILES)
    assert schema.count("CREATE TABLE [ingest_reference].[dbo].") == 2
    assert "[dbo].[ConnectorHealingQueue]" in schema
    assert "[dbo].[ConnectorHealingLogs]" in schema
    assert "TopicLagJobs" not in schema
    assert "TopicLagLogs" not in schema
    assert "+07:00" not in schema
    assert "\nGO\n" not in schema


def test_mssql_indexes_reference_declared_columns():
    column_pattern = re.compile(
        r"(?im)^\s*\[([A-Za-z][A-Za-z0-9]*)\]\s+"
        r"(?:UNIQUEIDENTIFIER|VARCHAR|NVARCHAR|BIT|SMALLINT|INT|DATETIMEOFFSET)\b"
    )
    index_column_pattern = re.compile(
        r"\[([A-Za-z][A-Za-z0-9]*)\](?=\s+(?:ASC|DESC)\b|\s*[,\)])",
        flags=re.IGNORECASE,
    )
    for file_name in TABLE_FILES:
        source = (INIT_TABLE / file_name).read_text(encoding="utf-8")
        declared = set(column_pattern.findall(source))
        for index in re.split(
            r"(?i)CREATE\s+(?:UNIQUE\s+)?NONCLUSTERED\s+INDEX",
            source,
        )[1:]:
            assert set(index_column_pattern.findall(index)) <= declared


def test_runtime_stored_procedures_match_healing_repository_calls():
    procedures = _procedures()
    declared = set(
        re.findall(
            r"CREATE OR ALTER PROCEDURE dbo\.([A-Za-z0-9]+)",
            procedures,
            flags=re.IGNORECASE,
        )
    )
    called = set()
    for path in (ROOT / "src" / "self_healthy_kafka" / "storage").glob("*.py"):
        called.update(
            re.findall(
                r"EXEC dbo\.([A-Za-z0-9]+)",
                path.read_text(encoding="utf-8"),
            )
        )

    assert {name.lower() for name in declared} == {
        name.lower() for name in PROCEDURE_NAMES
    }
    assert {name.lower() for name in called} == {
        name.lower() for name in PROCEDURE_NAMES
    }


def test_stored_procedure_files_are_dbeaver_compatible():
    assert {path.name for path in STORED_PROCEDURES.glob("sp*.sql")} == {
        f"{name}.sql" for name in PROCEDURE_NAMES
    }
    for path in STORED_PROCEDURES.glob("*.sql"):
        source = path.read_text(encoding="utf-8")
        assert not source.startswith("\ufeff")
        assert not re.search(r"(?im)^\s*GO\s*$", source)
        assert not re.search(r"(?im)^\s*USE\s+", source)
        assert not re.search(r"EXEC\s*\(\s*N?'CREATE", source, flags=re.IGNORECASE)


def test_cleanup_script_drops_removed_metrics_and_topic_lag_procedures():
    cleanup = (STORED_PROCEDURES / "drop-legacy-procedures.sql").read_text(
        encoding="utf-8"
    )
    for name in (
        "spGetOperationalMetrics",
        "spGetHealingAttemptMetrics",
        "spGetHealingRecoveredMetrics",
        "spGetHealingEscalatedMetrics",
        "spGetTopicLagStateMetrics",
        "spGetTopicLagEventMetrics",
        "spGetMonitoredTopics",
        "spUpdateTopicLagState",
        "spInsertTopicLagLog",
    ):
        assert f"DROP PROCEDURE IF EXISTS dbo.{name};" in cleanup


def test_no_static_connector_registry_seeds_remain():
    scripts = sorted(CONNECTOR_SEEDS.rglob("*.sql"))
    assert not scripts
