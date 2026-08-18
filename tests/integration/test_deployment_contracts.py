import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
FIXED_VIETNAM_OFFSET = "+" + "07:00"
INIT_TABLE = ROOT / "sql" / "init-table"
INGEST_REFERENCE = ROOT / "sql" / "ingest_reference"
STORED_PROCEDURES = INGEST_REFERENCE / "stored-procedures"
CONNECTOR_SEEDS = INGEST_REFERENCE / "connector"
TOPIC_JOB_SEEDS = INGEST_REFERENCE / "topic"
TABLE_FILES = (
    "Connectors.sql",
    "ConnectorHealingLogs.sql",
    "TopicLagJobs.sql",
    "TopicLagLogs.sql",
)


def read_schema() -> str:
    return "\n".join(
        (INIT_TABLE / file_name).read_text(encoding="utf-8") for file_name in TABLE_FILES
    )


def read_stored_procedures() -> str:
    return "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(STORED_PROCEDURES.glob("sp*.sql"))
    )


def test_environment_files_define_direct_runtime_endpoints():
    for environment in ("dev", "uat", "prod"):
        env_file = (ROOT / "env" / f"{environment}.env.example").read_text(encoding="utf-8")

        assert f"APP_ENV={environment}" in env_file
        assert "KAFKA_CONNECT_URL=" in env_file
        assert "KAFKA_CONNECT_TLS_VERIFY=" in env_file
        assert "KAFKA_BOOTSTRAP_SERVERS=" in env_file
        assert "MSSQL_CONNECTION_STRING=" in env_file


def test_python_package_exposes_direct_runtime_command():
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    run_script = (ROOT / "scripts" / "run.sh").read_text(encoding="utf-8")

    assert 'name = "self-healthy-kafka"' in project
    assert 'self-healthy-kafka = "self_healthy_kafka.main:main"' in project
    assert "python -m self_healthy_kafka.main" in run_script


def test_application_has_no_runtime_migration_reference():
    source = (ROOT / "src" / "self_healthy_kafka" / "startup.py").read_text(encoding="utf-8")

    assert "apply_migrations" not in source


def test_mssql_schema_follows_pascal_case_naming_convention():
    schema = read_schema()

    for table in (
        "Connectors",
        "ConnectorHealingLogs",
        "TopicLagJobs",
        "TopicLagLogs",
    ):
        assert f"CREATE TABLE [ingest_reference].[dbo].[{table}]" in schema
    assert "[ConnectorName] VARCHAR(255)" in schema
    assert "[Level] SMALLINT NOT NULL" in schema
    assert "[ConfigId] VARCHAR(100)" in schema
    assert "dbo.ConnectorCredentialConfigs" not in schema
    assert "dbo.KafkaServerConfig" not in schema
    assert "DEFAULT NEWSEQUENTIALID()" in schema
    assert "DATETIMEOFFSET(3)" in schema
    assert schema.count("DEFAULT SYSDATETIMEOFFSET()") == 4
    assert "CONSTRAINT DF_" not in schema
    assert FIXED_VIETNAM_OFFSET not in schema
    assert "\nGO\n" not in schema


def test_mssql_schema_is_split_into_one_file_per_table():
    schema = read_schema()

    assert schema.count("CREATE TABLE [ingest_reference].[dbo].") == 4
    assert {path.name for path in INIT_TABLE.glob("*.sql") if path.name in TABLE_FILES} == set(
        TABLE_FILES
    )
    for file_name in TABLE_FILES:
        source = (INIT_TABLE / file_name).read_text(encoding="utf-8")
        assert source.count("CREATE TABLE [ingest_reference].[dbo].") == 1
        table_name = Path(file_name).stem
        assert f"[dbo].[{table_name}]" in source
    assert "COL_LENGTH(" not in schema
    assert "ALTER TABLE" not in schema
    assert "DECLARE @VietnamTimestampDefaults" not in schema
    assert "CURSOR" not in schema
    assert "UPDATE dbo." not in schema


def test_mssql_indexes_reference_columns_declared_by_their_table():
    column_pattern = re.compile(
        r"(?im)^\s*\[([A-Za-z][A-Za-z0-9]*)\]\s+"
        r"(?:UNIQUEIDENTIFIER|VARCHAR|NVARCHAR|BIT|SMALLINT|INT|BIGINT|DATETIMEOFFSET)\b"
    )
    index_column_pattern = re.compile(
        r"\[([A-Za-z][A-Za-z0-9]*)\](?=\s+(?:ASC|DESC)\b|\s*[,\)])",
        flags=re.IGNORECASE,
    )

    for file_name in TABLE_FILES:
        source = (INIT_TABLE / file_name).read_text(encoding="utf-8")
        declared_columns = set(column_pattern.findall(source))
        index_sections = re.split(
            r"(?i)CREATE\s+(?:UNIQUE\s+)?NONCLUSTERED\s+INDEX",
            source,
        )[1:]

        for index_section in index_sections:
            referenced_columns = set(index_column_pattern.findall(index_section))
            assert referenced_columns <= declared_columns, (
                file_name,
                referenced_columns - declared_columns,
            )


def test_runtime_sql_does_not_force_a_fixed_timezone_offset():
    sql_sources = [
        ("table schemas", read_schema()),
        ("stored-procedures", read_stored_procedures()),
        *[
            (path.name, path.read_text(encoding="utf-8"))
            for path in sorted(TOPIC_JOB_SEEDS.rglob("*.sql"))
        ],
    ]

    for name, source in sql_sources:
        assert FIXED_VIETNAM_OFFSET not in source, name


def test_message_event_timestamps_are_not_rewritten_by_sql_contract():
    schema = read_schema()
    procedures = read_stored_procedures().lower()

    assert "ELSE SWITCHOFFSET([LastMessageAt]" not in schema
    assert "dateadd(hour, -7, t.[lastmessageat])" not in procedures
    assert "[lastmessageat] = switchoffset(@lastmessageat" not in procedures
    assert "[lastmessageat] = @lastmessageat" in procedures


def test_mssql_stored_procedures_follow_sp_verb_noun_format():
    procedures = read_stored_procedures()
    names = re.findall(
        r"CREATE OR ALTER PROCEDURE dbo\.([A-Za-z0-9]+)",
        procedures,
        flags=re.IGNORECASE,
    )
    expected_names = {
        "spGetConnectorContext",
        "spUpdateConnector",
        "spInsertConnectorHealingLog",
        "spGetOperationalMetrics",
        "spGetHealingAttemptMetrics",
        "spGetHealingRecoveredMetrics",
        "spGetHealingEscalatedMetrics",
        "spGetTopicLagStateMetrics",
        "spGetTopicLagEventMetrics",
        "spGetMonitoredTopics",
        "spUpdateTopicLagState",
        "spInsertTopicLagLog",
    }

    assert len(names) == len(expected_names)
    assert {name.lower() for name in names} == {name.lower() for name in expected_names}
    assert "update [dbo].[topiclagjobs]" in procedures.lower()
    assert "[isactive] = 0" in procedures.lower()

    assert {path.name for path in STORED_PROCEDURES.glob("sp*.sql")} == {
        f"{name}.sql" for name in expected_names
    }
    for path in STORED_PROCEDURES.glob("sp*.sql"):
        source = path.read_text(encoding="utf-8")
        declared = re.findall(
            r"CREATE OR ALTER PROCEDURE dbo\.([A-Za-z0-9]+)",
            source,
            flags=re.IGNORECASE,
        )
        assert [name.lower() for name in declared] == [path.stem.lower()]


def test_mssql_stored_procedure_files_are_standalone_and_drop_legacy_contract():
    procedure_files = sorted(STORED_PROCEDURES.glob("sp*.sql"))
    cleanup = (STORED_PROCEDURES / "drop-legacy-procedures.sql").read_text(encoding="utf-8")
    legacy_names = {
        "spGetConnectors",
        "spGetConnectorActivity",
        "spGetConnectorByName",
        "spGetConnectorById",
        "spGetLatestConnectorIncident",
        "spGetLatestConnectorHealingLog",
        "spGetConnectorHealingAttemptCounts",
        "spGetTopicLagMetrics",
        "spGetActiveTopicLagJobs",
        "spUpdateTopicLagJobs",
        "spGetCredentialConnector",
        "spGetKafkaServer",
    }

    for path in procedure_files:
        source = path.read_text(encoding="utf-8")
        assert source.lower().startswith("create or alter procedure dbo.")
        assert source.startswith(f"CREATE OR ALTER PROCEDURE dbo.{path.stem}")
        assert source.rstrip().endswith("END;")
        assert "EXEC(N'CREATE OR ALTER PROCEDURE" not in source
        assert "\nGO\n" not in source.upper()
    assert "SET XACT_ABORT ON" in cleanup
    assert "BEGIN TRANSACTION" in cleanup
    assert "COMMIT TRANSACTION" in cleanup
    assert "ROLLBACK TRANSACTION" in cleanup
    for name in legacy_names:
        assert f"DROP PROCEDURE IF EXISTS dbo.{name};" in cleanup


def test_stored_procedure_directory_is_dbeaver_script_compatible():
    for path in STORED_PROCEDURES.glob("*.sql"):
        source = path.read_text(encoding="utf-8")
        assert not source.startswith("\ufeff")
        assert not re.search(r"(?im)^\s*GO\s*$", source)
        assert not re.search(r"(?im)^\s*USE\s+", source)
        assert not re.search(r"EXEC\s*\(\s*N?'CREATE", source, flags=re.IGNORECASE)
        assert not re.search(r"(?<![A-Za-z0-9_])n'", source)
        for line in source.splitlines():
            assert line.count("'") % 2 == 0, f"unbalanced SQL literal in {path.name}: {line}"


def test_topic_lag_event_metrics_are_db_backed_from_topic_lag_logs():
    raw_procedure = (STORED_PROCEDURES / "spGetTopicLagEventMetrics.sql").read_text(
        encoding="utf-8"
    )
    procedure = raw_procedure.lower()

    assert "from [dbo].[topiclaglogs] as l" in procedure
    assert "join [dbo].[connectors] as c" in procedure
    assert "join [dbo].[topiclagjobs] as t" in procedure
    assert "where c.[isactive] = 1" in procedure
    assert "and t.[isactive] = 1" in procedure
    assert "json_value(l.[details], '$.lag_condition')" in procedure
    assert "nullif(json_value(l.[details], '$.lag_condition'), '')" in procedure
    assert "n'topic_idle'" not in raw_procedure
    assert raw_procedure.count("N'topic_idle'") == 2
    assert "count_big(1) as [totalevents]" in procedure
    assert "sysdatetimeoffset()" in procedure
    assert FIXED_VIETNAM_OFFSET not in procedure


def test_healing_attempt_metrics_include_all_steps_for_all_connectors():
    procedure = (
        (STORED_PROCEDURES / "spGetHealingAttemptMetrics.sql").read_text(encoding="utf-8").lower()
    )

    for event_type in (
        "health_failed_confirmed",
        "task_restart",
        "connector_restart",
        "connector_recreate_with_offset",
        "connector_recreate_without_offset",
    ):
        assert f"'{event_type}'" in procedure
    assert "cross join @healingevents as e" in procedure
    assert "left join [dbo].[connectorhealinglogs] as l" in procedure
    assert "on l.[connectorid] = c.[id]" in procedure
    assert "on l.[connectorname] = c.[connectorname]" not in procedure
    assert "coalesce(nullif(c.[jobname], ''), c.[connectorname])" in procedure
    assert "as [baseconnectorname]" in procedure
    assert "sum(convert(bigint, case when l.[id] is null then 0 else 1 end))" in procedure
    assert "where c.[isactive] = 1" not in procedure


def test_healing_recovered_metrics_use_current_active_connector_and_window():
    procedure = (
        (STORED_PROCEDURES / "spGetHealingRecoveredMetrics.sql").read_text(encoding="utf-8").lower()
    )

    assert "from [dbo].[connectors] as c" in procedure
    assert "join [dbo].[connectorhealinglogs] as l" in procedure
    assert "on l.[connectorid] = c.[id]" in procedure
    assert "where c.[isactive] = 1" in procedure
    assert "l.[createdat] >= dateadd(" in procedure
    assert "-@effectivewindowminutes" in procedure
    assert "c.[id]," in procedure
    assert "c.[connectorname]" in procedure


def test_operational_metrics_compatibility_wrapper_delegates_to_focused_procedures():
    procedure = (STORED_PROCEDURES / "spGetOperationalMetrics.sql").read_text(encoding="utf-8")

    for name in (
        "spGetHealingAttemptMetrics",
        "spGetHealingRecoveredMetrics",
        "spGetHealingEscalatedMetrics",
        "spGetTopicLagStateMetrics",
        "spGetTopicLagEventMetrics",
    ):
        assert f"execute dbo.{name}" in procedure
    assert "from [dbo].[ConnectorHealingLogs]" not in procedure
    assert "from [dbo].[TopicLagLogs]" not in procedure


def test_operational_metrics_use_locale_independent_epoch_and_null_safe_aggregates():
    procedures = read_stored_procedures().lower()

    assert "datetimeoffsetfromparts(" in procedures
    assert "1970-01-01" not in procedures
    assert "count_big(case" not in procedures
    assert "then 1 end" not in procedures


def test_healing_event_filters_match_application_event_type_case():
    procedures = read_stored_procedures()

    for event_type in (
        "HEALING_RECOVERED",
        "HEALING_ESCALATED",
        "HEALING_LEVEL_LIMIT_REACHED",
        "STATE_MACHINE_ERROR",
        "TASK_RESTART",
        "CONNECTOR_RESTART",
        "CONNECTOR_RECREATE_WITH_OFFSET",
        "CONNECTOR_RECREATE_WITH_OFFSET_FAILED",
        "CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT",
        "CONNECTOR_RECREATE_WITHOUT_OFFSET",
        "CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED",
    ):
        assert f"'{event_type}'" in procedures


def test_monitored_topics_includes_over_threshold_jobs_for_recovery_checks():
    procedures = read_stored_procedures().lower()

    assert "and (c.[failedcount] = 0 or t.[isoverthreshold] = 1)" in procedures
    monitored_topics = (
        (STORED_PROCEDURES / "spGetMonitoredTopics.sql").read_text(encoding="utf-8").lower()
    )
    assert "configtemplate" not in monitored_topics


def test_connector_context_returns_runtime_config_only_when_requested():
    connector_context = (
        (STORED_PROCEDURES / "spGetConnectorContext.sql").read_text(encoding="utf-8").lower()
    )

    assert "case when @includeruntimeconfig = 1" in connector_context
    assert "then c.[configtemplate]" in connector_context
    assert "c.*" not in connector_context


def test_python_calls_only_declared_stored_procedures():
    procedure_sql = read_stored_procedures()
    declared = set(
        name.lower()
        for name in re.findall(
            r"CREATE OR ALTER PROCEDURE dbo\.([A-Za-z0-9]+)",
            procedure_sql,
            flags=re.IGNORECASE,
        )
    )
    called = set()
    for path in (ROOT / "src" / "self_healthy_kafka" / "storage").glob("*.py"):
        called.update(
            name.lower()
            for name in re.findall(
                r"EXEC dbo\.([A-Za-z0-9]+)",
                path.read_text(encoding="utf-8"),
            )
        )

    assert called
    assert declared == called | {"spgetoperationalmetrics"}


def test_connector_and_topic_lag_job_seed_scripts_are_separated():
    connector_scripts = sorted(
        path for path in CONNECTOR_SEEDS.rglob("*.sql") if "topic-lag-job-inserts" not in path.parts
    )
    topic_job_scripts = sorted(TOPIC_JOB_SEEDS.rglob("*.sql"))

    assert connector_scripts
    assert topic_job_scripts
    for path in connector_scripts:
        source = path.read_text(encoding="utf-8")
        connector_inserts = re.findall(
            r"INSERT INTO\s+(?:\[dbo\]\.|dbo\.)Connectors",
            source,
            flags=re.IGNORECASE,
        )
        assert connector_inserts, path
        assert "TopicLagJobs" not in source, path
        assert "DECLARE " not in source, path
        assert "BEGIN TRANSACTION" not in source, path
        assert not re.search(
            r"UPDATE\s+(?:\[dbo\]\.|dbo\.)Connectors",
            source,
            flags=re.IGNORECASE,
        ), path

    for path in topic_job_scripts:
        source = path.read_text(encoding="utf-8")
        assert "[dbo].[TopicLagJobs]" in source, path
        assert "INSERT INTO [dbo].[Connectors]" not in source, path


def test_connector_seeds_are_grouped_into_one_file_per_source_system():
    expected_files = {
        "OTM-TMS.sql": 1,
        "TOPO-CLI.sql": 3,
        "TOPO-TCH.sql": 3,
    }

    assert {path.name for path in CONNECTOR_SEEDS.glob("*.sql")} == set(expected_files)
    for file_name, insert_count in expected_files.items():
        source = (CONNECTOR_SEEDS / file_name).read_text(encoding="utf-8")
        assert (
            len(
                re.findall(
                    r"INSERT INTO\s+(?:\[dbo\]\.|dbo\.)Connectors",
                    source,
                    flags=re.IGNORECASE,
                )
            )
            == insert_count
        )
        assert (
            len(
                re.findall(
                    r"^USE \[ingest_reference\];$",
                    source,
                    flags=re.IGNORECASE | re.MULTILINE,
                )
            )
            == 1
        )


def test_grouped_topo_tch_seed_preserves_all_suffixes_and_placeholders():
    source = (CONNECTOR_SEEDS / "TOPO-TCH.sql").read_text(encoding="utf-8")

    for connector_name in (
        "TOPO-TCH-G025",
        "TOPO-TCH-G026",
        "TOPO-TCH-G027",
    ):
        assert source.count(f"'{connector_name}'") >= 2

    assert source.count('"database.url": "{url}"') == 3
    assert source.count('"database.password": "{pwd}"') == 3
    assert source.count('"schema.history.internal.kafka.bootstrap.servers": "{kafka_server}"') == 3


def test_ingest_reference_others_is_the_only_configuration_insert_file():
    seed_dir = ROOT / "sql" / "ingest_reference"
    assert [path.name for path in seed_dir.glob("*.sql")] == ["others.sql"]

    source = (seed_dir / "others.sql").read_text(encoding="utf-8")
    assert "USE [ingest_reference]" in source
    assert (
        len(
            re.findall(
                r"INSERT INTO\s+(?:\[dbo\]\.|dbo\.)ETLConfiguration",
                source,
                flags=re.IGNORECASE,
            )
        )
        == 7
    )


def test_prometheus_uses_host_targets_and_local_rule_file():
    config_path = ROOT / "monitoring/prometheus/prometheus.yml"
    if not config_path.exists():
        pytest.skip("monitoring Prometheus config is not part of this branch")
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    assert "recording-rules.yml" in config["rule_files"]

    jobs = {item["job_name"]: item for item in config["scrape_configs"]}
    assert jobs["kafka-connect"]["static_configs"][0]["targets"] == ["127.0.0.1:9404"]
    assert jobs["self-healthy-kafka"]["static_configs"][0]["targets"] == ["127.0.0.1:8080"]


def test_grafana_alert_uses_recorded_metrics_and_or_semantics():
    rules_path = ROOT / "monitoring/grafana/provisioning/alerting/rules.yml"
    if not rules_path.exists():
        pytest.skip("monitoring Grafana rules are not part of this branch")
    rules = yaml.safe_load(rules_path.read_text(encoding="utf-8"))
    topic_group = next(
        group for group in rules["groups"] if group["name"] == "self-healthy-kafka-topic-lag"
    )
    alert = next(
        rule
        for rule in topic_group["rules"]
        if rule["uid"] == "self-healthy-kafka-topic-over-threshold"
    )
    expression = next(item for item in alert["data"] if item["refId"] == "A")["model"]["expr"]

    assert "kc_shs_max_topic_idle_minutes > 2" in expression
    assert "kc_shs_topic_source_lag_by_topic_minutes > 2" in expression
    assert " or " in expression
