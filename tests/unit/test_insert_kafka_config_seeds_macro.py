from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
MACRO = ROOT / "dbt" / "kc_shs" / "macros" / "insert_kafka_config_seeds.sql"


def test_insert_kafka_config_seeds_is_insert_only_and_transactional():
    source = MACRO.read_text(encoding="utf-8")
    normalized = " ".join(source.upper().split())

    assert "INSERT INTO [INGEST_REFERENCE].[DBO].[CONNECTORS]" in normalized
    assert "INSERT INTO [INGEST_REFERENCE].[DBO].[TOPICLAGJOBS]" in normalized
    assert "BEGIN TRANSACTION" in normalized
    assert "COMMIT TRANSACTION" in normalized
    assert "WHERE NOT EXISTS" in normalized
    assert " UPDATE " not in f" {normalized} "
    assert " DELETE " not in f" {normalized} "
    assert " MERGE " not in f" {normalized} "


def test_insert_kafka_config_seeds_defaults_to_preview():
    source = MACRO.read_text(encoding="utf-8")

    assert "insert_kafka_config_seeds(dry_run=true)" in source
    assert "adapter.get_relation" in source
    assert "Skipping connector configs" in source
    assert "Skipping topic lag configs" in source
    assert "Each ConnectorReference must match exactly one connector." in source
