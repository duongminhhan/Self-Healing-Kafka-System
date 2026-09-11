import json
from types import SimpleNamespace

import pytest
from qdrant_client import models

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.models import RagConfigurationError
from self_healthy_kafka.rag.payload_indexes import (
    PAYLOAD_INDEX_TYPES,
    inspect_payload_indexes,
)
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore


def _config() -> RagConfig:
    return RagConfig(
        enabled=True,
        qdrant_url="https://qdrant.example",
        qdrant_api_key="test-only",
        collection="healing_runbooks_v1",
        embedding_model="intfloat/multilingual-e5-small",
        embedding_size=384,
        search_mode="dense",
    )


def _info(payload_schema):
    return SimpleNamespace(
        config=SimpleNamespace(
            params=SimpleNamespace(
                vectors=models.VectorParams(size=384, distance=models.Distance.COSINE),
                sparse_vectors=None,
            )
        ),
        payload_schema=payload_schema,
    )


def _schema(**overrides):
    values = {
        field: SimpleNamespace(data_type=models.PayloadSchemaType(field_type))
        for field, field_type in PAYLOAD_INDEX_TYPES.items()
    }
    values.update(overrides)
    return values


def test_payload_index_inspection_reports_every_missing_index():
    plan = inspect_payload_indexes(
        {
            "tenant_id": SimpleNamespace(data_type=models.PayloadSchemaType.KEYWORD),
            "version": SimpleNamespace(data_type=models.PayloadSchemaType.INTEGER),
        }
    )

    assert set(plan.missing) == set(PAYLOAD_INDEX_TYPES) - {"tenant_id", "version"}
    assert not plan.incompatible
    assert not plan.valid


def test_payload_index_inspection_accepts_complete_correct_schema():
    plan = inspect_payload_indexes(_schema())

    assert plan.valid
    assert not plan.missing
    assert not plan.incompatible


def test_payload_index_inspection_reports_wrong_type():
    plan = inspect_payload_indexes(
        _schema(schema_version=SimpleNamespace(data_type=models.PayloadSchemaType.KEYWORD))
    )

    assert plan.incompatible["schema_version"].expected == "integer"
    assert plan.incompatible["schema_version"].actual == "keyword"


def test_apply_creates_only_missing_indexes_and_is_idempotent():
    class Client:
        def __init__(self):
            self.schema = _schema()
            self.schema.pop("connector_family")
            self.schema.pop("schema_version")
            self.created = []

        def collection_exists(self, name):
            return True

        def get_collection(self, name):
            return _info(self.schema)

        def create_payload_index(self, *, field_name, field_schema, **kwargs):
            self.created.append((field_name, field_schema.value))
            self.schema[field_name] = SimpleNamespace(data_type=field_schema)

    client = Client()
    store = QdrantRunbookStore(_config(), client=client)

    after, created = store.apply_missing_payload_indexes()
    second_after, second_created = store.apply_missing_payload_indexes()

    assert created == ("connector_family", "schema_version")
    assert client.created == [("connector_family", "keyword"), ("schema_version", "integer")]
    assert after.valid and second_after.valid
    assert second_created == ()


def test_apply_rejects_wrong_type_before_any_mutation():
    class Client:
        created = []

        def collection_exists(self, name):
            return True

        def get_collection(self, name):
            return _info(
                _schema(version=SimpleNamespace(data_type=models.PayloadSchemaType.KEYWORD))
            )

        def create_payload_index(self, **kwargs):
            self.created.append(kwargs)

    client = Client()
    with pytest.raises(RagConfigurationError, match="wrong type"):
        QdrantRunbookStore(_config(), client=client).apply_missing_payload_indexes()
    assert client.created == []


def test_read_only_validation_classifies_missing_index_as_configuration_error():
    class Client:
        def get_collection(self, name):
            return _info({})

    with pytest.raises(RagConfigurationError, match="missing:"):
        QdrantRunbookStore(_config(), client=Client()).validate_collection_schema()


def test_migration_cli_dry_run_does_not_apply(monkeypatch, tmp_path, capsys):
    from scripts import migrate_qdrant_payload_indexes as command

    env_file = tmp_path / "dev.env"
    env_file.write_text("RAG_ENABLED=true\n", encoding="utf-8")
    plan = inspect_payload_indexes({})

    class Store:
        def __init__(self, config):
            self.apply_count = 0

        def inspect_payload_indexes(self):
            return plan

        def apply_missing_payload_indexes(self):
            self.apply_count += 1
            raise AssertionError("dry-run must not mutate Qdrant")

    monkeypatch.setattr(command, "RagConfig", _config)
    monkeypatch.setattr(command, "QdrantRunbookStore", Store)

    assert command.main(["--env-file", str(env_file)]) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["dry_run"] is True
    assert output["applied"] is False
    assert len(output["planned_changes"]) == len(PAYLOAD_INDEX_TYPES)
