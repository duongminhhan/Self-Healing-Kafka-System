import importlib.util
import csv
import json
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "dbt"
    / "kc_shs"
    / "scripts"
    / "generate_seeds.py"
)
SPEC = importlib.util.spec_from_file_location("generate_seeds", SCRIPT_PATH)
assert SPEC and SPEC.loader
GENERATE_SEEDS = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GENERATE_SEEDS)


def test_kafka_json_masks_sensitive_connector_properties_without_mutating_source():
    properties = {
        "database.url": "jdbc:oracle:thin:@//172.24.49.12:1521/TOPOVN",
        "database.user": "APP_BIGDATA",
        "database.password": "secret",
        "schema.history.internal.kafka.bootstrap.servers": "kafka-1:9092",
        "name": "TOPO-TCI-G001.003",
    }

    generated = json.loads(
        GENERATE_SEEDS.kafka_json(properties, Path("TOPO_TCI_G001.json"))
    )

    assert generated["database.url"] == "{url}"
    assert generated["database.user"] == "{user}"
    assert generated["database.password"] == "{pwd}"
    assert (
        generated["schema.history.internal.kafka.bootstrap.servers"]
        == "{kafka_server}"
    )
    assert properties["database.user"] == "APP_BIGDATA"


def test_load_config_files_allows_an_empty_directory(tmp_path):
    assert GENERATE_SEEDS.load_json_files(tmp_path / "connectors") == []
    assert GENERATE_SEEDS.load_yaml_files(tmp_path / "topics") == []


def test_write_csv_writes_header_for_an_empty_seed(tmp_path):
    output = tmp_path / "topic_lag_job_configs.csv"

    GENERATE_SEEDS.write_csv(
        output,
        ["ConnectorReference", "TopicName", "IsActive"],
        [],
    )

    with output.open(encoding="utf-8", newline="") as handle:
        assert list(csv.reader(handle)) == [
            ["ConnectorReference", "TopicName", "IsActive"]
        ]
