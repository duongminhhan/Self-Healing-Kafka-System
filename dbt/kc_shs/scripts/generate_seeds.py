from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_ROOT = PROJECT_ROOT / "config"
GENERATED_ROOT = PROJECT_ROOT / "seeds" / "generated"
LIST_DELIMITERS = {
    "message.key.columns": ";",
    "table.include.list": ",",
}
MASKED_CONNECTOR_PROPERTIES = {
    "database.url": "{url}",
    "database.user": "{user}",
    "database.password": "{pwd}",
    "schema.history.internal.kafka.bootstrap.servers": "{kafka_server}",
}


def load_yaml_files(directory: Path) -> list[tuple[Path, dict[str, Any]]]:
    documents: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(directory.glob("*.yml")):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"{path} must contain one YAML object")
        documents.append((path, data))
    return documents


def load_json_files(directory: Path) -> list[tuple[Path, dict[str, Any]]]:
    documents: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(directory.glob("*.json")):
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"{path} must contain one JSON object")
        documents.append((path, data))
    return documents


def require(config: dict[str, Any], key: str, path: Path) -> Any:
    value = config.get(key)
    if value is None or value == "":
        raise ValueError(f"{path} is missing required value: {key}")
    return value


def kafka_json(properties: dict[str, Any], path: Path) -> str:
    normalized: dict[str, str] = {}
    for key, value in properties.items():
        if key in MASKED_CONNECTOR_PROPERTIES:
            normalized[key] = MASKED_CONNECTOR_PROPERTIES[key]
            continue
        if isinstance(value, list):
            if key not in LIST_DELIMITERS:
                raise ValueError(f"No list delimiter configured for Kafka property {key} in {path}")
            normalized[key] = LIST_DELIMITERS[key].join(str(item) for item in value)
        elif isinstance(value, (dict, tuple)) or value is None:
            raise ValueError(f"Unsupported Kafka property value for {key} in {path}")
        else:
            normalized[key] = str(value).lower() if isinstance(value, bool) else str(value)
    return json.dumps(normalized, ensure_ascii=False, indent=2)


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def build_connector_rows() -> tuple[list[dict[str, Any]], set[str]]:
    rows: list[dict[str, Any]] = []
    references: set[str] = set()
    connector_names: set[str] = set()
    for path, config in load_json_files(CONFIG_ROOT / "connectors"):
        job_name = str(require(config, "job_name", path))
        connector_name = str(require(config, "connector_name", path))
        properties = require(config, "properties", path)
        if not isinstance(properties, dict):
            raise ValueError(f"properties must be a YAML object in {path}")
        if connector_name in connector_names:
            raise ValueError(f"Duplicate connector_name: {connector_name}")
        connector_names.add(connector_name)
        references.update((job_name, connector_name))
        deployed_name = properties.get("name")
        if deployed_name:
            references.add(str(deployed_name))
        rows.append(
            {
                "JobName": job_name,
                "ConnectorName": connector_name,
                "ConnectorType": require(config, "connector_type", path),
                "IsActive": int(bool(require(config, "is_active", path))),
                "Level": int(require(config, "level", path)),
                "ConfigTemplate": kafka_json(properties, path),
                "ConfigId": require(config, "config_id", path),
            }
        )
    rows.sort(key=lambda row: str(row["ConnectorName"]))
    return rows, references


def build_topic_rows() -> list[dict[str, Any]]:
    topic_rows: list[dict[str, Any]] = []
    topic_keys: set[tuple[str, str]] = set()

    for path, config in load_yaml_files(CONFIG_ROOT / "topics"):
        connector_reference = str(require(config, "connector_reference", path))

        topics = config.get("topics", [])
        if not isinstance(topics, list):
            raise ValueError(f"topics must be a YAML list in {path}")
        is_active = int(bool(config.get("is_active", True)))
        for topic in topics:
            key = (connector_reference, str(topic))
            if key in topic_keys:
                raise ValueError(f"Duplicate connector/topic mapping {key} in {path}")
            topic_keys.add(key)
            topic_rows.append(
                {
                    "ConnectorReference": connector_reference,
                    "TopicName": topic,
                    "IsActive": is_active,
                }
            )

    topic_rows.sort(key=lambda row: (str(row["ConnectorReference"]), str(row["TopicName"])))
    return topic_rows


def main() -> None:
    connector_rows, _ = build_connector_rows()
    topic_rows = build_topic_rows()

    write_csv(
        GENERATED_ROOT / "connector_configs.csv",
        [
            "JobName",
            "ConnectorName",
            "ConnectorType",
            "IsActive",
            "Level",
            "ConfigTemplate",
            "ConfigId",
        ],
        connector_rows,
    )
    write_csv(
        GENERATED_ROOT / "topic_lag_job_configs.csv",
        ["ConnectorReference", "TopicName", "IsActive"],
        topic_rows,
    )
    print(f"Generated {len(connector_rows)} connector configs")
    print(f"Generated {len(topic_rows)} topic lag job configs")


if __name__ == "__main__":
    main()
