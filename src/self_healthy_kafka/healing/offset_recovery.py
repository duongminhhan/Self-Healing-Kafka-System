from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any


def build_replacement_connector(
    job: Mapping[str, Any],
    *,
    scn: str | None,
    preserve_schema_history: bool = False,
) -> tuple[str, dict[str, Any]]:
    current_name = job["connector_name"]
    latest_details = job.get("latest_event_details") or {}
    version_seed = latest_details.get("new_connector_name") or current_name
    connector_name = next_versioned_connector_name(str(version_seed))
    template = dict(job.get("active_config") or job.get("config_template") or {})
    if not template:
        raise ValueError("Connector runtime config is required for recreate")

    if isinstance(template.get("config"), dict):
        config = dict(template["config"])
    else:
        config = dict(template)

    config["name"] = connector_name
    if preserve_schema_history:
        if not config.get("schema.history.internal.kafka.topic"):
            raise ValueError(
                "Existing schema history topic is required for recreate with offset"
            )
    else:
        config["schema.history.internal.kafka.topic"] = f"schema-history.{connector_name}"
    if scn:
        config["snapshot.scn"] = scn
    return connector_name, config


def recreate_action(old_name: str, connector_name: str, suffix: str) -> str:
    if old_name == connector_name:
        return f"Recreated connector {connector_name} {suffix}"
    return f"Recreated connector {old_name} as {connector_name} {suffix}"


def next_versioned_connector_name(name: str) -> str:
    match = re.match(r"^(.+)\.(\d+)$", name)
    if not match:
        return f"{name}.001"
    base, sequence = match.groups()
    return f"{base}.{int(sequence) + 1:0{len(sequence)}d}"


def base_connector_name(name: str) -> str:
    match = re.match(r"^(.+)\.\d+$", name)
    return match.group(1) if match else name


def is_versioned_connector_name(name: str) -> bool:
    return bool(re.match(r"^.+\.\d+$", name))


def extract_last_scn(offsets: dict[str, Any] | None) -> tuple[str | None, str | None]:
    if not offsets:
        return None, None
    candidates: list[tuple[int, str, str]] = []
    for item in offsets.get("offsets", []):
        offset = item.get("offset") or {}
        for key in ("commit_scn", "commitScn", "scn", "snapshot_scn", "snapshotScn"):
            parsed = _parse_scn(offset.get(key))
            if parsed is not None:
                candidates.append((parsed, key, str(offset.get(key))))
    if not candidates:
        return None, None
    latest, field, raw = max(candidates, key=lambda candidate: candidate[0])
    commit_scn = raw if field in ("commit_scn", "commitScn") else None
    return str(latest), commit_scn


def has_offsets(offsets: dict[str, Any] | None) -> bool:
    return bool(offsets and offsets.get("offsets"))


def _parse_scn(value: Any) -> int | None:
    if value is None:
        return None
    match = re.match(r"^(\d+)", str(value).strip())
    if not match:
        return None
    return int(match.group(1))
