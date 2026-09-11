from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from self_healthy_kafka.rag.models import RagConfigurationError

# This is the sole payload-index contract used by ingestion, retrieval validation,
# migrations, promotion, and integration tests.
PAYLOAD_INDEX_TYPES: Mapping[str, str] = MappingProxyType(
    {
        "tenant_id": "keyword",
        "status": "keyword",
        "environment": "keyword",
        "connector_class": "keyword",
        "connector_type": "keyword",
        "connector_family": "keyword",
        "subsystem": "keyword",
        "error_codes": "keyword",
        "exception_classes": "keyword",
        "config_keys": "keyword",
        "runbook_id": "keyword",
        "source": "keyword",
        "version": "integer",
        "schema_version": "integer",
    }
)


@dataclass(frozen=True)
class PayloadIndexMismatch:
    expected: str
    actual: str

    def to_dict(self) -> dict[str, str]:
        return {"expected": self.expected, "actual": self.actual}


@dataclass(frozen=True)
class PayloadIndexPlan:
    existing: Mapping[str, str]
    missing: Mapping[str, str]
    incompatible: Mapping[str, PayloadIndexMismatch]

    @property
    def valid(self) -> bool:
        return not self.missing and not self.incompatible

    def to_dict(self) -> dict[str, Any]:
        return {
            "existing": dict(sorted(self.existing.items())),
            "missing": dict(sorted(self.missing.items())),
            "incompatible": {
                name: mismatch.to_dict()
                for name, mismatch in sorted(self.incompatible.items())
            },
            "valid": self.valid,
        }


def inspect_payload_indexes(payload_schema: Mapping[str, Any] | None) -> PayloadIndexPlan:
    schema = payload_schema or {}
    existing = {
        name: _payload_type(value)
        for name, value in schema.items()
    }
    missing: dict[str, str] = {}
    incompatible: dict[str, PayloadIndexMismatch] = {}
    for name, expected in PAYLOAD_INDEX_TYPES.items():
        actual = existing.get(name)
        if actual is None:
            missing[name] = expected
        elif actual != expected:
            incompatible[name] = PayloadIndexMismatch(expected=expected, actual=actual)
    return PayloadIndexPlan(
        existing=existing,
        missing=missing,
        incompatible=incompatible,
    )


def require_payload_indexes(plan: PayloadIndexPlan) -> None:
    errors: list[str] = []
    if plan.missing:
        errors.append("missing: " + ", ".join(sorted(plan.missing)))
    if plan.incompatible:
        details = ", ".join(
            f"{name} (expected {item.expected}, observed {item.actual})"
            for name, item in sorted(plan.incompatible.items())
        )
        errors.append("wrong type: " + details)
    if errors:
        raise RagConfigurationError("Qdrant payload index contract failed; " + "; ".join(errors))


def create_missing_payload_indexes(
    client: Any,
    *,
    collection: str,
    plan: PayloadIndexPlan,
) -> tuple[str, ...]:
    """Apply only missing indexes; incompatible indexes always fail closed."""

    if plan.incompatible:
        require_payload_indexes(plan)
    if not plan.missing:
        return ()

    from qdrant_client import models

    schema_types = {
        "keyword": models.PayloadSchemaType.KEYWORD,
        "integer": models.PayloadSchemaType.INTEGER,
    }
    created: list[str] = []
    for field, expected in sorted(plan.missing.items()):
        client.create_payload_index(
            collection_name=collection,
            field_name=field,
            field_schema=schema_types[expected],
            wait=True,
        )
        created.append(field)
    return tuple(created)


def _payload_type(value: Any) -> str:
    candidate = value
    if isinstance(value, Mapping):
        candidate = value.get("data_type", value.get("type", value))
    else:
        candidate = getattr(value, "data_type", value)
    candidate = getattr(candidate, "value", candidate)
    normalized = str(candidate).strip().lower()
    if "." in normalized:
        normalized = normalized.rsplit(".", 1)[-1]
    return normalized
