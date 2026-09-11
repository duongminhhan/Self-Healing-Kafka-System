from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal


class RagError(RuntimeError):
    """Base error for the runbook retrieval boundary."""


class RagConfigurationError(RagError):
    """The RAG feature was enabled without a usable configuration."""


class RunbookValidationError(RagError):
    """A source runbook does not satisfy the approved Markdown contract."""


class RagStoreError(RagError):
    """Qdrant could not complete a bounded store operation."""


class Route(str, Enum):
    ANALYTICS = "analytics"
    RUNBOOK = "runbook"
    COMBINED = "combined"


@dataclass(frozen=True)
class RunbookMetadata:
    runbook_id: str
    title: str
    version: int
    status: str
    connector_class: str
    error_codes: tuple[str, ...]
    environments: tuple[str, ...]
    owners: tuple[str, ...]
    updated_at: str
    tenant_id: str = "default"
    connector_type: str = ""
    connector_family: str = ""
    subsystem: str = ""
    symptoms: tuple[str, ...] = ()
    exception_classes: tuple[str, ...] = ()
    config_keys: tuple[str, ...] = ()
    error_signatures: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    user_phrases_vi: tuple[str, ...] = ()
    user_phrases_en: tuple[str, ...] = ()
    schema_version: int = 1


@dataclass(frozen=True)
class RunbookSection:
    name: str
    title: str
    text: str


@dataclass(frozen=True)
class RunbookDocument:
    metadata: RunbookMetadata
    source: str
    sections: tuple[RunbookSection, ...]


@dataclass(frozen=True)
class RunbookChunk:
    point_id: str
    content_hash: str
    tenant_id: str
    runbook_id: str
    title: str
    version: int
    status: str
    connector_class: str
    error_codes: tuple[str, ...]
    environments: tuple[str, ...]
    owners: tuple[str, ...]
    section: str
    section_title: str
    chunk_index: int
    source: str
    updated_at: str
    text: str
    connector_type: str = ""
    connector_family: str = ""
    subsystem: str = ""
    symptoms: tuple[str, ...] = ()
    exception_classes: tuple[str, ...] = ()
    config_keys: tuple[str, ...] = ()
    error_signatures: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    user_phrases_vi: tuple[str, ...] = ()
    user_phrases_en: tuple[str, ...] = ()
    schema_version: int = 1

    def payload(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "runbook_id": self.runbook_id,
            "title": self.title,
            "version": self.version,
            "status": self.status,
            "connector_class": self.connector_class,
            "connector_type": self.connector_type,
            "connector_family": self.connector_family,
            "subsystem": self.subsystem,
            "error_codes": list(self.error_codes),
            "symptoms": list(self.symptoms),
            "exception_classes": list(self.exception_classes),
            "config_keys": list(self.config_keys),
            "error_signatures": list(self.error_signatures),
            "aliases": list(self.aliases),
            "user_phrases_vi": list(self.user_phrases_vi),
            "user_phrases_en": list(self.user_phrases_en),
            "schema_version": self.schema_version,
            "environment": list(self.environments),
            "owners": list(self.owners),
            "section": self.section,
            "section_title": self.section_title,
            "chunk_index": self.chunk_index,
            "source": self.source,
            "updated_at": self.updated_at,
            "text": self.text,
            "content_hash": self.content_hash,
        }


@dataclass(frozen=True)
class RetrievalQuery:
    text: str
    tenant_id: str = "default"
    environment: str = "all"
    connector_class: str | None = None
    error_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RetrievedChunk:
    point_id: str
    score: float
    runbook_id: str
    title: str
    version: int
    section: str
    section_title: str
    source: str
    connector_class: str
    error_codes: tuple[str, ...]
    text: str
    tenant_id: str = ""
    status: str = ""
    environments: tuple[str, ...] = ()
    connector_type: str = ""
    connector_family: str = ""
    subsystem: str = ""
    symptoms: tuple[str, ...] = ()
    exception_classes: tuple[str, ...] = ()
    config_keys: tuple[str, ...] = ()
    error_signatures: tuple[str, ...] = ()
    aliases: tuple[str, ...] = ()
    user_phrases_vi: tuple[str, ...] = ()
    user_phrases_en: tuple[str, ...] = ()
    schema_version: int = 1

    def citation(self) -> Citation:
        return Citation(
            runbook_id=self.runbook_id,
            version=self.version,
            section=self.section,
            source=self.source,
        )


@dataclass
class SearchDiagnostics:
    """Non-sensitive retrieval telemetry for one Qdrant request."""

    search_mode: str
    collection: str
    dense_model: str
    sparse_model: str | None = None
    dense_candidate_count: int | None = None
    sparse_candidate_count: int | None = None
    fused_candidate_count: int = 0
    candidate_chunk_count: int = 0
    unique_runbook_count: int = 0
    duplicate_chunks_removed: int = 0
    selected_runbook_ids: tuple[str, ...] = ()
    payload_filter_fields: tuple[str, ...] = ()
    diversification_applied: bool = False
    selected_chunk_count: int = 0
    dense_candidate_limit: int | None = None
    sparse_candidate_limit: int | None = None
    dense_latency_ms: float | None = None
    sparse_latency_ms: float | None = None
    fusion_latency_ms: float | None = None
    fusion_fallback_reason: str | None = None
    total_retrieval_latency_ms: float = 0.0
    fallback_reason: str | None = None
    no_result_reason: str | None = None
    evidence_gate_applied: bool = False
    evidence_gate_passed: bool | None = None
    evidence_gate_reason: str | None = None
    requested_sections: tuple[str, ...] = ()
    selected_sections: tuple[str, ...] = ()
    section_coverage_applied: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "search_mode": self.search_mode,
            "collection": self.collection,
            "dense_model": self.dense_model,
            "sparse_model": self.sparse_model,
            "dense_candidate_count": self.dense_candidate_count,
            "sparse_candidate_count": self.sparse_candidate_count,
            "fused_candidate_count": self.fused_candidate_count,
            "candidate_chunk_count": self.candidate_chunk_count,
            "unique_runbook_count": self.unique_runbook_count,
            "duplicate_chunks_removed": self.duplicate_chunks_removed,
            "selected_runbook_ids": list(self.selected_runbook_ids),
            "payload_filter_fields": list(self.payload_filter_fields),
            "diversification_applied": self.diversification_applied,
            "selected_chunk_count": self.selected_chunk_count,
            "dense_candidate_limit": self.dense_candidate_limit,
            "sparse_candidate_limit": self.sparse_candidate_limit,
            "dense_latency_ms": self.dense_latency_ms,
            "sparse_latency_ms": self.sparse_latency_ms,
            "fusion_latency_ms": self.fusion_latency_ms,
            "fusion_fallback_reason": self.fusion_fallback_reason,
            "total_retrieval_latency_ms": self.total_retrieval_latency_ms,
            "fallback_reason": self.fallback_reason,
            "no_result_reason": self.no_result_reason,
            "evidence_gate_applied": self.evidence_gate_applied,
            "evidence_gate_passed": self.evidence_gate_passed,
            "evidence_gate_reason": self.evidence_gate_reason,
            "requested_sections": list(self.requested_sections),
            "selected_sections": list(self.selected_sections),
            "section_coverage_applied": self.section_coverage_applied,
        }


@dataclass(frozen=True)
class Citation:
    runbook_id: str
    version: int
    section: str
    source: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "runbook_id": self.runbook_id,
            "version": self.version,
            "section": self.section,
            "source": self.source,
        }


@dataclass(frozen=True)
class RouteDecision:
    route: Route
    connector_name: str | None = None
    connector_class: str | None = None
    error_codes: tuple[str, ...] = ()
    time_scope: str | None = None
    needs_clarification: bool = False
    clarification_question: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "route": self.route.value,
            "connector_name": self.connector_name,
            "connector_class": self.connector_class,
            "error_codes": list(self.error_codes),
            "time_scope": self.time_scope,
            "needs_clarification": self.needs_clarification,
            "clarification_question": self.clarification_question,
        }


@dataclass(frozen=True)
class ComposedAnswer:
    answer: str
    source: Literal["analytics", "runbook", "combined", "deterministic_fallback"]
    citations: tuple[Citation, ...] = ()
    fallback_reason: str | None = None


@dataclass
class IngestionReport:
    inserted: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped: int = 0
    removed: int = 0
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "inserted": self.inserted,
            "updated": self.updated,
            "unchanged": self.unchanged,
            "skipped": self.skipped,
            "removed": self.removed,
            "errors": list(self.errors),
        }
