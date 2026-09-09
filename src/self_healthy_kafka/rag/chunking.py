from __future__ import annotations

import hashlib
import json
import re
import uuid

from self_healthy_kafka.rag.models import RunbookChunk, RunbookDocument, RunbookSection
from self_healthy_kafka.redaction import redact_text

DEFAULT_MAX_CHUNK_CHARS = 2_400
RETRIEVAL_METADATA_LABELS = (
    ("Connector type", "connector_type"),
    ("Connector family", "connector_family"),
    ("Subsystem", "subsystem"),
    ("Error codes", "error_codes"),
    ("Exception classes", "exception_classes"),
    ("Configuration keys", "config_keys"),
    ("Error signatures", "error_signatures"),
    ("Aliases", "aliases"),
    ("Symptoms", "symptoms"),
    ("Vietnamese user phrases", "user_phrases_vi"),
    ("English user phrases", "user_phrases_en"),
)
_POINT_NAMESPACE = uuid.UUID("1696fed1-9505-49a0-9371-a88e6532fb25")
_SECTION_ALIASES = {
    "symptoms": "symptoms",
    "preconditions": "preconditions",
    "diagnostic steps": "diagnostic_steps",
    "recovery steps": "recovery_steps",
    "verification": "verification",
    "rollback": "rollback",
    "escalation": "escalation",
}


def normalize_section_name(title: str) -> str:
    normalized = re.sub(r"\s+", " ", title.strip().lower())
    return _SECTION_ALIASES.get(normalized, re.sub(r"[^a-z0-9]+", "_", normalized).strip("_"))


def stable_point_id(
    runbook_id: str,
    version: int,
    section: str,
    chunk_index: int,
    tenant_id: str = "default",
) -> str:
    identity = f"{tenant_id}:{runbook_id}:{version}:{section}:{chunk_index}"
    return str(uuid.uuid5(_POINT_NAMESPACE, identity))


def chunk_runbook(
    document: RunbookDocument,
    *,
    max_chars: int = DEFAULT_MAX_CHUNK_CHARS,
) -> list[RunbookChunk]:
    if max_chars < 400:
        raise ValueError("max_chars must be at least 400")
    chunks: list[RunbookChunk] = []
    for section in document.sections:
        for chunk_index, body in enumerate(_section_chunks(section, max_chars=max_chars)):
            prefix = (
                f"Runbook {document.metadata.runbook_id}: {document.metadata.title}\n"
                f"Section: {section.title}\n"
            )
            text = redact_text(prefix + body.strip())[:max_chars]
            digest = hashlib.sha256(
                json.dumps(
                    {
                        "tenant_id": document.metadata.tenant_id,
                        "runbook_id": document.metadata.runbook_id,
                        "title": document.metadata.title,
                        "version": document.metadata.version,
                        "status": document.metadata.status,
                        "connector_class": document.metadata.connector_class,
                        "error_codes": document.metadata.error_codes,
                        "environments": document.metadata.environments,
                        "owners": document.metadata.owners,
                        "connector_type": document.metadata.connector_type,
                        "connector_family": document.metadata.connector_family,
                        "subsystem": document.metadata.subsystem,
                        "symptoms": document.metadata.symptoms,
                        "exception_classes": document.metadata.exception_classes,
                        "config_keys": document.metadata.config_keys,
                        "error_signatures": document.metadata.error_signatures,
                        "aliases": document.metadata.aliases,
                        "user_phrases_vi": document.metadata.user_phrases_vi,
                        "user_phrases_en": document.metadata.user_phrases_en,
                        "schema_version": document.metadata.schema_version,
                        "updated_at": document.metadata.updated_at,
                        "section": section.name,
                        "section_title": section.title,
                        "chunk_index": chunk_index,
                        "source": document.source,
                        "text": text,
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            chunks.append(
                RunbookChunk(
                    point_id=stable_point_id(
                        document.metadata.runbook_id,
                        document.metadata.version,
                        section.name,
                        chunk_index,
                        document.metadata.tenant_id,
                    ),
                    content_hash=digest,
                    tenant_id=document.metadata.tenant_id,
                    runbook_id=document.metadata.runbook_id,
                    title=document.metadata.title,
                    version=document.metadata.version,
                    status=document.metadata.status,
                    connector_class=document.metadata.connector_class,
                    error_codes=document.metadata.error_codes,
                    environments=document.metadata.environments,
                    owners=document.metadata.owners,
                    section=section.name,
                    section_title=section.title,
                    chunk_index=chunk_index,
                    source=document.source,
                    updated_at=document.metadata.updated_at,
                    text=text,
                    connector_type=document.metadata.connector_type,
                    connector_family=document.metadata.connector_family,
                    subsystem=document.metadata.subsystem,
                    symptoms=document.metadata.symptoms,
                    exception_classes=document.metadata.exception_classes,
                    config_keys=document.metadata.config_keys,
                    error_signatures=document.metadata.error_signatures,
                    aliases=document.metadata.aliases,
                    user_phrases_vi=document.metadata.user_phrases_vi,
                    user_phrases_en=document.metadata.user_phrases_en,
                    schema_version=document.metadata.schema_version,
                )
            )
    return chunks


def retrieval_text(chunk: RunbookChunk) -> str:
    """Build deterministic, redacted embedding text without changing answer-facing text."""
    lines = [
        f"Runbook: {chunk.runbook_id} - {chunk.title}",
        f"Section: {chunk.section_title}",
    ]
    for label, field_name in RETRIEVAL_METADATA_LABELS:
        value = getattr(chunk, field_name)
        values = value if isinstance(value, tuple) else (value,)
        present = [str(item).strip() for item in values if str(item).strip()]
        if present:
            lines.append(f"{label}: {', '.join(present)}")
    lines.append(chunk.text)
    return redact_text("\n".join(lines))


def _section_chunks(section: RunbookSection, *, max_chars: int) -> list[str]:
    header_reserve = min(300, max_chars // 3)
    body_limit = max_chars - header_reserve
    paragraphs = [item.strip() for item in re.split(r"\n\s*\n", section.text) if item.strip()]
    result: list[str] = []
    current: list[str] = []
    current_size = 0
    for paragraph in paragraphs:
        pieces = _split_oversized_paragraph(paragraph, body_limit)
        for piece in pieces:
            extra = len(piece) + (2 if current else 0)
            if current and current_size + extra > body_limit:
                result.append("\n\n".join(current))
                current, current_size = [], 0
            current.append(piece)
            current_size += len(piece) + (2 if len(current) > 1 else 0)
    if current:
        result.append("\n\n".join(current))
    return result or [section.text[:body_limit]]


def _split_oversized_paragraph(paragraph: str, limit: int) -> list[str]:
    if len(paragraph) <= limit:
        return [paragraph]
    lines = [line.strip() for line in paragraph.splitlines() if line.strip()]
    if len(lines) > 1 and all(len(line) <= limit for line in lines):
        return lines
    pieces: list[str] = []
    remaining = paragraph
    while len(remaining) > limit:
        boundary = max(remaining.rfind(". ", 0, limit), remaining.rfind(" ", 0, limit))
        boundary = boundary + 1 if boundary >= limit // 2 else limit
        pieces.append(remaining[:boundary].strip())
        remaining = remaining[boundary:].strip()
    if remaining:
        pieces.append(remaining)
    return pieces
