from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path
from typing import Any, Protocol

import yaml

from self_healthy_kafka.rag.chunking import chunk_runbook, normalize_section_name, stable_point_id
from self_healthy_kafka.rag.models import (
    IngestionReport,
    RunbookDocument,
    RunbookMetadata,
    RunbookSection,
    RunbookValidationError,
)

REQUIRED_FRONT_MATTER = {
    "runbook_id",
    "title",
    "version",
    "status",
    "connector_class",
    "error_codes",
    "environments",
    "owners",
    "updated_at",
}
ALLOWED_STATUSES = {"approved", "draft", "deprecated"}
SEMANTIC_SECTIONS = {
    "symptoms",
    "preconditions",
    "diagnostic_steps",
    "recovery_steps",
    "verification",
    "rollback",
    "escalation",
}


class SyncStore(Protocol):
    def sync(self, chunks, *, tenant_id: str, dry_run: bool = False) -> IngestionReport: ...


def parse_runbook(path: Path, *, root: Path | None = None) -> RunbookDocument:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise RunbookValidationError(f"{path}: cannot read runbook: {exc}") from exc
    match = re.match(r"\A---\s*\n(.*?)\n---\s*\n(.*)\Z", raw, flags=re.DOTALL)
    if not match:
        raise RunbookValidationError(f"{path}: expected YAML front matter between --- markers")
    try:
        front_matter = yaml.safe_load(match.group(1))
    except yaml.YAMLError as exc:
        raise RunbookValidationError(f"{path}: invalid YAML front matter: {exc}") from exc
    if not isinstance(front_matter, dict):
        raise RunbookValidationError(f"{path}: front matter must be an object")
    missing = sorted(REQUIRED_FRONT_MATTER - set(front_matter))
    if missing:
        raise RunbookValidationError(f"{path}: missing front matter: {', '.join(missing)}")
    metadata = _metadata(front_matter, path)
    sections = _parse_sections(match.group(2), path)
    source = (
        path.as_posix() if root is None else path.resolve().relative_to(root.resolve()).as_posix()
    )
    return RunbookDocument(metadata=metadata, source=source, sections=tuple(sections))


def discover_runbooks(root: Path) -> tuple[list[RunbookDocument], list[str]]:
    documents: list[RunbookDocument] = []
    errors: list[str] = []
    for path in sorted(root.rglob("*.md")):
        try:
            documents.append(parse_runbook(path, root=root.parent))
        except RunbookValidationError as exc:
            errors.append(str(exc))
    identities: dict[tuple[str, int, str], str] = {}
    for document in documents:
        identity = (
            document.metadata.runbook_id,
            document.metadata.version,
            document.metadata.tenant_id,
        )
        previous = identities.get(identity)
        if previous is not None:
            errors.append(
                f"{document.source}: duplicate runbook_id/version/tenant also defined by {previous}"
            )
        else:
            identities[identity] = document.source
    return documents, errors


class RunbookIndexer:
    def __init__(self, store: SyncStore, *, max_chunk_chars: int = 2_400):
        self._store = store
        self._max_chunk_chars = max_chunk_chars

    def index(
        self, root: Path, *, tenant_id: str = "default", dry_run: bool = False
    ) -> IngestionReport:
        documents, errors = discover_runbooks(root)
        approved = [item for item in documents if item.metadata.status == "approved"]
        skipped = len(documents) - len(approved)
        if errors and not dry_run:
            return IngestionReport(skipped=skipped, errors=errors)
        chunks = [
            replace(
                chunk,
                tenant_id=tenant_id,
                point_id=stable_point_id(
                    chunk.runbook_id,
                    chunk.version,
                    chunk.section,
                    chunk.chunk_index,
                    tenant_id,
                ),
            )
            for document in approved
            for chunk in chunk_runbook(document, max_chars=self._max_chunk_chars)
        ]
        report = self._store.sync(chunks, tenant_id=tenant_id, dry_run=dry_run)
        report.skipped += skipped
        report.errors.extend(errors)
        return report


def _metadata(value: dict[str, Any], path: Path) -> RunbookMetadata:
    def required_text(key: str) -> str:
        result = str(value.get(key) or "").strip()
        if not result:
            raise RunbookValidationError(f"{path}: {key} must be a non-empty string")
        return result

    status = required_text("status").lower()
    if status not in ALLOWED_STATUSES:
        raise RunbookValidationError(
            f"{path}: status must be one of {', '.join(sorted(ALLOWED_STATUSES))}"
        )
    try:
        version = int(value["version"])
    except (TypeError, ValueError) as exc:
        raise RunbookValidationError(f"{path}: version must be a positive integer") from exc
    if version < 1:
        raise RunbookValidationError(f"{path}: version must be a positive integer")
    updated_at = required_text("updated_at")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", updated_at):
        raise RunbookValidationError(f"{path}: updated_at must use YYYY-MM-DD")
    return RunbookMetadata(
        runbook_id=required_text("runbook_id"),
        title=required_text("title"),
        version=version,
        status=status,
        connector_class=required_text("connector_class").lower(),
        error_codes=_string_list(value.get("error_codes"), "error_codes", path, uppercase=True),
        environments=_string_list(value.get("environments"), "environments", path),
        owners=_string_list(value.get("owners"), "owners", path),
        updated_at=updated_at,
        tenant_id=str(value.get("tenant_id") or "default").strip(),
    )


def _string_list(value: Any, name: str, path: Path, *, uppercase: bool = False) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise RunbookValidationError(f"{path}: {name} must be a non-empty list")
    result = tuple(str(item).strip() for item in value if str(item).strip())
    if not result:
        raise RunbookValidationError(f"{path}: {name} must contain non-empty strings")
    return (
        tuple(item.upper() for item in result)
        if uppercase
        else tuple(item.lower() for item in result)
    )


def _parse_sections(body: str, path: Path) -> list[RunbookSection]:
    matches = list(re.finditer(r"(?m)^##\s+(.+?)\s*$", body))
    if not matches:
        raise RunbookValidationError(f"{path}: expected at least one level-2 semantic section")
    sections: list[RunbookSection] = []
    for index, match in enumerate(matches):
        title = match.group(1).strip()
        name = normalize_section_name(title)
        end = matches[index + 1].start() if index + 1 < len(matches) else len(body)
        text = body[match.end() : end].strip()
        if name in SEMANTIC_SECTIONS and text:
            sections.append(RunbookSection(name=name, title=title, text=text))
    if not sections:
        raise RunbookValidationError(
            f"{path}: no supported semantic section with content was found"
        )
    return sections
