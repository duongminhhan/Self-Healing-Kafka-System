from pathlib import Path

import pytest

from self_healthy_kafka.rag.chunking import chunk_runbook, retrieval_text
from self_healthy_kafka.rag.ingestion import RunbookIndexer, parse_runbook
from self_healthy_kafka.rag.models import IngestionReport, RunbookValidationError


def _write(
    path: Path,
    *,
    status: str = "approved",
    body: str | None = None,
    runbook_id: str = "RB-TEST-001",
) -> Path:
    path.write_text(
        f"""---
runbook_id: {runbook_id}
title: Test runbook
version: 2
status: {status}
connector_class: oracle
error_codes: [ORA-01017]
environments: [uat, prod]
owners: [data-platform]
updated_at: 2026-09-07
schema_version: 2
connector_type: debezium-source
connector_family: debezium-oracle
subsystem: oracle-authentication
symptoms: [Oracle source cannot authenticate]
exception_classes: [java.sql.SQLException]
config_keys: [database.user]
error_signatures: [invalid username/password]
aliases: [Oracle login failure]
user_phrases_vi: [nguồn Oracle không đăng nhập được]
user_phrases_en: [Oracle connector login is denied]
---

## Symptoms

The task reports ORA-01017.

## Diagnostic steps

{body or '- Check the approved secret reference.\n- Confirm the database account is active.'}
""",
        encoding="utf-8",
    )
    return path


def test_parser_validates_front_matter_and_semantic_sections(tmp_path):
    document = parse_runbook(_write(tmp_path / "valid.md"), root=tmp_path)

    assert document.metadata.error_codes == ("ORA-01017",)
    assert document.metadata.connector_type == "debezium-source"
    assert document.metadata.subsystem == "oracle-authentication"
    assert document.metadata.config_keys == ("database.user",)
    assert document.metadata.schema_version == 2
    assert [section.name for section in document.sections] == ["symptoms", "diagnostic_steps"]
    assert document.source == "valid.md"


def test_parser_returns_actionable_error_for_missing_front_matter(tmp_path):
    path = tmp_path / "bad.md"
    path.write_text("## Symptoms\nBroken", encoding="utf-8")

    with pytest.raises(RunbookValidationError, match="expected YAML front matter"):
        parse_runbook(path)


def test_section_chunking_has_stable_ids_and_preserves_error_code(tmp_path):
    body = "\n\n".join(f"Step {number}: verify ORA-01017 safely." for number in range(40))
    document = parse_runbook(_write(tmp_path / "long.md", body=body), root=tmp_path)

    first = chunk_runbook(document, max_chars=500)
    second = chunk_runbook(document, max_chars=500)

    assert [item.point_id for item in first] == [item.point_id for item in second]
    assert len(first) > len(document.sections)
    assert all(len(item.text) <= 500 for item in first)
    assert any("ORA-01017" in item.text for item in first)
    assert "java.sql.SQLException" in retrieval_text(first[0])
    assert "database.user" in retrieval_text(first[0])
    assert first[0].payload()["subsystem"] == "oracle-authentication"


def test_optional_retrieval_metadata_changes_hash_but_not_stable_point_id(tmp_path):
    path = _write(tmp_path / "runbook.md")
    first = chunk_runbook(parse_runbook(path, root=tmp_path))[0]
    source = path.read_text(encoding="utf-8")
    path.write_text(
        source.replace("aliases: [Oracle login failure]", "aliases: [Oracle account rejected]"),
        encoding="utf-8",
    )
    second = chunk_runbook(parse_runbook(path, root=tmp_path))[0]

    assert first.point_id == second.point_id
    assert first.content_hash != second.content_hash


def test_retrieval_metadata_rejects_secret_values(tmp_path):
    path = _write(tmp_path / "secret.md")
    source = path.read_text(encoding="utf-8")
    path.write_text(
        source.replace(
            "aliases: [Oracle login failure]",
            "aliases: [password=my-private-password]",
        ),
        encoding="utf-8",
    )

    with pytest.raises(RunbookValidationError, match="must not contain credentials"):
        parse_runbook(path, root=tmp_path)


def test_required_metadata_rejects_secret_values(tmp_path):
    path = _write(tmp_path / "secret-owner.md")
    source = path.read_text(encoding="utf-8")
    path.write_text(
        source.replace("owners: [data-platform]", "owners: [password=my-private-password]"),
        encoding="utf-8",
    )

    with pytest.raises(RunbookValidationError, match="must not contain credentials"):
        parse_runbook(path, root=tmp_path)


def test_indexer_skips_non_approved_runbooks(tmp_path):
    root = tmp_path / "runbooks"
    root.mkdir()
    _write(root / "approved.md")
    _write(root / "draft.md", status="draft", runbook_id="RB-TEST-002")
    _write(root / "deprecated.md", status="deprecated", runbook_id="RB-TEST-003")

    class Store:
        chunks = []

        def sync(self, chunks, *, tenant_id, dry_run=False):
            self.chunks = list(chunks)
            return IngestionReport(inserted=len(self.chunks))

    store = Store()
    report = RunbookIndexer(store).index(root, dry_run=True)

    assert report.skipped == 2
    assert store.chunks
    assert {item.status for item in store.chunks} == {"approved"}


def test_indexer_fails_closed_before_mutating_when_any_runbook_is_invalid(tmp_path):
    root = tmp_path / "runbooks"
    root.mkdir()
    _write(root / "approved.md")
    (root / "invalid.md").write_text("## Symptoms\nMissing front matter", encoding="utf-8")

    class Store:
        called = False

        def sync(self, chunks, *, tenant_id, dry_run=False):
            self.called = True
            return IngestionReport()

    store = Store()
    report = RunbookIndexer(store).index(root, dry_run=False)

    assert store.called is False
    assert report.errors
