"""Offline deployment contracts, not a substitute for SQL Server execution."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SQL = ROOT / "sql" / "ingest_reference"


def test_manual_bundle_matches_canonical_sql():
    bundle = (ROOT / "sql" / "mssql-stored-procedures.sql").read_text(encoding="utf-8")
    for relative in (
        "stored-procedures/spInsertConnectorHealingLog.sql",
        "stored-procedures/spGetConnectorHealingQueue.sql",
        "views/vConnectorIncidentFacts.sql",
        "stored-procedures/spGetConnectorIncidentFacts.sql",
    ):
        source = (SQL / relative).read_text(encoding="utf-8").rstrip()
        assert "EXEC(N'" + source.replace("'", "''") + "');" in bundle


def test_confirmation_has_database_lock_and_persisted_hydration():
    insert = (SQL / "stored-procedures/spInsertConnectorHealingLog.sql").read_text().lower()
    assert "with (updlock, holdlock)" in insert
    assert insert.index("begin transaction") < insert.index("with (updlock, holdlock)")
    assert insert.index("if exists") < insert.index("insert into")
    assert "[eventtype] = 'health_failed_confirmed'" in insert
    hydrate = (SQL / "stored-procedures/spGetConnectorHealingQueue.sql").read_text()
    assert "as [FailureConfirmed]" in hydrate
    assert "as [ConfirmationCount]" in hydrate


def test_recovery_projection_requires_both_success_fields():
    source = (SQL / "views/vConnectorIncidentFacts.sql").read_text()
    assert "q.[CompletedAt] AS [CompletedAt]" in source
    assert "q.[QueueStatus] = 'COMPLETED' AND q.[FinalOutcome] = 'RECOVERED'" in source
    assert "THEN q.[CompletedAt] END AS [RecoveredAt]" in source
    assert "failure.[Message] AS [ErrorMessage]" in source

    procedure = (SQL / "stored-procedures/spGetConnectorIncidentFacts.sql").read_text()
    assert "[ErrorMessage]" in procedure
    assert "@Limit > 1001" in procedure
