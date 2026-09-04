"""Synthetic gold fixtures only; callers must supply a new temporary file path."""

import sqlite3
from contextlib import closing

VARIANTS = ("normal", "no_success", "missing", "malformed", "offsets", "negative", "zero")
EXPECTED = {
    "normal": [(35.0, 2, 2, 0)],
    "no_success": [(None, 0, 0, 0)],
    "missing": [(None, 2, 0, 2)],
    "malformed": [(None, 2, 0, 2)],
    "offsets": [(35.0, 2, 2, 0)],
    "negative": [(60.0, 2, 1, 1)],
    "zero": [(30.0, 2, 2, 0)],
}


def create_duration_fixture(path, variant):
    if variant not in VARIANTS or path.exists():
        raise ValueError("Requires a known variant and a new temporary fixture path.")
    rows = [
        (1, "alpha", "COMPLETED", "RECOVERED", "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"),
        (2, "beta", "COMPLETED", "RECOVERED", "2026-01-01T00:00:00Z", "2026-01-01T00:10:00Z"),
        (3, "gamma", "ESCALATED", "ESCALATED", "2026-01-01T00:00:00Z", "2026-01-01T04:00:00Z"),
    ]
    rows = [list(row) for row in rows]
    if variant == "no_success":
        for row in rows[:2]:
            row[2:4] = ["COMPLETED", "FAILED"]
    elif variant in {"missing", "malformed"}:
        for row in rows[:2]:
            row[5] = None if variant == "missing" else "invalid-time"
    elif variant == "offsets":
        for row in rows[:2]:
            row[4] = "2026-01-01T07:00:00+07:00"
    elif variant == "negative":
        rows[1][5] = "2025-12-31T23:00:00Z"
    elif variant == "zero":
        rows[1][5] = rows[1][4]
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(
            "CREATE TABLE ConnectorHealingQueue (QueueId INTEGER PRIMARY KEY, RootConnectorName TEXT, QueueStatus TEXT, FinalOutcome TEXT, ReceivedAt TEXT, CompletedAt TEXT)"
        )
        conn.executemany("INSERT INTO ConnectorHealingQueue VALUES (?,?,?,?,?,?)", rows)
