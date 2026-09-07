# Code-driven healing samples

The local `poc-mssql` database `ingest_reference` was reseeded on 2026-09-05.
Only `dbo.ConnectorHealingQueue` and `dbo.ConnectorHealingLogs` were replaced.
The generator runs the real healing state machine with mocked Kafka operations;
error messages and incidents are synthetic, not captured production failures.

## Verified population

- 27 incidents and 293 logs across 14 error categories.
- Statuses: COMPLETED 15, ESCALATED 9, WAITING 1, PENDING 1, PROCESSING 1.
- Categories include Oracle missing redo, cancelled query, snapshot too old and
  invalid credentials; JDBC connectivity and duplicate keys; converter and
  Schema Registry errors; Kafka authorization, authentication, TLS and oversized
  records; S3 access denial; and PostgreSQL removed WAL.
- Paths include task restarts, connector restarts, Oracle recreation with/without
  offsets, offset-patch failure, timeout, escalation, spontaneous recovery,
  connector-only failures and multiple failed tasks.
- PENDING and newly PROCESSING examples legitimately have no logs. Spontaneous
  recovery need not emit HEALING_RECOVERED. Non-Oracle samples do not recreate
  connectors. FAILED is not fabricated merely because the schema permits it.

Representative error references: [Kafka authorization](https://kafka.apache.org/40/javadoc/org/apache/kafka/common/errors/TopicAuthorizationException.html),
[oversized records](https://kafka.apache.org/40/javadoc/org/apache/kafka/common/errors/RecordTooLargeException.html),
[Oracle missing redo](https://docs.oracle.com/en/error-help/db/ora-01291/).

## Validation and reference answers

Post-commit readback matched every generated row. The queue stored procedure
hydrated all 27 incidents with matching counters and latest events. The focused
seed/healing test suite passed 38 tests; Ruff passed for the new Python files.
See `healing-samples-validation.json` for the applied reset report and
`generated-healing-samples.json` for scenario metadata and rows.

Run `healing-samples-reference.sql` for independent MSSQL answers. At insertion:

- Highest log count: sample-oracle-orders, 41 logs; next sample-jdbc-orders, 31.
- Successful duration: 15 valid incidents, average approximately 16.066851 minutes.
- Log count, incident count and confirmed-failure count are different metrics.

The old 6 queue rows and 16 log rows were backed up before deletion to:
`backups/healing/20260905T155607-efa948fd623041e09668812a7b6c2e49.json.bak`.
This full-row JSON backup can be used for restoration; no automatic restore was
performed. Keep it private. The reset used a transaction and rollback on failed
validation. No MSSQL schema, stored procedure or healing runtime was changed.

## Reuse

From the repository root:

```powershell
# Offline generation only; overwrites generated sample/report artifacts.
python -m scripts.seed_healing_samples

# DESTRUCTIVE: back up and replace both local PoC tables again.
# Stop healing workers first to avoid consuming the synthetic open incidents.
python -m scripts.seed_healing_samples --apply-local
```

Use `--anchor 2026-09-05T15:00:00+00:00` for reproducible timestamps.
The local-only apply mode discovers credentials in memory; it does not embed
them in artifacts. Do not run a live healing worker against these synthetic names.

The notebook SQLite snapshot was deliberately left unchanged. To test this new
population in a notebook, explicitly run its snapshot-refresh cell, then rebuild
the analytics workflow. Do not compare old Gold Query expected results against
the new population without regenerating independent references.

This dataset does not repair the separately reviewed healing/analytics defects,
nor does it prove that a model interprets every question correctly.
