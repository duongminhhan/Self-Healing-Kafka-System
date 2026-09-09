---
runbook_id: RB-ORACLE-002
title: Oracle LogMiner missing archived log
version: 1
status: approved
connector_class: oracle
error_codes: [ORA-01291]
environments: [all]
owners: [data-platform, oracle-dba]
updated_at: 2026-09-07
schema_version: 2
connector_type: debezium-source
connector_family: debezium-oracle
subsystem: oracle-logminer
symptoms:
  - LogMiner cannot find a required online or archived redo log
  - CDC repeatedly stops at the same source log gap
exception_classes:
  - io.debezium.DebeziumException
  - java.sql.SQLException
config_keys:
  - log.mining.strategy
  - log.mining.archive.destination.name
  - snapshot.mode
error_signatures:
  - missing logfile
  - LogMiner session is missing a log file
  - ORA-01291
aliases:
  - missing archived redo
  - LogMiner redo gap
user_phrases_vi:
  - database đã dọn mất redo mà connector cần đọc
  - CDC dừng vì thiếu archived log
user_phrases_en:
  - LogMiner cannot find the required archived redo log
  - Oracle CDC stopped at a missing log file
---

## Symptoms

- The Oracle Debezium task fails with `ORA-01291` or reports that LogMiner is missing a log file.
- Repeated restarts return to the same source-log gap.

## Preconditions

- Record the exact connector, task, owner worker, requested SCN range, and Oracle database identity.
- Do not infer data loss from a numeric SCN gap alone.

## Diagnostic steps

1. Ask the DBA to verify the required online or archived redo logs for the connector SCN range.
2. Correlate owner, table, operation, transaction identifier, commit SCN, and source timestamps.
3. Check archive retention and LogMiner permissions before changing connector offsets.

## Recovery steps

1. Restore or make the required archived log available when the DBA confirms it is safe.
2. Prefer the system's offset-preserving recovery path only when a valid offset and schema-history topic are available.
3. Treat recreation without an offset as a higher-risk fallback requiring explicit operational approval.

## Verification

- Verify LogMiner advances beyond the affected range and the connector tasks remain `RUNNING`.
- Reconcile source and target records before declaring recovery complete.

## Rollback

- Stop the replacement connector if reconciliation identifies duplicates, omissions, or an unsafe offset.

## Escalation

- Escalate to `oracle-dba` when redo is unavailable and to `data-platform` before any offset reset.
