---
runbook_id: RB-ORACLE-003
title: Oracle operation cancelled or timed out
version: 1
status: approved
connector_class: oracle
error_codes: [ORA-01013]
environments: [all]
owners: [data-platform, oracle-dba]
updated_at: 2026-09-10
schema_version: 2
connector_type: debezium-source
connector_family: debezium-oracle
subsystem: oracle-query-execution
symptoms:
  - Oracle reports that the current operation was cancelled
  - the connector repeatedly stops with ORA-01013
exception_classes:
  - java.sql.SQLTimeoutException
  - java.sql.SQLException
config_keys: []
error_signatures:
  - ORA-01013
  - user requested cancel of current operation
aliases:
  - Oracle operation cancelled
  - Oracle query timeout
user_phrases_vi:
  - thao tác Oracle bị hủy khi connector đang chạy
  - connector Oracle bị timeout và báo người dùng hủy thao tác
user_phrases_en:
  - Oracle operation was cancelled
  - connector query ended with ORA-01013
---

## Symptoms

- Oracle reports `ORA-01013: user requested cancel of current operation`.
- The message means the current Oracle operation received a cancellation request; by itself it does not prove that the database or connector is unhealthy.

## Preconditions

- Identify the connector, failed task, current owner worker, environment, and exact failure time.
- Preserve the first failure and surrounding redacted worker logs before restarting anything.

## Diagnostic steps

1. Determine whether an operator, client-side timeout, connector shutdown, worker rebalance, or network interruption cancelled the operation.
2. Correlate the connector task log and owner-worker log with the Oracle session at the same timestamp.
3. Check whether the error is a single cancelled operation or repeats on the same query or connector phase.
4. Ask the Oracle DBA to inspect the affected session and database wait information when the cancellation repeats.

## Recovery steps

1. If an operator intentionally cancelled a one-off operation, continue with the next approved operation; no database repair is implied by this code alone.
2. If a configured client or platform timeout caused repeated cancellations, correct the verified timeout or dependency condition through the approved configuration path.
3. Restart only the failed task after the cause has been addressed and the owner worker is known. Do not automatically increase timeouts or reset offsets.

## Verification

- Verify that the connector and all tasks remain `RUNNING` through the configured healthy-confirmation window.
- Confirm that the same operation advances and no new `ORA-01013` incident is recorded.

## Rollback

- Restore the previous approved timeout or connector configuration if the change causes new failures or longer blocked sessions.

## Escalation

- Escalate to `oracle-dba` and `data-platform` with the incident ID, connector, owner worker, timestamps, and redacted error sequence when cancellations repeat.
