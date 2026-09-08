---
runbook_id: RB-ORACLE-001
title: Oracle source authentication failure
version: 1
status: approved
connector_class: oracle
error_codes: [ORA-01017]
environments: [all]
owners: [data-platform]
updated_at: 2026-09-07
---

## Symptoms

- The Oracle connector reports `ORA-01017`, invalid username/password, or logon denied.
- Task or connector restarts repeat the same authentication error.

## Preconditions

- Confirm the connector is an Oracle Debezium source and identify its current owner worker.
- Use an approved secret-management path. Never paste database passwords into chat or a ticket.

## Diagnostic steps

1. Confirm that the configured Oracle account still exists and is not locked or expired.
2. Ask the database owner to test the account from the Kafka Connect network path.
3. Check whether the secret reference changed, expired, or points to the wrong environment.

## Recovery steps

1. Have the credential owner rotate or correct the secret through the approved secret store.
2. Restart the failed task or connector only after the credential has been verified.
3. If authentication still fails, stop automatic retries and escalate to the Oracle and platform owners.

## Verification

- Verify the connector and all tasks remain `RUNNING` after the configured healthy-confirmation window.
- Confirm a new `ORA-01017` event is not recorded. A restart request alone is not proof of recovery.

## Rollback

- Restore the previous secret version only when its validity and authorization have been confirmed.

## Escalation

- Escalate to `data-platform` and the Oracle account owner with connector name, environment, and redacted error code.
