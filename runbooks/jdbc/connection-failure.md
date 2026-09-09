---
runbook_id: RB-JDBC-001
title: JDBC source or sink connection failure
version: 1
status: approved
connector_class: jdbc
error_codes: [SQLSTATE-08001, CONNECTION_REFUSED]
environments: [all]
owners: [data-platform, database-operations]
updated_at: 2026-09-07
schema_version: 2
connector_type: jdbc
connector_family: jdbc-source-sink
subsystem: database-connectivity
symptoms:
  - database connection cannot be established
  - JDBC task fails before records are read or written
exception_classes:
  - java.sql.SQLTransientConnectionException
  - java.sql.SQLNonTransientConnectionException
  - org.apache.kafka.connect.errors.ConnectException
config_keys:
  - connection.url
  - connection.attempts
  - connection.backoff.ms
error_signatures:
  - Connection refused
  - SQLState 08001
  - The connection attempt failed
aliases:
  - JDBC connection refused
  - database listener unavailable
user_phrases_vi:
  - JDBC source không kết nối được database
  - sink bị từ chối kết nối tới cơ sở dữ liệu
user_phrases_en:
  - JDBC connector cannot connect to the database
  - database connection is refused
---

## Symptoms

- A JDBC task cannot open a database connection and reports `SQLSTATE-08001` or `CONNECTION_REFUSED`.
- Restarts fail before records can be read or written.

## Preconditions

- Confirm the target database and environment without exposing the JDBC URL or credentials.
- Identify the owner worker so that network tests use the correct source path.

## Diagnostic steps

1. Check DNS resolution, route, firewall, listener, TLS trust, and database availability from the organization's approved diagnostic host.
2. Confirm the database account is enabled and has the expected minimum permissions.
3. Compare connection-pool saturation and database session limits with the incident time.

## Recovery steps

1. Restore the unavailable network or database dependency, or correct the approved connector configuration.
2. Restart the affected task only after the dependency check succeeds.
3. Escalate recurring connection exhaustion instead of increasing retries without capacity evidence.

## Verification

- Confirm new records flow and the connector remains healthy after the confirmation window.
- Verify database sessions and connector lag return to an expected range.

## Rollback

- Revert the last connector configuration deployment when it introduced the connection failure.

## Escalation

- Escalate to `database-operations` for listener/account issues and to networking for path or firewall failures.
