---
runbook_id: RB-NET-001
title: Kafka Connect dependency timeout
version: 1
status: approved
connector_class: network
error_codes: [CONNECT_TIMEOUT, SINK_WRITER_TIMEOUT]
environments: [all]
owners: [data-platform, network-operations]
updated_at: 2026-09-07
schema_version: 2
connector_type: kafka-connect
connector_family: source-sink
subsystem: network-dependency
symptoms:
  - connector dependency call exceeds the configured timeout
  - source or sink repeatedly waits too long for an external service
exception_classes:
  - java.net.SocketTimeoutException
  - java.net.ConnectException
  - org.apache.kafka.common.errors.TimeoutException
config_keys:
  - connection.timeout.ms
  - request.timeout.ms
  - socket.timeout.ms
  - retry.backoff.ms
error_signatures:
  - Connection timed out
  - Read timed out
  - Timed out waiting for a node assignment
aliases:
  - connector dependency timeout
  - network timeout
user_phrases_vi:
  - connector chờ hệ thống đích quá lâu
  - source hoặc sink bị timeout khi gọi dependency
user_phrases_en:
  - connector dependency request timed out
  - sink writer waits too long for the target
---

## Symptoms

- A source or sink reports `CONNECT_TIMEOUT` or `SINK_WRITER_TIMEOUT`.
- The worker remains reachable but calls to an external dependency exceed their configured timeout.

## Preconditions

- Identify the owner worker and affected dependency. Keep hostnames, credentials, and payload data out of chat output.

## Diagnostic steps

1. Compare worker latency, connection-pool utilization, dependency health, DNS, and packet loss at the incident time.
2. Check whether the timeout is transient or repeats for the same records and partitions.
3. Confirm that retry and timeout values match the approved connector policy.

## Recovery steps

1. Restore the dependency or network path before restarting a deterministic timeout loop.
2. Retry the failed task when the dependency is healthy and the operation is safe to repeat.
3. Escalate sustained latency or capacity saturation; do not increase timeouts blindly.

## Verification

- Verify processing resumes without repeated timeout events and consumer or sink lag decreases.

## Rollback

- Revert any unproven timeout change and restore the approved configuration baseline.

## Escalation

- Escalate with timestamp, owner worker, connector, dependency class, and redacted latency evidence.
