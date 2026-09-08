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
