---
runbook_id: RB-KC-001
title: Kafka Connect task failure
version: 1
status: approved
connector_class: kafka-connect
error_codes: [TASK_FAILED, HEALTH_FAILED_CONFIRMED]
environments: [all]
owners: [data-platform]
updated_at: 2026-09-07
schema_version: 2
connector_type: kafka-connect
connector_family: source-sink
subsystem: task-runtime
symptoms:
  - one or more connector tasks are FAILED while the worker is reachable
  - task restart returns to the same failure
exception_classes:
  - org.apache.kafka.connect.errors.ConnectException
  - org.apache.kafka.connect.errors.RetriableException
config_keys:
  - errors.tolerance
  - errors.retry.timeout
  - errors.retry.delay.max.ms
error_signatures:
  - Task threw an uncaught and unrecoverable exception
  - WorkerSinkTask is being killed
  - WorkerSourceTask is being killed
aliases:
  - failed connector task
  - Kafka Connect task crash
user_phrases_vi:
  - một task của connector bị chết nhưng worker vẫn chạy
  - connector còn tồn tại nhưng task chuyển sang FAILED
user_phrases_en:
  - connector task failed while the worker is still running
  - one task keeps crashing after restart
---

## Symptoms

- The connector is present but one or more tasks report `FAILED`.
- The healing audit records `HEALTH_FAILED_CONFIRMED` after the debounce checks.

## Preconditions

- Identify the Kafka Connect worker that owns the connector and capture the redacted task trace.
- Distinguish a failed task from an administratively `PAUSED` or `STOPPED` connector.

## Diagnostic steps

1. Read the owner worker log around the first failure, not only the REST status trace.
2. Check the earliest specific cause, including source authentication, network, converter, schema, or sink errors.
3. Confirm whether all tasks fail consistently or a single task/partition is affected.

## Recovery steps

1. Correct the underlying configuration or dependency before repeating a restart for a deterministic error.
2. For a transient task-only failure, use the approved failed-task restart path.
3. If task retries are exhausted, follow the connector restart policy and then escalate rather than looping.

## Verification

- Confirm the connector and every task remain `RUNNING` for the configured healthy-confirmation window.
- Use `HEALING_RECOVERED` or verified queue completion as recovery evidence; do not infer success from `TASK_RESTART`.

## Rollback

- Revert the configuration change through its normal deployment path if the connector becomes less healthy.

## Escalation

- Escalate with the connector, failed task IDs, owner worker, first specific exception, and attempts already made.
