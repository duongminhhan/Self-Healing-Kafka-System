---
runbook_id: RB-KC-002
title: Automatic healing exhausted or escalated
version: 1
status: approved
connector_class: kafka-connect
error_codes: [HEALING_ESCALATED, HEALING_LEVEL_LIMIT_REACHED, MAX_RETRIES_REACHED]
environments: [all]
owners: [data-platform]
updated_at: 2026-09-07
---

## Symptoms

- The audit contains `HEALING_ESCALATED`, `HEALING_LEVEL_LIMIT_REACHED`, or `MAX_RETRIES_REACHED`.
- Allowed task restart, connector restart, or Oracle recreation attempts no longer produce a stable healthy state.

## Preconditions

- Treat the incident as requiring manual review. Do not bypass the configured healing level or replay uncertain actions automatically.

## Diagnostic steps

1. Review the complete event sequence and identify the first specific error before the retry events.
2. Confirm which actions were requested and which outcomes were actually verified.
3. For an uncertain external action or failed audit write, reconcile Kafka Connect state before any manual replay.

## Recovery steps

1. Correct the underlying dependency, permission, data, or configuration issue identified in the first failure.
2. Obtain the required approval before a destructive offset reset or recreation without offsets.
3. Resume automation only after current connector state and audit state agree.

## Verification

- Confirm all tasks stay `RUNNING`, data movement is reconciled, and the queue reaches a verified terminal outcome.

## Rollback

- Stop a replacement connector and return to the last verified configuration if validation fails.

## Escalation

- Escalate with the incident ID, connector, owner worker, event sequence, and redacted evidence. Do not include secrets.
