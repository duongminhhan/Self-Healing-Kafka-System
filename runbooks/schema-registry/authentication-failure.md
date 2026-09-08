---
runbook_id: RB-SR-001
title: Schema Registry authentication failure
version: 1
status: approved
connector_class: schema-registry
error_codes: [SCHEMA_REGISTRY_AUTHENTICATION_FAILED, HTTP-401]
environments: [all]
owners: [data-platform]
updated_at: 2026-09-07
---

## Symptoms

- Converter or connector calls to Schema Registry return `HTTP-401` or `SCHEMA_REGISTRY_AUTHENTICATION_FAILED`.
- Task restarts repeat before record conversion succeeds.

## Preconditions

- Do not expose Schema Registry credentials or authorization headers.
- Confirm the affected connector and owner worker use the intended environment endpoint.

## Diagnostic steps

1. Verify the secret reference, account status, endpoint environment, and TLS identity through approved tooling.
2. Check whether the account can read the required subjects without granting broader permissions.
3. Compare the failure time with credential rotation or access-policy changes.

## Recovery steps

1. Correct or rotate the credential in the approved secret store.
2. Restart the failed task only after authentication is independently verified.
3. Escalate repeated authorization failures to the Schema Registry owner.

## Verification

- Confirm schema lookup succeeds, records resume processing, and no new authentication event appears.

## Rollback

- Restore a previously verified secret version only with owner approval.

## Escalation

- Escalate with subject name, environment, connector, and redacted HTTP status; omit tokens and headers.
