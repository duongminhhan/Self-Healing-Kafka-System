# dbt Analytics Progress

This document records the data-contract decisions and risks behind dbt
analytics work. It intentionally excludes credentials, raw data, SQL output,
and long test logs.

## 2026-09-17 - Project and permission preflight

- Need: make the dbt SQL Server project reproducible before adding analytics models.
- Source contract: `ConnectorHealingQueue`, `ConnectorHealingLogs`, and legacy `vConnectorIncidentFacts` remain read-only sources.
- Technique: use dbt staging/marts later; retain the legacy view as the parity reference before semantic-layer cutover.
- Guardrail: preflight checks least-required source and target-schema permissions without executing DDL.
- Result: the configured SQL Server target satisfies the phase-one read and target-schema permission requirements.
- Initial boundary: the preflight itself made no model, data-quality-test, or runtime semantic-source change.

## 2026-09-17 - Canonical incident mart and parity gate

- Need: raw queue/log joins can multiply incidents and distort analytics metrics.
- Data contract: one canonical row is the earliest `HEALTH_FAILED_CONFIRMED` event for one queue incident.
- Technique: SQL Server dbt staging views feed a canonical incident view; quality tests enforce identity, state, timestamp, error-code and connector rules.
- Decision: retain `dbo.vConnectorIncidentFacts` as the bounded, stable-order parity reference; error messages remain internal analysis data and are not exposed by this phase.
- CI decision: PR/push uses parse-only validation with an ephemeral, non-connecting profile; schedule/manual runs build against an isolated SQL Server container seeded with deterministic incident rows, so parity is not vacuous.
- Boundary: no aggregate mart, freshness SLA, or semantic-runtime cutover is included until separately reviewed.

### Gate interpretation

- A staging/source test failure is a source-contract or source-quality issue, not a semantic answer.
- A parity failure is a dbt transformation mismatch unless a repeat against the same stable source snapshot proves the source changed during the run.
- Freshness is unknown until a business SLA is approved; a passing build is not freshness evidence.
- dbt does not call a planner or model. The chatbot still reads `dbo.vConnectorIncidentFacts`; therefore no runtime semantic mismatch can be claimed or masked in this phase. Rollback is the current runtime path, and a cutover requires a separately approved immutable snapshot.

## 2026-09-18 - Safe semantic-source cutover foundation

- Need: a tested dbt mart cannot improve chatbot answers while the compiler is pinned to the legacy view.
- Source contract: `vSemanticConnectorIncidentFacts` projects the legacy semantic columns from the canonical dbt mart; its error-message field is credential-redacted and bounded before the existing Python redaction pass, while the runtime maps only fixed `legacy` and `dbt` sources.
- Technique: `legacy` remains default, `shadow` serves legacy while comparing bounded canonical outputs asynchronously, and `dbt` has no legacy fallback.
- Guardrail: comparison logs only a short request id, plan fingerprint, source keys, bounded row counts and a category; raw SQL, error messages and payloads are excluded.
- Risk: without a proven common snapshot, any shadow comparison is `inconclusive`; source freshness remains unknown without an approved watermark/SLA.
- Rollback: set `CHAT_ANALYTICS_FACT_SOURCE=legacy` and restart the service; no source-schema migration is involved.
- Deployment boundary: the durable target/schema still needs operator approval before any dbt DDL is run there.

## 2026-09-18 - Isolated target validation

- Need: verify that the compatibility contract can be deployed without changing the legacy runtime source.
- Decision: models are validated only in the isolated `dbt_dev` schema; `legacy` remains the default runtime mode.
- Freshness: the available target has no approved watermark/SLA and snapshot isolation is unavailable, so freshness is `unknown` and shadow comparisons remain `inconclusive`.
- Next gate: an owner must approve a durable target and a common-snapshot method before shadow evidence can support any dbt cutover.

## 2026-09-18 - Validation command isolation

- Need: optional Qdrant client imports could delay a harmless `--help` check and hide whether repository-source isolation actually works.
- Technique: defer Qdrant and RAG imports to the explicitly destructive alias-apply branch; bootstrap still selects this checkout's `src` first.
- Guardrail: help and dry-run remain local operations, while alias mutation still requires the explicit cutover environment gate.
