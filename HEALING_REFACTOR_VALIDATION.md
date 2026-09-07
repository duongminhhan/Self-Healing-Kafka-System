# Healing correctness refactor

## Scope and root causes

1. A healthy first poll previously skipped completion when failed_count was zero.
   It now uses the existing recovery completion path. Without an automated action,
   no HEALING_RECOVERED event is invented. Completion persistence can be retried.
2. A confirmed webhook inflated failed_count before the equality check, skipping
   HEALTH_FAILED_CONFIRMED. Confirmation now has its own persisted-history flag;
   observation counts remain actual observations. The confirmation insert takes
   a parent queue update/range lock and checks existing events in one transaction.
   Polling, repeated webhooks and a fresh process use the same path. The flag is
   set in memory only after persistence succeeds; a failed confirmation write
   prevents the Kafka action. The SQL procedure handles uncertain-commit replay.
3. CompletedAt was exposed as RecoveredAt even for escalation. The view now exposes
   terminal CompletedAt separately and sets RecoveredAt only for COMPLETED/RECOVERED.
   The analytics consumer checks both fields, requires offset-aware timestamps,
   excludes negative/missing durations and reports valid/excluded counts. Zero is
   preserved. Its metric remains **confirmed failure to recovery**, not ReceivedAt
   to completion; the notebook's end-to-end metric is a different measurement.

The action dispatcher now has one level check for normal and retry actions.
RESTART_ONLY caps the effective level even if an inconsistent caller supplies 4.
The missing-connector retry path uses the configured retry budget and same gate.
Existing policy, executor and repository abstractions remain in use.

## Changed files

- domain/healing.py: persisted confirmation attribute on ConnectorJob.
- healing/db_state_machine.py: healthy completion, confirmation and action dispatch.
- healing/policy.py and helpers.py: explicit confirmation and mode restrictions.
- webhook/analytics_chat.py: successful duration population and diagnostics.
- Three canonical procedures and vConnectorIncidentFacts under sql/ingest_reference.
- sql/mssql-stored-procedures.sql: executable manual deployment bundle.
- scripts/seed_healing_samples.py: in-memory fixture hydration/deduplication updated
  to match the procedure contract; generated data files were not regenerated.
- Regression tests: test_db_state_machine.py, test_recovery_duration.py and
  test_healing_sql_contract.py. Existing sample/policy/action tests are reused.
- redaction.py, logging_config.py and storage/log_repository.py: shared redaction
  before database persistence and at the formatted stdout boundary, with
  test_healing_redaction.py covering structured configurations and credentials.

Python paths above are relative to src/self_healthy_kafka, tests to tests/unit.
Unrelated notebook edits were left intact. No commit or push was performed.

## Deployment (manual, not performed)

Stop all healing workers before rollout. Review the target database explicitly.
Apply sql/mssql-stored-procedures.sql to that database, then deploy Python and
restart workers. The bundle uses EXEC batches and needs no GO or sqlcmd mode.
It updates only the following objects, in dependency order:

1. spInsertConnectorHealingLog
2. spGetConnectorHealingQueue
3. vConnectorIncidentFacts
4. spGetConnectorIncidentFacts

The bundle is an update for an existing installation, not a bootstrap schema.
No table data is deleted, backfilled, refreshed or reseeded. Existing duplicate
confirmations are not silently removed. Before rollout, inspect duplicate
HEALTH_FAILED_CONFIRMED rows grouped by QueueId and decide historical repair
separately. Deduplication assumes writers use the stored procedure; direct table
INSERT permissions bypass it. Restrict the runtime identity accordingly.

The added output columns are additive for named-column consumers. Deploy SQL
first: old SQL lacks FailureConfirmed/QueueStatus, so Python alone is not a
complete rollout. The new analytics code fails closed on absent status rather
than including an unverified successful duration. Coordinate positional/custom
consumers and rollback of view/procedure/Python together.

UAT and production require manual application; the application does not migrate.
SQL Server execution is currently **unverified**: Docker's Linux engine pipe was
unavailable during validation. Static bundle tests prove synchronization only,
not SQL syntax, lock behavior or permissions. Before production, run concurrent
confirmation inserts and rollback/retry tests in a disposable database.

## Validation evidence and boundaries

- Initial focused baseline: 38 passed.
- Healthy-first-poll test failed before the fix (complete called zero times).
- Webhook test failed before the fix (confirmation event missing).
- Duration cases failed before the fix (escalation/open included, negative duration
  accepted, mixed timezone subtraction raised TypeError).
- Expanded focused suite: 35 passed at the intermediate check.
- Final runtime unit and deployment-contract suite: **149 passed**.
- Ruff passed on runtime source and changed/new test/generator files.
- mypy: no issues in **39 source files**. git diff --check passed.
- Tests importing application config need these process-local values in this
  checkout: OLLAMA_REQUEST_TIMEOUT_SECONDS=30, OLLAMA_THINK=false,
  OLLAMA_MAX_TOKENS=512, OLLAMA_CONTEXT_LOG_LIMIT=10. They do not enable live calls.

All executed behavioral tests use mocks/in-memory fixtures. The sample generator
was called through tests, not its apply CLI. Existing MSSQL data and SQLite
snapshots were not written; there were no live Kafka operations.

## Remaining operational risks

This is not a distributed exactly-once action system. Queue creation already
uses database locking and an open-root uniqueness constraint. Confirmation writes
now serialize in the DB, but action processing still uses process-local locks:
multiple worker processes can race on stale jobs. Run one healing worker until
a separately designed durable claim/lease with fencing is implemented.

A Kafka action and its subsequent log/queue commit are separate operations.
Exceptions escaping action dispatch now quarantine the incident and attempt to
persist HEALING_ESCALATED with outcome_uncertain=true before terminal completion.
This intentionally treats uncertainty conservatively, including a failure before
the external request: it never claims that an action definitely ran or succeeded.
If persistence remains unavailable, the current process retains quarantine and
retries escalation persistence, not the Kafka action. A restored terminal audit
finishes escalation without checking health or running another action.
Tests cover action audit failure, repeated checks during an outage and restoration.

If the process dies before any quarantine record can commit, its memory is lost;
normal retry counters are not a crash-safe exactly-once bound. Stop workers and
reconcile uncertain incidents manually after such an outage, before restarting.
A durable action-intent/outcome protocol remains broader follow-up work.

Log redaction now removes sensitive nested keys/config objects, password/token
assignments, HTTP Basic/Bearer credentials and URL userinfo from persisted payloads
and stdout diagnostics. It does not mutate the action's runtime configuration.
Arbitrary unlabeled secrets in free-form text cannot be recognized reliably;
retain restricted log access and do not place secrets in connector names/messages.
No production security or concurrency guarantees were inferred from mock tests.

## Reproduce final checks

```powershell
$env:OLLAMA_REQUEST_TIMEOUT_SECONDS='30'
$env:OLLAMA_THINK='false'
$env:OLLAMA_MAX_TOKENS='512'
$env:OLLAMA_CONTEXT_LOG_LIMIT='10'
$runtimeTests = @(Get-ChildItem tests/unit/test_*.py | Where-Object {
    $_.Name -notmatch '^test_(notebook|qwen|semantic)'
} | ForEach-Object FullName)
python -m pytest @runtimeTests tests/integration -q --tb=short
python -m mypy src/self_healthy_kafka
python -m ruff check src/self_healthy_kafka scripts/seed_healing_samples.py tests/unit/test_db_state_machine.py tests/unit/test_recovery_duration.py tests/unit/test_healing_sql_contract.py tests/unit/test_healing_redaction.py
git diff --check
```

The excluded notebook suites are not part of this runtime refactor; the full
collection failure is disclosed above, not reported as a pass. All three original
defects have observed red-before/green-after tests. Database execution remains
unverified as explicitly permitted by the goal when isolation is unavailable.
