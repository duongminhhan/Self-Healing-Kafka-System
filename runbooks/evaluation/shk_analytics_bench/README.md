# SHK-AnalyticsBench

`SHK-AnalyticsBench` evaluates the controlled analytics path:

```text
question and bounded context -> semantic plan -> deterministic read-only T-SQL -> verified outcome
```

It is not a Text-to-SQL corpus. Models never train on SQL, response prose,
credentials, or production logs. The canonical semantic plan is the only SFT
target; the compiler, catalog, evidence validation, and outcome taxonomy stay
the runtime authority.

## Splits and review

- `train.jsonl`: candidate examples for supervised training after a business
  reviewer marks them `approved`.
- `validation.jsonl`: model/prompt-selection cases. Never export as training.
- `hidden_test.jsonl`: release gate only. Never add to prompts, few-shot
  examples, or training exports.

Every case has a `family_id`; a family may occur in one split only. The loader
also rejects exact normalized question/context duplicates across selected
splits. The initial train labels were approved by Minh An on 2026-09-16. Their
reference executions are explicitly classified as synthetic fixtures, so they
can validate offline contracts but can never contribute to a live outcome or
execution score.

Most cases use `evaluation_scope: adapter` (the default) and are passed to a
planner adapter. `offline_contract` is reserved for injected infrastructure
failures, such as a planner failure after correction; it verifies the outcome
contract but is excluded from model-accuracy denominators.

## Commands

```powershell
python scripts/evaluate_shk_analytics_bench.py
python scripts/evaluate_shk_analytics_bench.py --split hidden_test
python scripts/export_semantic_planner_training_data.py --output .\artifacts\semantic-plan-train.jsonl
```

The export command fails until approved training labels exist. `--include-seed`
is allowed only to test output format locally; it must not feed a training job.

### 1. Offline contract

This mode needs neither provider nor MSSQL. It validates labels, semantic
compilation, split isolation, and safety gates.

```powershell
python scripts/evaluate_shk_analytics_bench.py
python scripts/evaluate_shk_analytics_bench.py --split hidden_test
```

### 2. Live plan-only baseline

This mode uses provider credits and performs read-only MSSQL access, so it is
always explicit. It measures canonical plan, route, time scope, top-N/tie,
clarification, and safety only. Its `outcome_accuracy` and
`execution_result_accuracy` remain `null` because the configured live source
does not identify itself as an immutable benchmark snapshot.

```powershell
$env:SELF_HEALTHY_KAFKA_ENV_FILE = (Resolve-Path ".\env\dev.env")
$env:SHK_ANALYTICS_BENCH_LIVE = "true"
python scripts/evaluate_shk_analytics_bench.py --live --split validation
```

It requires `CHAT_ANALYTICS_ENABLED=true`, `HF_CHAT_ENDPOINT_URL`,
`HF_CHAT_TOKEN`, `HF_CHAT_MODEL_ID`, and valid MSSQL configuration. Do not put
those values in a command, report, or benchmark artifact.

### 3. Snapshot-matched baseline

Capture is read-only but intentionally requires an explicit confirmation. It
accepts only separately reviewed executable cases and produces a redacted
`draft` manifest; it never overwrites an existing file.

```powershell
python scripts/capture_shk_analytics_snapshot.py `
  --approve-capture `
  --snapshot-id baseline-20260916 `
  --source-identity mssql-dev-analytics-snapshot `
  --case-id <approved-case-id> `
  --output .\runbooks\evaluation\shk_analytics_bench\snapshots\baseline-20260916.json
```

The reviewer must verify the canonical facts and create a reviewed manifest;
the review command recalculates its integrity hash and never replaces the
draft:

```powershell
python scripts/review_shk_analytics_snapshot.py `
  --approve-review `
  --snapshot .\runbooks\evaluation\shk_analytics_bench\snapshots\baseline-20260916.json `
  --output .\runbooks\evaluation\shk_analytics_bench\snapshots\baseline-20260916-reviewed.json `
  --status approved --reviewer "<reviewer>" --reviewed-at 2026-09-16
```

Then separately replace the case's synthetic reference with the matching
immutable snapshot ID, manifest hash, source-identity hash, timezone, and
canonical time boundary. The adapter must also return the exact reviewed
semantic plan for that case. Only an adapter backed by that exact snapshot can
then pass `--snapshot <manifest>` and receive execution/outcome scores. The
current live database adapter
deliberately cannot claim this identity.

The manifest schema is `snapshot-schema.json`. It stores only allowlisted
canonical facts required for metric/rank/state/time-scope verification; source
connector identifiers and the source's logical identity are SHA-256 pseudonyms.
Use only a non-sensitive logical alias for `--source-identity`, never a host,
DSN, username, or credential. The manifest also carries review and
integrity metadata, and rejects SQL, bound parameters, raw rows/logs,
credentials, and personal data.

## Adapter serving assessment

The current runtime exposes only `HF_CHAT_ENDPOINT_URL`, `HF_CHAT_TOKEN`, and
`HF_CHAT_MODEL_ID`. It has no adapter artifact, adapter revision, or deployment
selection setting, so this repository cannot prove that the configured provider
can serve a LoRA adapter safely. Before an adapter experiment, obtain the
provider's serving contract, record the immutable adapter revision, define
rollback/shadow routing, and keep `HF_CHAT_MODEL_ID` unchanged until the hidden
safety gate and cost/latency targets are approved.

## Before LoRA

Collect separately reviewed immutable snapshot references, run the current
model's plan-only baseline, then use a snapshot-backed adapter for execution
scoring. Require all safety-critical hidden cases to pass before considering
an adapter rollout.
