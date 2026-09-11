# Accuracy validation — 2026-09-10

This report separates deterministic/offline checks, mocked contract checks, and live cloud
measurements. It does not treat an unrun provider request as an accuracy pass.

## Scope and data identity

- Text-to-SQL snapshot: `self_healthy_kafka_snapshot.db`, SHA-256
  `313ef0c375e3fcd2f0d5f4eb2c5c9e263d73a1bc5a1880df49d909bd6e40bc76`.
- Runbook retrieval corpus: 93 cases (92 measured), SHA-256
  `d04db914ec63e04a92bfb7c1d4fea2dbbfadf722ed03b98502b9537a6c849d8a`.
- Retrieval split: 71 tuning cases and 21 untouched holdout cases.
- Qwen is the only answer/planning model used by this implementation. Hybrid retrieval does not
  call an answer model.

## Implemented accuracy controls

- Dynamic schema and few-shot selection limit irrelevant context while recording stable selection
  IDs for replay.
- The strict semantic compiler enforces metric grain, joins, denominator, status semantics,
  timestamp/timezone, NULL behavior, ordering, limit, and duplicate policy before SQL executes.
- Bounded self-correction distinguishes provider failures, invalid model contracts, rejected SQL,
  execution mismatch, and clarification mismatch.
- Claim-level grounding rejects unsupported claims, preserves supported prose, and supplements only
  missing verified values. The deterministic fallback produces Vietnamese prose before any table.
- Multi-turn stores bounded structured state (resolved entity, verified plan/facts, time range and
  evidence IDs), never raw conversation history. The UI sends only the current question plus a
  validated conversation ID.
- Runbook retrieval has a versioned Hybrid Qdrant collection, typed payload-index contract,
  evidence gate, diversity cap, dense fallback and shadow-mode observability.

## Text-to-SQL result

The current deterministic compiler passed every executable tuning and holdout case: 19/19. Three
clarification cases are deliberately contract-only offline and are not counted as passes. There
were no compiler mismatches.

| Case | Split | Offline status |
| --- | --- | --- |
| gold-1 | gold | pass |
| gold-2 | gold | pass |
| gold-3 | gold | pass |
| gold-4 | gold | pass |
| gold-5 | gold | pass |
| gold-6 | gold | pass |
| gold-7 | gold | pass |
| gold-natural-incident-ranking-typo | gold | pass |
| gold-status-or-filter | gold | pass |
| gold-daily-incident-buckets-vietnam | gold | pass |
| gold-confirmed-failure-ranking-natural | gold | pass |
| gold-unsupported-owner-question | gold | contract-only |
| holdout-independent-totals | holdout | pass |
| holdout-natural-day-independent-totals | holdout | pass |
| holdout-natural-recovery-rate | holdout | pass |
| holdout-natural-recovery-duration | holdout | pass |
| holdout-natural-unfinished | holdout | pass |
| holdout-ingestion-time-unavailable | holdout | contract-only |
| holdout-grain | holdout | pass |
| holdout-filtered-events | holdout | pass |
| holdout-latest | holdout | pass |
| holdout-ambiguous | holdout | contract-only |

The only partial Qwen live run was stopped by HTTP 402 billing. Before the fix, `gold-1` matched in
both legacy (9.636 s) and strict (4.420 s). `gold-2` exposed a strict semantic-default defect: the
phrase “cho mỗi RootConnectorName ... kể cả connector không có log” was interpreted as one scalar
total. The default planner now recognizes per-entity grouping and preserves zero-log connectors;
the exact case and a natural paraphrase pass deterministic execution. A post-fix Qwen live result is
not available, so this report does not claim live model accuracy or a legacy-versus-strict winner.

## Multi-turn result

Three structured-state scenarios pass with an injected fact provider: ordinal follow-up, isolation
between conversation IDs, and temporal override. This is a mocked contract evaluation, not a live
database/model accuracy score.

## Live Runbook retrieval result

Qdrant Cloud was measured for five repetitions per configuration. The selected tuning configuration
is dense weight `2.0`, sparse weight `1.0`, 20 dense and 20 sparse candidates, final top-k `5`, and
at most two chunks per runbook.

| Split / mode | R@1 | R@5 | MRR | nDCG@5 | Correct no-answer | Wrong runbook | p50 ms | p95 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Tuning dense | 0.8492 | 0.9127 | 0.8810 | 0.8888 | 0.3750 | 0.1972 | 450.269 | 537.026 |
| Tuning hybrid | 0.9683 | 0.9841 | 0.9841 | 0.9841 | 1.0000 | 0.0141 | 449.853 | 469.531 |
| Holdout dense | 0.7500 | 0.9500 | 0.8417 | 0.8683 | 0.0000 | 0.2857 | 470.235 | 545.290 |
| Holdout hybrid | 0.9000 | 1.0000 | 0.9750 | 0.9815 | 1.0000 | 0.0476 | 463.492 | 537.303 |

On holdout, Hybrid improved R@1 by 0.15, R@5 by 0.05, MRR by 0.1333, and nDCG@5 by
0.1133. Wrong-runbook rate fell by 0.2381 and correct no-answer improved from 0 to 1. There were no
retrieval failures, filter violations, regressed cases, or Hybrid false positives in this corpus.
The five-run promotion gate passed.

The live operation created `healing_runbooks_v2`, inserted 49 chunks, and created the 14 required
payload indexes. It did not modify `healing_runbooks_v1`, change an alias, or enable Hybrid by
default. Keep dense as primary and run real shadow traffic before an explicit promotion decision;
the benchmark corpus alone does not represent production query distribution.

## Validation performed

- Python: 498 passed, 1 live-Qdrant test skipped by default.
- Explicit live Qdrant contract against v2: 1 passed.
- Frontend unit/contract: 40 passed; TypeScript, ESLint and production build passed.
- Frontend E2E on isolated ports with system Edge: 6 passed.
- Ruff: passed. Mypy: passed for 51 source files.
- Qwen notebook: 19 cells, zero stored outputs and zero execution counts.
- Deliverable secret scan: no token-shaped values in tracked or unignored new files.

## Reproduce from PowerShell

Run from the repository root. Local secrets remain in the ignored environment file; do not copy
them into commands, reports, notebook outputs, or Git.

```powershell
python -m notebooks.evaluation.evaluate_qwen_semantics `
  --output "$env:TEMP\qwen-semantics-offline.json"

# Charged live Qwen run; requires a working HF account/key and currently cannot pass HTTP 402.
python -m notebooks.evaluation.evaluate_qwen_semantics --live `
  --modes legacy shadow strict `
  --output "$env:TEMP\qwen-semantics-live.json"

$env:RUNBOOK_RAG_LIVE_TEST = "true"
$env:RAG_RETRIEVAL_MODE = "hybrid"
$env:RAG_SEARCH_MODE = "hybrid"
$env:QDRANT_COLLECTION = "healing_runbooks_v2"
$env:QDRANT_HYBRID_COLLECTION = "healing_runbooks_v2"
python scripts/evaluate_runbook_retrieval.py --live --repetitions 5 `
  --summary-only --require-gates `
  --output "$env:TEMP\runbook-hybrid-production.json"

python -m notebooks.evaluation.evaluate_multi_turn_context
python -m pytest -q
python -m ruff check src notebooks scripts tests
python -m mypy

Push-Location apps/chat-ui
pnpm test
pnpm typecheck
pnpm lint
pnpm build
$env:PLAYWRIGHT_CHANNEL = "msedge"
$env:PLAYWRIGHT_UI_PORT = "3100"
$env:PLAYWRIGHT_BACKEND_PORT = "18081"
pnpm test:e2e
Pop-Location
```

## Remaining limits

- Full live Qwen accuracy and latency are unmeasured after the semantic fix because the provider
  returned HTTP 402. Offline compiler success is not a substitute for live model evaluation.
- Multi-turn evaluation uses mocked facts. A live end-to-end database/Qwen conversation still needs
  a restored provider budget and a controlled UAT environment.
- Hybrid passed the labelled corpus but has not yet accumulated production shadow-traffic metrics.
- Snapshot timestamps describe the local SQLite artifact, not guaranteed source freshness or an
  atomic MSSQL snapshot.
