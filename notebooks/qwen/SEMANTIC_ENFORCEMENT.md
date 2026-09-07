# Qwen semantic enforcement

This opt-in HF workflow preserves the configured Qwen model/provider. Other providers
do not opt in. No deleted provider artifacts are restored. No source/snapshot refresh is needed.

## Run

Install `notebooks/qwen/requirements.txt` in the notebook kernel, then restart the kernel.
The HF SDK range is restricted to the tested 1.29 minor series. Keep secrets in the ignored
root `.env` or process environment, never notebook cells or outputs.

```text
HF_TOKEN=<secret supplied outside the notebook>
HF_MODEL_ID=<keep current Qwen model>
HF_PROVIDER=<keep current provider>
QWEN_SEMANTIC_MODE=legacy
HF_STRUCTURED_OUTPUT=auto
HF_SQL_REQUEST_TIMEOUT_SECONDS=30
HF_RESPONSE_REQUEST_TIMEOUT_SECONDS=30
HF_MAX_TOKENS=2048
HF_SQL_MAX_TOKEN_CEILING=4096
HF_RESPONSE_MAX_TOKENS=1500
HF_AGENT_MAX_STEPS=3
```

`HF_MAX_TOKENS` is the initial SQL output budget, not a minimum imposed on explicit
settings. An existing `.env` value of 1024 remains 1024. Only `finish_reason=length`
doubles the next budgeted SQL call, up to `HF_SQL_MAX_TOKEN_CEILING`. Its default is
4096, or the explicit initial budget if higher. Set the ceiling equal to the initial
budget to disable growth. `HF_MODEL_OUTPUT_TOKEN_LIMIT` optionally supplies a known
lower deployment output cap for both SQL and response requests. Initial SQL settings
exceeding either cap, or `HF_RESPONSE_MAX_TOKENS` exceeding the deployment cap, fail
before inference; explicit settings are not silently clamped.
There is no universal provider cap discovery via HF auto routing. Configure a lower cap
for deployments that need one; unsupported parameters are service errors, not permission
to increase or silently switch providers. Growth does not add attempts; rerunning the
question resets the initial budget. Response budget is independent and never auto-grown.

The [HF chat API](https://huggingface.co/docs/inference-providers/tasks/chat-completion)
defines `max_tokens` as the maximum generated completion tokens and returns finish reasons
and usage. The [Qwen model card](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507)
describes the model, not every hosted provider's capacity. Actual requested budgets,
finish reasons and token usage are recorded per call; missing usage stays unknown.

Open `text_to_sql_self_healthy_kafka.ipynb`. Run zero-based cells **3, 10, 12, 17, 19, 20**
in order for inference on the existing snapshot. Cell 0 is optional dependency installation.
**Do not Run All**: cells 5, 8, 15 remain the existing explicit loader/refresh path.
Edit your question in Cell A (19), then run Cell B (20). Rerun configuration/workflow cells
after settings or snapshot changes. Restart the kernel after changing runtime modules.
After editing `.env`, also restart the kernel: already-loaded environment variables retain
precedence over file values. Direct `os.environ` changes need only the configuration/workflow cells rerun.

`QWEN_SEMANTIC_MODE` defaults to `legacy`. `strict` compiles validated plans and never falls
back to free SQL. `shadow` retains the legacy result and collects a separate strict comparison
only within the remaining total SQL-call budget. Stage timings, traces, plan, assumptions,
parameters, diagnostics and response source are visible. Modified inference outputs were cleared
so saved old results do not masquerade as a new run; refresh code and the user's question remain.

The separate HTTP timeouts fall back to `HF_REQUEST_TIMEOUT_SECONDS=30`. These are SDK
HTTP-operation limits, not a total multi-call wall-clock SLA. Existing result row/byte and
SQLite execution limits remain active. `BENCHMARK_SQLITE_PATH` selects the existing database;
`HF_SQL_ALLOWED_TABLES` optionally restricts discovered tables.

## Enforced scope

`notebooks/shared/semantic_plan.py` contains typed/source-backed catalog definitions,
bounded plans and mandatory validation inside a closed compiler. Model input cannot inject
SQL identifiers, expressions or operators. Evaluation Gold plans are never runtime templates.

Supported compositions: incident/event populations, projection, multiple group dimensions,
incident/log counts, receipt-to-completion averages with quality counts, AND filters using
typed comparisons/NULL predicates, time ranges, numeric HAVING or comparison to the grouped
population mean, ordering/limit, and latest incident status per root. Logical root names and
physical replacement names remain separate fields.

Incident aggregates preserve zero-log incidents without JOIN fanout. Event JOINs retain orphan
logs with NULL parent attributes; incident counts only include matched parent IDs. Used identity
keys must be unique/non-null; SQLite nullable TEXT primary keys are rejected. Latest status uses
ReceivedAt instant then QueueId descending over the whole snapshot, not live connector health.

Duration averages exclude missing/unparseable/negative intervals, retain zero, normalize filter
offsets to UTC, and require matched/valid/excluded counts. Successful healing requires COMPLETED
and RECOVERED. Missing durations are not replaced with zero. Read-only table allowlists, SQLite
timeouts and snapshot/WAL cache invalidation remain independent of the model.

Outside this compiler version: arbitrary formulas/ratios, arbitrary nested aggregates/windows,
UNION, OR, time buckets and arbitrary distinct measures. These require clarification, never silent
approximation or a strict-mode free-SQL fallback. Extend catalog/compiler/tests for new metrics;
do not add question-string routing. Explicit legacy mode retains the wider existing SQL language.

Validation proves plan invariants, not complete equivalence to natural language: a model may still
choose the wrong permitted metric/filter. Response checks verify references, literal values/numbers
and coverage, not all semantic contradictions in Vietnamese. JSON conformance, successful SQL or
passing evidence checks do not prove end-to-end business correctness.

## HF output and service contract

Contracts are state-specific: legacy generation requires `kind=sql`, `sql`, and
`interpretation`, or a clarification; strict planning accepts only a semantic plan or
clarification. `accept_result` is allowed only in legacy review after a verified candidate
exists. Shadow uses each branch's own contract. All payloads are validated locally against
the exact contract sent to the provider. Invalid/truncated output cannot reach SQLite.
Accepting a pending result also restores that candidate's interpretation, even after a
replacement SQL attempt was rejected; rejected SQL cannot relabel the accepted evidence.

The wire schema is a root object with a typed `kind` discriminator and conditional
branches. Live probes on the configured Qwen/auto route repeatedly over-clarified with
root union schemas; the conditional shape produced SQL for the original question without
changing the prompt's metric or removing clarification. This is observed routing behavior,
not proof about provider internals or universal support for conditional constrained decoding.
Local branch enforcement remains mandatory even when a provider accepts the schema.

`workflow.metrics['calls']` contains mode/stage/contract, requested budget, bounded
output kind and validation details, finish reason, per-call tokens and latency, correction
and result-review counts. `response_source` and `fallback_reason` identify the answer path.
No raw model body, thinking, unknown model-controlled keys or credential is logged by this
diagnostic layer. Missing required fields and invalid kind/type are correction feedback,
not fabricated defaults. The SQL and response cells remain separately timed.

Schema requests follow the official [HF structured-output guide](https://huggingface.co/docs/inference-providers/guides/structured-output).
`auto` attempts schema on the ordinary budgeted request, without an extra generation preflight.
Only an explicit HTTP 400/422 format-unsupported error permits the next budgeted SQL call to use
prompt JSON/local validation. No retry is hidden inside the adapter, and no provider changes.
At the response stage, a rejected format yields deterministic fallback for that answer; later
calls use local validation. `json_schema` forbids format downgrade; `local` uses local validation
only. Wire shape validation never replaces strict business validation/compiler enforcement.

Adapter content is capped at 16,000 UTF-8 bytes. Empty output, truncation, safety block, JSON/schema
errors, timeout, authentication, unsupported model/provider/parameters, billing and quota have
distinct diagnostics. Thinking content is never used as a final answer. Missing token counts stay
unknown. SQL planning/repair/review/format-rejection calls share a hard maximum of 3; shadow
records separate calls/tokens/latency. A shadow billing block also prevents response generation.

`HF_PROVIDER=auto` identifies the configured routing policy, **not a verified upstream host**.
API model/finish/format metadata is recorded when available. Locally valid JSON after a schema
request does not prove constrained decoding. Exact model/provider/schema support remains a live
verification item after billing is restored; local enforcement works without that support.

## Evaluation

From the repository root, default checks are offline/read-only:

```powershell
python -m notebooks.evaluation.evaluate_qwen_semantics
python -m notebooks.evaluation.evaluate_qwen_semantics --split holdout
```

Only when billing/key are available, explicitly request charged inference:

```powershell
python -m notebooks.evaluation.evaluate_qwen_semantics --live --modes legacy shadow strict
```

The evaluator reuses the same questions, snapshot, Qwen/provider and generation settings, rotating
mode order per question. It reports first/final execution matches, valid SQL rate, latency, fallback,
API calls, tokens, corrections/reviews and service failures. HTTP 401/402/403/429 stops remaining
modes/cases. Unrun cases are never passes. Accuracy denominators include attempted service failures
as unsuccessful; separate service-error counters prevent attributing those failures to SQL semantics.

Gold/holdout plans are evaluation-only. Offline results measure the compiler, not Qwen. The
ambiguous holdout is unrun offline, not passed. Auto routing, tiny data, cloud load and single-run
sampling limit causal latency/accuracy claims. Pinning an upstream provider is an explicit user
configuration choice. No provider is silently changed for this comparison.

Tests cover grain/binds/enums/NULL/duration/timezone, metamorphic log duplication/time offsets,
service stops, malformed/blocked output, actual installed SDK HTTP request counts, and ordered
Qwen notebook cells from repo/provider directories. HTTP is mocked, not live-model accuracy.
Other consumers have offline regressions. No refresh cells or live provider endpoints are used.

```powershell
python -m pytest tests/unit/test_qwen_contract_recovery.py tests/unit/test_qwen_adapter.py tests/unit/test_qwen_semantic_integration.py tests/unit/test_semantic_plan.py tests/unit/test_notebook_analytics.py tests/unit/test_notebook_grounding.py tests/unit/test_notebook_nemotron.py -q
```

Historical generated validation reports have been removed. Run the evaluator on the
current snapshot to obtain fresh results; offline tests do not establish live model
accuracy or latency, and matching rows on a small fixture is not a semantic proof.
