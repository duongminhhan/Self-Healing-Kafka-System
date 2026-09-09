# Running self-healthy-kafka

Create a Python 3.12 virtual environment and install the package:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
cp env/prod.env.example env/prod.env
```

Install Microsoft ODBC Driver 18 for SQL Server on the host. Update the chosen
environment file with the real Kafka Connect, SQL Server, webhook, and healing
settings.

For a first database installation, execute these table scripts in DBeaver:

```text
sql/init-table/ConnectorHealingQueue.sql
sql/init-table/ConnectorHealingLogs.sql
sql/ingest_reference/views/vConnectorIncidentFacts.sql
```

Then execute the runtime stored procedures:

```text
sql/ingest_reference/stored-procedures/spEnqueueConnectorHealing.sql
sql/ingest_reference/stored-procedures/spGetConnectorHealingQueue.sql
sql/ingest_reference/stored-procedures/spGetConnectorHealingLogs.sql
sql/ingest_reference/stored-procedures/spSearchConnectorHealingLogs.sql
sql/ingest_reference/stored-procedures/spGetConnectorIncidentFacts.sql
sql/ingest_reference/stored-procedures/spGetConnectorFailureRanking.sql
sql/ingest_reference/stored-procedures/spInsertConnectorHealingLog.sql
sql/ingest_reference/stored-procedures/spUpdateConnectorHealingQueue.sql
```

For an existing database, also execute
`sql/ingest_reference/stored-procedures/drop-legacy-procedures.sql` to remove
retired metric and topic-lag procedures. It does not drop historical tables.

Start the app:

```bash
bash scripts/run.sh prod
```

Other environments:

```bash
bash scripts/run.sh uat
bash scripts/run.sh dev
```

Run one connector reconciliation pass:

```bash
APP_ENV=uat SELF_HEALTHY_KAFKA_ENV_FILE=env/uat.env \
  python -m self_healthy_kafka.main --health-check-once
```

The webhook server exposes `GET /health` and the configured Grafana POST path.
The application does not expose a custom metrics endpoint.

## Chat API

The backend exposes `POST /api/v1/chat` on the webhook port when chat analytics
or runbook RAG is enabled. The optional [chat UI](apps/chat-ui/README.md) runs
separately with Next.js and assistant-ui LocalRuntime. Its server-side BFF injects
the private `CHAT_API_TOKEN`; the browser sends only the current question. Natural
answers, citations, route/source badges and closed technical details are separate.
The default UI binds to loopback; end-user authentication is required before
shared deployment. Visual session history is not semantic multi-turn. Keep
diagnostics disabled for ordinary users and never expose backend credentials.

## Optional Hugging Face analytics planner

For bounded Vietnamese analysis (time filters, rankings, grouping, recovery
state), apply `sql/ingest_reference/views/vConnectorIncidentFacts.sql` and
`sql/ingest_reference/stored-procedures/spGetConnectorIncidentFacts.sql`, then
set `CHAT_ANALYTICS_ENABLED=true`. Set `CHAT_ANALYTICS_TIMEZONE` explicitly.
To use a Hugging Face Dedicated Endpoint, set `HF_CHAT_ENDPOINT_URL`,
`HF_CHAT_TOKEN`, and `HF_CHAT_MODEL_ID`; these values stay in the backend and
are never returned to the browser. The planner can return only a validated JSON
query plan; the app calls the fixed read-only procedure with bound parameters.
UAT/Prod DBAs must apply both SQL scripts manually before enabling the flag.

## Optional approved-runbook RAG with Qdrant Cloud

Runbook RAG is opt-in and uses the existing Qwen/Hugging Face endpoint for
answer composition. Qdrant Cloud Inference embeds approved Markdown runbooks;
Qdrant is only a replaceable search index. The files under `runbooks/` remain
the authoritative, version-controlled source.

Dense retrieval remains the default and the production baseline. The canonical
selector is `RAG_RETRIEVAL_MODE=dense|hybrid|shadow`; the existing
`RAG_SEARCH_MODE`/`QDRANT_COLLECTION` pair is still accepted for backward
compatibility. Do not change the default to Hybrid unless the untouched holdout
passes every promotion gate described below.

Dense mode preserves the original `healing_runbooks_v1` contract: one unnamed
cosine vector and the existing `RAG_SCORE_THRESHOLD`. Hybrid mode uses a
separate versioned collection (for example `healing_runbooks_v2`) with one named
dense vector and one named BM25 sparse vector. Both Prefetch channels receive
the same tenant, approved-status, environment, connector taxonomy and
error-code filters. Qdrant then fuses their candidate rankings with server-side
weighted RRF. Weighted RRF requires Qdrant 1.17 or newer and
`qdrant-client>=1.17`; see the official
[Hybrid Queries documentation](https://qdrant.tech/documentation/search/hybrid-queries/).
The runtime fails with an explicit configuration error when weighted RRF is not
supported. It uses equal-weight RRF only when
`RAG_HYBRID_WEIGHTED_RRF_FALLBACK_TO_EQUAL=true` is explicitly configured and
records the fallback reason. A fused RRF score is not comparable with the
legacy cosine score, so the final fused list is not filtered by
`RAG_SCORE_THRESHOLD`.

Approved runbooks use semantic metadata schema v2 (`connector_type`,
`connector_family`, `subsystem`, symptoms, exception classes, config keys,
error signatures, aliases and Vietnamese/English user phrases). Indexing embeds
a controlled retrieval text built from the title, safe metadata and section
content; the original section text remains the answer-facing evidence. Invalid
metadata and secret-like values fail validation rather than entering Qdrant.

1. In the Qdrant Cloud cluster's **Inference** tab, enable Cloud Inference and
   select a supported multilingual embedding model. Set
   `QDRANT_EMBEDDING_MODEL` to that exact model identifier and
   `QDRANT_EMBEDDING_SIZE` to its documented vector dimension.
2. Create a least-privilege Database API key and set `QDRANT_URL` and
   `QDRANT_API_KEY` only in the selected private environment file or secret
   store. Do not commit either value.
3. Validate all Markdown and preview the number of chunks without contacting
   Qdrant:

   ```bash
   SELF_HEALTHY_KAFKA_ENV_FILE=env/uat.env python -m scripts.index_runbooks
   ```

4. Explicitly index the approved corpus:

   ```bash
   RAG_ENABLED=true SELF_HEALTHY_KAFKA_ENV_FILE=env/uat.env \
     python -m scripts.index_runbooks --apply
   ```

   Re-running this command is idempotent. It updates changed chunks and removes
   stale points for the configured tenant. Draft and deprecated runbooks are
   never indexed. To roll back, check out the desired runbook revision and run
   the same command again.

### Create and validate the hybrid v2 collection

Keep v1 intact. Configure the following values in a private environment file.
The two collection names are used by canonical service and shadow modes;
`QDRANT_COLLECTION` plus `RAG_SEARCH_MODE` identify the concrete collection
schema used by the indexing utility:

```dotenv
RAG_RETRIEVAL_MODE=dense
RAG_SEARCH_MODE=hybrid
QDRANT_COLLECTION=healing_runbooks_v2
QDRANT_DENSE_COLLECTION=healing_runbooks_v1
QDRANT_HYBRID_COLLECTION=healing_runbooks_v2
QDRANT_DENSE_VECTOR_NAME=dense
QDRANT_SPARSE_VECTOR_NAME=sparse
QDRANT_DENSE_EMBEDDING_MODEL=
QDRANT_SPARSE_EMBEDDING_MODEL=qdrant/bm25
RAG_FINAL_TOP_K=5
RAG_CANDIDATE_LIMIT=15
RAG_MAX_CHUNKS_PER_RUNBOOK=2
RAG_DIVERSIFICATION_ENABLED=
RAG_HYBRID_DENSE_CANDIDATES=15
RAG_HYBRID_SPARSE_CANDIDATES=15
RAG_FUSION_METHOD=rrf
RAG_FUSION_LIMIT=15
RAG_HYBRID_DENSE_WEIGHT=1
RAG_HYBRID_SPARSE_WEIGHT=1
RAG_HYBRID_WEIGHTED_RRF_FALLBACK_TO_EQUAL=false
RAG_DENSE_SCORE_THRESHOLD=
RAG_SPARSE_SCORE_THRESHOLD=
RAG_HYBRID_FALLBACK_TO_DENSE=false
RAG_EVIDENCE_GATE_ENABLED=
RAG_EVIDENCE_MIN_SCORE=0
RAG_EVIDENCE_MIN_MARGIN=0
RAG_EVIDENCE_REQUIRE_ANCHOR_MATCH=true
```

An empty `QDRANT_DENSE_EMBEDDING_MODEL` inherits the legacy
`QDRANT_EMBEDDING_MODEL`. Confirm in the Qdrant Cloud Inference UI that both
configured model IDs are supported. `QDRANT_EMBEDDING_SIZE` must match the
dense model. Candidate limits are bounded to 1-100. The legacy
`RAG_DENSE_CANDIDATE_LIMIT` and `RAG_SPARSE_CANDIDATE_LIMIT` names are accepted,
but the `RAG_HYBRID_*_CANDIDATES` names above are canonical. A blank channel
threshold means no additional override; the dense channel inherits
`RAG_SCORE_THRESHOLD`.

Diversification groups candidates by runbook, removes duplicate chunks and
round-robins the best runbooks before enforcing `RAG_FINAL_TOP_K` and the context
limit. The evidence gate returns a grounded no-answer when no candidate has
sufficient semantic or exact error/config/exception evidence. Blank
`RAG_DIVERSIFICATION_ENABLED` and `RAG_EVIDENCE_GATE_ENABLED` values keep legacy
Dense behavior unchanged but enable both controls automatically in Hybrid and
Shadow modes. Calibrate score and margin thresholds on tuning data only; do not
copy a fused RRF threshold into Dense mode or tune it on the holdout.

Preview the corpus without touching Qdrant, then explicitly create/index v2:

```powershell
$env:SELF_HEALTHY_KAFKA_ENV_FILE = "env/uat.env"
python -m scripts.index_runbooks
$env:RAG_ENABLED = "true"
python -m scripts.index_runbooks --apply
```

The apply command creates only the configured collection, validates both named
vector schemas, indexes both vectors on the same stable point ID and preserves
incremental inserted/updated/unchanged/removed behavior. It never drops v1 and
does not switch an alias. Search and upsert fail with a configuration error if
hybrid mode points at a dense-only collection.

### Validate the Gold contract and tune Hybrid

The checked-in Gold Retrieval corpus currently contains 93 cases: 92 measured
cases (71 tuning and 21 untouched holdout) plus one fixture-only case. It covers
exact errors, exact config keys, semantic and mixed-language queries,
connector-specific and filter-sensitive cases, multi-symptom questions, typos,
ambiguity and hard negatives. Validate that contract offline first:

```powershell
python -m scripts.evaluate_runbook_retrieval --summary-only
```

Offline output must say `benchmark_status=not_run` and
`promotion_gate.passed=false`. Dataset validation, a dry run, a quick run or an
unreachable external service is never evidence that Hybrid passed.

Run the same Gold questions against both collections without an answer LLM. A
promotion-capable benchmark uses the end-to-end `router` profile, five
repetitions and the full tuning grid: RRF weights 1:1, 2:1 and 3:1 crossed with
dense/sparse candidate limits 10, 20 and 30. The best tuning configuration is
then evaluated once as a configuration (five measured repetitions per case)
against the untouched holdout alongside Dense:

```powershell
$env:RUNBOOK_RAG_LIVE_TEST = "true"
$env:RAG_ENABLED = "true"
$report = Join-Path $env:TEMP "runbook-hybrid-benchmark.json"
python -m scripts.evaluate_runbook_retrieval --live --repetitions 5 `
  --profile router --summary-only --require-gates `
  --dense-collection healing_runbooks_v1 `
  --hybrid-collection healing_runbooks_v2 `
  --output $report
if ($LASTEXITCODE -ne 0) { Write-Warning "Quality gates failed; keep Dense active." }
```

`--quick` evaluates only the currently configured weights/candidate limits once
and is explicitly non-promotable. `--profile controlled` is diagnostic only
because it can apply expected labels as filters; never use it to authorize
production promotion.

The report separates Recall@1/3/5, section Recall@5, MRR, graded nDCG@5,
exact-error/config-key/semantic/mixed results, correct no-answer and
wrong-runbook rates, filter violations, retrieval failures, fallback rate,
candidate diversity/count and p50/p95 latency. It also lists improved,
regressed and Hybrid false-positive cases. Running the evaluator without
`--live` validates the dataset but does not manufacture benchmark scores.
Server-side fusion returns the fused list but not each Prefetch list, so runtime
`dense_candidate_count` and `sparse_candidate_count` remain `null`; their
configured limits and the actual fused count are reported instead. Qdrant also
does not expose separate channel timing inside one fused API call, so the fused
request and total latency are measured without inventing dense/sparse timings.

Hybrid is eligible for promotion only when the untouched holdout report has
`promotion_gate.passed=true`, is based on at least five repetitions and has no
retrieval failures. The Gold contract must also contain at least 75 measured
cases and at least 20% untouched holdout cases. The gate additionally requires:

- no regression versus Dense in Recall@1/3/5 or semantic, exact-error and
  exact-config-key Recall@5;
- correct no-answer rate at least 90%, wrong-runbook rate at most 10%, zero
  filter violations, retrieval failure rate within the configured limit and
  zero retrieval fallback (so an unsupported weighted-RRF server cannot pass by
  silently using equal RRF or Dense);
- Hybrid p95 latency no more than 20% above Dense p95.

If any gate fails or is unmeasured, keep `RAG_RETRIEVAL_MODE=dense`. Do not
manually reinterpret a failed report as a pass.

### Run Hybrid in shadow mode

Shadow mode keeps Dense as the synchronous answer path and samples Hybrid into
a bounded background queue. It performs retrieval only: it does not invoke a
second answer LLM and cannot change or delay the user response. Logs contain
sanitized runbook IDs, rank changes, latency, no-answer state and fallback
reason, but not the question, chunk text, credentials or raw exceptions.

```dotenv
RAG_RETRIEVAL_MODE=shadow
QDRANT_DENSE_COLLECTION=healing_runbooks_v1
QDRANT_HYBRID_COLLECTION=healing_runbooks_v2
RAG_HYBRID_SHADOW_SAMPLE_RATE=0.10
RAG_HYBRID_TIMEOUT_SECONDS=10
RAG_HYBRID_SHADOW_QUEUE_SIZE=32
RAG_DIAGNOSTICS_ENABLED=false
```

`RAG_HYBRID_SHADOW_ENABLED=true` remains a backward-compatible shortcut when
`RAG_RETRIEVAL_MODE` is blank; prefer the explicit `shadow` mode for new
deployments.

Start with a low sample rate in UAT/canary, monitor
`runbook_rag_shadow_completed`, `runbook_rag_shadow_failed` and
`runbook_rag_shadow_dropped`, then increase only if latency, error and false
positive signals remain acceptable. Queue-full, timeout and Hybrid failures are
isolated from Dense. Set the sample rate to `0` or restore
`RAG_RETRIEVAL_MODE=dense` for immediate rollback.

### Promote a versioned collection and roll back safely

The promotion helper is dry-run by default and makes no Qdrant request. It
requires an explicit version in the physical collection name, a benchmark
report whose holdout gate passed, and a compare-and-swap expectation for the
current alias. It rejects offline, quick or stale reports: the five-run holdout
gate is recalculated from the recorded metrics and the report's Gold dataset
SHA-256 must match the `--dataset` file used during promotion. Before the atomic
alias switch it idempotently indexes approved runbooks, validates the hybrid
vector schema, tenant-scoped exact document count, payload indexes and retrieval
smoke cases. It never deletes the old collection.

```powershell
python -m scripts.promote_qdrant_collection `
  --collection healing_runbooks_v2 `
  --alias healing_runbooks `
  --benchmark-report $report `
  --expected-current-collection healing_runbooks_v1

$env:QDRANT_HYBRID_PROMOTION = "true"
python -m scripts.promote_qdrant_collection `
  --collection healing_runbooks_v2 `
  --alias healing_runbooks `
  --benchmark-report $report `
  --expected-current-collection healing_runbooks_v1 `
  --apply
```

Do not run `--apply` when the dry-run says `benchmark_gate_valid=false`.
Promotion refuses a target that is the active Dense collection or that is not
versioned. The apply output records `previous_collection` and an exact
`rollback_command`. Preserve both with the benchmark artifact and deployment
record.

The alias helper is also dry-run by default. Use the apply output's exact
previous collection and compare-and-swap the expected current target during
rollback:

```powershell
python -m scripts.manage_qdrant_alias --alias healing_runbooks `
  --collection healing_runbooks_v1 `
  --expected-current-collection healing_runbooks_v2
$env:QDRANT_ALIAS_CUTOVER = "true"
python -m scripts.manage_qdrant_alias --alias healing_runbooks `
  --collection healing_runbooks_v1 `
  --expected-current-collection healing_runbooks_v2 --apply
```

After a successful promotion, explicitly deploy
`RAG_RETRIEVAL_MODE=hybrid` and point `QDRANT_HYBRID_COLLECTION` at the promoted
alias, then restart and smoke-test the Chat API. An alias cutover by itself does
not enable Hybrid. Restore `RAG_RETRIEVAL_MODE=dense`, restart and smoke-test if
the canary degrades. `RAG_HYBRID_FALLBACK_TO_DENSE=false` is intentional: a
Hybrid failure remains visible rather than silently changing retrieval
behavior. Automated tests and ordinary indexing never modify aliases.

### User-facing response contract

The Chat API keeps the natural answer and citations while returning structured
`status`, `reason`, `evidence`, `recommended_runbooks` and `candidates` fields.
Evidence contains bounded matched labels such as an error code, exception class,
config key or signature; it never exposes raw chunk text or Qdrant scores. An
unsupported query or weak match returns `status=no_answer` with a specific
reason instead of inventing a runbook. Material ambiguity may return
`status=needs_clarification`, while Qdrant/provider failures return
`status=degraded` and remain distinguishable from a valid no-answer.

Keep `RAG_DIAGNOSTICS_ENABLED=false` in the user-facing deployment. Detailed
candidate counts, diversification/evidence-gate decisions and fallback reasons
belong only in admin/debug output and structured backend logs.

Temporary benchmark collections can be removed with a separately guarded
helper. It refuses any collection name that does not contain `test` and is a
dry-run unless both the flag and environment guard are supplied:

```powershell
python -m scripts.delete_qdrant_test_collection `
  --collection healing_runbooks_hybrid_test_20260908
$env:QDRANT_TEST_CLEANUP = "true"
python -m scripts.delete_qdrant_test_collection `
  --collection healing_runbooks_hybrid_test_20260908 --apply
```

Deletion is not reversible; re-run the v2 indexing command to recreate it.

Hybrid search adds sparse inference and fusion work, so it can cost more and be
slower than dense-only retrieval. Use the Gold report rather than model size or
anecdotal examples to decide whether exact-token recall justifies that cost.
Chat history remains a separate application concern: hybrid retrieval alone
does not create semantic multi-turn conversations.

### Enable the Chat API

Keep `CHAT_API_ENABLED=true`, configure the existing `HF_CHAT_ENDPOINT_URL`,
`HF_CHAT_TOKEN`, and `HF_CHAT_MODEL_ID`, then enable `RAG_ENABLED=true` and
restart the service. Pure analytics questions retain the existing
fixed-procedure path; runbook and combined questions retrieve only approved,
tenant/environment-filtered chunks.

Run a live retrieval smoke test only when intentionally allowed:

```bash
RUNBOOK_RAG_LIVE_TEST=true RAG_ENABLED=true \
  SELF_HEALTHY_KAFKA_ENV_FILE=env/uat.env \
  python -m scripts.retrieve_runbooks "ORA-01017 cần xử lý thế nào?"
```

The offline evaluation reports routing accuracy while leaving retrieval and
generation metrics as `null` instead of pretending an external service ran:

```bash
SELF_HEALTHY_KAFKA_ENV_FILE=env/dev.env.example \
  python -m scripts.evaluate_runbook_rag
```

Use `--live` with `RUNBOOK_RAG_LIVE_TEST=true` only after indexing. Rotate a
Qdrant key by creating a replacement key, updating the deployment secret,
restarting and smoke-testing the service, then revoking the old key. Monitor
the structured `runbook_rag_completed` log for route, source, fallback reason,
retrieval hit count and latency; the question and retrieved text are not logged.
Add `--with-generation` to the live evaluator to call the configured Qwen/HF
endpoint and measure citation precision, forbidden-claim rate, required-claim
recall, fallback rate, and end-to-end latency. A live run that cannot reach an
external service is reported as a failure, never as a passed case.

## Local chatbot context test

Set `CHAT_API_ENABLED=true` and a private `CHAT_API_TOKEN` in the selected
`env/<environment>.env` file. Execute the runtime procedure scripts above,
then restart the application. The API shares the webhook port and is read-only:

```bash
curl -sS \
  -H "Authorization: Bearer $CHAT_API_TOKEN" \
  "http://127.0.0.1:8080/api/v1/incidents?status=all&limit=20"

curl -sS \
  -H "Authorization: Bearer $CHAT_API_TOKEN" \
  "http://127.0.0.1:8080/api/v1/healing-logs?limit=20"
```

To ask the app in normal language, install a local Ollama model, set
`OLLAMA_ENABLED=true`, `OLLAMA_MODEL`, and `OLLAMA_CONTEXT_LOG_LIMIT` (for
example `3`), then restart the app. For every question the app performs one
parameterized retrieval from `ConnectorHealingLogs`, redacts the retrieved rows,
and sends only that evidence plus the original question to Ollama. Ollama never
receives SQL Server credentials, arbitrary SQL, or a Kafka Connect write endpoint:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $CHAT_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"question":"liệt kê top connector chết nhiều nhất"}' \
  "http://127.0.0.1:8080/api/v1/chat"
```

For a local CPU-only Ollama container, bound generation with
`OLLAMA_MAX_TOKENS=256`. The response contains `sources` with the exact
redacted rows retrieved from the DB; use their log IDs to verify the answer.
See [CHATBOT_TEST_SCENARIOS.md](CHATBOT_TEST_SCENARIOS.md) for the full local
validation procedure.
