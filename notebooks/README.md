# Notebook experiments

## Nemotron 3 Ultra / Ollama Cloud

New snapshot-only notebook: `nemotron/text_to_sql_self_healthy_kafka_nemotron.ipynb`.
Its `adapter.py` calls **https://ollama.com** directly; it does not install or use
local Ollama and never falls back to HF/Gemini. Analytics, semantic catalog,
SQL enforcement and evidence validation remain in `shared/`.

From the repository root:

```powershell
python -m pip install -r notebooks/nemotron/requirements.txt
python -m notebooks.evaluation.evaluate --backend nemotron
python -m notebooks.evaluation.evaluate --backend nemotron --fixture normal
```

Set credentials in process environment or create the Git-ignored root
`.env.nemotron` yourself (never commit it):

```dotenv
OLLAMA_API_KEY=<your-new-key>
NEMOTRON_MODEL_ID=nemotron-3-ultra
NEMOTRON_REQUEST_TIMEOUT_SECONDS=120
NEMOTRON_SQL_MAX_TOKENS=4096
NEMOTRON_RESPONSE_MAX_TOKENS=4096
NEMOTRON_THINKING=true
NEMOTRON_SQL_THINKING=true
NEMOTRON_RESPONSE_THINKING=false
```

Restart the kernel after changing environment configuration. Run A (configuration
and Cloud model-list check), B (existing snapshot), C (SQL), then D (Vietnamese
response). Both repo-root and provider-folder kernel working directories work.
Stage-specific thinking variables override `NEMOTRON_THINKING`; if absent, both
stages retain the previous shared setting. The example above keeps SQL thinking
enabled and disables it only for Vietnamese response generation.
Rerun D without rerunning SQL; after changing the question, run C again. Clear
notebook outputs before saving/sharing. Missing credentials fail explicitly.
`BENCHMARK_SQLITE_PATH` overrides the root `self_healthy_kafka_snapshot.db`;
relative paths resolve from the repository root. No MSSQL refresh is performed.

Intentional paid inference, only with a valid configured key:

```powershell
python -m notebooks.evaluation.evaluate --backend nemotron --live --case 1
python -m notebooks.evaluation.evaluate --backend nemotron --live
```

HF is still the default evaluator backend. Compare providers without refreshing
the snapshot between runs. Offline reference checks and mocked model responses
do **not** measure model accuracy. Missing-key runs report live skipped; no
unexecuted model case is a pass. Quota/billing errors stop live generation.

Cloud model ID `nemotron-3-ultra` was confirmed in the official
[model list](https://ollama.com/api/tags); the CLI `:cloud` suffix is not used.
[Cloud API documentation](https://docs.ollama.com/cloud) describes direct bearer
authentication. [Structured output documentation](https://docs.ollama.com/capabilities/structured-outputs)
currently excludes Cloud: this adapter requests JSON through the shared prompt
and validates it locally, without sending `format`. `think` is boolean here;
unverified named thinking levels are rejected. Output budget uses `num_predict`;
provider parameter rejections surface as errors, not silent fallbacks.

Thinking-only, truncated, malformed and ungrounded responses are not accepted as
answers. Thinking text is not logged. Unknown token counts remain null. SQL has
a total three-call correction/review budget, plus an independent response call.
Raw log Message/Details reads are blocked by the SQLite authorizer even through
aliases, expressions or filters. These are partial semantic guards, not complete
ontology enforcement or a proof that a model-generated SQL query is correct.

The Gemini paths below describe the previous layout. Gemini files are currently
deleted in the user's worktree; this change does not restore them. Its old tests
requiring those files are not part of the Nemotron/Qwen regression gate.

## Layout

- `qwen/`: Hugging Face/Qwen notebook and provider dependencies.
- `gemini/`: Gemini notebook, API adapter, dependencies and historical live report.
- `shared/`: analytics runtime, semantic catalog, diagnostics and few-shot examples. One implementation for both providers.
- `evaluation/`: common gold queries, comparison rules and synthetic fixtures.
- Tests remain in `tests/unit/test_notebook*.py` for normal test discovery.

Close old notebook tabs and restart kernels before opening the new paths. Saved outputs were cleared to avoid showing stale runs; SQL/response stage separation and model defaults are unchanged.

## Run

Install from the repository root:

```powershell
python -m pip install -r notebooks/qwen/requirements.txt
python -m pip install -r notebooks/gemini/requirements.txt
```

Open `qwen/text_to_sql_self_healthy_kafka.ipynb` or `gemini/text_to_sql_self_healthy_kafka_gemini.ipynb`. Run setup, Step A (SQL) and Step B (Vietnamese response). Kernels can start from repository root or from either provider directory; setup resolves imports and configuration against the repository root.

Credentials stay outside source: Qwen reads root `.env` (`HF_TOKEN`, `HF_MODEL_ID`, `HF_PROVIDER`). Gemini reads root `.env.gemini` first (`GEMINI_API_KEY`, `GEMINI_MODEL_ID`), then `.env` without overwriting environment values. No credentials or databases were moved.

Both use root `self_healthy_kafka_snapshot.db` by default. `BENCHMARK_SQLITE_PATH` can override it; relative notebook paths resolve against repository root.

**Qwen retains its MSSQL refresh cells.** Running all its cells refreshes the local snapshot and requires MSSQL/Docker. Gemini is snapshot-only. Do not refresh while comparing providers. This reorganization does not change either flow.

## Evaluate

Run from repository root (offline by default):

```powershell
python -m notebooks.evaluation.evaluate
python -m notebooks.evaluation.evaluate --backend gemini
python -m notebooks.evaluation.evaluate --backend gemini --fixture normal
```

Add `--live` only for intentional API calls, which may incur charges. HF remains the evaluator default. `python notebooks/evaluation/evaluate.py` also works. Results in `gemini/evaluation_report.json` are historical, not new live results from this move.
