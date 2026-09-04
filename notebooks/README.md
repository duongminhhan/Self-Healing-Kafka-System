# Notebook experiments

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
