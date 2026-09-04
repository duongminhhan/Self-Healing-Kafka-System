# Nemotron response thinking ablation — 2026-09-04

SQL retained from the prior thinking=true run, re-executed against the unchanged
snapshot and checked against independent Gold rows. This test did not regenerate
SQL or measure fresh end-to-end pipeline latency. Four cases, eight paid response
calls, counterbalanced order (off/on, on/off, off/on, on/off). Within each pair,
question, evidence, interpretation=null, examples and 4096 output budget matched.

| Case | Thinking on (seconds) | Thinking off (seconds) |
|---|---:|---:|
| Incident ranking | 27.01 | 24.40 |
| JOIN log counts by root | 27.79 | 25.66 |
| Severity aggregation | 58.18 | 25.91 |
| NULL AttemptNo | 49.10 | 26.39 |
| **Mean** | **40.52** | **25.59** |

Mean response latency decreased 36.86% (14.94 seconds). Total reported output
tokens decreased from 1995 to 853, or 57.24%; this is not a billing calculation.
All four answers in each configuration passed evidence validation with no
fallback. Manual inspection found preserved names/counts/NULL values; off-mode
wording was slightly shorter. API metadata confirms thinking_present=false in
every off response and true in every on response.

Recommendation: for this Nemotron bot, keep SQL thinking on and turn response
thinking off. Adapter now supports NEMOTRON_SQL_THINKING=true and
NEMOTRON_RESPONSE_THINKING=false. When unset, both inherit NEMOTRON_THINKING,
preserving previous defaults. No credential or local environment file was saved.

This is a small single-run paired experiment, not a statistical guarantee or a
new SQL-accuracy test. Cloud latency varied; two cases account for most of the
mean improvement. No claim of a 36.86% full-pipeline speedup is justified. The
earlier Qwen response baseline was 3.93 seconds on these cases, but was measured
at another time. Disabling response thinking does not establish that Nemotron
is faster than Qwen. Thinking-off has not been tested on complex narratives.

Reproduce with an environment OLLAMA_API_KEY and:
`python -m notebooks.evaluation.response_thinking_experiment`
The script checks the historical snapshot hash and refuses changed evidence.
It makes eight generation calls plus model-list preflight; credentials never
appear in its output. Detailed observations are in response-thinking-2026-09-04.json.
