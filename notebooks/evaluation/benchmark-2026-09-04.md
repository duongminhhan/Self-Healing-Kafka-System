# Live comparison — 2026-09-04

## Recommendation
Keep Qwen/Qwen3-4B-Instruct-2507 as the default for the current small-schema bot, once HF billing is restored. Do not replace it with the current Nemotron thinking-enabled configuration for interactive requests. This is a provisional deployment choice, not proof Qwen is intrinsically more accurate.

## Evidence
Snapshot unchanged: 2 tables, 6 queue entries, 16 logs; SHA256 F2296FFEBC19928A3FFA987082510F6AAD17DF70A52D2DDC5B36128002287E75.
Independent Gold SQL and identical ordered-result matching rules were used. No SQL/data/provider configuration was modified for this comparison.

| Same completed cases 1–4 | Qwen / HF auto | Nemotron / Ollama Cloud |
|---|---:|---:|
| First/final execution match | 4/4 | 4/4 |
| Valid SQL | 4/4 | 4/4 |
| Mean SQL stage | 3.49 s | 36.04 s |
| Mean response stage | 3.93 s | 39.08 s |
| Mean combined stages | 7.42 s | 75.12 s |
| Response fallback | 0/4 | 0/4 |

Nemotron was approximately 10.1x slower on this paired sample.

### Full available coverage
- Qwen cases 1–4 completed correctly. Case 5 produced matching empty-result SQL but the result-review request hit HTTP 402. Cases 6–7 and the clarification probe were not run. Billing failure is not a model accuracy failure.
- Nemotron completed all 7 gold cases: 6/7 first/final execution matches (85.7%); 7/7 SQL statements valid. No inference service errors after the full credential was supplied.
- Nemotron case 6 selected CurrentConnectorName rather than the Gold contract's RootConnectorName. It returned conn-oracle-cdc-payments-v2 instead of conn-oracle-cdc-payments. The generic word "connector" in the question is a semantic ambiguity risk; this is a contract mismatch, not a syntax error. The response accurately repeated the SQL rows, so this discrepancy originated in SQL planning rather than invented response values.
- Nemotron case 7 correctly returned average 35 minutes, matched=2, valid=2, excluded=0.
- Nemotron case 5 used the deterministic empty-result fallback (expected, not hallucination), accounting for the 1/7 fallback rate.
- Nemotron clarification probe succeeded with 2 SQL-stage API calls and 101.45 s. Excluded from the paired latency table.

## Limitations
One sequential run per case, tiny snapshot, no concurrency/load test, no p95 estimate. API endpoint load and network are included in latency. Qwen used 1024/1500 output-token budgets; Nemotron used 4096/4096 and thinking=true. This compares current application configurations, not isolated model throughput. Nemotron thinking=false is untested. HF auto does not identify the underlying host in this telemetry. Response grounding checks passed on the paired sample, but are not a comprehensive human-language correctness evaluation.

The initial abbreviated Ollama credential returned HTTP 401 before inference; that rejected request is excluded from performance/accuracy comparison. Credentials were used only in the child process and are absent from these artifacts.

## Next gate
Restore HF billing and use a broader healing fixture with explicit root/current semantics, then rerun both providers with repeated trials and service failures reported separately. Strengthen metric-grain enforcement before assuming a larger model fixes grounding. No conclusion about large schemas or production-scale datasets follows from this run.

