"""Qwen-only mode comparison. Default: offline compiler checks, zero API calls.

Run --live explicitly to incur HF inference charges. Never refreshes a snapshot.
The same questions, snapshot, client and generation settings serve all modes.
"""

import argparse
import hashlib
import json
import os
import sqlite3
import statistics
import time
import warnings
from contextlib import closing
from pathlib import Path

from dotenv import load_dotenv

from notebooks.evaluation.evaluate import matches
from notebooks.evaluation.semantic_cases import SEMANTIC_CASES
from notebooks.qwen.adapter import QwenClient
from notebooks.shared.analytics import QueryError, Snapshot
from notebooks.shared.semantic_plan import compile_plan
from notebooks.shared.semantic_workflow import SemanticWorkflow

ROOT = Path(__file__).resolve().parents[2]
MODES = ("legacy", "shadow", "strict")


def fingerprint(path):
    with Path(path).open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def evaluate(
    snapshot,
    *,
    client=None,
    model="Qwen/Qwen3-4B-Instruct-2507",
    provider="auto",
    modes=MODES,
    cases=SEMANTIC_CASES,
    sql_max_tokens=1024,
    response_max_tokens=1500,
    max_attempts=3,
    few_shot=True,
):
    if not modes or set(modes) - set(MODES):
        raise ValueError("Select legacy, shadow, strict")
    before = fingerprint(snapshot.path)
    before_signature = snapshot.signature()
    report = {
        "kind": "live_qwen_mode_comparison" if client else "offline_compiler_checks",
        "snapshot_sha256": before,
        "snapshot": snapshot.metadata(),
        "configuration": {
            "model": model,
            "provider": provider,
            "modes": list(modes),
            "sql_max_tokens": sql_max_tokens,
            "response_max_tokens": response_max_tokens,
            "max_sql_calls": min(3, max(1, max_attempts)),
            "temperature": 0,
            "few_shot": few_shot,
            "timeouts": getattr(client, "timeouts", None),
            "structured_output": getattr(client, "structured_output", None),
        },
        "model_accuracy": None,
        "records": [],
        "service_blocked": False,
        "comparison": "Ordered rows/columns, aliases ignored, NULL exact, numeric absolute tolerance 0.005",
        "limitations": [
            "Small evaluation set; no statistical superiority claim",
            "Shadow uses only remaining calls from the shared 3-call SQL budget",
            "Provider auto is routing policy, not a verified fixed inference host",
        ],
    }
    try:
        for index, case in enumerate(cases):
            expected = None
            if case["reference_sql"]:
                with closing(
                    sqlite3.connect(snapshot.path.as_uri() + "?mode=ro", uri=True)
                ) as conn:
                    expected = conn.execute(case["reference_sql"]).fetchall()
            if client is None:
                record = {
                    "case": case["id"],
                    "split": case["split"],
                    "model_executed": False,
                    "compiler_match": None,
                }
                if case["plan"] is not None:
                    compiled = compile_plan(case["plan"], snapshot)
                    result = snapshot.execute(compiled.sql, compiled.parameters)
                    record.update(
                        compiler_match=matches(result, expected),
                        sql=result["sql"],
                        parameters=compiled.parameters,
                        rows=result["rows"],
                        expected=expected,
                    )
                else:
                    record["not_run_reason"] = (
                        "Clarification behavior requires model or separate mock contract test"
                    )
                report["records"].append(record)
                continue
            # Counterbalance position by question, without changing generation settings.
            order = list(modes[index % len(modes) :]) + list(modes[: index % len(modes)])
            for mode in order:
                if report["service_blocked"]:
                    report["records"].append(
                        {
                            "case": case["id"],
                            "split": case["split"],
                            "mode": mode,
                            "status": "not_run_service_block",
                        }
                    )
                    continue
                flow = SemanticWorkflow(
                    snapshot,
                    client,
                    model_id=model,
                    provider=provider,
                    mode=mode,
                    sql_max_tokens=sql_max_tokens,
                    response_max_tokens=response_max_tokens,
                    max_attempts=max_attempts,
                    few_shot=few_shot,
                )
                record = {
                    "case": case["id"],
                    "split": case["split"],
                    "mode": mode,
                    "status": "attempted",
                    "first_match": False,
                    "final_match": False,
                    "clarification_match": None,
                    "fallback": False,
                }
                start = time.perf_counter()
                try:
                    result = flow.query(case["question"])
                    if case.get("expected_clarification"):
                        record["clarification_match"] = bool(flow.clarification) and result is None
                    else:
                        record["first_match"] = matches(flow.first_result, expected)
                        record["final_match"] = matches(result, expected)
                    response = flow.respond() if result is not None or flow.clarification else None
                    record.update(
                        result=result,
                        response=response,
                        fallback=bool(response and response["source"] == "verified_table_fallback"),
                    )
                except QueryError as exc:
                    record["error"] = str(exc)
                record.update(
                    metrics=flow.metrics,
                    trace=flow.trace,
                    plan=flow.semantic_plan,
                    shadow=flow.shadow,
                    clarification=flow.clarification,
                    end_to_end_seconds=time.perf_counter() - start,
                )
                errors = [t for t in flow.trace if t.get("status") == "api_error"]
                response_error = flow.metrics.get("response_service_error")
                if response_error:
                    errors.append(response_error)
                record["service_errors"] = errors
                if any(
                    e.get("http_status") in {401, 402, 403, 429}
                    or e.get("category")
                    in {"authentication", "billing", "quota", "quota_or_billing"}
                    for e in errors
                ):
                    report["service_blocked"] = True
                report["records"].append(record)
        report["summaries"] = summarize(report["records"], modes) if client else {}
        report["offline_compiler_passed"] = (
            all(r["compiler_match"] is not False for r in report["records"])
            if client is None
            else None
        )
        return report
    finally:
        if fingerprint(snapshot.path) != before or snapshot.signature() != before_signature:
            raise RuntimeError("Snapshot changed during evaluation; comparison is invalid")


def summarize(records, modes):
    summaries = {}
    for mode in modes:
        selected = [r for r in records if r.get("mode") == mode]
        attempted = [r for r in selected if r["status"] == "attempted"]
        sql_cases = [r for r in attempted if r["clarification_match"] is None]
        n = len(sql_cases)
        attempts = sum(r["metrics"]["sql_attempts"] for r in attempted)
        summaries[mode] = {
            "attempted": len(attempted),
            "not_run": len(selected) - len(attempted),
            "sql_accuracy_denominator": n,
            "accuracy_policy": "Attempted SQL cases including service failures; unrun and clarification cases excluded",
            "first_attempt_execution_accuracy": sum(r["first_match"] for r in sql_cases) / n
            if n
            else None,
            "final_execution_accuracy": sum(r["final_match"] for r in sql_cases) / n if n else None,
            "valid_sql_rate": sum(r["metrics"]["valid_sql_attempts"] for r in attempted) / attempts
            if attempts
            else None,
            "service_error_cases": sum(bool(r["service_errors"]) for r in attempted),
            "clarification_passed": sum(r["clarification_match"] is True for r in attempted),
            "fallback_rate": sum(r["fallback"] for r in attempted) / len(attempted)
            if attempted
            else None,
            "mean_end_to_end_seconds": statistics.mean(r["end_to_end_seconds"] for r in attempted)
            if attempted
            else None,
            "total_sql_api_calls": sum(
                r["metrics"].get("total_sql_api_calls", r["metrics"]["sql_api_calls"])
                for r in attempted
            ),
            "response_api_calls": sum(r["metrics"]["response_api_calls"] for r in attempted),
        }
    return summaries


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", default=str(ROOT / "self_healthy_kafka_snapshot.db"))
    parser.add_argument("--modes", nargs="+", choices=MODES, default=list(MODES))
    parser.add_argument("--split", choices=["all", "gold", "holdout"], default="all")
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    load_dotenv(ROOT / ".env")
    model = os.getenv("HF_MODEL_ID", "Qwen/Qwen3-4B-Instruct-2507").strip()
    provider = os.getenv("HF_PROVIDER", "auto").strip().lower() or "auto"
    client = None
    skip_reason = "Live not requested"
    if args.live:
        if not model.lower().startswith("qwen/"):
            warnings.warn(
                "Configured model is not Qwen; refusing Qwen comparison without changing configuration",
                stacklevel=1,
            )
            skip_reason = "Configured model is not Qwen"
        elif not os.getenv("HF_TOKEN", "").strip():
            skip_reason = "HF_TOKEN unavailable"
        else:
            client = QwenClient(
                structured_output=os.getenv("HF_STRUCTURED_OUTPUT", "auto").strip().lower(),
                model=model,
                provider=provider,
                api_key=os.environ["HF_TOKEN"],
                sql_timeout=float(os.getenv("HF_SQL_REQUEST_TIMEOUT_SECONDS", "30")),
                response_timeout=float(os.getenv("HF_RESPONSE_REQUEST_TIMEOUT_SECONDS", "30")),
            )
    cases = [c for c in SEMANTIC_CASES if args.split == "all" or c["split"] == args.split]
    report = evaluate(
        Snapshot(args.snapshot, row_limit=1000),
        client=client,
        model=model,
        provider=provider,
        modes=tuple(args.modes),
        cases=cases,
        sql_max_tokens=int(os.getenv("HF_MAX_TOKENS", "1024")),
        response_max_tokens=int(os.getenv("HF_RESPONSE_MAX_TOKENS", "1500")),
        max_attempts=int(os.getenv("HF_AGENT_MAX_STEPS", "3")),
        few_shot=os.getenv("HF_FEW_SHOT", "true").lower() in {"true", "1", "yes"},
    )
    if client is None:
        report["live_not_run_reason"] = skip_reason
        report["live_cases_not_run"] = len(cases) * len(args.modes)
    print(json.dumps(report, ensure_ascii=True, indent=2))
    if client:
        return int(
            report["service_blocked"]
            or any(
                r["status"] != "attempted"
                or not (r["final_match"] or r["clarification_match"] is True)
                for r in report["records"]
            )
        )
    return int(not report["offline_compiler_passed"])


if __name__ == "__main__":
    raise SystemExit(main())
