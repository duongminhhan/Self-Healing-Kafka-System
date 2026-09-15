"""Evaluate the semantic chat API without treating mocks as model accuracy.

The default mode validates independently authored semantic-plan fixtures only.
``--live`` is intentionally opt-in, uses the configured provider and the
read-only incident procedure, and sends at most 30 chat requests.
"""

# ruff: noqa: E402 -- direct script execution must prefer this checkout's src tree.

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if __package__:
    from ._repo_bootstrap import bootstrap_repo_src
else:
    # ``runpy``/unit tests import this file outside the scripts directory;
    # direct CLI execution also needs the current checkout's helper rather
    # than an arbitrary directory inherited through PYTHONPATH.
    _SCRIPT_DIR = str(Path(__file__).resolve().parent)
    if _SCRIPT_DIR not in sys.path:
        sys.path.insert(0, _SCRIPT_DIR)
    from _repo_bootstrap import bootstrap_repo_src

bootstrap_repo_src()

from self_healthy_kafka.config import AnalyticsChatConfig, RagConfig, cfg
from self_healthy_kafka.semantic.outcome import cannot_verify, classify_execution, degraded
from self_healthy_kafka.semantic.planner import compile_analytics_request, parse_semantic_plan
from self_healthy_kafka.storage.mssql import HealingRepository
from self_healthy_kafka.webhook.analytics_chat import AnalyticsChatService

_STOP_REASONS = {
    "semantic_planner_authentication",
    "semantic_planner_quota_or_billing",
}
_OUTCOMES = {
    "verified_results", "verified_empty", "cannot_verify", "needs_clarification", "degraded",
}


def _load_cases(path: Path) -> list[dict[str, Any]]:
    records = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not records:
        raise ValueError("evaluation dataset is empty")
    if len(records) > 30:
        raise ValueError("evaluation dataset must contain at most 30 cases")
    for item in records:
        if not isinstance(item.get("id"), str) or not isinstance(item.get("question"), str):
            raise ValueError("every evaluation record needs id and question")
        if "expected_plan" in item:
            plan = parse_semantic_plan(item["expected_plan"])
            if plan.data_request:
                compile_analytics_request(plan)
        outcome = item.get("expected_outcome")
        if outcome is not None and outcome not in _OUTCOMES:
            raise ValueError("expected_outcome is unsupported")
        fixture = item.get("outcome_fixture")
        if fixture is not None:
            if not isinstance(fixture, dict) or fixture.get("mode") not in {
                "executed", "planner_invalid", "needs_clarification", "source_unavailable",
            }:
                raise ValueError("outcome_fixture is unsupported")
            if fixture["mode"] == "executed":
                for field in ("source_row_count", "fact_count"):
                    if not isinstance(fixture.get(field), int) or fixture[field] < 0:
                        raise ValueError(f"outcome_fixture.{field} must be a non-negative integer")
                if not isinstance(fixture.get("truncated"), bool):
                    raise ValueError("outcome_fixture.truncated must be boolean")
    return records


def _canonical_plan(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    request = value.get("data_request")
    if isinstance(request, dict):
        request = dict(request)
        for name in ("metrics", "dimensions", "detail_fields"):
            if isinstance(request.get(name), list):
                request[name] = sorted(request[name])
        filters = request.get("filters")
        if isinstance(filters, dict):
            filters = dict(filters)
            for name in ("event_type", "outcome"):
                if isinstance(filters.get(name), list):
                    filters[name] = sorted(filters[name])
            request["filters"] = filters
    guidance = value.get("guidance_request")
    if isinstance(guidance, dict):
        guidance = dict(guidance)
        if isinstance(guidance.get("error_codes"), list):
            guidance["error_codes"] = sorted(guidance["error_codes"])
    return {
        "data_request": request,
        "guidance_request": guidance,
        "clarification": value.get("clarification"),
        "conversation_action": value.get("conversation_action"),
        "inherited_fields": sorted(value.get("inherited_fields") or []),
        "derived_route": value.get("derived_route"),
    }


def _summary(records: list[dict[str, Any]], *, live: bool) -> dict[str, Any]:
    attempted = [item for item in records if item["status"] == "attempted"]
    comparable = [item for item in attempted if item["plan_match"] is not None]
    plan_matches = [bool(item["plan_match"]) for item in comparable]
    route_matches = [bool(item["route_match"]) for item in comparable]
    execution = [item for item in attempted if item["execution_match"] is not None]
    fallbacks = [item for item in attempted if item.get("fallback_reason")]
    clarification = [item for item in attempted if item.get("expected_clarification") is not None]
    clarification_matches = [bool(item["clarification_match"]) for item in clarification]
    latencies = [float(item["latency_seconds"]) for item in attempted]
    api_calls = sum(int(item.get("api_calls") or 0) for item in attempted)
    input_tokens = sum(int(item.get("input_tokens") or 0) for item in attempted)
    output_tokens = sum(int(item.get("output_tokens") or 0) for item in attempted)
    return {
        "live": live,
        "case_count": len(records),
        "attempted": len(attempted),
        "service_failure_count": sum(item["status"] == "service_failure" for item in records),
        "semantic_plan_accuracy": statistics.fmean(plan_matches) if plan_matches else None,
        "route_accuracy": statistics.fmean(route_matches) if route_matches else None,
        "first_attempt_execution_accuracy": (
            statistics.fmean(bool(item["first_execution_match"]) for item in execution) if execution else None
        ),
        "final_execution_accuracy": (
            statistics.fmean(bool(item["execution_match"]) for item in execution) if execution else None
        ),
        "valid_sql_rate": None,
        "grounding_failure_count": sum(
            str(item.get("fallback_reason") or "").startswith("grounding_failure") for item in attempted
        ),
        "clarification_accuracy": statistics.fmean(clarification_matches) if clarification_matches else None,
        "fallback_rate": len(fallbacks) / len(attempted) if attempted else None,
        "api_calls": api_calls,
        "input_tokens": input_tokens or None,
        "output_tokens": output_tokens or None,
        "latency_median_seconds": statistics.median(latencies) if latencies else None,
        "latency_p95_seconds": _p95(latencies),
        "latency_sample_size": len(latencies),
    }


def _p95(values: list[float]) -> float | None:
    if len(values) < 2:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(0.95 * (len(ordered) - 1))))
    return ordered[index]


def _model_totals(result: dict[str, Any]) -> tuple[int, int, int]:
    usage = result.get("model_usage") or {}
    calls = [item for stage in ("planning", "analytics_response", "runbook") for item in usage.get(stage, [])]
    return (
        len(calls),
        sum(int(item.get("input_tokens") or 0) for item in calls),
        sum(int(item.get("output_tokens") or 0) for item in calls),
    )


def evaluate_offline(cases: list[dict[str, Any]]) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for item in cases:
        plan = parse_semantic_plan(item["expected_plan"]) if "expected_plan" in item else None
        compiled = compile_analytics_request(plan).to_dict() if plan and plan.data_request else None
        fixture_outcome = _evaluate_outcome_fixture(item)
        records.append({
            "id": item["id"], "status": "offline_contract_passed",
            # A no-plan fixture may represent a genuine clarification, an
            # invalid plan, or an unavailable source.  Do not collapse the
            # latter two into a clarification in the evaluator report.
            "expected_route": (
                plan.route.value if plan and plan.route
                else "clarification" if item.get("expected_clarification")
                else None
            ),
            "compiled": compiled,
            "expected_outcome": item.get("expected_outcome"),
            "fixture_outcome": fixture_outcome,
            "outcome_contract_match": (
                fixture_outcome == item.get("expected_outcome")
                if item.get("expected_outcome") is not None else None
            ),
        })
    outcome_records = [item for item in records if item["outcome_contract_match"] is not None]
    if any(not item["outcome_contract_match"] for item in outcome_records):
        raise ValueError("outcome fixture did not satisfy its expected outcome")
    return {
        "mode": "offline",
        "package": __import__("self_healthy_kafka").__file__,
        "records": records,
        "summary": {
            "live": False,
            "case_count": len(records),
            "offline_contract_passed": len(records),
            "outcome_contract_passed": len(outcome_records),
            "semantic_plan_accuracy": None,
            "route_accuracy": None,
            "first_attempt_execution_accuracy": None,
            "final_execution_accuracy": None,
            "valid_sql_rate": None,
            "not_measured": [
                "Offline fixtures validate parser/compiler contracts only; no model inference was run.",
                "No independent live snapshot result was supplied, so execution accuracy is not measured.",
            ],
        },
    }


def _evaluate_outcome_fixture(item: dict[str, Any]) -> str | None:
    """Exercise the outcome invariant without provider, network, or database.

    These fixtures model the result of an independently executed, bounded
    query. They intentionally do not pretend to measure model accuracy.
    """

    fixture = item.get("outcome_fixture")
    if not isinstance(fixture, dict):
        return None
    mode = fixture["mode"]
    if mode == "executed":
        outcome = classify_execution(
            row_count=fixture["source_row_count"],
            fact_count=fixture["fact_count"],
            truncated=fixture["truncated"],
        )
        if outcome.outcome == "verified_empty" and not outcome.permits_empty_claim:
            raise ValueError("verified_empty invariant failed")
        return outcome.outcome
    if mode == "planner_invalid":
        return cannot_verify(reason="fixture_planner_invalid").outcome
    if mode == "source_unavailable":
        return degraded(reason="fixture_source_unavailable").outcome
    return "needs_clarification"


def evaluate_live(cases: list[dict[str, Any]]) -> dict[str, Any]:
    if os.getenv("SEMANTIC_CHAT_LIVE_EVALUATION", "false").lower() not in {"1", "true", "yes", "on"}:
        raise ValueError("--live requires SEMANTIC_CHAT_LIVE_EVALUATION=true")
    chat_config = AnalyticsChatConfig()
    if not chat_config.enabled:
        raise ValueError("--live requires CHAT_ANALYTICS_ENABLED=true")
    if not chat_config.hf_endpoint_url or not chat_config.hf_token or not chat_config.hf_model_id:
        raise ValueError("--live requires configured HF_CHAT_ENDPOINT_URL, HF_CHAT_TOKEN, and HF_CHAT_MODEL_ID")
    repository = HealingRepository(cfg.mssql.connection_string, cfg.mssql.connection_timeout_seconds)
    rag_config = RagConfig()
    service = AnalyticsChatService(
        chat_config,
        incident_facts=repository.list_incident_facts_for_chat,
        execute_incident_query=repository.execute_compiled_incident_query,
        rag_config=rag_config if rag_config.enabled else None,
    )
    service.validate()
    records: list[dict[str, Any]] = []
    try:
        for item in cases:
            started = time.perf_counter()
            result = service.ask(item["question"], conversation_id=f"semantic-eval:{item['id']}")
            latency = time.perf_counter() - started
            actual_plan = _canonical_plan(result.get("semantic_plan"))
            expected_plan = _canonical_plan(parse_semantic_plan(item["expected_plan"]).to_dict()) if "expected_plan" in item else None
            expected_clarification = item.get("expected_clarification")
            plan_match = actual_plan == expected_plan if expected_plan is not None else None
            route_match = (
                result.get("route") == item.get("expected_route")
                if item.get("expected_route") is not None else plan_match
            )
            api_calls, input_tokens, output_tokens = _model_totals(result)
            reason = str(result.get("reason") or "")
            reference_result = item.get("reference_result")
            actual_result = ((result.get("verified_result") or {}).get("rows"))
            execution_match = actual_result == reference_result if reference_result is not None else None
            record = {
                "id": item["id"], "status": "attempted", "latency_seconds": latency,
                "plan_match": plan_match, "route_match": route_match,
                # A fixture can carry an independently calculated, static
                # reference result.  If absent, we deliberately report this
                # metric as unmeasured rather than treating a successful call
                # as proof of execution accuracy.
                "first_execution_match": execution_match, "execution_match": execution_match,
                "expected_clarification": expected_clarification,
                "clarification_match": (
                    result.get("status") == "needs_clarification"
                    if expected_clarification is not None else None
                ),
                "fallback_reason": result.get("fallback_reason"), "reason": result.get("reason"),
                "api_calls": api_calls, "input_tokens": input_tokens, "output_tokens": output_tokens,
            }
            records.append(record)
            if reason in _STOP_REASONS:
                records.extend({"id": remaining["id"], "status": "service_failure", "reason": reason,
                                "plan_match": None, "route_match": None, "execution_match": None}
                               for remaining in cases[len(records):])
                break
    finally:
        service.close()
    return {
        "mode": "live",
        "package": __import__("self_healthy_kafka").__file__,
        "as_of": datetime.now(timezone.utc).isoformat(),
        "records": records,
        "summary": _summary(records, live=True),
        "not_measured": [
            "Execution accuracy remains null until this dataset includes independently calculated reference results for the unchanged snapshot.",
            "This evaluator does not refresh snapshots or mutate MSSQL.",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", type=Path, default=Path("runbooks/evaluation/semantic_chat_cases.jsonl"))
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    try:
        cases = _load_cases(args.dataset)
        report = evaluate_live(cases) if args.live else evaluate_offline(cases)
    except (OSError, ValueError) as exc:
        print(json.dumps({"error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
