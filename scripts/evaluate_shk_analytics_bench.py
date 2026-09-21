"""Run the offline SHK-AnalyticsBench or score an explicitly configured model."""

# ruff: noqa: E402 -- direct script execution must bind this checkout first.

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Sequence

if __package__:
    from ._repo_bootstrap import bootstrap_repo_src
else:
    _SCRIPT_DIR = str(Path(__file__).resolve().parent)
    if _SCRIPT_DIR not in sys.path:
        sys.path.insert(0, _SCRIPT_DIR)
    from _repo_bootstrap import bootstrap_repo_src

bootstrap_repo_src()

from self_healthy_kafka.evaluation.analytics_bench import (
    BENCHMARK_VERSION,
    SPLITS,
    AnalyticsBenchError,
    evaluate_adapter,
    evaluate_offline,
    load_benchmark,
)
from self_healthy_kafka.evaluation.semantic_adapter import SemanticAdapterResult
from self_healthy_kafka.evaluation.snapshot import load_snapshots, project_canonical_facts


class ConfiguredAnalyticsAdapter:
    """The current HF-backed service behind the provider-neutral interface."""

    name = "configured_hf_analytics_service"

    def __init__(self) -> None:
        # Delayed imports keep the offline benchmark free of environment and
        # database configuration requirements.
        from self_healthy_kafka.config import AnalyticsChatConfig, RagConfig, cfg
        from self_healthy_kafka.storage.mssql import HealingRepository
        from self_healthy_kafka.webhook.analytics_chat import AnalyticsChatService

        config = AnalyticsChatConfig()
        if (
            not config.enabled
            or not config.hf_endpoint_url
            or not config.hf_token
            or not config.hf_model_id
        ):
            raise AnalyticsBenchError(
                "--live requires enabled analytics and configured HF endpoint, token, and model id"
            )
        repository = HealingRepository(
            cfg.mssql.connection_string, cfg.mssql.connection_timeout_seconds
        )
        rag_config = RagConfig()
        self._service = AnalyticsChatService(
            config,
            incident_facts=repository.list_incident_facts_for_chat,
            execute_incident_query=repository.execute_compiled_incident_query,
            rag_config=rag_config if rag_config.enabled else None,
        )
        self._service.validate()

    def close(self) -> None:
        self._service.close()

    def evaluate(
        self,
        *,
        question: str,
        prior_questions: Sequence[str] = (),
        conversation_id: str,
    ) -> SemanticAdapterResult:
        for prior in prior_questions:
            self._service.ask(prior, conversation_id=conversation_id)
        started = time.perf_counter()
        result = self._service.ask(question, conversation_id=conversation_id)
        latency = time.perf_counter() - started
        usage = result.get("model_usage") or {}
        calls = [
            item
            for stage in ("planning", "analytics_response", "runbook")
            for item in usage.get(stage, [])
        ]
        return SemanticAdapterResult(
            semantic_plan=result.get("semantic_plan")
            if isinstance(result.get("semantic_plan"), dict)
            else None,
            route=result.get("route") if isinstance(result.get("route"), str) else None,
            outcome=result.get("outcome") if isinstance(result.get("outcome"), str) else None,
            query_executed=result.get("query_executed")
            if isinstance(result.get("query_executed"), bool)
            else None,
            evidence_complete=result.get("evidence_complete")
            if isinstance(result.get("evidence_complete"), bool)
            else None,
            row_count=result.get("row_count") if isinstance(result.get("row_count"), int) else None,
            answer=str(result.get("answer") or ""),
            latency_seconds=latency,
            api_calls=len(calls),
            input_tokens=sum(int(item.get("input_tokens") or 0) for item in calls),
            output_tokens=sum(int(item.get("output_tokens") or 0) for item in calls),
            correction_count=max(0, int(result.get("planning_attempts") or 1) - 1),
            source_timezone=(
                result.get("time_range_applied", {}).get("timezone")
                if isinstance(result.get("time_range_applied"), dict)
                and isinstance(result["time_range_applied"].get("timezone"), str)
                else None
            ),
            time_range_applied=(
                result.get("time_range_applied")
                if isinstance(result.get("time_range_applied"), dict)
                else None
            ),
            canonical_facts=project_canonical_facts(
                result.get("verified_result", {}).get("rows")
                if isinstance(result.get("verified_result"), dict)
                else []
            ),
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--benchmark-dir",
        type=Path,
        default=Path("runbooks/evaluation/shk_analytics_bench"),
    )
    parser.add_argument("--split", choices=(*SPLITS, "all"), default="all")
    parser.add_argument(
        "--live", action="store_true", help="Call the configured provider and read-only source."
    )
    parser.add_argument(
        "--snapshot",
        action="append",
        type=Path,
        default=[],
        help="Approved immutable snapshot manifest(s) eligible for execution scoring.",
    )
    args = parser.parse_args()
    splits = SPLITS if args.split == "all" else (args.split,)
    try:
        cases = load_benchmark(args.benchmark_dir, splits=splits)
        if not args.live:
            report = evaluate_offline(cases)
        else:
            if os.getenv("SHK_ANALYTICS_BENCH_LIVE", "false").lower() not in {
                "1",
                "true",
                "yes",
                "on",
            }:
                raise AnalyticsBenchError("--live requires SHK_ANALYTICS_BENCH_LIVE=true")
            adapter = ConfiguredAnalyticsAdapter()
            try:
                report = evaluate_adapter(cases, adapter, snapshots=load_snapshots(args.snapshot))
            finally:
                adapter.close()
    except (OSError, ValueError, AnalyticsBenchError) as exc:
        print(
            json.dumps(
                {"benchmark_version": BENCHMARK_VERSION, "error": str(exc)}, ensure_ascii=False
            )
        )
        return 2
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
