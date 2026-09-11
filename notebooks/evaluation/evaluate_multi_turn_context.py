"""Evaluate structured multi-turn context against the real service, with mocked facts.

This command makes no database or model request.  Its report is explicitly
labelled mocked and must not be presented as live end-to-end accuracy.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from _repo_bootstrap import bootstrap_repo_src  # noqa: E402

bootstrap_repo_src()

from notebooks.evaluation.semantic_cases import MULTI_TURN_CASES  # noqa: E402
from self_healthy_kafka.config import AnalyticsChatConfig  # noqa: E402
from self_healthy_kafka.webhook.analytics_chat import AnalyticsChatService  # noqa: E402


def evaluate(cases=MULTI_TURN_CASES):
    records = []
    for case in cases:
        calls = []

        def facts(**kwargs):
            calls.append(kwargs)
            return [dict(row) for row in case["facts"]]

        service = AnalyticsChatService(
            AnalyticsChatConfig(enabled=True, timezone="UTC", hf_endpoint_url=""),
            incident_facts=facts,
            now=lambda: datetime.fromisoformat(case["now_utc"]),
        )
        turn_records = []
        try:
            for turn in case["turns"]:
                response = service.ask(
                    turn["question"], conversation_id=turn["conversation_id"]
                )
                conversation = response.get("conversation", {})
                observed = {
                    "status": response.get("status", "ok"),
                    "source": response.get("source"),
                    "context_used": conversation.get("context_used"),
                    "action": conversation.get("action"),
                    "evidence_ids": response.get("evidence_ids"),
                    "time_range": (response.get("query_plan") or {}).get("time_range"),
                }
                expected = turn["expected"]
                passed = all(observed.get(key) == value for key, value in expected.items())
                turn_records.append({
                    "question": turn["question"],
                    "expected": expected,
                    "observed": {key: observed.get(key) for key in expected},
                    "passed": passed,
                })
        finally:
            service.close()
        records.append({
            "case": case["id"],
            "split": case["split"],
            "turns": turn_records,
            "backend_calls": len(calls),
            "expected_backend_calls": case["expected_backend_calls"],
            "passed": (
                all(turn["passed"] for turn in turn_records)
                and len(calls) == case["expected_backend_calls"]
            ),
        })
    return {
        "kind": "mocked_structured_context_contract",
        "live_model": False,
        "live_database": False,
        "case_count": len(records),
        "pass_count": sum(record["passed"] for record in records),
        "records": records,
    }


def main():
    report = evaluate()
    # ASCII JSON is portable to the default Windows PowerShell code page.
    print(json.dumps(report, ensure_ascii=True, indent=2))
    return int(report["pass_count"] != report["case_count"])


if __name__ == "__main__":
    raise SystemExit(main())
