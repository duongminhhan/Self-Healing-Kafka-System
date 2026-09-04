"""Read-only reference checks; --live evaluates HF (default) or --backend gemini."""

import argparse
import json
import math
import os
import sqlite3
import sys
import tempfile
from contextlib import closing
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

from notebooks.evaluation.fixtures import EXPECTED, VARIANTS, create_duration_fixture  # noqa: E402
from notebooks.shared.analytics import QueryError, Snapshot, Workflow  # noqa: E402

CASES = [
    (
        "Top 5 RootConnectorName bị rơi vào hàng chờ tự sửa nhiều lần nhất? Đếm incident đã lưu, sắp xếp số lần giảm dần rồi tên tăng dần khi bằng nhau. Chỉ trả tên và số lần.",
        "SELECT RootConnectorName,COUNT(QueueId) FROM ConnectorHealingQueue GROUP BY RootConnectorName ORDER BY COUNT(QueueId) DESC,RootConnectorName LIMIT 5",
    ),
    (
        "Cho mỗi RootConnectorName, trả tên và tổng số healing log của tất cả incident, kể cả connector không có log. Sắp xếp tên tăng dần.",
        "SELECT q.RootConnectorName,COUNT(l.Id) FROM ConnectorHealingQueue q LEFT JOIN ConnectorHealingLogs l ON l.QueueId=q.QueueId GROUP BY q.RootConnectorName ORDER BY q.RootConnectorName",
    ),
    (
        "Đếm số log theo Severity, trả Severity và số log, sắp xếp Severity tăng dần.",
        "SELECT Severity,COUNT(*) FROM ConnectorHealingLogs GROUP BY Severity ORDER BY Severity",
    ),
    (
        "Trả Id và AttemptNo của các log có AttemptNo NULL, sắp xếp Id tăng dần.",
        "SELECT Id,AttemptNo FROM ConnectorHealingLogs WHERE AttemptNo IS NULL ORDER BY Id",
    ),
    (
        "Trả RootConnectorName và số incident nhận từ 2026-09-02 00:00 UTC đến trước 2026-09-03 00:00 UTC. Sắp xếp tên tăng dần.",
        "SELECT RootConnectorName,COUNT(*) FROM ConnectorHealingQueue WHERE julianday(ReceivedAt)>=julianday('2026-09-02T00:00:00Z') AND julianday(ReceivedAt)<julianday('2026-09-03T00:00:00Z') GROUP BY RootConnectorName ORDER BY RootConnectorName",
    ),
    (
        "Connector nào có ít incident hơn mức trung bình số incident trên mỗi root? Trả tên, số incident, sắp xếp tên tăng dần.",
        "WITH counts AS (SELECT RootConnectorName,COUNT(*) n FROM ConnectorHealingQueue GROUP BY RootConnectorName) SELECT RootConnectorName,n FROM counts WHERE n<(SELECT AVG(n) FROM counts) ORDER BY RootConnectorName",
    ),
]

CASES.append(
    (
        "Thời gian xử lý trung bình (tính bằng phút) từ lúc nhận (ReceivedAt) đến khi hoàn tất (CompletedAt) của các queue thành công là bao nhiêu? Trả lần lượt trung bình phút, matched_count, valid_duration_count, excluded_duration_count.",
        "WITH durations AS (SELECT (julianday(CompletedAt)-julianday(ReceivedAt))*1440.0 AS minutes FROM ConnectorHealingQueue WHERE QueueStatus='COMPLETED' AND FinalOutcome='RECOVERED') SELECT ROUND(AVG(CASE WHEN minutes>=0 THEN minutes END),2),COUNT(*),COUNT(CASE WHEN minutes>=0 THEN 1 END),COUNT(*)-COUNT(CASE WHEN minutes>=0 THEN 1 END) FROM durations",
    )
)


def matches(result, expected):
    """Ordered bag comparison: aliases ignored, NULL exact, numerics within 0.005.

    Gold questions specify column and tie order. No sorting away missing/duplicate rows.
    """
    if result is None or result["truncated"] or len(result["rows"]) != len(expected):
        return False
    actual = [tuple(row[c["name"]] for c in result["columns"]) for row in result["rows"]]

    def equal(a, b):
        if a is None or b is None:
            return a is b
        if isinstance(a, (int, float)) and isinstance(b, (int, float)):
            return math.isclose(a, b, rel_tol=0, abs_tol=0.005)
        return type(a) is type(b) and a == b

    return all(
        len(a) == len(b) and all(equal(x, y) for x, y in zip(a, b))
        for a, b in zip(actual, expected)
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", default=str(ROOT / "self_healthy_kafka_snapshot.db"))
    parser.add_argument("--case", type=int, help="Run one 1-based gold case for diagnosis.")
    parser.add_argument("--backend", choices=["hf", "gemini"], default="hf")
    parser.add_argument(
        "--fixture",
        choices=VARIANTS,
        help="Evaluate duration case against a new synthetic temporary SQLite database; never changes the real snapshot.",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        help="Send schema and bounded results to the selected backend; may incur charges.",
    )
    args = parser.parse_args()
    if args.case is not None and not 1 <= args.case <= len(CASES):
        parser.error("--case is outside the gold case range")
    if args.backend == "gemini":
        load_dotenv(ROOT / ".env.gemini")
    load_dotenv(ROOT / ".env")
    fixture_directory = None
    if args.fixture:
        fixture_directory = tempfile.TemporaryDirectory(prefix="notebook-gold-")
        args.snapshot = str(Path(fixture_directory.name) / "fixture.db")
        create_duration_fixture(Path(args.snapshot), args.fixture)
        args.case = len(CASES)
    snapshot = Snapshot(args.snapshot, row_limit=1000)
    print(json.dumps({"snapshot": snapshot.metadata()}, ensure_ascii=True))
    client = None
    model = os.getenv("HF_MODEL_ID", "Qwen/Qwen3-4B-Instruct-2507")
    provider = os.getenv("HF_PROVIDER", "auto").strip().lower() or "auto"
    gemini_template = None
    if args.live and args.backend == "gemini":
        if not os.getenv("GEMINI_API_KEY", "").strip():
            print("LIVE SKIPPED: GEMINI_API_KEY is unavailable; no provider fallback.")
        else:
            from notebooks.gemini.adapter import make_gemini_workflow

            gemini_template = make_gemini_workflow(args.snapshot, row_limit=1000)
            snapshot = gemini_template.snapshot
            client = gemini_template.client
            model, provider = gemini_template.model_id, gemini_template.provider
    elif args.live:
        token = os.getenv("HF_TOKEN", "").strip()
        if not token:
            print("LIVE SKIPPED: HF_TOKEN is unavailable in environment/.env.")
        else:
            from huggingface_hub import InferenceClient

            client = InferenceClient(model=model, provider=provider, api_key=token, timeout=30)

    def new_flow():
        if gemini_template is not None:
            return Workflow(
                snapshot,
                client,
                model_id=model,
                provider=provider,
                max_attempts=3,
                sql_max_tokens=gemini_template.sql_max_tokens,
                response_max_tokens=gemini_template.response_max_tokens,
                few_shot=gemini_template.few_shot,
            )
        return Workflow(
            snapshot,
            client,
            model_id=model,
            provider=provider,
            few_shot=os.getenv("HF_FEW_SHOT", "true").strip().lower() in {"true", "1", "yes"},
        )

    failures = 0
    service_blocked = False
    reports = []
    for index, (question, reference_sql) in enumerate(CASES):
        if args.case is not None and args.case != index + 1:
            continue
        with closing(sqlite3.connect(snapshot.path.as_uri() + "?mode=ro", uri=True)) as conn:
            expected = conn.execute(reference_sql).fetchall()
        if args.fixture and expected != EXPECTED[args.fixture]:
            raise AssertionError(
                "Gold SQL disagrees with independently specified fixture expectation."
            )
        if client:
            flow = new_flow()
            try:
                result = flow.query(question)
                ok = matches(result, expected)
                response = flow.respond() if result is not None or flow.clarification else None
                reports.append(
                    {
                        "first": matches(flow.first_result, expected),
                        "final": ok,
                        "fallback": bool(
                            response and response["source"] == "verified_table_fallback"
                        ),
                        "response_service_error": flow.metrics.get("response_service_error"),
                        "metrics": flow.metrics,
                    }
                )
                print(
                    json.dumps(
                        {
                            "case": index + 1,
                            "semantic_match": ok,
                            "first_attempt_match": matches(flow.first_result, expected),
                            "sql": result["sql"] if result else None,
                            "rows": result["rows"] if result else None,
                            "expected": expected,
                            "diagnostics": result.get("diagnostics") if result else None,
                            "response": response,
                            "metrics": flow.metrics,
                            "clarification": flow.clarification,
                        },
                        ensure_ascii=True,
                    )
                )
                failures += not ok
                if flow.metrics.get("response_service_error", {}).get("http_status") in {
                    401,
                    402,
                    403,
                    429,
                }:
                    service_blocked = True
                    print(
                        json.dumps(
                            {
                                "live_run_stopped": "Response service unavailable; remaining cases were not run."
                            }
                        )
                    )
                    break
            except QueryError as exc:
                reports.append(
                    {
                        "first": matches(flow.first_result, expected),
                        "final": False,
                        "fallback": False,
                        "infrastructure_error": any(
                            t.get("status") == "api_error" for t in flow.trace
                        ),
                        "metrics": flow.metrics,
                    }
                )
                print(
                    json.dumps(
                        {
                            "case": index + 1,
                            "error": str(exc),
                            "trace": flow.trace,
                            "metrics": flow.metrics,
                        },
                        ensure_ascii=True,
                    )
                )
                failures += 1
                if any(t.get("http_status") in {401, 402, 403, 429} for t in flow.trace):
                    service_blocked = True
                    print(
                        json.dumps(
                            {
                                "live_run_stopped": "Authentication, billing, permission or rate limit requires external action; remaining cases were not run."
                            }
                        )
                    )
                    break
        else:
            result = snapshot.execute(reference_sql)
            actual = [tuple(row[c["name"]] for c in result["columns"]) for row in result["rows"]]
            ok = actual == expected and not result["truncated"]
            print(
                json.dumps(
                    {
                        "case": index + 1,
                        "local_reference_match": ok,
                        "rows": len(actual),
                        "sql_seconds": result["sql_seconds"],
                    }
                )
            )
            failures += not ok
    if reports:
        n = len(reports)
        attempts = sum(r["metrics"]["sql_attempts"] for r in reports)
        print(
            json.dumps(
                {
                    "evaluation_summary": {
                        "mode": "live_" + args.backend,
                        "model": model,
                        "provider": provider,
                        "cases": n,
                        "planned_cases": 1 if args.case else len(CASES),
                        "not_run_cases": (1 if args.case else len(CASES)) - n,
                        "service_blocked": service_blocked,
                        "infrastructure_error_cases": sum(
                            r.get("infrastructure_error", False) for r in reports
                        ),
                        "response_service_error_cases": sum(
                            bool(r.get("response_service_error")) for r in reports
                        ),
                        "accuracy_denominator": "Attempted cases, including SQL service failures as unsuccessful; unrun cases excluded and reported separately.",
                        "first_attempt_execution_accuracy": sum(r["first"] for r in reports) / n,
                        "final_execution_accuracy": sum(r["final"] for r in reports) / n,
                        "valid_sql_rate": sum(r["metrics"]["valid_sql_attempts"] for r in reports)
                        / attempts
                        if attempts
                        else None,
                        "correction_frequency": sum(
                            r["metrics"]["sql_attempts"] > 1 for r in reports
                        )
                        / n,
                        "review_frequency": sum(r["metrics"]["result_reviews"] > 0 for r in reports)
                        / n,
                        "fallback_frequency": sum(r["fallback"] for r in reports) / n,
                        "sql_api_calls": sum(r["metrics"]["sql_api_calls"] for r in reports),
                        "response_api_calls": sum(
                            r["metrics"]["response_api_calls"] for r in reports
                        ),
                        "mean_sql_seconds": sum(
                            r["metrics"].get("sql_stage_seconds", 0) for r in reports
                        )
                        / n,
                        "mean_response_seconds": sum(
                            r["metrics"].get("response_stage_seconds", 0) for r in reports
                        )
                        / n,
                        "comparison": "Ordered rows and columns; aliases ignored; NULL exact; absolute numeric tolerance 0.005; tie order explicit in gold question.",
                        "first_attempt_definition": "First SQL-stage model call must produce executable matching SQL; clarification/invalid output counts as failure.",
                    }
                },
                ensure_ascii=True,
            )
        )
    elif not client:
        print(json.dumps({"mode": "local_executor_checks_only", "model_accuracy": None}))
    if client and args.case is None and not service_blocked:
        flow = new_flow()
        try:
            flow.query("Connector nào tệ nhất?")
            clarified = bool(flow.clarification)
            print(json.dumps({"ambiguous_question_clarified": clarified, "metrics": flow.metrics}))
            failures += not clarified
        except QueryError as exc:
            print(json.dumps({"ambiguous_question_error": str(exc), "metrics": flow.metrics}))
            failures += 1
    if fixture_directory:
        fixture_directory.cleanup()
    if gemini_template is not None:
        client.close()
    return int(bool(failures))


if __name__ == "__main__":
    raise SystemExit(main())
