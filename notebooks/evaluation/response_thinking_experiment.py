"""Live paired response-only ablation; replay verified SQL, never regenerate it."""

import hashlib
import json
import os
from pathlib import Path

from dotenv import load_dotenv

from notebooks.evaluation.evaluate import CASES, matches
from notebooks.nemotron.adapter import make_nemotron_workflow


def main():
    root = Path(__file__).resolve().parents[2]
    load_dotenv(root / ".env.nemotron")
    baseline = json.loads(
        (root / "notebooks/evaluation/benchmark-2026-09-04.json").read_text(encoding="utf-8")
    )
    db = root / "self_healthy_kafka_snapshot.db"
    if (
        hashlib.sha256(db.read_bytes()).hexdigest().lower()
        != baseline["snapshot"]["sha256"].lower()
    ):
        raise RuntimeError("Snapshot changed; do not compare against stale evidence.")
    os.environ["NEMOTRON_SQL_THINKING"] = "true"
    records = {r["case"]: r for r in baseline["nemotron"]["records"] if "case" in r}
    flow = make_nemotron_workflow(db)
    try:
        print(json.dumps({"preflight": flow.client.check_model()}), flush=True)
        for case in range(1, 5):
            prior = records[case]
            result = flow.snapshot.execute(prior["sql"])
            if not matches(result, prior["expected"]):
                raise RuntimeError("Replayed SQL differs from independent gold result")
            # Counterbalance order; same evidence, interpretation and prompt in each pair.
            for thinking in [False, True] if case % 2 else [True, False]:
                flow.reset()
                flow.question = CASES[case - 1][0]
                flow.result = result
                flow.interpretation = None
                flow.client.stage_thinking["response"] = thinking
                answer = flow.respond()
                print(
                    json.dumps(
                        {
                            "case": case,
                            "response_thinking": thinking,
                            "sql_origin": "replayed from prior thinking=true run; no new SQL inference",
                            "response": answer,
                            "metrics": flow.metrics,
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                if flow.metrics.get("response_service_error"):
                    return 1
    finally:
        flow.client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
