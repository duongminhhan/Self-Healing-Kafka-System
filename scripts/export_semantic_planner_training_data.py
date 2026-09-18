"""Export human-approved SHK benchmark labels for a future LoRA job."""

# ruff: noqa: E402 -- direct script execution must bind this checkout first.

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

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
    AnalyticsBenchError,
    export_training_records,
    load_benchmark,
    write_jsonl,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--benchmark-dir", type=Path, default=Path("runbooks/evaluation/shk_analytics_bench")
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--include-seed",
        action="store_true",
        help="Allow unapproved seed labels for local format testing only; never use this for a training run.",
    )
    args = parser.parse_args()
    try:
        cases = load_benchmark(args.benchmark_dir, splits=("train",))
        records = export_training_records(cases, include_seed=args.include_seed)
        count = write_jsonl(args.output, records)
    except (OSError, ValueError, AnalyticsBenchError) as exc:
        print(
            json.dumps(
                {"benchmark_version": BENCHMARK_VERSION, "error": str(exc)}, ensure_ascii=False
            )
        )
        return 2
    print(
        json.dumps(
            {"benchmark_version": BENCHMARK_VERSION, "exported_records": count}, ensure_ascii=False
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
