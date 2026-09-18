"""Capture an explicitly approved, read-only SHK-AnalyticsBench reference.

This command does not call a model.  It executes only reviewed semantic plans
through the deterministic compiler and writes a redacted *draft* manifest.  A
separate reviewer must approve that manifest before a baseline can use it.
"""

# ruff: noqa: E402 -- direct script execution must bind this checkout first.

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
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
    load_benchmark,
)
from self_healthy_kafka.evaluation.snapshot import (
    SnapshotReferenceError,
    build_snapshot_manifest,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--benchmark-dir", type=Path, default=Path("runbooks/evaluation/shk_analytics_bench")
    )
    parser.add_argument("--snapshot-id", required=True)
    parser.add_argument(
        "--source-identity",
        required=True,
        help="Stable logical source alias only; never provide a host, DSN, or credential.",
    )
    parser.add_argument("--case-id", action="append", required=True)
    parser.add_argument(
        "--output",
        type=Path,
        help="Manifest path under <benchmark-dir>/snapshots; defaults to <snapshot-id>.json.",
    )
    parser.add_argument(
        "--approve-capture",
        action="store_true",
        help="Required confirmation before the script reads the configured MSSQL source.",
    )
    args = parser.parse_args()
    if not args.approve_capture:
        print(json.dumps({"error": "--approve-capture is required"}, ensure_ascii=False))
        return 2
    snapshot_dir = (args.benchmark_dir / "snapshots").resolve()
    output = (args.output or snapshot_dir / f"{args.snapshot_id}.json").resolve()
    try:
        output.relative_to(snapshot_dir)
    except ValueError:
        print(
            json.dumps(
                {"error": "snapshot output must stay under benchmark snapshots"}, ensure_ascii=False
            )
        )
        return 2
    if output.exists():
        print(json.dumps({"error": "refusing to replace an existing snapshot"}, ensure_ascii=False))
        return 2
    try:
        cases = load_benchmark(args.benchmark_dir)
        requested = tuple(dict.fromkeys(args.case_id))
        selected = [case for case in cases if case.id in requested]
        if len(selected) != len(requested):
            raise SnapshotReferenceError("one or more requested case ids do not exist")
        # Delay environment-dependent imports until capture is explicitly
        # approved, keeping --help and offline work free of database config.
        from self_healthy_kafka.config import AnalyticsChatConfig, cfg
        from self_healthy_kafka.storage.mssql import HealingRepository

        analytics = AnalyticsChatConfig()
        if not analytics.enabled:
            raise SnapshotReferenceError("analytics capture is disabled")
        repository = HealingRepository(
            cfg.mssql.connection_string, cfg.mssql.connection_timeout_seconds
        )
        manifest = build_snapshot_manifest(
            benchmark_version=BENCHMARK_VERSION,
            snapshot_id=args.snapshot_id,
            timezone_name=analytics.timezone,
            cases=selected,
            execute=repository.execute_compiled_incident_query,
            now=datetime.now(timezone.utc),
            source_identity=args.source_identity,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        # Deliberately do not surface database, query, or driver text.
        print(json.dumps({"error": "snapshot_capture_failed"}, ensure_ascii=False))
        return 2
    print(
        json.dumps(
            {
                "snapshot_id": manifest["snapshot_id"],
                "status": manifest["status"],
                "case_count": len(manifest["cases"]),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
