"""Write a separately reviewed version of a SHK benchmark snapshot manifest."""

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

from self_healthy_kafka.evaluation.snapshot import (
    apply_snapshot_review,
    load_snapshot,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reviewer", required=True)
    parser.add_argument("--reviewed-at", required=True, help="Review date in YYYY-MM-DD format.")
    parser.add_argument("--status", choices=("approved", "rejected"), required=True)
    parser.add_argument(
        "--approve-review",
        action="store_true",
        help="Required confirmation before writing the reviewed manifest.",
    )
    args = parser.parse_args()
    if not args.approve_review:
        print(json.dumps({"error": "--approve-review is required"}, ensure_ascii=False))
        return 2
    snapshot_path = args.snapshot.resolve()
    output = args.output.resolve()
    if output.parent != snapshot_path.parent:
        print(
            json.dumps(
                {"error": "reviewed snapshot must stay beside its draft"}, ensure_ascii=False
            )
        )
        return 2
    if output.exists():
        print(json.dumps({"error": "refusing to replace an existing snapshot"}, ensure_ascii=False))
        return 2
    try:
        reviewed = apply_snapshot_review(
            load_snapshot(snapshot_path),
            status=args.status,
            reviewer=args.reviewer,
            reviewed_at=args.reviewed_at,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(reviewed, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        print(json.dumps({"error": "snapshot_review_failed"}, ensure_ascii=False))
        return 2
    print(
        json.dumps(
            {
                "snapshot_id": reviewed["snapshot_id"],
                "status": reviewed["status"],
                "integrity": reviewed["integrity"]["content_sha256"],
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
