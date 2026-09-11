"""Inspect Qdrant payload indexes; create only missing indexes with --apply."""

# ruff: noqa: E402 -- direct script execution must prefer this checkout's src tree.

from __future__ import annotations

import argparse
import json
from pathlib import Path

from dotenv import load_dotenv

if __package__:
    from ._repo_bootstrap import bootstrap_repo_src
else:
    from _repo_bootstrap import bootstrap_repo_src

bootstrap_repo_src()

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.models import RagConfigurationError, RagStoreError
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path("env/dev.env"),
        help="Environment file containing the Qdrant configuration (default: env/dev.env).",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create only missing indexes. Without this flag, no mutation is performed.",
    )
    args = parser.parse_args(argv)
    if not args.env_file.is_file():
        parser.error(f"environment file not found: {args.env_file}")
    load_dotenv(args.env_file, override=True)

    config = RagConfig()
    store = QdrantRunbookStore(config)
    try:
        before = store.inspect_payload_indexes()
        report = {
            "dry_run": not args.apply,
            "collection": config.collection,
            "vector_schema_compatible": True,
            "payload_indexes_before": before.to_dict(),
            "planned_changes": [
                {"operation": "create_payload_index", "field": field, "type": field_type}
                for field, field_type in sorted(before.missing.items())
            ],
            "deletes_collection": False,
            "alters_documents": False,
        }
        if before.incompatible:
            report["applied"] = False
            report["reason"] = "incompatible_payload_index_types"
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 2
        if not args.apply:
            report["applied"] = False
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0

        after, created = store.apply_missing_payload_indexes()
        report.update(
            {
                "dry_run": False,
                "applied": True,
                "created_indexes": list(created),
                "payload_indexes_after": after.to_dict(),
            }
        )
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except (RagConfigurationError, RagStoreError) as exc:
        print(
            json.dumps(
                {
                    "dry_run": not args.apply,
                    "collection": config.collection,
                    "applied": False,
                    "error": str(exc),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
