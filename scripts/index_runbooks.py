"""Validate runbooks by default; mutate Qdrant only with --apply and RAG enabled."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.ingestion import RunbookIndexer
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("runbooks"))
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Create/update/remove Qdrant points. Without this flag the command is a dry run.",
    )
    args = parser.parse_args()
    config = RagConfig()
    if args.apply and not config.enabled:
        parser.error("--apply requires RAG_ENABLED=true")
    report = RunbookIndexer(
        QdrantRunbookStore(config), max_chunk_chars=config.max_chunk_chars
    ).index(args.root, tenant_id=args.tenant_id, dry_run=not args.apply)
    print(
        json.dumps(
            {
                "dry_run": not args.apply,
                "search_mode": config.search_mode,
                "collection": config.collection,
                **report.to_dict(),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 1 if report.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
