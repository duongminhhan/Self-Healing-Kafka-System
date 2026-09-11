"""Validate runbooks by default; mutate Qdrant only with --apply and RAG enabled."""

# ruff: noqa: E402 -- direct script execution must prefer this checkout's src tree.

from __future__ import annotations

import argparse
import json
from pathlib import Path

if __package__:
    from ._repo_bootstrap import bootstrap_repo_src
else:
    from _repo_bootstrap import bootstrap_repo_src

bootstrap_repo_src()

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
    store = QdrantRunbookStore(config)
    created_indexes: tuple[str, ...] = ()
    if args.apply:
        store.ensure_collection()
        _, created_indexes = store.apply_missing_payload_indexes()
    report = RunbookIndexer(store, max_chunk_chars=config.max_chunk_chars).index(
        args.root, tenant_id=args.tenant_id, dry_run=not args.apply
    )
    print(
        json.dumps(
            {
                "dry_run": not args.apply,
                "search_mode": config.search_mode,
                "collection": config.collection,
                "created_payload_indexes": list(created_indexes),
                **report.to_dict(),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 1 if report.errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
