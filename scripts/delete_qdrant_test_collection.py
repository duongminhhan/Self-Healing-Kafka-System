"""Preview or delete one explicitly named Qdrant test collection."""

# ruff: noqa: E402 -- direct script execution must prefer this checkout's src tree.

from __future__ import annotations

import argparse
import json
import os

if __package__:
    from ._repo_bootstrap import bootstrap_repo_src
else:
    from _repo_bootstrap import bootstrap_repo_src

bootstrap_repo_src()

from qdrant_client import QdrantClient

from self_healthy_kafka.config import RagConfig


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if "test" not in args.collection.casefold():
        parser.error("cleanup is restricted to collection names containing 'test'")
    if not args.apply:
        print(json.dumps({"dry_run": True, "collection": args.collection}, indent=2))
        return 0
    if os.getenv("QDRANT_TEST_CLEANUP", "false").lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        parser.error("--apply requires QDRANT_TEST_CLEANUP=true")
    config = RagConfig()
    config.validate()
    client = QdrantClient(
        url=config.qdrant_url,
        api_key=config.qdrant_api_key,
        timeout=int(config.request_timeout_seconds),
    )
    existed = client.collection_exists(args.collection)
    if existed:
        client.delete_collection(
            args.collection,
            timeout=int(config.request_timeout_seconds),
        )
    print(
        json.dumps(
            {
                "dry_run": False,
                "collection": args.collection,
                "existed": existed,
                "exists_after": client.collection_exists(args.collection),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
