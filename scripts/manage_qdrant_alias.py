"""Preview or explicitly switch one Qdrant alias after collection validation."""

# ruff: noqa: E402 -- direct script execution must prefer this checkout's src tree.

from __future__ import annotations

import argparse
import json
import os
from dataclasses import replace

if __package__:
    from ._repo_bootstrap import bootstrap_repo_src
else:
    from _repo_bootstrap import bootstrap_repo_src

bootstrap_repo_src()

from qdrant_client import QdrantClient, models

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--alias", required=True)
    parser.add_argument("--collection", required=True)
    parser.add_argument(
        "--expected-current-collection",
        help="Optional compare-and-swap guard. Use 'none' when the alias must not exist.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Validate the target and atomically replace the alias. Default is dry-run.",
    )
    args = parser.parse_args()
    if not args.apply:
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "alias": args.alias,
                    "target_collection": args.collection,
                    "expected_current_collection": args.expected_current_collection,
                    "warning": "No Qdrant request was made.",
                },
                indent=2,
            )
        )
        return 0
    if os.getenv("QDRANT_ALIAS_CUTOVER", "false").lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        parser.error("--apply requires QDRANT_ALIAS_CUTOVER=true")

    config = replace(RagConfig(), collection=args.collection)
    config.validate()
    client = QdrantClient(
        url=config.qdrant_url,
        api_key=config.qdrant_api_key,
        cloud_inference=True,
        timeout=int(config.request_timeout_seconds),
    )
    QdrantRunbookStore(config, client=client).validate_collection_schema()
    aliases = client.get_aliases().aliases
    previous = next(
        (item.collection_name for item in aliases if item.alias_name == args.alias),
        None,
    )
    expected = (
        None
        if args.expected_current_collection
        and args.expected_current_collection.casefold() == "none"
        else args.expected_current_collection
    )
    if expected is not None or args.expected_current_collection is not None:
        if previous != args.collection and previous != expected:
            parser.error(
                f"alias compare-and-swap failed: expected {expected!r}, observed {previous!r}"
            )
    if previous == args.collection:
        print(
            json.dumps(
                {
                    "dry_run": False,
                    "alias": args.alias,
                    "previous_collection": previous,
                    "target_collection": args.collection,
                    "changed": False,
                },
                indent=2,
            )
        )
        return 0
    actions: list[models.CreateAliasOperation | models.DeleteAliasOperation] = []
    if previous is not None:
        actions.append(
            models.DeleteAliasOperation(delete_alias=models.DeleteAlias(alias_name=args.alias))
        )
    actions.append(
        models.CreateAliasOperation(
            create_alias=models.CreateAlias(
                collection_name=args.collection,
                alias_name=args.alias,
            )
        )
    )
    client.update_collection_aliases(
        actions,
        timeout=int(config.request_timeout_seconds),
    )
    print(
        json.dumps(
            {
                "dry_run": False,
                "alias": args.alias,
                "previous_collection": previous,
                "target_collection": args.collection,
                "changed": True,
                "rollback_collection": previous,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
