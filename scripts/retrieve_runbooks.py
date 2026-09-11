"""Explicit live retrieval probe for the configured Qdrant Cloud collection."""

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

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.models import RetrievalQuery
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore
from self_healthy_kafka.rag.retriever import RunbookRetriever


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("question")
    parser.add_argument("--connector-class")
    parser.add_argument("--error-code", action="append", default=[])
    args = parser.parse_args()
    if os.getenv("RUNBOOK_RAG_LIVE_TEST", "false").lower() not in {"1", "true", "yes", "on"}:
        parser.error("live retrieval requires RUNBOOK_RAG_LIVE_TEST=true")
    config = RagConfig()
    config.validate()
    retriever = RunbookRetriever(config, QdrantRunbookStore(config))
    items = retriever.retrieve(
        RetrievalQuery(
            text=args.question,
            tenant_id=config.tenant_id,
            environment=config.environment,
            connector_class=args.connector_class,
            error_codes=tuple(value.upper() for value in args.error_code),
        )
    )
    print(
        json.dumps(
            {
                "diagnostics": (
                    retriever.last_diagnostics.to_dict()
                    if retriever.last_diagnostics is not None
                    else None
                ),
                "results": [
                    {
                        "runbook_id": item.runbook_id,
                        "version": item.version,
                        "section": item.section,
                        "source": item.source,
                        "score": item.score,
                        "text": item.text,
                    }
                    for item in items
                ],
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
