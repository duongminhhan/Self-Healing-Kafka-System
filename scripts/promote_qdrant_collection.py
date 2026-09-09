"""Build and promote one versioned Hybrid Runbook collection after a passing holdout."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from dataclasses import replace
from pathlib import Path
from typing import Any

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.evaluation import (
    GoldRetrievalCase,
    build_promotion_gate,
    load_gold_retrieval,
)
from self_healthy_kafka.rag.ingestion import RunbookIndexer
from self_healthy_kafka.rag.models import RetrievalQuery
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore
from self_healthy_kafka.rag.retriever import RunbookRetriever

REQUIRED_PAYLOAD_INDEXES = {
    "tenant_id",
    "status",
    "environment",
    "connector_class",
    "connector_type",
    "connector_family",
    "subsystem",
    "error_codes",
    "exception_classes",
    "config_keys",
    "runbook_id",
    "source",
    "version",
    "schema_version",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--collection", required=True)
    parser.add_argument("--alias", required=True)
    parser.add_argument("--benchmark-report", type=Path)
    parser.add_argument("--root", type=Path, default=Path("runbooks"))
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("runbooks/evaluation/gold_retrieval.jsonl"),
    )
    parser.add_argument("--tenant-id", default="default")
    parser.add_argument(
        "--expected-current-collection",
        help="Required on apply. Use 'none' only when creating a new alias.",
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    _validate_versioned_target(args.collection, args.alias)
    report = _read_report(args.benchmark_report) if args.benchmark_report else None
    report_errors = (
        validate_promotion_report(report, args.collection, dataset_path=args.dataset)
        if report is not None
        else ["benchmark_report_not_provided"]
    )
    plan = {
        "dry_run": not args.apply,
        "target_collection": args.collection,
        "alias": args.alias,
        "root": str(args.root),
        "dataset": str(args.dataset),
        "benchmark_gate_valid": not report_errors,
        "benchmark_gate_errors": report_errors,
        "operations": [
            "create_or_validate_versioned_hybrid_collection",
            "idempotently_sync_approved_runbooks",
            "validate_filtered_document_count_and_payload_indexes",
            "run_retrieval_smoke_cases",
            "atomically_switch_alias_if_every_check_passes",
        ],
        "deletes_old_collection": False,
    }
    if not args.apply:
        plan["warning"] = "No Qdrant request was made."
        print(json.dumps(plan, ensure_ascii=False, indent=2))
        return 0

    if os.getenv("QDRANT_HYBRID_PROMOTION", "false").lower() not in {
        "1",
        "true",
        "yes",
        "on",
    }:
        parser.error("--apply requires QDRANT_HYBRID_PROMOTION=true")
    if args.expected_current_collection is None:
        parser.error("--apply requires --expected-current-collection")
    if report_errors:
        parser.error("benchmark report cannot authorize promotion: " + "; ".join(report_errors))

    from qdrant_client import QdrantClient, models

    base = RagConfig()
    if args.collection in {base.dense_collection, base.collection}:
        parser.error("target hybrid collection must differ from the active dense collection")
    config = replace(
        base,
        retrieval_mode="hybrid",
        search_mode="hybrid",
        collection=args.collection,
    )
    config.validate()
    client = QdrantClient(
        url=config.qdrant_url,
        api_key=config.qdrant_api_key,
        cloud_inference=True,
        timeout=int(config.request_timeout_seconds),
    )
    store = QdrantRunbookStore(config, client=client)
    indexer = RunbookIndexer(store, max_chunk_chars=config.max_chunk_chars)
    preview = indexer.index(args.root, tenant_id=args.tenant_id, dry_run=True)
    if preview.errors:
        parser.error("runbook validation failed before Qdrant mutation")
    indexed = indexer.index(args.root, tenant_id=args.tenant_id, dry_run=False)
    if indexed.errors:
        parser.error("runbook indexing failed")
    store.validate_collection_schema()

    tenant_filter = models.Filter(
        must=[
            models.FieldCondition(
                key="tenant_id",
                match=models.MatchValue(value=args.tenant_id),
            )
        ]
    )
    count = client.count(
        collection_name=args.collection,
        count_filter=tenant_filter,
        exact=True,
        timeout=int(config.request_timeout_seconds),
    ).count
    if count != preview.inserted:
        parser.error(
            f"document count mismatch for tenant: expected {preview.inserted}, observed {count}"
        )
    info = client.get_collection(args.collection)
    missing_indexes = sorted(REQUIRED_PAYLOAD_INDEXES - set(info.payload_schema or {}))
    if missing_indexes:
        parser.error("missing payload indexes: " + ", ".join(missing_indexes))

    smoke = _run_smoke_cases(
        load_gold_retrieval(args.dataset),
        RunbookRetriever(config, store),
        tenant_id=args.tenant_id,
    )
    smoke_failures = [item for item in smoke if not item["passed"]]
    if smoke_failures:
        parser.error(
            "retrieval smoke tests failed: "
            + ", ".join(str(item["case_id"]) for item in smoke_failures)
        )

    aliases = client.get_aliases().aliases
    previous = next(
        (item.collection_name for item in aliases if item.alias_name == args.alias),
        None,
    )
    expected = (
        None
        if args.expected_current_collection.casefold() == "none"
        else args.expected_current_collection
    )
    if previous != args.collection and previous != expected:
        parser.error(
            f"alias compare-and-swap failed: expected {expected!r}, observed {previous!r}"
        )
    changed = previous != args.collection
    if changed:
        actions: list[Any] = []
        if previous is not None:
            actions.append(
                models.DeleteAliasOperation(
                    delete_alias=models.DeleteAlias(alias_name=args.alias)
                )
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
                **plan,
                "dry_run": False,
                "index_report": indexed.to_dict(),
                "document_count": count,
                "payload_indexes_valid": True,
                "smoke_cases": smoke,
                "previous_collection": previous,
                "changed": changed,
                "rollback_command": (
                    "python scripts/manage_qdrant_alias.py "
                    f"--alias {args.alias} --collection {previous} "
                    f"--expected-current-collection {args.collection} --apply"
                    if previous
                    else None
                ),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def validate_promotion_report(
    report: dict[str, Any],
    target_collection: str,
    *,
    dataset_path: Path | None = None,
) -> list[str]:
    gate = report.get("promotion_gate")
    if not isinstance(gate, dict):
        return ["promotion_gate_missing"]
    errors: list[str] = []
    if report.get("benchmark_status") != "measured":
        errors.append("benchmark_was_not_measured")
    if gate.get("passed") is not True:
        errors.append("promotion_gate_not_passed")
    if gate.get("target_collection") != target_collection:
        errors.append("promotion_gate_target_collection_mismatch")
    if gate.get("dataset_split") != "holdout":
        errors.append("promotion_gate_not_based_on_holdout")
    if gate.get("dataset_contract_valid") is not True:
        errors.append("promotion_gate_dataset_contract_invalid")
    dataset = report.get("dataset")
    if not isinstance(dataset, dict) or dataset.get("contract_valid") is not True:
        errors.append("benchmark_dataset_contract_invalid")
    elif (
        int(dataset.get("measured_case_count") or 0) < 75
        or float(dataset.get("holdout_ratio") or 0) < 0.20
    ):
        errors.append("benchmark_dataset_size_or_holdout_invalid")
    if gate.get("failures") != []:
        errors.append("promotion_gate_contains_failures")
    if int(gate.get("benchmark_repetitions") or 0) < 5:
        errors.append("promotion_gate_has_fewer_than_5_repetitions")
    repetitions = int(report.get("repetitions") or 0)
    if repetitions < 5:
        errors.append("benchmark_has_fewer_than_5_repetitions")
    if repetitions != int(gate.get("benchmark_repetitions") or 0):
        errors.append("benchmark_repetition_count_mismatch")

    holdout = report.get("holdout")
    dense = holdout.get("dense") if isinstance(holdout, dict) else None
    hybrid = holdout.get("hybrid") if isinstance(holdout, dict) else None
    if not isinstance(dense, dict) or not isinstance(hybrid, dict):
        errors.append("benchmark_holdout_metrics_missing")
    else:
        recomputed = build_promotion_gate(
            dense,
            hybrid,
            target_collection=target_collection,
            dataset_split="holdout",
            benchmark_repetitions=repetitions,
            dataset_contract_valid=bool(
                isinstance(dataset, dict) and dataset.get("contract_valid") is True
            ),
        )
        if not recomputed["passed"]:
            errors.append("recomputed_promotion_gate_not_passed")
        if recomputed["failures"] != gate.get("failures"):
            errors.append("promotion_gate_failures_do_not_match_metrics")

    if dataset_path is not None:
        if not dataset_path.is_file():
            errors.append("benchmark_dataset_file_missing")
        elif not isinstance(dataset, dict) or dataset.get("sha256") != hashlib.sha256(
            dataset_path.read_bytes()
        ).hexdigest():
            errors.append("benchmark_dataset_sha256_mismatch")
    return errors


def _run_smoke_cases(
    cases: list[GoldRetrievalCase],
    retriever: RunbookRetriever,
    *,
    tenant_id: str,
) -> list[dict[str, Any]]:
    chosen: list[GoldRetrievalCase] = []
    represented: set[str] = set()
    for case in cases:
        if (
            case.fixture_only
            or case.expected_no_answer
            or case.category not in {"exact_error", "exact_config_key"}
        ):
            continue
        if case.expected_runbook_ids[0] in represented:
            continue
        chosen.append(case)
        represented.add(case.expected_runbook_ids[0])
        if len(chosen) >= 7:
            break
    results: list[dict[str, Any]] = []
    for case in chosen:
        chunks = retriever.retrieve(
            RetrievalQuery(
                text=case.question,
                tenant_id=tenant_id,
                environment=case.environment,
                connector_class=case.expected_connector_class,
                error_codes=case.expected_error_codes,
            )
        )
        ids = list(dict.fromkeys(item.runbook_id for item in chunks))
        results.append(
            {
                "case_id": case.case_id,
                "expected_runbook_ids": list(case.expected_runbook_ids),
                "retrieved_runbook_ids": ids,
                "passed": bool(set(ids) & set(case.expected_runbook_ids)),
            }
        )
    if len(represented) < 7:
        results.append(
            {
                "case_id": "smoke-corpus-coverage",
                "expected_runbook_ids": [],
                "retrieved_runbook_ids": [],
                "passed": False,
            }
        )
    return results


def _validate_versioned_target(collection: str, alias: str) -> None:
    if collection == alias:
        raise ValueError("collection and alias must differ")
    if not re.search(r"(?:^|[_-])v\d+(?:$|[_-])", collection, re.IGNORECASE):
        raise ValueError("target collection must contain an explicit version such as _v2")


def _read_report(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("benchmark report must contain a JSON object")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
