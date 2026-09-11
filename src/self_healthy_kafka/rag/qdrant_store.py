from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from typing import Any, TypeVar

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.chunking import retrieval_text
from self_healthy_kafka.rag.models import (
    IngestionReport,
    RagConfigurationError,
    RagStoreError,
    RetrievalQuery,
    RetrievedChunk,
    RunbookChunk,
    SearchDiagnostics,
)
from self_healthy_kafka.rag.payload_indexes import (
    PayloadIndexPlan,
    create_missing_payload_indexes,
    inspect_payload_indexes,
    require_payload_indexes,
)

T = TypeVar("T")


class QdrantRunbookStore:
    """Official-client adapter supporting legacy dense and named-vector hybrid search."""

    def __init__(self, config: RagConfig, *, client: Any | None = None):
        self._config = config
        self._client = client
        self._local = threading.local()

    @property
    def last_search_diagnostics(self) -> SearchDiagnostics | None:
        """Return diagnostics for the current request thread only."""

        value = getattr(self._local, "last_search_diagnostics", None)
        return value if isinstance(value, SearchDiagnostics) else None

    @last_search_diagnostics.setter
    def last_search_diagnostics(self, value: SearchDiagnostics | None) -> None:
        self._local.last_search_diagnostics = value

    def _get_client(self):
        if self._client is not None:
            return self._client
        self._config.validate()
        try:
            from qdrant_client import QdrantClient
        except ImportError as exc:
            raise RagConfigurationError("qdrant-client is required when RAG is enabled") from exc
        self._client = QdrantClient(
            url=self._config.qdrant_url,
            api_key=self._config.qdrant_api_key,
            cloud_inference=True,
            timeout=int(self._config.request_timeout_seconds),
        )
        return self._client

    def ensure_collection(self) -> None:
        """Create only the collection and reject incompatible vector schemas.

        Payload-index migration is intentionally separate and must be invoked explicitly.
        """
        self._config.validate()
        client = self._get_client()
        try:
            from qdrant_client import models

            if not client.collection_exists(self._config.collection):
                create_args: dict[str, Any] = {
                    "collection_name": self._config.collection,
                    "timeout": int(self._config.request_timeout_seconds),
                }
                if self._config.search_mode == "hybrid":
                    create_args.update(
                        vectors_config={
                            self._config.dense_vector_name: models.VectorParams(
                                size=self._config.embedding_size,
                                distance=models.Distance.COSINE,
                            )
                        },
                        sparse_vectors_config={
                            self._config.sparse_vector_name: models.SparseVectorParams(
                                modifier=models.Modifier.IDF
                            )
                        },
                    )
                else:
                    create_args["vectors_config"] = models.VectorParams(
                        size=self._config.embedding_size,
                        distance=models.Distance.COSINE,
                    )
                client.create_collection(**create_args)
            info = client.get_collection(self._config.collection)
            self._validate_collection_info(info)
        except RagConfigurationError:
            raise
        except Exception as exc:
            raise RagStoreError(f"Qdrant collection setup failed: {_error_label(exc)}") from exc

    def validate_collection_schema(self) -> None:
        """Read-only validation of vector and payload-index contracts."""
        self._config.validate()
        try:
            info = self._get_client().get_collection(self._config.collection)
            self._validate_collection_info(info)
            require_payload_indexes(
                inspect_payload_indexes(getattr(info, "payload_schema", {}) or {})
            )
        except RagConfigurationError:
            raise
        except Exception as exc:
            raise RagStoreError(
                f"Qdrant collection validation failed: {_error_label(exc)}"
            ) from exc

    def inspect_payload_indexes(self) -> PayloadIndexPlan:
        """Return a read-only payload-index plan for the configured collection."""

        self._config.validate()
        try:
            client = self._get_client()
            if not client.collection_exists(self._config.collection):
                raise RagConfigurationError(
                    f"Qdrant collection {self._config.collection!r} does not exist"
                )
            info = client.get_collection(self._config.collection)
            self._validate_collection_info(info)
            return inspect_payload_indexes(getattr(info, "payload_schema", {}) or {})
        except RagConfigurationError:
            raise
        except Exception as exc:
            raise RagStoreError(
                f"Qdrant payload-index inspection failed: {_error_label(exc)}"
            ) from exc

    def apply_missing_payload_indexes(self) -> tuple[PayloadIndexPlan, tuple[str, ...]]:
        """Explicitly create missing indexes; never create collections or alter points."""

        plan = self.inspect_payload_indexes()
        try:
            client = self._get_client()
            created = create_missing_payload_indexes(
                client,
                collection=self._config.collection,
                plan=plan,
            )
            refreshed = self.inspect_payload_indexes()
            require_payload_indexes(refreshed)
            return refreshed, created
        except RagConfigurationError:
            raise
        except Exception as exc:
            raise RagStoreError(
                f"Qdrant payload-index migration failed: {_error_label(exc)}"
            ) from exc

    def sync(
        self,
        chunks: Iterable[RunbookChunk],
        *,
        tenant_id: str,
        dry_run: bool = False,
    ) -> IngestionReport:
        desired = {chunk.point_id: chunk for chunk in chunks if chunk.tenant_id == tenant_id}
        if dry_run:
            return IngestionReport(inserted=len(desired))
        self.ensure_collection()
        try:
            existing = self._existing_payloads(tenant_id)
            inserted = [item for key, item in desired.items() if key not in existing]
            updated = [
                item
                for key, item in desired.items()
                if key in existing and existing[key].get("content_hash") != item.content_hash
            ]
            unchanged = len(desired) - len(inserted) - len(updated)
            for batch in _batches(inserted + updated, size=64):
                self._upsert(batch)
            stale = sorted(set(existing) - set(desired))
            if stale:
                from qdrant_client import models

                stale_points: list[int | str | uuid.UUID] = list(stale)
                self._call_with_retry(
                    lambda: self._get_client().delete(
                        collection_name=self._config.collection,
                        points_selector=models.PointIdsList(points=stale_points),
                        wait=True,
                    )
                )
            return IngestionReport(
                inserted=len(inserted),
                updated=len(updated),
                unchanged=unchanged,
                removed=len(stale),
            )
        except RagStoreError:
            raise
        except Exception as exc:
            raise RagStoreError(f"Qdrant synchronization failed: {_error_label(exc)}") from exc

    def search(
        self,
        query: RetrievalQuery,
        *,
        limit: int,
        score_threshold: float,
    ) -> list[RetrievedChunk]:
        started = time.perf_counter()
        diagnostics = SearchDiagnostics(
            search_mode=self._config.search_mode,
            collection=self._config.collection,
            dense_model=self._config.effective_dense_embedding_model,
            sparse_model=(
                self._config.sparse_embedding_model
                if self._config.search_mode == "hybrid"
                else None
            ),
            dense_candidate_limit=(
                self._config.effective_dense_candidate_limit
                if self._config.search_mode == "hybrid"
                else limit
            ),
            sparse_candidate_limit=(
                self._config.effective_sparse_candidate_limit
                if self._config.search_mode == "hybrid"
                else None
            ),
        )
        self.last_search_diagnostics = diagnostics
        try:
            self.validate_collection_schema()
            query_filter = _payload_filter(query)
            diagnostics.payload_filter_fields = _payload_filter_fields(query)
            if self._config.search_mode == "hybrid":
                try:
                    points, latency_ms, fusion_fallback = self._hybrid_search(
                        query,
                        query_filter=query_filter,
                        limit=limit,
                    )
                    diagnostics.fusion_latency_ms = latency_ms
                    diagnostics.fusion_fallback_reason = fusion_fallback
                except RagConfigurationError:
                    raise
                except Exception as exc:
                    if not self._config.hybrid_fallback_to_dense:
                        raise
                    diagnostics.fallback_reason = f"hybrid_query_failed:{_error_label(exc)}"
                    points, latency_ms = self._dense_search(
                        query,
                        query_filter=query_filter,
                        limit=limit,
                        score_threshold=self._config.effective_dense_score_threshold,
                        using=self._config.dense_vector_name,
                    )
                    diagnostics.dense_latency_ms = latency_ms
                    diagnostics.dense_candidate_count = len(points)
            else:
                points, latency_ms = self._dense_search(
                    query,
                    query_filter=query_filter,
                    limit=limit,
                    score_threshold=score_threshold,
                )
                diagnostics.dense_latency_ms = latency_ms
                diagnostics.dense_candidate_count = len(points)
            points = _deduplicate_points(points)
            diagnostics.fused_candidate_count = len(points)
            if not points:
                diagnostics.no_result_reason = "no_qdrant_candidates"
            return [_retrieved(point) for point in points]
        except RagConfigurationError:
            raise
        except RagStoreError:
            raise
        except Exception as exc:
            raise RagStoreError(f"Qdrant retrieval failed: {_error_label(exc)}") from exc
        finally:
            diagnostics.total_retrieval_latency_ms = round(
                (time.perf_counter() - started) * 1000,
                3,
            )

    def _dense_search(
        self,
        query: RetrievalQuery,
        *,
        query_filter: Any,
        limit: int,
        score_threshold: float,
        using: str | None = None,
    ) -> tuple[list[Any], float]:
        from qdrant_client import models

        request: dict[str, Any] = {
            "collection_name": self._config.collection,
            "query": models.Document(
                text=query.text,
                model=self._config.effective_dense_embedding_model,
            ),
            "query_filter": query_filter,
            "limit": limit,
            "score_threshold": score_threshold,
            "with_payload": True,
            "with_vectors": False,
            "timeout": int(self._config.request_timeout_seconds),
        }
        if using is not None:
            request["using"] = using
        started = time.perf_counter()
        response = self._call_with_retry(lambda: self._get_client().query_points(**request))
        return list(response.points), round((time.perf_counter() - started) * 1000, 3)

    def _hybrid_search(
        self,
        query: RetrievalQuery,
        *,
        query_filter: Any,
        limit: int,
    ) -> tuple[list[Any], float, str | None]:
        from qdrant_client import models

        dense_prefetch = models.Prefetch(
            query=models.Document(
                text=query.text,
                model=self._config.effective_dense_embedding_model,
            ),
            using=self._config.dense_vector_name,
            filter=query_filter,
            score_threshold=self._config.effective_dense_score_threshold,
            limit=self._config.effective_dense_candidate_limit,
        )
        sparse_prefetch = models.Prefetch(
            query=models.Document(
                text=query.text,
                model=self._config.sparse_embedding_model,
            ),
            using=self._config.sparse_vector_name,
            filter=query_filter,
            score_threshold=self._config.sparse_score_threshold,
            limit=self._config.effective_sparse_candidate_limit,
        )
        query_model, weighted, fusion_fallback = self._rrf_query(models)

        def execute(fusion_query: Any) -> Any:
            return self._call_with_retry(
                lambda: self._get_client().query_points(
                    collection_name=self._config.collection,
                    prefetch=[dense_prefetch, sparse_prefetch],
                    query=fusion_query,
                    query_filter=query_filter,
                    limit=min(limit, self._config.effective_fusion_limit),
                    with_payload=True,
                    with_vectors=False,
                    timeout=int(self._config.request_timeout_seconds),
                )
            )

        started = time.perf_counter()
        try:
            response = execute(query_model)
        except Exception as exc:
            if not weighted or not _weighted_rrf_unsupported(exc):
                raise
            if not self._config.hybrid_weighted_rrf_fallback_to_equal:
                raise RagConfigurationError(
                    "Configured Qdrant server does not support weighted RRF; "
                    "upgrade it to 1.17+ or explicitly enable equal-RRF fallback"
                ) from exc
            fusion_fallback = "weighted_rrf_unsupported:equal_rrf"
            response = execute(models.FusionQuery(fusion=models.Fusion.RRF))
        return (
            list(response.points),
            round((time.perf_counter() - started) * 1000, 3),
            fusion_fallback,
        )

    def _rrf_query(self, models: Any) -> tuple[Any, bool, str | None]:
        weights = [
            float(self._config.hybrid_dense_weight),
            float(self._config.hybrid_sparse_weight),
        ]
        if weights == [1.0, 1.0]:
            return models.FusionQuery(fusion=models.Fusion.RRF), False, None
        try:
            return models.RrfQuery(rrf=models.Rrf(weights=weights)), True, None
        except (AttributeError, TypeError) as exc:
            if self._config.hybrid_weighted_rrf_fallback_to_equal:
                return (
                    models.FusionQuery(fusion=models.Fusion.RRF),
                    False,
                    "weighted_rrf_client_unsupported:equal_rrf",
                )
            raise RagConfigurationError(
                "qdrant-client does not support weighted RRF; install qdrant-client>=1.17"
            ) from exc

    def _existing_payloads(self, tenant_id: str) -> dict[str, dict[str, Any]]:
        from qdrant_client import models

        result: dict[str, dict[str, Any]] = {}
        offset = None
        query_filter = models.Filter(
            must=[models.FieldCondition(key="tenant_id", match=models.MatchValue(value=tenant_id))]
        )
        while True:
            points, offset = self._call_with_retry(
                lambda: self._get_client().scroll(
                    collection_name=self._config.collection,
                    scroll_filter=query_filter,
                    limit=256,
                    offset=offset,
                    with_payload=["content_hash"],
                    with_vectors=False,
                )
            )
            for point in points:
                result[str(point.id)] = dict(point.payload or {})
            if offset is None:
                return result

    def _upsert(self, chunks: list[RunbookChunk]) -> None:
        from qdrant_client import models

        points = []
        for chunk in chunks:
            indexed_text = retrieval_text(chunk)
            dense_document = models.Document(
                text=indexed_text,
                model=self._config.effective_dense_embedding_model,
            )
            vector: Any = dense_document
            if self._config.search_mode == "hybrid":
                vector = {
                    self._config.dense_vector_name: dense_document,
                    self._config.sparse_vector_name: models.Document(
                        text=indexed_text,
                        model=self._config.sparse_embedding_model,
                    ),
                }
            points.append(
                models.PointStruct(
                    id=chunk.point_id,
                    vector=vector,
                    payload=chunk.payload(),
                )
            )
        self._call_with_retry(
            lambda: self._get_client().upsert(
                collection_name=self._config.collection,
                points=points,
                wait=True,
            )
        )

    def _validate_collection_info(self, info: Any) -> None:
        vectors, sparse_vectors = _collection_vectors(info)
        if self._config.search_mode == "hybrid":
            if not isinstance(vectors, Mapping):
                raise RagConfigurationError(
                    "Hybrid mode requires a collection with named dense vectors; "
                    "the configured collection is dense-only or has no vector schema"
                )
            dense = vectors.get(self._config.dense_vector_name)
            if dense is None:
                raise RagConfigurationError(
                    "Hybrid collection is missing named dense vector "
                    f"{self._config.dense_vector_name!r}"
                )
            if _size(dense) != self._config.embedding_size:
                raise RagConfigurationError(
                    "Hybrid dense vector size does not match QDRANT_EMBEDDING_SIZE"
                )
            if not isinstance(sparse_vectors, Mapping) or (
                self._config.sparse_vector_name not in sparse_vectors
            ):
                raise RagConfigurationError(
                    "Hybrid collection is missing named sparse vector "
                    f"{self._config.sparse_vector_name!r}"
                )
            return
        if isinstance(vectors, Mapping):
            raise RagConfigurationError(
                "Dense mode expects the legacy unnamed dense vector collection"
            )
        if _size(vectors) != self._config.embedding_size:
            raise RagConfigurationError(
                "Qdrant collection vector size does not match QDRANT_EMBEDDING_SIZE"
            )

    def _call_with_retry(self, operation: Callable[[], T]) -> T:
        """Retry a bounded SDK operation; point upserts/deletes are idempotent."""
        for attempt in range(self._config.max_retries + 1):
            try:
                return operation()
            except Exception:
                if attempt >= self._config.max_retries:
                    raise
                time.sleep(min(0.2 * (2**attempt), 1.0))
        raise AssertionError("unreachable")


def _payload_filter(query: RetrievalQuery) -> Any:
    from qdrant_client import models

    must: list[Any] = [
        models.FieldCondition(key="tenant_id", match=models.MatchValue(value=query.tenant_id)),
        models.FieldCondition(key="status", match=models.MatchValue(value="approved")),
        models.FieldCondition(
            key="environment",
            match=models.MatchAny(any=[query.environment, "all"]),
        ),
    ]
    if query.connector_class:
        must.append(
            models.Filter(
                should=[
                    models.FieldCondition(
                        key=field,
                        match=models.MatchValue(value=query.connector_class.lower()),
                    )
                    for field in (
                        "connector_class",
                        "connector_type",
                        "connector_family",
                        "subsystem",
                    )
                ],
            )
        )
    if query.error_codes:
        must.append(
            models.FieldCondition(
                key="error_codes",
                match=models.MatchAny(any=list(query.error_codes)),
            )
        )
    return models.Filter(must=must)


def _payload_filter_fields(query: RetrievalQuery) -> tuple[str, ...]:
    """Report filter categories without exposing tenant/environment/error values."""

    fields = ["tenant_id", "status", "environment"]
    if query.connector_class:
        fields.append("connector_taxonomy")
    if query.error_codes:
        fields.append("error_codes")
    return tuple(fields)


def _batches(items: list[RunbookChunk], *, size: int) -> Iterable[list[RunbookChunk]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _retrieved(point: Any) -> RetrievedChunk:
    payload = dict(point.payload or {})
    return RetrievedChunk(
        point_id=str(point.id),
        score=float(point.score),
        runbook_id=str(payload.get("runbook_id") or ""),
        title=str(payload.get("title") or ""),
        version=int(payload.get("version") or 0),
        section=str(payload.get("section") or ""),
        section_title=str(payload.get("section_title") or ""),
        source=str(payload.get("source") or ""),
        connector_class=str(payload.get("connector_class") or ""),
        connector_type=str(payload.get("connector_type") or ""),
        connector_family=str(payload.get("connector_family") or ""),
        subsystem=str(payload.get("subsystem") or ""),
        error_codes=tuple(str(item) for item in payload.get("error_codes") or []),
        symptoms=tuple(str(item) for item in payload.get("symptoms") or []),
        exception_classes=tuple(
            str(item) for item in payload.get("exception_classes") or []
        ),
        config_keys=tuple(str(item) for item in payload.get("config_keys") or []),
        error_signatures=tuple(
            str(item) for item in payload.get("error_signatures") or []
        ),
        aliases=tuple(str(item) for item in payload.get("aliases") or []),
        user_phrases_vi=tuple(str(item) for item in payload.get("user_phrases_vi") or []),
        user_phrases_en=tuple(str(item) for item in payload.get("user_phrases_en") or []),
        schema_version=int(payload.get("schema_version") or 1),
        text=str(payload.get("text") or ""),
        tenant_id=str(payload.get("tenant_id") or ""),
        status=str(payload.get("status") or ""),
        environments=tuple(str(item) for item in payload.get("environment") or []),
    )


def _collection_vectors(info: Any) -> tuple[Any, Any]:
    config = getattr(info, "config", None)
    params = getattr(config, "params", None)
    return getattr(params, "vectors", None), getattr(params, "sparse_vectors", None)


def _size(vector: Any) -> int | None:
    size = getattr(vector, "size", None)
    return int(size) if isinstance(size, int) else None


def _deduplicate_points(points: list[Any]) -> list[Any]:
    """Keep the highest-scoring occurrence of each point with stable tie ordering."""
    by_id: dict[str, tuple[int, Any]] = {}
    for index, point in enumerate(points):
        point_id = str(point.id)
        previous = by_id.get(point_id)
        if previous is None or float(point.score) > float(previous[1].score):
            by_id[point_id] = (index, point)
    return [value[1] for value in sorted(by_id.values(), key=lambda item: item[0])]


def _error_label(exc: Exception) -> str:
    text = str(exc).casefold()
    if isinstance(exc, TimeoutError) or "timeout" in text or "timed out" in text:
        return "TimeoutError"
    if "quota" in text or "payment required" in text or "status code 402" in text:
        return "QuotaOrBillingError"
    if "rate limit" in text or "status code 429" in text:
        return "RateLimitError"
    if "unsupported" in text and "model" in text:
        return "UnsupportedModelError"
    if "unauthorized" in text or "forbidden" in text or "status code 401" in text:
        return "AuthenticationError"
    return type(exc).__name__


def _weighted_rrf_unsupported(exc: Exception) -> bool:
    text = str(exc).casefold()
    capability_terms = ("unsupported", "unknown field", "extra_forbidden", "validation")
    return ("rrf" in text or "weight" in text) and any(term in text for term in capability_terms)
