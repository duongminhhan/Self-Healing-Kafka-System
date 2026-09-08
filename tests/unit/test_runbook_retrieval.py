from types import SimpleNamespace

import pytest
from qdrant_client import models

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.models import RagStoreError, RetrievalQuery, RetrievedChunk
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore
from self_healthy_kafka.rag.retriever import RunbookRetriever


def _config(**overrides):
    values = {
        "enabled": True,
        "qdrant_url": "https://qdrant.example",
        "qdrant_api_key": "test-only",
        "collection": "test",
        "embedding_model": "intfloat/multilingual-e5-small",
        "embedding_size": 384,
        "search_mode": "dense",
        "dense_vector_name": "dense",
        "sparse_vector_name": "sparse",
        "dense_embedding_model": "",
        "sparse_embedding_model": "qdrant/bm25",
        "top_k": 2,
        "request_timeout_seconds": 10,
        "score_threshold": 0.6,
        "dense_score_threshold": None,
        "sparse_score_threshold": None,
        "dense_candidate_limit": 0,
        "sparse_candidate_limit": 0,
        "fusion_method": "rrf",
        "fusion_limit": 0,
        "hybrid_fallback_to_dense": False,
        "max_chunk_chars": 2400,
        "max_context_chars": 8000,
        "environment": "uat",
        "tenant_id": "tenant-a",
        "diagnostics_enabled": False,
    }
    values.update(overrides)
    return RagConfig(**values)


def _chunk(
    point_id,
    score,
    section="recovery_steps",
    text="Safe recovery step",
    *,
    runbook_id="RB-1",
    title="Runbook",
    connector_class="oracle",
    error_codes=("ORA-01017",),
):
    return RetrievedChunk(
        point_id=point_id,
        score=score,
        runbook_id=runbook_id,
        title=title,
        version=1,
        section=section,
        section_title=section,
        source="runbooks/test.md",
        connector_class=connector_class,
        error_codes=error_codes,
        text=text,
    )


def _collection_info(*, hybrid=False, payload_schema=None):
    vectors = models.VectorParams(size=384, distance=models.Distance.COSINE)
    sparse_vectors = {}
    if hybrid:
        vectors = {"dense": vectors}
        sparse_vectors = {"sparse": models.SparseVectorParams(modifier=models.Modifier.IDF)}
    return SimpleNamespace(
        config=SimpleNamespace(
            params=SimpleNamespace(vectors=vectors, sparse_vectors=sparse_vectors)
        ),
        payload_schema=payload_schema or {},
    )


def test_retriever_deduplicates_sections_and_rejects_low_scores():
    class Store:
        def search(self, query, *, limit, score_threshold):
            assert limit == 6
            return [_chunk("a", 0.9), _chunk("b", 0.8), _chunk("c", 0.5, "verification")]

    result = RunbookRetriever(_config(), Store()).retrieve(RetrievalQuery("question"))

    assert [item.point_id for item in result] == ["a"]


def test_retriever_rejects_high_scoring_vector_matches_without_issue_anchor():
    class Store:
        def search(self, query, *, limit, score_threshold):
            return [
                _chunk(
                    "a",
                    0.91,
                    "rollback",
                    "Revert the last connector configuration deployment.",
                    runbook_id="RB-JDBC-001",
                    title="JDBC connection failure",
                ),
                _chunk(
                    "b",
                    0.86,
                    "rollback",
                    "Return to the last verified configuration.",
                    runbook_id="RB-KC-002",
                    title="Automatic healing exhausted",
                ),
            ]

    result = RunbookRetriever(_config(), Store()).retrieve(
        RetrievalQuery("Runbook cho lỗi filesystem inode corruption là gì?")
    )

    assert result == []


def test_retriever_keeps_only_runbooks_with_matching_explicit_issue_anchor():
    class Store:
        def search(self, query, *, limit, score_threshold):
            return [
                _chunk(
                    "matching",
                    0.9,
                    text="Retries are exhausted and manual review is required.",
                    runbook_id="RB-KC-002",
                    title="Automatic healing exhausted",
                    connector_class="kafka-connect",
                    error_codes=("MAX_RETRIES_REACHED",),
                ),
                _chunk(
                    "unrelated",
                    0.88,
                    text="Revert an unproven timeout change.",
                    runbook_id="RB-NET-001",
                    title="Kafka Connect dependency timeout",
                    connector_class="network",
                    error_codes=("CONNECT_TIMEOUT",),
                ),
            ]

    result = RunbookRetriever(_config(), Store()).retrieve(
        RetrievalQuery("Runbook cho lỗi retry exhausted là gì?")
    )

    assert [item.point_id for item in result] == ["matching"]


def test_retriever_preserves_natural_vietnamese_retry_exhausted_question():
    class Store:
        def search(self, query, *, limit, score_threshold):
            return [
                _chunk(
                    "retry",
                    0.9,
                    text="Healing retries are exhausted and require manual review.",
                    runbook_id="RB-KC-002",
                    title="Automatic healing exhausted",
                    connector_class="kafka-connect",
                    error_codes=("MAX_RETRIES_REACHED",),
                )
            ]

    result = RunbookRetriever(_config(), Store()).retrieve(
        RetrievalQuery("Quy trình xử lý khi healing đã retry hết là gì?")
    )

    assert [item.point_id for item in result] == ["retry"]


def test_qdrant_query_enforces_approved_tenant_environment_and_exact_code_filters():
    class Client:
        request = None

        def get_collection(self, name):
            return _collection_info()

        def query_points(self, **kwargs):
            self.request = kwargs
            return SimpleNamespace(points=[])

    client = Client()
    store = QdrantRunbookStore(_config(), client=client)
    store.search(
        RetrievalQuery(
            "ORA login error",
            tenant_id="tenant-a",
            environment="uat",
            connector_class="oracle",
            error_codes=("ORA-01017",),
        ),
        limit=5,
        score_threshold=0.6,
    )

    conditions = {
        condition.key: condition.match for condition in client.request["query_filter"].must
    }
    assert conditions["tenant_id"].value == "tenant-a"
    assert conditions["status"].value == "approved"
    assert set(conditions["environment"].any) == {"uat", "all"}
    assert conditions["connector_class"].value == "oracle"
    assert conditions["error_codes"].any == ["ORA-01017"]


def test_qdrant_query_retries_only_up_to_configured_limit():
    class Client:
        calls = 0

        def get_collection(self, name):
            return _collection_info()

        def query_points(self, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("transient")
            return SimpleNamespace(points=[])

    client = Client()
    store = QdrantRunbookStore(_config(max_retries=1), client=client)

    assert store.search(RetrievalQuery("question"), limit=5, score_threshold=0.6) == []
    assert client.calls == 2


def test_qdrant_timeout_is_bounded_and_classified_as_store_error():
    class Client:
        calls = 0

        def get_collection(self, name):
            return _collection_info()

        def query_points(self, **kwargs):
            self.calls += 1
            raise TimeoutError("transient")

    client = Client()
    store = QdrantRunbookStore(_config(max_retries=1), client=client)

    with pytest.raises(RagStoreError, match="Qdrant retrieval failed: TimeoutError"):
        store.search(RetrievalQuery("question"), limit=5, score_threshold=0.6)
    assert client.calls == 2


def test_qdrant_sync_is_idempotent_and_removes_stale_points(tmp_path):
    from self_healthy_kafka.rag.chunking import chunk_runbook
    from self_healthy_kafka.rag.ingestion import parse_runbook

    path = tmp_path / "runbook.md"
    path.write_text(
        """---
runbook_id: RB-1
title: One
version: 1
status: approved
connector_class: oracle
error_codes: [ORA-01017]
environments: [all]
owners: [team]
updated_at: 2026-09-07
---
## Recovery steps
- Verify credentials.
""",
        encoding="utf-8",
    )
    chunks = chunk_runbook(parse_runbook(path, root=tmp_path))

    class Client:
        def __init__(self):
            self.points = {
                "stale-id": SimpleNamespace(id="stale-id", payload={"content_hash": "old"})
            }
            self.payload_schema = {}

        def collection_exists(self, name):
            return True

        def get_collection(self, name):
            return _collection_info(payload_schema=self.payload_schema)

        def create_payload_index(self, field_name, **kwargs):
            self.payload_schema[field_name] = True

        def scroll(self, **kwargs):
            return list(self.points.values()), None

        def upsert(self, points, **kwargs):
            for point in points:
                self.points[str(point.id)] = point

        def delete(self, points_selector, **kwargs):
            for point_id in points_selector.points:
                self.points.pop(str(point_id), None)

    client = Client()
    store = QdrantRunbookStore(_config(), client=client)
    first = store.sync(chunks, tenant_id="default")
    second = store.sync(chunks, tenant_id="default")

    assert first.inserted == 1 and first.removed == 1
    assert second.unchanged == 1 and second.inserted == 0


def test_hybrid_collection_creation_has_named_dense_and_sparse_vectors():
    class Client:
        created = None
        payload_schema = {}

        def collection_exists(self, name):
            return False

        def create_collection(self, **kwargs):
            self.created = kwargs

        def get_collection(self, name):
            return _collection_info(hybrid=True, payload_schema=self.payload_schema)

        def create_payload_index(self, field_name, **kwargs):
            self.payload_schema[field_name] = True

    client = Client()
    QdrantRunbookStore(
        _config(search_mode="hybrid", collection="healing_runbooks_v2"),
        client=client,
    ).ensure_collection()

    assert set(client.created["vectors_config"]) == {"dense"}
    assert client.created["vectors_config"]["dense"].distance is models.Distance.COSINE
    assert set(client.created["sparse_vectors_config"]) == {"sparse"}
    assert client.created["sparse_vectors_config"]["sparse"].modifier is models.Modifier.IDF


def test_hybrid_upsert_stores_dense_and_sparse_documents_on_one_point(tmp_path):
    from self_healthy_kafka.rag.chunking import chunk_runbook
    from self_healthy_kafka.rag.ingestion import parse_runbook

    path = tmp_path / "runbook.md"
    path.write_text(
        """---
runbook_id: RB-1
title: One
version: 1
status: approved
connector_class: kafka-connect
error_codes: [TASK_FAILED]
environments: [all]
owners: [team]
updated_at: 2026-09-08
---
## Diagnostic steps
- Inspect the owner worker for TASK_FAILED.
""",
        encoding="utf-8",
    )
    chunks = chunk_runbook(parse_runbook(path, root=tmp_path))

    class Client:
        points = []
        payload_schema = {
            **{
                field: True
                for field in (
                    "tenant_id",
                    "status",
                    "environment",
                    "connector_class",
                    "error_codes",
                    "runbook_id",
                    "source",
                )
            },
            "version": True,
        }

        def collection_exists(self, name):
            return True

        def get_collection(self, name):
            return _collection_info(hybrid=True, payload_schema=self.payload_schema)

        def scroll(self, **kwargs):
            return [], None

        def upsert(self, points, **kwargs):
            self.points.extend(points)

    client = Client()
    QdrantRunbookStore(_config(search_mode="hybrid"), client=client).sync(
        chunks,
        tenant_id="default",
    )

    assert len(client.points) == 1
    assert set(client.points[0].vector) == {"dense", "sparse"}
    assert client.points[0].vector["dense"].model == "intfloat/multilingual-e5-small"
    assert client.points[0].vector["sparse"].model == "qdrant/bm25"


def test_hybrid_query_uses_shared_filter_two_prefetches_and_server_rrf():
    class Client:
        request = None

        def get_collection(self, name):
            return _collection_info(hybrid=True)

        def query_points(self, **kwargs):
            self.request = kwargs
            return SimpleNamespace(points=[])

    client = Client()
    store = QdrantRunbookStore(
        _config(
            search_mode="hybrid",
            dense_candidate_limit=7,
            sparse_candidate_limit=9,
            fusion_limit=4,
        ),
        client=client,
    )
    store.search(
        RetrievalQuery(
            "TASK_FAILED on Oracle",
            tenant_id="tenant-a",
            environment="uat",
            connector_class="oracle",
            error_codes=("TASK_FAILED",),
        ),
        limit=10,
        score_threshold=0.99,
    )

    dense, sparse = client.request["prefetch"]
    assert dense.using == "dense" and dense.limit == 7
    assert sparse.using == "sparse" and sparse.limit == 9
    assert dense.filter == sparse.filter == client.request["query_filter"]
    conditions = {item.key: item.match for item in dense.filter.must}
    assert conditions["tenant_id"].value == "tenant-a"
    assert conditions["status"].value == "approved"
    assert set(conditions["environment"].any) == {"uat", "all"}
    assert conditions["connector_class"].value == "oracle"
    assert conditions["error_codes"].any == ["TASK_FAILED"]
    assert client.request["query"].fusion is models.Fusion.RRF
    assert client.request["limit"] == 4
    assert "score_threshold" not in client.request


def test_hybrid_rrf_score_is_not_compared_with_dense_cosine_threshold():
    point = SimpleNamespace(
        id="one",
        score=0.02,
        payload={
            "runbook_id": "RB-1",
            "title": "Task failure",
            "version": 1,
            "section": "diagnostic_steps",
            "section_title": "Diagnostic steps",
            "source": "runbooks/one.md",
            "connector_class": "kafka-connect",
            "error_codes": ["TASK_FAILED"],
            "text": "Inspect the owner worker.",
        },
    )

    class Client:
        def get_collection(self, name):
            return _collection_info(hybrid=True)

        def query_points(self, **kwargs):
            return SimpleNamespace(points=[point])

    config = _config(search_mode="hybrid", score_threshold=0.99)
    found = RunbookRetriever(config, QdrantRunbookStore(config, client=Client())).retrieve(
        RetrievalQuery("Task bị lỗi cần kiểm tra gì?")
    )

    assert [item.point_id for item in found] == ["one"]


def test_hybrid_rejects_legacy_dense_only_collection_before_query():
    class Client:
        queried = False

        def get_collection(self, name):
            return _collection_info()

        def query_points(self, **kwargs):
            self.queried = True
            return SimpleNamespace(points=[])

    from self_healthy_kafka.rag.models import RagConfigurationError

    client = Client()
    store = QdrantRunbookStore(_config(search_mode="hybrid"), client=client)
    with pytest.raises(RagConfigurationError, match="named dense vectors"):
        store.search(RetrievalQuery("question"), limit=5, score_threshold=0.6)
    assert client.queried is False


def test_hybrid_fallback_to_named_dense_requires_explicit_configuration():
    class Client:
        requests = []

        def get_collection(self, name):
            return _collection_info(hybrid=True)

        def query_points(self, **kwargs):
            self.requests.append(kwargs)
            if "prefetch" in kwargs:
                raise TimeoutError("hybrid timed out")
            return SimpleNamespace(points=[])

    without_fallback = Client()
    store = QdrantRunbookStore(_config(search_mode="hybrid"), client=without_fallback)
    with pytest.raises(RagStoreError, match="TimeoutError"):
        store.search(RetrievalQuery("question"), limit=5, score_threshold=0.6)
    assert len(without_fallback.requests) == 2

    with_fallback = Client()
    store = QdrantRunbookStore(
        _config(search_mode="hybrid", hybrid_fallback_to_dense=True),
        client=with_fallback,
    )
    assert store.search(RetrievalQuery("question"), limit=5, score_threshold=0.6) == []
    assert any(request.get("using") == "dense" for request in with_fallback.requests)
    assert store.last_search_diagnostics.fallback_reason.startswith("hybrid_query_failed")


def test_hybrid_deduplicates_duplicate_point_ids_using_highest_score():
    def point(score, title):
        return SimpleNamespace(
            id="same-id",
            score=score,
            payload={
                "runbook_id": "RB-1",
                "title": title,
                "version": 1,
                "section": "diagnostic_steps",
                "section_title": "Diagnostic steps",
                "source": "runbooks/one.md",
                "connector_class": "kafka-connect",
                "error_codes": ["TASK_FAILED"],
                "text": "Inspect the owner worker.",
                "tenant_id": "tenant-a",
                "status": "approved",
                "environment": ["all"],
            },
        )

    class Client:
        def get_collection(self, name):
            return _collection_info(hybrid=True)

        def query_points(self, **kwargs):
            return SimpleNamespace(points=[point(0.01, "low"), point(0.03, "high")])

    store = QdrantRunbookStore(_config(search_mode="hybrid"), client=Client())
    found = store.search(RetrievalQuery("task failure"), limit=5, score_threshold=0.9)

    assert len(found) == 1
    assert found[0].score == 0.03
    assert found[0].title == "high"


def test_retriever_has_stable_point_id_order_when_scores_are_equal():
    class Store:
        def search(self, query, *, limit, score_threshold):
            return [
                _chunk("b", 0.8, section="verification"),
                _chunk("a", 0.8, section="diagnostic_steps"),
            ]

    found = RunbookRetriever(_config(), Store()).retrieve(RetrievalQuery("question"))

    assert [item.point_id for item in found] == ["a", "b"]
