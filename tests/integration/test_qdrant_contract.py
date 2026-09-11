import os
from collections.abc import Mapping

import pytest

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.payload_indexes import PAYLOAD_INDEX_TYPES, inspect_payload_indexes

pytestmark = pytest.mark.skipif(
    os.getenv("RUNBOOK_RAG_LIVE_TEST", "false").lower() not in {"1", "true", "yes", "on"},
    reason="live Qdrant contract is explicitly opt-in",
)


def test_configured_qdrant_collection_has_required_payload_indexes():
    from qdrant_client import QdrantClient

    config = RagConfig()
    config.validate()
    client = QdrantClient(
        url=config.qdrant_url,
        api_key=config.qdrant_api_key,
        cloud_inference=True,
        timeout=int(config.request_timeout_seconds),
    )
    info = client.get_collection(config.collection)

    payload_plan = inspect_payload_indexes(info.payload_schema)
    assert payload_plan.valid, payload_plan.to_dict()
    assert set(PAYLOAD_INDEX_TYPES) <= set(info.payload_schema)
    vectors = info.config.params.vectors
    sparse_vectors = info.config.params.sparse_vectors
    if config.search_mode == "hybrid":
        assert isinstance(vectors, Mapping)
        assert config.dense_vector_name in vectors
        assert vectors[config.dense_vector_name].size == config.embedding_size
        assert isinstance(sparse_vectors, Mapping)
        assert config.sparse_vector_name in sparse_vectors
    else:
        assert not isinstance(vectors, Mapping)
        assert vectors.size == config.embedding_size
