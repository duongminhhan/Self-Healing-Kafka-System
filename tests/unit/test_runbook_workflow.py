from dataclasses import replace

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.answer_composer import GroundedAnswerComposer
from self_healthy_kafka.rag.models import (
    RagConfigurationError,
    RagStoreError,
    RetrievedChunk,
    SearchDiagnostics,
)
from self_healthy_kafka.rag.retriever import RunbookRetriever
from self_healthy_kafka.rag.workflow import RunbookRagWorkflow, extract_verified_facts


def _config(**overrides):
    values = {
        "enabled": True,
        "qdrant_url": "https://qdrant.example",
        "qdrant_api_key": "test-only",
        "collection": "runbooks",
        "embedding_model": "configured-cluster-model",
        "embedding_size": 384,
        "top_k": 3,
        "request_timeout_seconds": 10,
        "score_threshold": 0.5,
        "max_chunk_chars": 2400,
        "max_context_chars": 8000,
        "environment": "uat",
        "tenant_id": "tenant-a",
        "diagnostics_enabled": False,
    }
    values.update(overrides)
    return RagConfig(**values)


def _retrieved():
    return RetrievedChunk(
        point_id="one",
        score=0.92,
        runbook_id="RB-ORACLE-001",
        title="Oracle authentication",
        version=1,
        section="diagnostic_steps",
        section_title="Diagnostic steps",
        source="runbooks/oracle/invalid-credentials.md",
        connector_class="oracle",
        error_codes=("ORA-01017",),
        text="Check the approved credential reference for ORA-01017.",
    )


def test_runbook_route_keeps_backward_compatible_response_fields():
    class Store:
        def search(self, query, *, limit, score_threshold):
            return [_retrieved()]

    workflow = RunbookRagWorkflow(
        _config(),
        retriever=RunbookRetriever(_config(), Store()),
        composer=GroundedAnswerComposer(),
    )

    result = workflow.ask("Runbook xử lý ORA-01017 là gì?", analytics_ask=lambda _: {})

    assert result["route"] == "runbook"
    assert result["source"] == "deterministic_fallback"
    assert result["query_plan"] is None
    assert result["sources"] == []
    assert result["citations"][0]["runbook_id"] == "RB-ORACLE-001"
    assert result["status"] == "ok"
    assert result["recommended_runbooks"][0]["title"] == "Oracle authentication"
    assert "Oracle authentication" in result["answer"]


def test_runbook_response_explains_exact_config_key_evidence_without_debug_scores():
    class Store:
        def search(self, query, *, limit, score_threshold):
            return [
                replace(
                    _retrieved(),
                    runbook_id="RB-NET-001",
                    title="Kafka Connect dependency timeout",
                    connector_class="network",
                    connector_type="kafka-connect",
                    error_codes=("CONNECT_TIMEOUT",),
                    config_keys=("connection.timeout.ms",),
                )
            ]

    workflow = RunbookRagWorkflow(
        _config(),
        retriever=RunbookRetriever(_config(), Store()),
        composer=GroundedAnswerComposer(),
    )

    result = workflow.ask(
        "connection.timeout.ms bị vượt quá, tôi nên làm gì?",
        analytics_ask=lambda _: {},
    )

    assert result["evidence"][0]["match_basis"] == "config_key"
    assert result["evidence"][0]["matched_config_keys"] == ["connection.timeout.ms"]
    assert "score" not in result["evidence"][0]
    assert "Kafka Connect dependency timeout" in result["answer"]


def test_no_answer_is_structured_and_does_not_invent_a_runbook():
    class Store:
        last_search_diagnostics = SearchDiagnostics(
            search_mode="hybrid",
            collection="v2",
            dense_model="dense",
            sparse_model="sparse",
            no_result_reason="strong_anchor_not_found",
        )

        def search(self, query, *, limit, score_threshold):
            return []

    config = _config(search_mode="hybrid", evidence_gate_enabled=True)
    workflow = RunbookRagWorkflow(
        config,
        retriever=RunbookRetriever(config, Store()),
        composer=GroundedAnswerComposer(),
    )

    result = workflow.ask(
        "Runbook cho lỗi CUDA_KERNEL_FAILURE là gì?",
        analytics_ask=lambda _: {},
    )

    assert result["status"] == "no_answer"
    assert result["reason"] == "strong_anchor_not_found"
    assert result["candidates"] == []
    assert result["recommended_runbooks"] == []
    assert "chưa tìm thấy" in result["answer"].casefold()


def test_qdrant_failure_degrades_to_verified_analytics_result():
    class Store:
        def search(self, query, *, limit, score_threshold):
            raise RagStoreError("offline")

    analytics = {
        "answer": "orders đang có 1 incident.",
        "sources": [{"source": "vConnectorIncidentFacts", "items": []}],
        "query_plan": {"dataset": "connector_incidents"},
        "from_at": None,
        "to_at": None,
        "row_count": 1,
        "evidence_ids": ["incident-1"],
    }
    workflow = RunbookRagWorkflow(
        _config(),
        retriever=RunbookRetriever(_config(), Store()),
        composer=GroundedAnswerComposer(),
    )

    result = workflow.ask("Connector orders đang lỗi, tại sao?", analytics_ask=lambda _: analytics)

    assert result["answer"] == analytics["answer"]
    assert result["source"] == "analytics"
    assert result["fallback_reason"] == "qdrant_service_error"


def test_incompatible_hybrid_collection_returns_configuration_fallback():
    class Store:
        def search(self, query, *, limit, score_threshold):
            raise RagConfigurationError("hybrid collection is dense-only")

    config = _config(search_mode="hybrid")
    workflow = RunbookRagWorkflow(
        config,
        retriever=RunbookRetriever(config, Store()),
        composer=GroundedAnswerComposer(),
    )

    result = workflow.ask("Runbook xử lý task failure là gì?", analytics_ask=lambda _: {})

    assert result["source"] == "deterministic_fallback"
    assert result["fallback_reason"] == "qdrant_configuration_error"


def test_analytics_route_does_not_call_qdrant():
    class Store:
        def search(self, query, *, limit, score_threshold):
            raise AssertionError("analytics-only route must not retrieve runbooks")

    analytics = {"answer": "Có 3 incident.", "sources": [], "evidence_ids": []}
    workflow = RunbookRagWorkflow(
        _config(),
        retriever=RunbookRetriever(_config(), Store()),
        composer=GroundedAnswerComposer(),
    )

    result = workflow.ask("Có tổng cộng bao nhiêu incident?", analytics_ask=lambda _: analytics)

    assert result["answer"] == "Có 3 incident."
    assert result["route"] == "analytics"
    assert result["citations"] == []


def test_question_secret_is_redacted_before_qdrant_receives_it():
    class Store:
        query = None

        def search(self, query, *, limit, score_threshold):
            self.query = query
            return []

    store = Store()
    workflow = RunbookRagWorkflow(
        _config(),
        retriever=RunbookRetriever(_config(), store),
        composer=GroundedAnswerComposer(),
    )

    workflow.ask(
        "Runbook khi password=my-private-value không hoạt động?",
        analytics_ask=lambda _: {},
    )

    assert "my-private-value" not in store.query.text
    assert "[REDACTED]" in store.query.text


def test_only_verified_analytics_sources_become_combined_facts():
    facts = extract_verified_facts(
        {
            "sources": [
                {
                    "source": "vConnectorIncidentFacts",
                    "items": [
                        {
                            "incident_id": "one",
                            "connector_name": "orders",
                            "error_code": "ORA-01017",
                            "password": "must-not-pass",
                        }
                    ],
                },
                {"source": "unverified_debug_table", "items": [{"error_code": "ORA-99999"}]},
            ]
        }
    )

    assert facts == [
        {
            "incident_id": "one",
            "connector_name": "orders",
            "error_code": "ORA-01017",
        }
    ]


def test_enabled_rag_rejects_missing_qdrant_configuration():
    config = _config(qdrant_url="", qdrant_api_key="", embedding_model="")

    try:
        config.validate()
    except ValueError as exc:
        assert "QDRANT_URL" in str(exc)
        assert "QDRANT_API_KEY" in str(exc)
        assert "QDRANT_EMBEDDING_MODEL" in str(exc)
    else:
        raise AssertionError("missing RAG configuration must be rejected")
