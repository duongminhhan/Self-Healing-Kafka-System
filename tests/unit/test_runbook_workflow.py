from __future__ import annotations

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.answer_composer import GroundedAnswerComposer
from self_healthy_kafka.rag.models import RagConfigurationError, RagStoreError, RetrievedChunk
from self_healthy_kafka.rag.retriever import RunbookRetriever
from self_healthy_kafka.rag.workflow import RunbookRagWorkflow, extract_verified_facts
from self_healthy_kafka.semantic.catalog import CATALOG_VERSION
from self_healthy_kafka.semantic.planner import parse_semantic_plan


def _config(**overrides):
    values = {
        "enabled": True, "qdrant_url": "https://qdrant.example", "qdrant_api_key": "test-only",
        "collection": "runbooks", "embedding_model": "configured-cluster-model", "embedding_size": 384,
        "top_k": 3, "request_timeout_seconds": 10, "score_threshold": 0.5, "max_chunk_chars": 2400,
        "max_context_chars": 8000, "environment": "uat", "tenant_id": "tenant-a", "diagnostics_enabled": False,
    }
    values.update(overrides)
    return RagConfig(**values)


def _chunk():
    return RetrievedChunk(
        point_id="one", score=0.92, runbook_id="RB-ORACLE-001", title="Oracle authentication",
        version=1, section="diagnostic_steps", section_title="Diagnostic steps",
        source="runbooks/oracle/invalid-credentials.md", connector_class="oracle",
        error_codes=("ORA-01017",), text="Check the approved credential reference for ORA-01017.",
    )


def _plan(*, data=True, guidance=True, purpose="remediation"):
    return parse_semantic_plan({
        "version": CATALOG_VERSION,
        "data_request": {
            "metrics": ["incident_count"], "dimensions": ["connector"], "filters": {},
            "sort": {"metric": "incident_count", "direction": "desc"}, "limit": 5,
            "comparison": None, "detail_fields": [],
        } if data else None,
        "guidance_request": {
            "needed": guidance, "purpose": purpose if guidance else None,
            "error_codes": ["ORA-01017"] if guidance else [], "connector_class": "oracle" if guidance else None,
        },
        "clarification": None, "conversation_action": "none", "inherited_fields": [],
    })


def _analytics():
    evidence = [{
        "fact_id": "analytics:orders:1", "rank": 1,
        "entity": {"connector": "orders"},
        "metrics": [{"name": "failure_count", "label": "số incident", "value": 1,
                     "unit": "incident", "aggregation": "count_distinct_incident"}],
        "details": {}, "detail_values": {},
        "time_range": {"from_at": None, "to_at": None, "timestamp": "failure_at"},
        "status": None, "grain": "aggregated connector incident facts",
        "source": "vConnectorIncidentFacts", "evidence_ids": ["one"], "complete": True,
        "semantic_catalog_version": CATALOG_VERSION,
    }]
    return {
        "answer": "orders có 1 incident.",
        "source": "analytics",
        "sources": [{"source": "vConnectorIncidentFacts", "items": [{
            "incident_id": "one", "connector_name": "orders", "error_code": "ORA-01017",
        }]}],
        "query_plan": {"dataset": "connector_incidents"}, "semantic_plan": {"version": CATALOG_VERSION},
        "from_at": None, "to_at": None, "row_count": 1, "evidence_ids": ["one"], "evidence": evidence,
        "claims": [{"fact_id": "analytics:orders:1", "entity": {"connector": "orders"},
                    "metric": "failure_count", "value": 1,
                    "time_range": {"from_at": None, "to_at": None, "timestamp": "failure_at"},
                    "status": None, "text": "orders có số incident là 1"}],
        "verified_result": {"rows": [{"connector_name": "orders", "failure_count": 1}],
                            "columns": ["connector_name", "failure_count"]},
    }


def test_runbook_only_route_is_driven_by_plan_and_keeps_response_contract():
    class Store:
        def search(self, query, *, limit, score_threshold):
            assert query.error_codes == ("ORA-01017",)
            return [_chunk()]

    workflow = RunbookRagWorkflow(_config(), retriever=RunbookRetriever(_config(), Store()), composer=GroundedAnswerComposer())
    result = workflow.ask("Một câu hỏi", plan=_plan(data=False), analytics_ask=lambda _: (_ for _ in ()).throw(AssertionError()))

    assert result["route"] == "runbook"
    assert result["query_plan"] is None
    assert result["citations"][0]["runbook_id"] == "RB-ORACLE-001"
    assert result["status"] == "ok"


def test_analytics_only_plan_never_calls_qdrant():
    class Store:
        def search(self, *_args, **_kwargs):
            raise AssertionError("analytics plan must not retrieve runbooks")

    workflow = RunbookRagWorkflow(_config(), retriever=RunbookRetriever(_config(), Store()), composer=GroundedAnswerComposer())
    result = workflow.ask("Một câu hỏi", plan=_plan(guidance=False), analytics_ask=lambda _: _analytics())

    assert result["route"] == "analytics"
    assert result["answer"].startswith("orders có 1 incident.")


def test_rag_failure_keeps_verified_analytics_result():
    class Store:
        def search(self, *_args, **_kwargs):
            raise RagStoreError("offline")

    workflow = RunbookRagWorkflow(_config(), retriever=RunbookRetriever(_config(), Store()), composer=GroundedAnswerComposer())
    result = workflow.ask("Một câu hỏi", plan=_plan(), analytics_ask=lambda _: _analytics())

    assert result["answer"].startswith("orders có 1 incident.")
    assert result["source"] == "analytics"
    assert result["fallback_reason"] == "qdrant_service_error"
    assert result["status"] == "ok"
    assert "kho runbook" in result["answer"]


def test_missing_runbook_does_not_replace_analytics_evidence_with_generic_advice():
    class Store:
        def search(self, *_args, **_kwargs):
            return []

    workflow = RunbookRagWorkflow(_config(), retriever=RunbookRetriever(_config(), Store()), composer=GroundedAnswerComposer())
    result = workflow.ask("Một câu hỏi", plan=_plan(), analytics_ask=lambda _: _analytics())

    assert result["source"] == "analytics"
    assert result["answer"].startswith("orders có 1 incident.")
    assert "chưa tìm thấy runbook" in result["answer"]


def test_combined_success_keeps_analytics_answer_and_fact_evidence_intact():
    class Store:
        def search(self, *_args, **_kwargs):
            return [_chunk()]

    analytics = _analytics()
    workflow = RunbookRagWorkflow(
        _config(), retriever=RunbookRetriever(_config(), Store()), composer=GroundedAnswerComposer()
    )
    result = workflow.ask("Một câu hỏi", plan=_plan(), analytics_ask=lambda _: analytics)

    assert result["route"] == "combined"
    assert result["source"] == "combined"
    assert result["answer"].startswith("orders có 1 incident.")
    assert "Hướng dẫn liên quan:" in result["answer"]
    assert result["analytics_evidence"] == analytics["evidence"]
    assert result["claims"] == analytics["claims"]
    assert result["evidence"][0]["runbook_id"] == "RB-ORACLE-001"
    assert result["verified_result"] == analytics["verified_result"]


def test_qdrant_configuration_failure_is_explicit():
    class Store:
        def search(self, *_args, **_kwargs):
            raise RagConfigurationError("dense only")

    workflow = RunbookRagWorkflow(_config(), retriever=RunbookRetriever(_config(), Store()), composer=GroundedAnswerComposer())
    result = workflow.ask("Một câu hỏi", plan=_plan(data=False), analytics_ask=lambda _: {})

    assert result["source"] == "deterministic_fallback"
    assert result["fallback_reason"] == "qdrant_configuration_error"


def test_only_verified_incident_sources_become_combined_facts():
    facts = extract_verified_facts({
        "sources": [
            {"source": "vConnectorIncidentFacts", "items": [{"incident_id": "one", "connector_name": "orders", "password": "no"}]},
            {"source": "debug", "items": [{"error_code": "ORA-99999"}]},
        ]
    })

    assert facts == [{"incident_id": "one", "connector_name": "orders"}]


def test_dbt_compatibility_view_is_an_allowlisted_verified_incident_source():
    facts = extract_verified_facts({
        "sources": [{
            "source": "vSemanticConnectorIncidentFacts",
            "items": [{"incident_id": "one", "connector_name": "orders"}],
        }],
    })

    assert facts == [{"incident_id": "one", "connector_name": "orders"}]
