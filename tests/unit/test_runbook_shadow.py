import threading
import time

from self_healthy_kafka.rag.models import RetrievalQuery, RetrievedChunk, SearchDiagnostics
from self_healthy_kafka.rag.shadow import ShadowRetrievalCoordinator


def _chunk(point_id: str, runbook_id: str) -> RetrievedChunk:
    return RetrievedChunk(
        point_id=point_id,
        score=0.9,
        runbook_id=runbook_id,
        title="Runbook",
        version=1,
        section="diagnostic_steps",
        section_title="Diagnostic steps",
        source="runbooks/test.md",
        connector_class="kafka-connect",
        error_codes=("TASK_FAILED",),
        text="Inspect the owner worker.",
    )


class _Retriever:
    def __init__(self, chunks=None, *, error=None, wait_on=None):
        self.chunks = list(chunks or [])
        self.error = error
        self.wait_on = wait_on
        self.called = threading.Event()
        self.last_diagnostics = SearchDiagnostics(
            search_mode="dense",
            collection="test",
            dense_model="dense",
            total_retrieval_latency_ms=4.0,
            payload_filter_fields=("tenant_id", "status", "environment"),
        )

    def retrieve(self, query):
        self.called.set()
        if self.wait_on is not None:
            self.wait_on.wait(timeout=2)
        if self.error is not None:
            raise self.error
        return self.chunks


def test_shadow_returns_primary_without_waiting_for_hybrid():
    release = threading.Event()
    primary = _Retriever([_chunk("p", "RB-DENSE")])
    shadow = _Retriever([_chunk("s", "RB-HYBRID")], wait_on=release)
    events = []
    coordinator = ShadowRetrievalCoordinator(
        primary,
        shadow,
        sample_rate=1,
        queue_size=2,
        shutdown_timeout_seconds=1,
        sampler=lambda: 0,
        event_sink=lambda event, payload: events.append((event, payload)),
    )

    started = time.perf_counter()
    result = coordinator.retrieve(RetrievalQuery("TASK_FAILED"))
    elapsed = time.perf_counter() - started

    assert [item.runbook_id for item in result] == ["RB-DENSE"]
    assert elapsed < 0.5
    assert coordinator.last_diagnostics is primary.last_diagnostics
    assert shadow.called.wait(timeout=1)
    release.set()
    deadline = time.monotonic() + 2
    while not events and time.monotonic() < deadline:
        time.sleep(0.01)
    coordinator.close()
    assert events[0][0] == "runbook_rag_shadow_completed"
    assert events[0][1]["primary_runbook_ids"] == ["RB-DENSE"]
    assert events[0][1]["shadow_runbook_ids"] == ["RB-HYBRID"]
    assert events[0][1]["primary_filter_fields"] == [
        "tenant_id",
        "status",
        "environment",
    ]
    assert events[0][1]["shadow_filter_fields"] == [
        "tenant_id",
        "status",
        "environment",
    ]
    assert "question" not in events[0][1]


def test_shadow_sampling_zero_does_not_call_hybrid():
    shadow = _Retriever([_chunk("s", "RB-HYBRID")])
    coordinator = ShadowRetrievalCoordinator(
        _Retriever([_chunk("p", "RB-DENSE")]),
        shadow,
        sample_rate=0,
        queue_size=1,
        shutdown_timeout_seconds=1,
        sampler=lambda: 0.5,
    )

    result = coordinator.retrieve(RetrievalQuery("TASK_FAILED"))
    coordinator.close()

    assert [item.runbook_id for item in result] == ["RB-DENSE"]
    assert shadow.called.is_set() is False


def test_shadow_failure_is_isolated_and_sanitized():
    events = []
    coordinator = ShadowRetrievalCoordinator(
        _Retriever([_chunk("p", "RB-DENSE")]),
        _Retriever(error=RuntimeError("secret raw log must not escape")),
        sample_rate=1,
        queue_size=1,
        shutdown_timeout_seconds=1,
        sampler=lambda: 0,
        event_sink=lambda event, payload: events.append((event, payload)),
    )

    result = coordinator.retrieve(RetrievalQuery("password=private"))
    deadline = time.monotonic() + 2
    while not events and time.monotonic() < deadline:
        time.sleep(0.01)
    coordinator.close()

    assert [item.runbook_id for item in result] == ["RB-DENSE"]
    assert events[0][0] == "runbook_rag_shadow_failed"
    serialized = repr(events[0][1])
    assert "secret raw log" not in serialized
    assert "password" not in serialized
    assert events[0][1]["error_type"] == "RuntimeError"


def test_shadow_full_queue_drops_work_without_blocking_primary():
    release = threading.Event()
    events = []
    shadow = _Retriever([_chunk("s", "RB-HYBRID")], wait_on=release)
    coordinator = ShadowRetrievalCoordinator(
        _Retriever([_chunk("p", "RB-DENSE")]),
        shadow,
        sample_rate=1,
        queue_size=1,
        shutdown_timeout_seconds=1,
        sampler=lambda: 0,
        event_sink=lambda event, payload: events.append((event, payload)),
    )

    coordinator.retrieve(RetrievalQuery("first"))
    assert shadow.called.wait(timeout=1)
    coordinator.retrieve(RetrievalQuery("second"))
    coordinator.retrieve(RetrievalQuery("third"))

    assert any(event == "runbook_rag_shadow_dropped" for event, _ in events)
    release.set()
    coordinator.close()


def test_shadow_event_sink_failure_cannot_break_primary_or_kill_worker():
    shadow = _Retriever([_chunk("s", "RB-HYBRID")])

    def broken_sink(event, payload):
        raise RuntimeError("telemetry unavailable")

    coordinator = ShadowRetrievalCoordinator(
        _Retriever([_chunk("p", "RB-DENSE")]),
        shadow,
        sample_rate=1,
        queue_size=1,
        shutdown_timeout_seconds=1,
        sampler=lambda: 0,
        event_sink=broken_sink,
    )

    result = coordinator.retrieve(RetrievalQuery("TASK_FAILED"))
    assert shadow.called.wait(timeout=1)
    deadline = time.monotonic() + 1
    while not coordinator.is_alive and time.monotonic() < deadline:
        time.sleep(0.01)

    assert [item.runbook_id for item in result] == ["RB-DENSE"]
    assert coordinator.is_alive
    coordinator.close()
