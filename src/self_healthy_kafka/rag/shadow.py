from __future__ import annotations

import logging
import queue
import random
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from self_healthy_kafka.rag.models import RetrievalQuery, RetrievedChunk, SearchDiagnostics

logger = logging.getLogger(__name__)


class Retriever(Protocol):
    @property
    def last_diagnostics(self) -> SearchDiagnostics | None: ...

    def retrieve(self, query: RetrievalQuery) -> list[RetrievedChunk]: ...


EventSink = Callable[[str, dict[str, Any]], None]
Sampler = Callable[[], float]


@dataclass(frozen=True)
class _ShadowJob:
    query: RetrievalQuery
    primary_runbook_ids: tuple[str, ...]
    primary_latency_ms: float | None
    primary_filter_fields: tuple[str, ...]


class ShadowRetrievalCoordinator:
    """Return dense retrieval immediately and compare sampled hybrid results off-thread.

    This class deliberately exposes the same small interface as ``RunbookRetriever`` so
    the answer workflow remains unaware of shadow traffic. It never invokes the answer
    model. Shadow jobs are best-effort: a full queue, a Qdrant error, or shutdown cannot
    change the primary chunks returned to the caller.
    """

    def __init__(
        self,
        primary: Retriever,
        shadow: Retriever,
        *,
        sample_rate: float,
        queue_size: int,
        shutdown_timeout_seconds: float,
        sampler: Sampler = random.random,
        event_sink: EventSink | None = None,
    ) -> None:
        if not 0 <= sample_rate <= 1:
            raise ValueError("shadow sample_rate must be between 0 and 1")
        if queue_size < 1:
            raise ValueError("shadow queue_size must be positive")
        if shutdown_timeout_seconds <= 0:
            raise ValueError("shadow shutdown timeout must be positive")
        self._primary = primary
        self._shadow = shadow
        self._sample_rate = sample_rate
        self._sampler = sampler
        self._event_sink = event_sink
        self._shutdown_timeout_seconds = shutdown_timeout_seconds
        self._queue: queue.Queue[_ShadowJob] = queue.Queue(maxsize=queue_size)
        self._stopping = threading.Event()
        self._local = threading.local()
        self._worker = threading.Thread(
            target=self._run,
            name="runbook-rag-shadow",
            daemon=True,
        )
        self._worker.start()

    @property
    def last_diagnostics(self) -> SearchDiagnostics | None:
        """Return diagnostics captured for the current primary request thread."""

        value = getattr(self._local, "last_diagnostics", None)
        return value if isinstance(value, SearchDiagnostics) else None

    @property
    def is_alive(self) -> bool:
        return self._worker.is_alive()

    def retrieve(self, query: RetrievalQuery) -> list[RetrievedChunk]:
        started = time.perf_counter()
        chunks = self._primary.retrieve(query)
        diagnostics = self._primary.last_diagnostics
        self._local.last_diagnostics = diagnostics
        primary_latency_ms = _diagnostic_latency(diagnostics, started)

        if self._stopping.is_set() or self._sampler() >= self._sample_rate:
            return chunks

        job = _ShadowJob(
            query=query,
            primary_runbook_ids=_unique_runbook_ids(chunks),
            primary_latency_ms=primary_latency_ms,
            primary_filter_fields=_filter_fields(diagnostics),
        )
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            self._emit(
                "runbook_rag_shadow_dropped",
                {
                    "reason": "queue_full",
                    "primary_runbook_ids": list(job.primary_runbook_ids),
                    "primary_filter_fields": list(job.primary_filter_fields),
                },
                level=logging.WARNING,
            )
        return chunks

    def close(self) -> None:
        """Stop accepting jobs and wait only for the configured bounded interval."""

        if self._stopping.is_set():
            return
        self._stopping.set()
        self._worker.join(timeout=self._shutdown_timeout_seconds)
        if self._worker.is_alive():
            self._emit(
                "runbook_rag_shadow_shutdown_timeout",
                {"reason": "worker_busy"},
                level=logging.WARNING,
            )

    def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                job = self._queue.get(timeout=0.1)
            except queue.Empty:
                continue
            try:
                if not self._stopping.is_set():
                    self._run_job(job)
            finally:
                self._queue.task_done()

    def _run_job(self, job: _ShadowJob) -> None:
        started = time.perf_counter()
        try:
            chunks = self._shadow.retrieve(job.query)
            diagnostics = self._shadow.last_diagnostics
            shadow_latency_ms = _diagnostic_latency(diagnostics, started)
            shadow_ids = _unique_runbook_ids(chunks)
            self._emit(
                "runbook_rag_shadow_completed",
                {
                    "primary_runbook_ids": list(job.primary_runbook_ids),
                    "shadow_runbook_ids": list(shadow_ids),
                    "rank_changes": _rank_changes(job.primary_runbook_ids, shadow_ids),
                    "primary_latency_ms": job.primary_latency_ms,
                    "shadow_latency_ms": shadow_latency_ms,
                    "primary_no_answer": not job.primary_runbook_ids,
                    "shadow_no_answer": not shadow_ids,
                    "primary_filter_fields": list(job.primary_filter_fields),
                    "shadow_filter_fields": list(_filter_fields(diagnostics)),
                    "shadow_fallback_reason": _safe_reason(diagnostics),
                },
            )
        except Exception as exc:  # shadow failures must never affect the primary request
            self._emit(
                "runbook_rag_shadow_failed",
                {
                    "primary_runbook_ids": list(job.primary_runbook_ids),
                    "error_type": type(exc).__name__,
                    "shadow_latency_ms": round((time.perf_counter() - started) * 1000, 3),
                },
                level=logging.WARNING,
            )

    def _emit(
        self,
        event: str,
        values: dict[str, Any],
        *,
        level: int = logging.INFO,
    ) -> None:
        payload = {"event": event, **values}
        if self._event_sink is not None:
            try:
                self._event_sink(event, payload)
            except Exception:  # telemetry must never affect retrieval or kill the worker
                logger.exception(
                    "Runbook RAG shadow event sink failed",
                    extra={"event": "runbook_rag_shadow_event_sink_failed"},
                )
            return
        logger.log(level, "Runbook RAG shadow retrieval event", extra=payload)


def _unique_runbook_ids(chunks: list[RetrievedChunk]) -> tuple[str, ...]:
    result: list[str] = []
    for item in chunks:
        runbook_id = _safe_runbook_id(item.runbook_id)
        if runbook_id and runbook_id not in result:
            result.append(runbook_id)
    return tuple(result)


def _safe_runbook_id(value: str) -> str:
    """Keep telemetry bounded and free of arbitrary payload text."""

    return re.sub(r"[^A-Za-z0-9_.:-]", "_", str(value))[:128]


def _diagnostic_latency(
    diagnostics: SearchDiagnostics | None,
    started: float,
) -> float:
    if diagnostics is not None and diagnostics.total_retrieval_latency_ms:
        return round(float(diagnostics.total_retrieval_latency_ms), 3)
    return round((time.perf_counter() - started) * 1000, 3)


def _safe_reason(diagnostics: SearchDiagnostics | None) -> str | None:
    if diagnostics is None:
        return None
    reason = (
        diagnostics.fallback_reason
        or diagnostics.fusion_fallback_reason
        or diagnostics.no_result_reason
    )
    return re.sub(r"[^A-Za-z0-9_.:-]", "_", reason)[:160] if reason else None


def _filter_fields(diagnostics: SearchDiagnostics | None) -> tuple[str, ...]:
    if diagnostics is None:
        return ()
    return tuple(
        field
        for field in diagnostics.payload_filter_fields
        if field in {"tenant_id", "status", "environment", "connector_taxonomy", "error_codes"}
    )


def _rank_changes(
    primary: tuple[str, ...],
    shadow: tuple[str, ...],
) -> list[dict[str, int | str | None]]:
    primary_ranks = {value: index for index, value in enumerate(primary, 1)}
    shadow_ranks = {value: index for index, value in enumerate(shadow, 1)}
    ordered = list(dict.fromkeys((*primary, *shadow)))
    return [
        {
            "runbook_id": value,
            "primary_rank": primary_ranks.get(value),
            "shadow_rank": shadow_ranks.get(value),
        }
        for value in ordered
        if primary_ranks.get(value) != shadow_ranks.get(value)
    ]
