from __future__ import annotations

import re
import unicodedata
from typing import Protocol

from self_healthy_kafka.config import RagConfig
from self_healthy_kafka.rag.models import RetrievalQuery, RetrievedChunk, SearchDiagnostics


class SearchStore(Protocol):
    def search(
        self, query: RetrievalQuery, *, limit: int, score_threshold: float
    ) -> list[RetrievedChunk]: ...


class RunbookRetriever:
    def __init__(self, config: RagConfig, store: SearchStore):
        self._config = config
        self._store = store

    @property
    def last_diagnostics(self) -> SearchDiagnostics | None:
        value = getattr(self._store, "last_search_diagnostics", None)
        return value if isinstance(value, SearchDiagnostics) else None

    def retrieve(self, query: RetrievalQuery) -> list[RetrievedChunk]:
        candidates = self._store.search(
            query,
            limit=self._config.effective_fusion_limit,
            score_threshold=self._config.effective_dense_score_threshold,
        )
        pre_guard_count = len(candidates)
        candidates = _filter_explicit_issue_matches(query, candidates)
        selected: list[RetrievedChunk] = []
        seen: set[tuple[str, int, str]] = set()
        used_chars = 0
        for item in sorted(candidates, key=lambda value: (-value.score, value.point_id)):
            # RRF scores are ranks, not cosine similarities. Channel-specific thresholds
            # are already applied inside each hybrid Prefetch.
            if (
                self._config.search_mode == "dense"
                and item.score < self._config.effective_dense_score_threshold
            ):
                continue
            identity = (item.runbook_id, item.version, item.section)
            if identity in seen:
                continue
            remaining = self._config.max_context_chars - used_chars
            if remaining <= 0:
                break
            if len(item.text) > remaining:
                item = RetrievedChunk(**{**item.__dict__, "text": item.text[:remaining]})
            selected.append(item)
            seen.add(identity)
            used_chars += len(item.text)
            if len(selected) >= self._config.top_k:
                break
        diagnostics = self.last_diagnostics
        if diagnostics is not None:
            diagnostics.selected_chunk_count = len(selected)
            if not selected and diagnostics.no_result_reason is None:
                diagnostics.no_result_reason = (
                    "explicit_issue_guard_filtered_all"
                    if pre_guard_count and not candidates
                    else "no_chunk_after_limits"
                )
        return selected


_EXPLICIT_ISSUE_LOOKUP = re.compile(
    r"(?:\brunbook\b|\bhướng\s+dẫn\b|\bcách\s+(?:xử\s+lý|khắc\s+phục)\b)"
    r"[^?\n]{0,160}?\b(?:lỗi|error|incident|sự\s+cố)\b(?P<issue>[^?\n]+)",
    re.IGNORECASE,
)
_ISSUE_TRAILER = re.compile(
    r"\b(?:của\s+(?:connector|kết\s+nối)|là\s+gì|thì\s+làm\s+gì|"
    r"nên\s+làm\s+gì|what\s+is|how\s+to)\b.*$",
    re.IGNORECASE,
)
_LOOKUP_STOPWORDS = {
    "runbook",
    "huong",
    "dan",
    "cach",
    "xu",
    "ly",
    "khac",
    "phuc",
    "loi",
    "error",
    "incident",
    "connector",
    "ket",
    "noi",
    "cho",
    "cua",
    "la",
    "gi",
    "nao",
    "mot",
    "the",
    "thi",
    "lam",
    "nen",
    "dang",
    "bi",
    "toi",
    "can",
    "co",
    "khong",
    "va",
    "voi",
    "khi",
    "trong",
    "truong",
    "hop",
    "for",
    "what",
    "which",
    "how",
    "the",
    "this",
    "that",
    "from",
    "with",
    "into",
    "issue",
    "problem",
    "guide",
    "handle",
    "fix",
}


def _filter_explicit_issue_matches(
    query: RetrievalQuery,
    candidates: list[RetrievedChunk],
) -> list[RetrievedChunk]:
    """Reject vector-only matches for an explicit, otherwise unscoped issue lookup."""
    if not candidates or query.error_codes or query.connector_class:
        return candidates
    issue_terms = _explicit_issue_terms(query.text)
    if not issue_terms:
        return candidates

    matching_runbooks: set[str] = set()
    for candidate in candidates:
        strong_terms = _lexical_terms(
            " ".join((candidate.title, candidate.connector_class, *candidate.error_codes))
        )
        body_terms = _lexical_terms(candidate.text)
        if issue_terms & strong_terms or len(issue_terms & body_terms) >= min(2, len(issue_terms)):
            matching_runbooks.add(candidate.runbook_id)
    return [item for item in candidates if item.runbook_id in matching_runbooks]


def _explicit_issue_terms(text: str) -> set[str]:
    match = _EXPLICIT_ISSUE_LOOKUP.search(text)
    if not match:
        return set()
    issue = _ISSUE_TRAILER.sub("", match.group("issue")).strip(" .,:;-'\"")
    return _lexical_terms(issue)


def _lexical_terms(text: str) -> set[str]:
    normalized = "".join(
        character
        for character in unicodedata.normalize("NFKD", text.casefold())
        if not unicodedata.combining(character)
    )
    return {
        token
        for token in re.findall(r"[a-z0-9]+", normalized)
        if len(token) >= 3 and token not in _LOOKUP_STOPWORDS
    }
