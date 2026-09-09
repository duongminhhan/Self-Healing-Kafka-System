from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from dataclasses import replace
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
        diagnostics = self.last_diagnostics
        candidate_count = len(candidates)
        if diagnostics is not None:
            diagnostics.candidate_chunk_count = candidate_count
        candidates = _filter_explicit_issue_matches(query, candidates)
        guard_filtered = candidate_count - len(candidates)
        evidence_filtered = False
        if self._config.effective_evidence_gate_enabled:
            candidates, evidence_reason = _apply_evidence_gate(
                query,
                candidates,
                min_score=self._config.effective_evidence_score_threshold,
                min_margin=self._config.evidence_min_margin,
                require_anchor_match=self._config.evidence_require_anchor_match,
            )
            if diagnostics is not None:
                diagnostics.evidence_gate_applied = True
                diagnostics.evidence_gate_passed = bool(candidates)
                diagnostics.evidence_gate_reason = evidence_reason
            evidence_filtered = not candidates

        ranked, duplicate_count = _select_ranked_chunks(
            candidates,
            limit=self._config.effective_top_k,
            max_chunks_per_runbook=self._config.max_chunks_per_runbook,
            diversify=self._config.effective_diversification_enabled,
        )
        selected: list[RetrievedChunk] = []
        used_chars = 0
        for item in ranked:
            # RRF scores are ranks, not cosine similarities. Channel-specific thresholds
            # are already applied inside each hybrid Prefetch.
            if (
                self._config.search_mode == "dense"
                and item.score < self._config.effective_dense_score_threshold
            ):
                continue
            remaining = self._config.max_context_chars - used_chars
            if remaining <= 0:
                break
            if len(item.text) > remaining:
                item = replace(item, text=item.text[:remaining])
            selected.append(item)
            used_chars += len(item.text)
            if len(selected) >= self._config.effective_top_k:
                break
        if diagnostics is not None:
            diagnostics.selected_chunk_count = len(selected)
            diagnostics.unique_runbook_count = len(
                {(item.runbook_id, item.version) for item in candidates}
            )
            diagnostics.selected_runbook_ids = tuple(
                dict.fromkeys(item.runbook_id for item in selected)
            )
            diagnostics.diversification_applied = (
                self._config.effective_diversification_enabled and bool(candidates)
            )
            diagnostics.duplicate_chunks_removed = duplicate_count
            if not selected and diagnostics.no_result_reason is None:
                diagnostics.no_result_reason = (
                    diagnostics.evidence_gate_reason
                    if evidence_filtered and diagnostics.evidence_gate_reason
                    else (
                        "explicit_issue_guard_filtered_all"
                        if guard_filtered and not candidates
                        else "no_chunk_after_limits"
                    )
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
    if not candidates:
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


_CONFIG_OR_EXCEPTION = re.compile(
    r"\b(?:[A-Za-z_$][A-Za-z0-9_$-]*\.)+[A-Za-z_$][A-Za-z0-9_$-]*\b"
)
_TECHNICAL_CODE = re.compile(
    r"\b(?:ORA-\d{5}|SQLSTATE-?[0-9A-Z]{5}|HTTP-?\d{3}|"
    r"[A-Z][A-Z0-9]+(?:_[A-Z0-9]+){1,6})\b",
    re.IGNORECASE,
)
_DOMAIN_EXCLUSION = re.compile(
    r"\b(?:không\s+phải|not|unrelated\s+to)\s+"
    r"(?:kafka(?:\s+connect)?|connector|schema\s+registry|oracle|jdbc|debezium)\b",
    re.IGNORECASE,
)
_DOMAIN_TERMS = {
    "kafka",
    "connect",
    "connector",
    "debezium",
    "oracle",
    "jdbc",
    "schema",
    "registry",
    "worker",
    "task",
    "source",
    "sink",
    "cdc",
    "logminer",
    "redo",
    "healing",
    "database",
    "converter",
    "offset",
    "runbook",
}


def _apply_evidence_gate(
    query: RetrievalQuery,
    candidates: list[RetrievedChunk],
    *,
    min_score: float,
    min_margin: float,
    require_anchor_match: bool,
) -> tuple[list[RetrievedChunk], str]:
    if not candidates:
        return [], "no_qdrant_candidates"
    if _DOMAIN_EXCLUSION.search(query.text):
        return [], "explicit_domain_exclusion"

    anchors = _technical_anchors(query)
    filtered = candidates
    if anchors and require_anchor_match:
        filtered = [item for item in candidates if _candidate_matches_anchor(item, anchors)]
        if not filtered:
            return [], "strong_anchor_not_found"

    ordered = sorted(filtered, key=lambda value: (-value.score, value.point_id))
    if min_score and ordered[0].score < min_score:
        return [], "score_below_evidence_threshold"
    if not anchors and min_margin:
        # Compare the strongest *runbooks*, not adjacent chunks. Two high-scoring
        # sections from the same runbook are corroborating evidence; treating their
        # small score gap as ambiguity creates false no-answer responses.
        best_runbook_scores: dict[tuple[str, int], float] = {}
        for item in ordered:
            identity = (item.runbook_id, item.version)
            best_runbook_scores.setdefault(identity, item.score)
        ranked_runbook_scores = sorted(best_runbook_scores.values(), reverse=True)
        if (
            len(ranked_runbook_scores) > 1
            and ranked_runbook_scores[0] - ranked_runbook_scores[1] < min_margin
        ):
            return [], "rank_margin_below_evidence_threshold"

    if not anchors and not _has_domain_context(query.text):
        query_terms = _lexical_terms(query.text)
        filtered = [
            item
            for item in ordered
            if len(query_terms & _candidate_terms(item)) >= min(2, len(query_terms))
        ]
        if not filtered:
            return [], "insufficient_domain_evidence"
        ordered = filtered
    return ordered, "accepted"


def _technical_anchors(query: RetrievalQuery) -> set[str]:
    values = set(query.error_codes)
    values.update(match.group(0) for match in _TECHNICAL_CODE.finditer(query.text))
    values.update(match.group(0) for match in _CONFIG_OR_EXCEPTION.finditer(query.text))
    return {_canonical_anchor(value) for value in values if _canonical_anchor(value)}


def _candidate_matches_anchor(item: RetrievedChunk, anchors: set[str]) -> bool:
    values = (
        *item.error_codes,
        *item.exception_classes,
        *item.config_keys,
        *item.error_signatures,
        item.title,
        item.text,
    )
    corpus = {_canonical_anchor(value) for value in values}
    return any(anchor and any(anchor in value for value in corpus) for anchor in anchors)


def _canonical_anchor(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).casefold())


def _has_domain_context(text: str) -> bool:
    terms = _lexical_terms(text)
    return bool(terms & _DOMAIN_TERMS)


def _candidate_terms(item: RetrievedChunk) -> set[str]:
    return _lexical_terms(
        " ".join(
            (
                item.title,
                item.connector_class,
                item.connector_type,
                item.connector_family,
                item.subsystem,
                *item.error_codes,
                *item.symptoms,
                *item.exception_classes,
                *item.config_keys,
                *item.error_signatures,
                *item.aliases,
                *item.user_phrases_vi,
                *item.user_phrases_en,
                item.text,
            )
        )
    )


def _select_ranked_chunks(
    candidates: list[RetrievedChunk],
    *,
    limit: int,
    max_chunks_per_runbook: int,
    diversify: bool,
) -> tuple[list[RetrievedChunk], int]:
    ordered = sorted(candidates, key=lambda value: (-value.score, value.point_id))
    unique: list[RetrievedChunk] = []
    seen_points: set[str] = set()
    for item in ordered:
        # A long section can legitimately be split into multiple chunks. Qdrant point
        # identity is the actual duplicate boundary; runbook caps below control context
        # concentration without silently dropping distinct section content.
        if item.point_id in seen_points:
            continue
        seen_points.add(item.point_id)
        unique.append(item)
    duplicate_count = len(ordered) - len(unique)
    if not diversify:
        return unique[:limit], duplicate_count

    groups: dict[tuple[str, int], list[RetrievedChunk]] = defaultdict(list)
    for item in unique:
        groups[(item.runbook_id, item.version)].append(item)
    ranked_groups = sorted(
        groups.items(),
        key=lambda value: (-value[1][0].score, value[0][0], value[0][1]),
    )
    result: list[RetrievedChunk] = []
    for chunk_rank in range(max_chunks_per_runbook):
        for _, chunks in ranked_groups:
            if chunk_rank < len(chunks):
                result.append(chunks[chunk_rank])
                if len(result) >= limit:
                    return result, duplicate_count
    return result, duplicate_count
