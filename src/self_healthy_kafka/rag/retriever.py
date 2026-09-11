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
        multi_domain = _requests_multi_domain_comparison(query)
        search_limit = self._config.effective_fusion_limit
        if multi_domain:
            search_limit = max(search_limit, self._config.effective_top_k * 8)
        candidates = self._store.search(
            query,
            limit=search_limit,
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

        diversify = (
            self._config.effective_diversification_enabled
            or multi_domain
        )
        ranked, duplicate_count = _select_ranked_chunks(
            candidates,
            limit=self._config.effective_top_k,
            max_chunks_per_runbook=self._config.max_chunks_per_runbook,
            diversify=diversify,
        )
        requested_sections = _requested_section_coverage(query)
        ranked, section_coverage_applied = _ensure_section_coverage(
            candidates,
            ranked,
            requested_sections=requested_sections,
            limit=self._config.effective_top_k,
            cover_all_selected_runbooks=multi_domain,
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
                diversify and bool(candidates)
            )
            diagnostics.duplicate_chunks_removed = duplicate_count
            diagnostics.requested_sections = requested_sections
            diagnostics.selected_sections = tuple(
                dict.fromkeys(item.section for item in selected)
            )
            diagnostics.section_coverage_applied = section_coverage_applied
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
    issue_concepts = _issue_concepts(query.text)
    named_domain_classes = {
        connector_class
        for phrase, connector_class in _RUNBOOK_DOMAIN_CLASSES.items()
        if phrase in _normalized_text(query.text)
    }

    matching_runbooks: set[str] = set()
    for candidate in candidates:
        if (
            len(named_domain_classes) > 1
            and candidate.connector_class not in named_domain_classes
        ):
            continue
        if issue_concepts:
            candidate_concepts = _issue_concepts(_candidate_concept_text(candidate))
            if issue_concepts & candidate_concepts:
                matching_runbooks.add(candidate.runbook_id)
            continue
        strong_terms = _lexical_terms(
            " ".join((candidate.title, candidate.connector_class, *candidate.error_codes))
        )
        body_terms = _lexical_terms(candidate.text)
        if issue_terms & strong_terms or len(issue_terms & body_terms) >= min(2, len(issue_terms)):
            matching_runbooks.add(candidate.runbook_id)
    return [item for item in candidates if item.runbook_id in matching_runbooks]


def _issue_concepts(text: str) -> set[str]:
    normalized = _normalized_text(text)
    concepts: set[str] = set()
    concept_phrases = {
        "authentication": (
            "xac thuc",
            "dang nhap",
            "authentication",
            "credential",
            "unauthorized",
            "logon",
            "login",
        ),
        "cancellation": (
            "huy thao tac",
            "operation cancel",
            "cancelled",
            "canceled",
        ),
        "logminer": ("logminer", "redo log", "archived log", "missing log"),
        "retry_exhausted": (
            "retry het",
            "het retry",
            "max retries",
            "max retry",
            "healing escalat",
        ),
    }
    for concept, phrases in concept_phrases.items():
        if any(phrase in normalized for phrase in phrases):
            concepts.add(concept)
    return concepts


def _candidate_concept_text(item: RetrievedChunk) -> str:
    return " ".join(
        (
            item.title,
            item.connector_class,
            item.connector_type,
            item.connector_family,
            item.subsystem,
            *item.error_codes,
            *item.symptoms,
            *item.exception_classes,
            *item.error_signatures,
            *item.aliases,
            *item.user_phrases_vi,
            *item.user_phrases_en,
        )
    )


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


_REMEDIATION_PHRASES = (
    "cach xu ly",
    "xu ly",
    "khac phuc",
    "cach sua",
    "lam gi",
    "huong dan",
    "remediation",
    "recover",
    "resolve",
    "troubleshoot",
    "fix",
)
_DETAIL_PHRASES = (
    "noi dung loi",
    "thong diep loi",
    "loi day du",
    "error message",
    "full error",
    "what does",
    "la gi",
)
_ESCALATION_PHRASES = (
    "retry het",
    "het retry",
    "max retries",
    "max retry",
    "escalat",
    "nang cap",
)
_KNOWLEDGE_LOOKUP_PHRASES = (
    "runbook nao",
    "runbook ap dung",
    "runbook phu hop",
    "which runbook",
)
_RUNBOOK_DOMAIN_CLASSES = {
    "oracle": "oracle",
    "jdbc": "jdbc",
    "kafka connect": "kafka-connect",
    "schema registry": "schema-registry",
    "network": "network",
}
_ALTERNATIVE_PHRASES = (" hay ", " hoac ", " or ", "chua biet", "giua")


def _requested_section_coverage(query: RetrievalQuery) -> tuple[str, ...]:
    normalized = _normalized_text(query.text)
    if any(phrase in normalized for phrase in _REMEDIATION_PHRASES):
        sections = ["diagnostic_steps", "recovery_steps"]
        if any(phrase in normalized for phrase in _ESCALATION_PHRASES):
            sections.append("escalation")
        return tuple(sections)
    if any(phrase in normalized for phrase in _KNOWLEDGE_LOOKUP_PHRASES):
        return ("diagnostic_steps",)
    if _technical_anchors(query) and any(
        phrase in normalized for phrase in _DETAIL_PHRASES
    ):
        return ("symptoms",)
    return ()


def _requests_multi_domain_comparison(query: RetrievalQuery) -> bool:
    normalized = f" {_normalized_text(query.text)} "
    domains = {phrase for phrase in _RUNBOOK_DOMAIN_CLASSES if phrase in normalized}
    return len(domains) > 1 and any(
        phrase in normalized for phrase in _ALTERNATIVE_PHRASES
    )


def _normalized_text(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value.casefold())
    ascii_text = "".join(
        character for character in decomposed if not unicodedata.combining(character)
    )
    return " ".join(re.findall(r"[a-z0-9]+", ascii_text))


def _ensure_section_coverage(
    candidates: list[RetrievedChunk],
    selected: list[RetrievedChunk],
    *,
    requested_sections: tuple[str, ...],
    limit: int,
    cover_all_selected_runbooks: bool,
) -> tuple[list[RetrievedChunk], bool]:
    """Keep intent-critical sections from the strongest retrieved runbook."""
    if not candidates or not selected or not requested_sections or limit <= 0:
        return selected, False

    strongest = min(candidates, key=lambda item: (-item.score, item.point_id))
    strongest_identity = (strongest.runbook_id, strongest.version)
    result = list(selected)
    original_positions = {item.point_id: index for index, item in enumerate(result)}
    changed = False
    identities = (
        list(dict.fromkeys((item.runbook_id, item.version) for item in result))
        if cover_all_selected_runbooks
        else [strongest_identity]
    )
    ordered_candidates = sorted(
        candidates, key=lambda value: (-value.score, value.point_id)
    )
    for identity in identities:
        identity_candidates = [
            item
            for item in ordered_candidates
            if (item.runbook_id, item.version) == identity
        ]
        for section in requested_sections:
            if any(
                item.section == section
                and (item.runbook_id, item.version) == identity
                for item in result
            ):
                continue
            replacement = next(
                (item for item in identity_candidates if item.section == section),
                None,
            )
            if replacement is None:
                continue
            if len(result) < limit:
                result.append(replacement)
                changed = True
                continue

            replace_index = next(
                (
                    index
                    for index in range(len(result) - 1, -1, -1)
                    if (result[index].runbook_id, result[index].version) == identity
                    and result[index].section not in requested_sections
                ),
                None,
            )
            if replace_index is None and not cover_all_selected_runbooks:
                replace_index = next(
                    (
                        index
                        for index in range(len(result) - 1, -1, -1)
                        if result[index].section not in requested_sections
                    ),
                    None,
                )
            if replace_index is None:
                continue
            result[replace_index] = replacement
            changed = True
    section_priority = {
        section: index for index, section in enumerate(requested_sections)
    }
    reordered = sorted(
        result,
        key=lambda item: (
            section_priority.get(item.section, len(section_priority)),
            original_positions.get(item.point_id, len(original_positions)),
        ),
    )
    changed = changed or [item.point_id for item in reordered] != [
        item.point_id for item in result
    ]
    return reordered[:limit], changed
