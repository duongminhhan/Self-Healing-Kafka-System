from __future__ import annotations

import re
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
        search_limit = self._config.effective_fusion_limit
        candidates = self._store.search(
            query,
            limit=search_limit,
            score_threshold=self._config.effective_dense_score_threshold,
        )
        diagnostics = self.last_diagnostics
        candidate_count = len(candidates)
        if diagnostics is not None:
            diagnostics.candidate_chunk_count = candidate_count
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

        diversify = self._config.effective_diversification_enabled
        ranked, duplicate_count = _select_ranked_chunks(
            candidates,
            limit=self._config.effective_top_k,
            max_chunks_per_runbook=self._config.max_chunks_per_runbook,
            diversify=diversify,
        )
        requested_sections = _requested_sections(query.purpose)
        ranked, section_coverage_applied = _ensure_section_coverage(
            candidates,
            ranked,
            requested_sections=requested_sections,
            limit=self._config.effective_top_k,
            cover_all_selected_runbooks=False,
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
                    else "no_chunk_after_limits"
                )
        return selected

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

    # Anchors are semantic identifiers already validated by the planner.  Do
    # not mine the original natural-language text here: doing so would create
    # a hidden phrase-based routing path after the planner.
    anchors = {_canonical_anchor(value) for value in query.error_codes}
    anchors.discard("")
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

    return ordered, "accepted"

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


def _requested_sections(purpose: str | None) -> tuple[str, ...]:
    """Map validated guidance semantics to the runbook evidence needed.

    This is a business definition, not a natural-language classifier.  The
    semantic planner validates ``purpose`` before a RetrievalQuery is built.
    """
    if purpose == "remediation":
        return ("diagnostic_steps", "recovery_steps")
    if purpose == "diagnosis":
        return ("diagnostic_steps",)
    if purpose == "meaning":
        return ("symptoms",)
    return ()


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
