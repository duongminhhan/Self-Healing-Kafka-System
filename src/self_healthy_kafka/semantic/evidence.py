"""Evidence packets and generic grounded rendering for analytics answers."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Callable

from self_healthy_kafka.semantic.catalog import SEMANTIC_CATALOG
from self_healthy_kafka.semantic.planner import SemanticPlan
from self_healthy_kafka.semantic.presentation import PresentationFacts, SemanticResponseRenderer
from self_healthy_kafka.storage.common import json_safe
from self_healthy_kafka.webhook.analytics import QueryPlan

_PRESENTATION = SEMANTIC_CATALOG["presentation"]
_METRIC_SEMANTIC_NAMES = {
    "failure_count": "incident_count",
    "recovered_count": "recovered_incident_count",
    "open_count": "open_incident_count",
    "average_recovery_minutes": "average_recovery_minutes",
    "recovery_rate_percent": "recovery_rate",
}
_METRIC_LABELS = {
    query_metric: (
        _PRESENTATION["metrics"][semantic_metric]["label_vi"],
        _PRESENTATION["metrics"][semantic_metric]["unit_vi"],
    )
    for query_metric, semantic_metric in _METRIC_SEMANTIC_NAMES.items()
}
_DIMENSION_LABELS = {
    "connector_name": _PRESENTATION["subjects"]["connector"]["singular_vi"],
    "job_name": _PRESENTATION["subjects"]["root_connector"]["singular_vi"],
    "failure_code": _PRESENTATION["subjects"]["error"]["singular_vi"],
    "error_code": _PRESENTATION["subjects"]["error_code"]["singular_vi"],
    "final_outcome": _PRESENTATION["fields"]["outcome"]["label_vi"],
}
_DETAIL_FIELDS = {
    "error_message": "error_message",
    "connector_name": "connector",
    "job_name": "root_connector",
    "final_outcome": "outcome",
    "queue_status": "queue_status",
    "recovery_rate_numerator": "recovery_rate_numerator",
    "recovery_rate_denominator": "recovery_rate_denominator",
}
_DETAIL_LABELS = {
    query_field: _PRESENTATION["detail_fields"][catalog_field]
    for query_field, catalog_field in _DETAIL_FIELDS.items()
}
_TECHNICAL_IDENTIFIER = re.compile(r"\b(?:ORA-\d{5}|[A-Z][A-Z0-9]+(?:_[A-Z0-9]+){1,6})\b")


def build_evidence(
    facts: list[dict[str, Any]],
    *,
    query_plan: QueryPlan,
    semantic_plan: SemanticPlan,
    from_at: datetime | None,
    to_at: datetime | None,
    truncated: bool,
    source: str = "vConnectorIncidentFacts",
) -> list[dict[str, Any]]:
    """Attach stable fact identifiers and their exact semantic scope."""

    evidence: list[dict[str, Any]] = []
    for position, fact in enumerate(facts, start=1):
        dimensions = {
            _DIMENSION_LABELS[field]: json_safe(fact.get(field))
            for field in query_plan.group_by
            if fact.get(field) is not None
        }
        metrics = [
            {
                "name": metric.name,
                "label": _METRIC_LABELS[metric.name][0],
                "value": json_safe(fact.get(metric.name)),
                "unit": _METRIC_LABELS[metric.name][1],
                "aggregation": metric.aggregation,
            }
            for metric in query_plan.metrics
        ]
        details = {
            _DETAIL_LABELS[field]: json_safe(fact.get(field))
            for field in query_plan.details
            if fact.get(field) not in {None, ""}
        }
        detail_values = {
            field: json_safe(fact.get(field))
            for field in query_plan.details
            if fact.get(field) not in {None, ""}
        }
        # A percentage alone is not an interpretable recovery rate.  The
        # aggregate preserves its exact numerator and denominator so the
        # response model can state the population it is describing without
        # inventing one.
        if any(metric.name == "recovery_rate_percent" for metric in query_plan.metrics):
            for field in ("recovery_rate_numerator", "recovery_rate_denominator"):
                if fact.get(field) is not None:
                    details[_DETAIL_LABELS[field]] = json_safe(fact[field])
                    detail_values[field] = json_safe(fact[field])
        identity = {
            "dimensions": dimensions,
            "metrics": metrics,
            "evidence_ids": list(fact.get("evidence_ids") or []),
            "range": [from_at.isoformat() if from_at else None, to_at.isoformat() if to_at else None],
        }
        digest = hashlib.sha256(
            json.dumps(identity, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:20]
        evidence.append(
            {
                "fact_id": f"analytics:{digest}",
                "rank": int(fact.get("rank")) if isinstance(fact.get("rank"), int) and fact["rank"] > 0 else (position if query_plan.group_by else None),
                "tie_count": int(fact.get("tie_count")) if isinstance(fact.get("tie_count"), int) else None,
                "coverage": "boundary_tie_truncated" if fact.get("tie_truncated") else ("incomplete" if truncated else "complete"),
                "dimension": list(query_plan.group_by),
                "entity": dimensions,
                "metrics": metrics,
                "details": details,
                "detail_values": detail_values,
                "time_range": {
                    "from_at": from_at.isoformat() if from_at else None,
                    "to_at": to_at.isoformat() if to_at else None,
                    "timestamp": "failure_at",
                },
                "status": _status(fact),
                "grain": "aggregated connector incident facts" if query_plan.group_by else "aggregated connector incidents",
                "source": source,
                "evidence_ids": list(fact.get("evidence_ids") or []),
                "complete": not truncated,
                "semantic_catalog_version": semantic_plan.version,
            }
        )
    return evidence


def render_evidence(presentation: PresentationFacts) -> str:
    """A generic, data-driven renderer used only after response failure.

    It has no question or intent branches.  Every emitted business value comes
    directly from the evidence packet.
    """

    evidence = [dict(item) for item in presentation.summary_rows]
    if not presentation.query_executed or not presentation.evidence_complete or not evidence:
        raise ValueError("render_evidence requires complete, non-empty evidence")
    source = _PRESENTATION["sources"].get(presentation.source, presentation.source)
    scope = f"Trong {source} {presentation.time_scope},"
    first_text = _fact_text(
        evidence[0], include_rank=len(evidence) == 1, include_tie=False,
        details_limit=presentation.summary_detail_limit,
    )
    if len(evidence) == 1:
        lead = f"{scope} {first_text}."
    else:
        lines = [
            f"{index}. {_fact_text(item, include_rank=False, include_tie=False, details_limit=presentation.summary_detail_limit)}."
            for index, item in enumerate(evidence, start=1)
        ]
        lead = f"{scope}\n\n" + "\n".join(lines)
    notice = _summary_notice(presentation)
    if notice:
        lead += f"\n\n{notice}"
    return lead


class AnalyticsResponseComposer:
    """Natural-language response stage with typed fact claims and one correction."""

    def __init__(self, generate: Callable[..., dict[str, Any]] | None, *, max_tokens: int = 900):
        self._generate = generate
        self._max_tokens = max_tokens

    def compose(
        self,
        *,
        presentation: PresentationFacts,
    ) -> tuple[str, str, str | None, int, list[dict[str, Any]]]:
        evidence = [dict(item) for item in presentation.rows]
        if presentation.outcome != "verified_results":
            answer = SemanticResponseRenderer().render_outcome(presentation)
            return answer, "deterministic_outcome_renderer", presentation.safe_failure_reason, 0, []
        if not presentation.query_executed or not presentation.evidence_complete or not evidence:
            # The caller must create ``cannot_verify`` rather than ask this
            # renderer to infer an empty answer from absent evidence.
            raise ValueError("verified results require complete evidence")
        summary_evidence = [dict(item) for item in presentation.summary_rows]
        if self._generate is None:
            answer = render_evidence(presentation)
            return answer, "deterministic_evidence_renderer", "response_model_not_configured", 0, _deterministic_claims(summary_evidence)
        correction: str | None = None
        for attempt in range(1, 3):
            try:
                candidate = self._generate(
                    _messages(presentation, correction), max_tokens=self._max_tokens
                )
                answer, claims = _validate_response(candidate, summary_evidence)
                notice = _summary_notice(presentation)
                if notice:
                    answer = f"{answer.rstrip()}\n\n{notice}"
                return answer, "huggingface", None, attempt, claims
            except Exception as exc:
                correction = _error_label(exc)
        answer = render_evidence(presentation)
        return answer, "deterministic_evidence_renderer", f"grounding_failure:{correction}", 2, _deterministic_claims(summary_evidence)


def _messages(
    presentation: PresentationFacts,
    correction: str | None,
) -> list[dict[str, str]]:
    system = (
        "Return only JSON with keys answer and claims. Write concise natural Vietnamese in answer, using at most three short facts. "
        "Do not use SQL, raw logs, credentials, or hidden diagnostics. Each factual statement about an entity, "
        "metric, value, time, status, error code, or message must have one matching claim. A claim has exactly "
        "fact_id, entity, metric, value, time_range, status, and text. metric is either a metric name or "
        "detail:<field>. Copy entity, metric, value, time_range, and status from one evidence item exactly. "
        "claim.text must include the entity name where present, the Vietnamese metric/detail label, and the exact "
        "value. It may not negate a positive value. "
        "Use only presentation_facts.summary_rows; do not enumerate hidden rows or invent totals/ties. "
        "Do not state live connector health from historical data. Do not add remediation because this response "
        "stage is analytics only."
    )
    payload: dict[str, Any] = {
        "presentation_facts": presentation.to_dict(),
    }
    if correction:
        payload["validation_feedback"] = correction
        payload["instruction"] = "Return a corrected JSON response grounded only in the evidence."
    return [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}]


def _validate_response(candidate: dict[str, Any], evidence: list[dict[str, Any]]) -> tuple[str, list[dict[str, Any]]]:
    if not isinstance(candidate, dict):
        raise ValueError("response is not an object")
    answer = candidate.get("answer")
    claims = candidate.get("claims")
    if not isinstance(answer, str) or not answer.strip() or len(answer.strip()) > 8_000:
        raise ValueError("answer is invalid")
    if _contains_empty_conclusion(answer):
        # The response model only runs for ``verified_results``.  A negative
        # conclusion belongs exclusively to the deterministic verified-empty
        # renderer, whose execution invariant is checked by the caller.
        raise ValueError("answer contains an unsupported negative conclusion")
    if not isinstance(claims, list) or not claims:
        raise ValueError("claims are required")
    by_id = {str(item["fact_id"]): item for item in evidence}
    claim_texts: list[str] = []
    validated_claims: list[dict[str, Any]] = []
    seen_claims: set[tuple[str, str]] = set()
    for claim in claims:
        if not isinstance(claim, dict) or set(claim) != {
            "fact_id", "entity", "metric", "value", "time_range", "status", "text"
        }:
            raise ValueError("claim contract is invalid")
        fact = by_id.get(str(claim["fact_id"]))
        if fact is None:
            raise ValueError("claim references unknown evidence")
        if claim["entity"] != fact["entity"] or claim["time_range"] != fact["time_range"] or claim["status"] != fact["status"]:
            raise ValueError("claim scope does not match evidence")
        matching_metrics = [
            metric for metric in fact["metrics"]
            if metric["name"] == claim["metric"] and metric["value"] == claim["value"]
        ]
        detail_field = str(claim["metric"])[7:] if str(claim["metric"]).startswith("detail:") else None
        matching_detail = (
            detail_field is not None
            and fact.get("detail_values", {}).get(detail_field) == claim["value"]
        )
        if not matching_metrics and not matching_detail:
            raise ValueError("claim metric/value does not match evidence")
        identity = (str(claim["fact_id"]), str(claim["metric"]))
        if identity in seen_claims:
            raise ValueError("claim is duplicated")
        seen_claims.add(identity)
        text = claim["text"]
        if not isinstance(text, str) or not text.strip() or len(text) > 1_200:
            raise ValueError("claim text is invalid")
        _validate_claim_text(text, fact, matching_metrics[0] if matching_metrics else None, detail_field)
        claim_texts.append(text.strip())
        validated_claims.append({
            "fact_id": fact["fact_id"],
            "entity": fact["entity"],
            "metric": claim["metric"],
            "value": claim["value"],
            "time_range": fact["time_range"],
            "status": fact["status"],
            "text": text.strip(),
        })
    # The prose must surface every validated claim. This does not attempt to
    # interpret arbitrary Vietnamese prose; it prevents a detached answer that
    # silently omits the typed claim contract.
    normalized_answer = _canonical(answer)
    if any(_canonical(text) not in normalized_answer for text in claim_texts):
        raise ValueError("answer omits a validated claim")
    _validate_answer_identifiers(answer, evidence)
    return answer.strip(), validated_claims


def _validate_claim_text(
    text: str,
    fact: dict[str, Any],
    metric: dict[str, Any] | None,
    detail_field: str | None,
) -> None:
    corpus = _canonical(json.dumps({"entity": fact["entity"], "metrics": fact["metrics"], "details": fact["details"]}, ensure_ascii=False))
    for identifier in _TECHNICAL_IDENTIFIER.findall(text.upper()):
        if _canonical(identifier) not in corpus:
            raise ValueError("claim has an unsupported technical identifier")
    normalized = _canonical(text)
    for value in fact["entity"].values():
        if isinstance(value, str) and value and _canonical(value) not in normalized:
            raise ValueError("claim text omits its entity")
    if metric is not None:
        label = _canonical(str(metric["label"]))
        if label not in normalized or not _value_in_text(metric["value"], normalized):
            raise ValueError("claim text does not bind metric label and value")
        if _is_positive_number(metric["value"]) and _contains_negative_claim(text):
            raise ValueError("claim text negates a positive metric")
    elif detail_field is not None:
        label = _canonical(_DETAIL_LABELS[detail_field])
        value = fact.get("detail_values", {}).get(detail_field)
        if label not in normalized or not _value_in_text(value, normalized):
            raise ValueError("claim text does not bind detail label and value")


def _deterministic_claims(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    claims: list[dict[str, Any]] = []
    for fact in evidence:
        fact_text = _fact_text(fact)
        for metric in fact["metrics"]:
            claims.append({
                "fact_id": fact["fact_id"],
                "entity": fact["entity"],
                "metric": metric["name"],
                "value": metric["value"],
                "time_range": fact["time_range"],
                "status": fact["status"],
                "text": fact_text,
                "rank": fact.get("rank"),
                "tie_count": fact.get("tie_count"),
            })
        for field, value in fact.get("detail_values", {}).items():
            claims.append({
                "fact_id": fact["fact_id"],
                "entity": fact["entity"],
                "metric": f"detail:{field}",
                "value": value,
                "time_range": fact["time_range"],
                "status": fact["status"],
                "text": f"{_DETAIL_LABELS[field]}: {_display_value(value)}.",
                "rank": fact.get("rank"),
                "tie_count": fact.get("tie_count"),
            })
    return claims


def _validate_answer_identifiers(answer: str, evidence: list[dict[str, Any]]) -> None:
    corpus = _canonical(json.dumps(evidence, ensure_ascii=False))
    for identifier in _TECHNICAL_IDENTIFIER.findall(answer.upper()):
        if _canonical(identifier) not in corpus:
            raise ValueError("answer has an unsupported technical identifier")


def _fact_text(
    fact: dict[str, Any], *, include_rank: bool = True, include_tie: bool = True,
    details_limit: int = 0,
) -> str:
    entity = fact["entity"]
    entity_text = ", ".join(f"{label} {value}" for label, value in entity.items())
    metrics = []
    for metric in fact["metrics"]:
        value = metric["value"]
        if value is None:
            metrics.append(f"{metric['label']} chưa thể tính từ dữ liệu hợp lệ")
        elif metric["unit"] == "phút":
            metrics.append(f"{metric['label']} là {value} phút")
        else:
            metrics.append(f"{metric['label']} là {value}")
    prefix = entity_text or "Toàn bộ phạm vi"
    rank = fact.get("rank")
    tie_count = fact.get("tie_count")
    rank_prefix = f"Hạng {rank}: " if include_rank and isinstance(rank, int) and rank > 0 else ""
    tie_suffix = (
        f" (đồng hạng với {tie_count - 1} kết quả khác)"
        if include_tie and isinstance(tie_count, int) and tie_count > 1 else ""
    )
    details = [
        f"{label}: {_display_value(value)}"
        for label, value in fact.get("details", {}).items()
    ][:max(0, details_limit)]
    detail_suffix = f"; {'; '.join(details)}" if details else ""
    return f"{rank_prefix}{prefix} có {', '.join(metrics)}{tie_suffix}{detail_suffix}"


def _summary_notice(presentation: PresentationFacts) -> str | None:
    if not presentation.has_more_verified_results or not presentation.detail_accessible:
        return None
    policy = _PRESENTATION["summary_policy"]
    if presentation.boundary_tie_truncated and presentation.boundary_tie_count:
        summary_rows = presentation.summary_rows
        if summary_rows:
            shown_at_boundary = sum(
                1 for item in summary_rows
                if item.get("rank") == summary_rows[-1].get("rank")
            )
            tied_remaining = presentation.boundary_tie_count - shown_at_boundary
            if tied_remaining > 0:
                return str(policy["boundary_tie_vi"]).format(
                    count=tied_remaining, rank=summary_rows[-1].get("rank")
                )
    return str(policy["more_results_vi"]).format(count=presentation.remaining_count)


def _detail_lines(evidence: list[dict[str, Any]]) -> list[str]:
    result: list[str] = []
    seen: set[tuple[str, str]] = set()
    for fact in evidence:
        for label, value in fact["details"].items():
            display = _display_value(value)
            pair = (label, display)
            if pair not in seen:
                seen.add(pair)
                result.append(f"{label}: {display}.")
    return result[:5]


def _status(fact: dict[str, Any]) -> dict[str, Any] | None:
    values = {
        key: fact.get(key)
        for key in ("final_outcome", "queue_status")
        if fact.get(key) is not None
    }
    return values or None


def _canonical(value: str) -> str:
    return "".join(character for character in value.casefold() if character.isalnum())


def _display_value(value: Any) -> str:
    if isinstance(value, list):
        return "; ".join(_display_value(item) for item in value)
    return str(value)


def _value_in_text(value: Any, normalized_text: str) -> bool:
    if value is None:
        return any(token in normalized_text for token in ("chuathletinh", "khongthetinh", "null"))
    if isinstance(value, list):
        return all(_value_in_text(item, normalized_text) for item in value)
    text_value = _canonical(_display_value(value))
    if text_value in normalized_text:
        return True
    if isinstance(value, float) and value.is_integer():
        return _canonical(str(int(value))) in normalized_text
    return False


def _is_positive_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def _contains_negative_claim(text: str) -> bool:
    return bool(re.search(r"\b(?:không\s+có|không\s+ghi\s+nhận|không\s+hề|no|none|zero)\b", text, re.I))


def _contains_empty_conclusion(text: str) -> bool:
    return bool(re.search(
        r"\b(?:không\s+có|chưa\s+ghi\s+nhận|không\s+tìm\s+thấy|no\s+(?:connector|incident|result)|none)\b",
        text,
        re.I,
    ))


def _error_label(exc: Exception) -> str:
    return type(exc).__name__
