"""Validated semantic plans and the compiler to the safe analytics DSL."""

from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass, replace
from typing import Any, Callable

import httpx

from self_healthy_kafka.rag.models import Route
from self_healthy_kafka.semantic.catalog import CATALOG_VERSION, SEMANTIC_CATALOG, catalog_for_model
from self_healthy_kafka.webhook.analytics import QueryPlan, parse_plan, parse_time_range


class SemanticPlanError(ValueError):
    """A planner response is malformed or asks for an unsupported capability."""

    def __init__(self, message: str, *, diagnostics: dict[str, Any] | None = None):
        super().__init__(message)
        self.diagnostics = diagnostics or {}


_METRICS = {
    "incident_count": ("failure_count", "count_distinct_incident"),
    "recovered_incident_count": ("recovered_count", "count_distinct_incident"),
    "open_incident_count": ("open_count", "count_distinct_incident"),
    "average_recovery_minutes": ("average_recovery_minutes", "average_recovery_minutes"),
    "recovery_rate": ("recovery_rate_percent", "recovery_rate_percent"),
}
_DIMENSIONS = {
    "connector": "connector_name",
    "root_connector": "job_name",
    "error": "failure_code",
    "error_code": "error_code",
    "outcome": "final_outcome",
}
_DETAILS = {
    "error_message": "error_message",
    "connector": "connector_name",
    "root_connector": "job_name",
    "outcome": "final_outcome",
    "queue_status": "queue_status",
}
_TOP_LEVEL = {
    "version", "data_request", "guidance_request", "clarification",
    "conversation_action", "inherited_fields",
}
_INTENTS = {
    "failed_connectors",
    "incidents",
    "top_error_signature",
    "recovery_rate",
}
_SUBJECT_DIMENSIONS = {
    "root_connector": {"root_connector"},
    "current_connector": {"connector"},
    "error_signature": {"error", "error_code"},
    "incident": set(),
}
_SUBJECT_ALIASES = {
    "error": "error_signature",
    "error_code": "error_signature",
}
_TIME_SCOPE_ORIGINS = {"explicit", "inherited", "default", "unspecified"}


@dataclass(frozen=True)
class SemanticCueContract:
    """Explicit semantic constraints recovered from a user question.

    This is a bounded vocabulary check, not a second planner and not a source
    of answers or SQL.  It lets the backend reject a model output that changes
    an entity or time scope the user actually supplied.
    """

    subject: str | None
    metric: str | None
    ranking: str | None
    limit: int | None
    include_ties_requested: bool
    time_scope: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject": self.subject,
            "metric": self.metric,
            "ranking": self.ranking,
            "limit": self.limit,
            "include_ties_requested": self.include_ties_requested,
            "time_scope": dict(self.time_scope) if self.time_scope else None,
            "time_scope_origin": "explicit" if self.time_scope else "unspecified",
        }


@dataclass(frozen=True)
class GuidanceRequest:
    needed: bool
    purpose: str | None = None
    error_codes: tuple[str, ...] = ()
    connector_class: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "needed": self.needed,
            "purpose": self.purpose,
            "error_codes": list(self.error_codes),
            "connector_class": self.connector_class,
        }


@dataclass(frozen=True)
class SemanticPlan:
    """Provider-independent, business-level request plan.

    ``route`` is deliberately derived by this backend property; it is not an
    accepted model field.
    """

    version: str
    data_request: dict[str, Any] | None
    guidance_request: GuidanceRequest
    clarification: str | None
    conversation_action: str
    inherited_fields: tuple[str, ...] = ()
    semantic_enforced: bool = False
    time_scope_resolution: dict[str, Any] | None = None

    @property
    def route(self) -> Route | None:
        if self.clarification or self.conversation_action == "clear_context":
            return None
        if self.data_request and self.guidance_request.needed:
            return Route.COMBINED
        if self.data_request:
            return Route.ANALYTICS
        if self.guidance_request.needed:
            return Route.RUNBOOK
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "data_request": self.data_request,
            "guidance_request": self.guidance_request.to_dict(),
            "clarification": self.clarification,
            "conversation_action": self.conversation_action,
            "inherited_fields": list(self.inherited_fields),
            "derived_route": self.route.value if self.route else None,
            "time_scope_resolution": self.time_scope_resolution,
        }

    def context(self) -> dict[str, Any]:
        """Bounded state suitable to send to a later planner call."""

        return {
            "previous_plan": self.to_dict(),
            "inheritance_rule": "Reuse a prior field only when the current question explicitly refers to it. Do not carry a filter into a new topic.",
        }


def parse_semantic_plan(value: object) -> SemanticPlan:
    if not isinstance(value, dict):
        raise SemanticPlanError("planner output must be a JSON object")
    if not set(value) <= _TOP_LEVEL:
        raise SemanticPlanError("planner output contains an unsupported field")
    if str(value.get("version") or "") != CATALOG_VERSION:
        raise SemanticPlanError("planner catalog version is unsupported")
    action = str(value.get("conversation_action") or "none")
    if action not in {"none", "clear_context"}:
        raise SemanticPlanError("conversation_action is unsupported")
    clarification = value.get("clarification")
    if clarification is not None:
        if not isinstance(clarification, str) or not clarification.strip() or len(clarification) > 600:
            raise SemanticPlanError("clarification must be bounded text")
        clarification = clarification.strip()
    inherited = value.get("inherited_fields") or []
    if not isinstance(inherited, list) or not all(
        isinstance(item, str) and item in {"metrics", "dimensions", "filters", "time_scope", "detail_fields"}
        for item in inherited
    ):
        raise SemanticPlanError("inherited_fields is unsupported")
    data_request = _parse_data_request(value.get("data_request"))
    guidance = _parse_guidance(value.get("guidance_request"))
    if action == "clear_context":
        if data_request or guidance.needed or clarification:
            raise SemanticPlanError("clear_context cannot contain a data, guidance, or clarification request")
    elif clarification:
        if data_request or guidance.needed:
            raise SemanticPlanError("clarification cannot be combined with an executable request")
    elif not data_request and not guidance.needed:
        raise SemanticPlanError("planner must request data, guidance, or clarification")
    return SemanticPlan(
        version=CATALOG_VERSION,
        data_request=data_request,
        guidance_request=guidance,
        clarification=clarification,
        conversation_action=action,
        inherited_fields=tuple(dict.fromkeys(inherited)),
    )


def _parse_data_request(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise SemanticPlanError("data_request must be an object")
    allowed = {
        "intent", "subject", "metric", "ranking", "time_scope", "time_scope_origin",
        "metrics", "dimensions", "filters", "sort", "limit", "comparison", "detail_fields", "tie_policy",
    }
    if not set(value) <= allowed:
        raise SemanticPlanError("data_request contains an unsupported field")
    metrics = value.get("metrics")
    if not isinstance(metrics, list) or not metrics or len(metrics) > 4:
        raise SemanticPlanError("data_request.metrics must contain one to four supported metrics")
    metric_names: list[str] = []
    for metric in metrics:
        if not isinstance(metric, str) or metric not in _METRICS:
            raise SemanticPlanError("data_request contains an unsupported metric")
        if metric not in metric_names:
            metric_names.append(metric)
    dimensions = value.get("dimensions") or []
    if not isinstance(dimensions, list) or len(dimensions) > 4 or not all(
        isinstance(item, str) and item in _DIMENSIONS for item in dimensions
    ):
        raise SemanticPlanError("data_request.dimensions contains an unsupported dimension")
    filters = dict(value.get("filters") or {})
    if not isinstance(filters, dict) or not set(filters) <= {
        "time_range", "event_type", "outcome", "connector", "error_code"
    }:
        raise SemanticPlanError("data_request.filters contains an unsupported filter")
    _validate_filters(filters)
    if filters.get("time_range") is not None:
        filters["time_range"] = _canonical_time_scope(filters["time_range"])
    # Incident-grain analytics always means confirmed failures.  This is a
    # catalog rule, not an interpretation of wording supplied by a user.
    filters.setdefault("event_type", ["HEALTH_FAILED_CONFIRMED"])
    sort = value.get("sort")
    if sort is None:
        sort = {"metric": metric_names[0], "direction": "desc"}
    if not isinstance(sort, dict) or set(sort) != {"metric", "direction"}:
        raise SemanticPlanError("data_request.sort must contain metric and direction")
    if sort.get("metric") not in metric_names or sort.get("direction") not in {"asc", "desc"}:
        raise SemanticPlanError("data_request.sort is unsupported")
    limit = value.get("limit", 20)
    if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
        raise SemanticPlanError("data_request.limit must be between 1 and 100")
    comparison = value.get("comparison")
    if comparison not in {None, "previous_period"}:
        raise SemanticPlanError("data_request.comparison is unsupported")
    details = value.get("detail_fields") or []
    if not isinstance(details, list) or len(details) > 5 or not all(
        isinstance(item, str) and item in _DETAILS for item in details
    ):
        raise SemanticPlanError("data_request.detail_fields contains an unsupported field")
    time_scope = value.get("time_scope", filters.get("time_range"))
    if time_scope is not None:
        _validate_filters({"time_range": time_scope})
        time_scope = _canonical_time_scope(time_scope)
        if filters.get("time_range") is not None and filters["time_range"] != time_scope:
            raise SemanticPlanError("data_request time_scope must match filters.time_range")
        filters["time_range"] = dict(time_scope)
    elif filters.get("time_range") is not None:
        raise SemanticPlanError("data_request time_scope must match filters.time_range")

    raw_origin = value.get("time_scope_origin")
    if raw_origin is None:
        # This preserves parsing compatibility for stored diagnostic fixtures.
        # A live planner still has to pass cue enforcement before execution.
        origin = "unspecified" if time_scope is None else "default"
    else:
        origin = raw_origin
    if not isinstance(origin, str) or origin not in _TIME_SCOPE_ORIGINS:
        raise SemanticPlanError("data_request time_scope_origin is unsupported")
    if time_scope is None and origin != "unspecified":
        raise SemanticPlanError("data_request time_scope requires a non-unspecified origin")
    if time_scope is not None and origin == "unspecified":
        raise SemanticPlanError("data_request time_scope cannot have unspecified origin")

    subject = value.get("subject")
    if subject == "connector":
        # Older bounded diagnostic fixtures used ``connector`` as a broad
        # subject.  Preserve their meaning only when the accompanying
        # dimension makes it unambiguous; live plans are prompted to name the
        # root/current business subject explicitly.
        subject = "root_connector" if "root_connector" in dimensions else "current_connector"
    elif isinstance(subject, str):
        subject = _SUBJECT_ALIASES.get(subject, subject)
    if subject is None:
        subject = _derive_subject(dimensions)
    if not isinstance(subject, str) or subject not in _SUBJECT_DIMENSIONS:
        raise SemanticPlanError("data_request subject is unsupported")
    _validate_subject_dimensions(subject, dimensions)

    metric = value.get("metric")
    if metric is None:
        metric = metric_names[0] if len(metric_names) == 1 else None
    if metric is not None and (not isinstance(metric, str) or metric not in metric_names):
        raise SemanticPlanError("data_request metric is unsupported")

    ranking = value.get("ranking")
    if ranking is None and dimensions:
        ranking = "descending" if sort["direction"] == "desc" else "ascending"
    if ranking is not None and ranking not in {"ascending", "descending"}:
        raise SemanticPlanError("data_request ranking is unsupported")
    if ranking is not None and sort["direction"] != ("desc" if ranking == "descending" else "asc"):
        raise SemanticPlanError("data_request ranking must match sort direction")

    intent = value.get("intent")
    if intent is None:
        # Preserve compatibility with bounded, older planner outputs while
        # exposing an explicit intent from this point onward.  The derivation
        # is solely from the validated business fields, never question text.
        intent = _infer_intent(metric_names, dimensions, filters, limit)
    if not isinstance(intent, str) or intent not in _INTENTS:
        raise SemanticPlanError("data_request.intent is unsupported")
    _validate_intent(intent, subject, metric_names, dimensions, filters, limit)
    tie_policy = value.get("tie_policy", "exact_limit" if ranking is not None else "include_ties")
    if tie_policy not in {"include_ties", "exact_limit"}:
        raise SemanticPlanError("data_request tie_policy is unsupported")
    if tie_policy == "exact_limit" and not dimensions:
        raise SemanticPlanError("data_request exact tie_policy requires a dimension")
    return {
        "intent": intent,
        "subject": subject,
        "metric": metric,
        "ranking": ranking,
        "time_scope": dict(time_scope) if time_scope is not None else None,
        "time_scope_origin": origin,
        "metrics": metric_names,
        "dimensions": list(dict.fromkeys(dimensions)),
        "filters": filters,
        "sort": {"metric": sort["metric"], "direction": sort["direction"]},
        "limit": limit,
        "comparison": comparison,
        "detail_fields": list(dict.fromkeys(details)),
        "tie_policy": tie_policy,
    }


def _infer_intent(
    metrics: list[str], dimensions: list[str], filters: dict[str, Any], limit: int
) -> str:
    if filters.get("outcome") == ["FAILED"] and any(
        dimension in {"connector", "root_connector"} for dimension in dimensions
    ):
        return "failed_connectors"
    if "recovery_rate" in metrics:
        return "recovery_rate"
    if any(dimension in {"error", "error_code"} for dimension in dimensions) and limit == 1:
        return "top_error_signature"
    return "incidents"


def _derive_subject(dimensions: list[str]) -> str:
    has_root_connector = "root_connector" in dimensions
    has_current_connector = "connector" in dimensions
    has_error = any(dimension in _SUBJECT_DIMENSIONS["error_signature"] for dimension in dimensions)
    if (has_root_connector or has_current_connector) and has_error:
        return "incident"
    if has_root_connector:
        return "root_connector"
    if has_current_connector:
        return "current_connector"
    if has_error:
        return "error_signature"
    return "incident"


def _validate_subject_dimensions(subject: str, dimensions: list[str]) -> None:
    allowed = _SUBJECT_DIMENSIONS[subject]
    if subject == "incident":
        return
    if not dimensions or not set(dimensions) <= allowed:
        raise SemanticPlanError(f"{subject} subject requires a compatible dimension")


def _validate_intent(
    intent: str,
    subject: str,
    metrics: list[str],
    dimensions: list[str],
    filters: dict[str, Any],
    limit: int,
) -> None:
    if intent == "failed_connectors":
        if subject not in {"root_connector", "current_connector"} or "incident_count" not in metrics or not any(
            dimension in {"connector", "root_connector"} for dimension in dimensions
        ) or filters.get("outcome") != ["FAILED"]:
            raise SemanticPlanError("failed_connectors requires connector dimension, incident_count, and outcome FAILED")
    elif intent == "top_error_signature":
        if subject != "error_signature" or "incident_count" not in metrics or not any(
            dimension in {"error", "error_code"} for dimension in dimensions
        ) or limit != 1:
            raise SemanticPlanError("top_error_signature requires one error dimension, incident_count, and limit 1")
    elif intent == "recovery_rate" and (subject != "incident" or metrics != ["recovery_rate"]):
        raise SemanticPlanError("recovery_rate requires incident subject and only the recovery_rate metric")


def _validate_filters(filters: dict[str, Any]) -> None:
    time_range = filters.get("time_range")
    if time_range is not None:
        try:
            parse_time_range(time_range)
        except ValueError as exc:
            raise SemanticPlanError("data_request time_range is unsupported") from exc
    event_type = filters.get("event_type")
    if event_type is not None and event_type != ["HEALTH_FAILED_CONFIRMED"]:
        raise SemanticPlanError("data_request event_type is unsupported")
    outcome = filters.get("outcome")
    if outcome is not None:
        # The repository layer accepts one outcome parameter.  Rejecting a
        # multi-value request is safer than silently executing only its first
        # value and changing the meaning of the user's filter.
        if (
            not isinstance(outcome, list)
            or len(outcome) != 1
            or not set(outcome) <= {"RECOVERED", "FAILED", "ESCALATED", "OPEN"}
        ):
            raise SemanticPlanError("data_request outcome is unsupported")
    for field in ("connector", "error_code"):
        item = filters.get(field)
        if item is not None and (not isinstance(item, str) or not item.strip() or len(item) > 255):
            raise SemanticPlanError(f"data_request {field} is unsupported")


def _canonical_time_scope(value: object) -> dict[str, Any]:
    """Return the exact catalog representation accepted by the safe DSL."""

    try:
        parsed = parse_time_range(value)
    except ValueError as exc:
        raise SemanticPlanError("data_request time_range is unsupported") from exc
    if parsed is None:
        raise SemanticPlanError("data_request time_range is unsupported")
    return parsed.to_dict()


def semantic_cue_contract(question: str) -> SemanticCueContract:
    """Extract explicit business constraints from question vocabulary.

    Accent-insensitive matching is deliberate.  A tiny edit-distance allowance
    for a single word handles normal typing slips without becoming a free-form
    keyword planner.  Ambiguous terms never produce a constraint by themselves.
    """

    normalized = _normalize_question(question)
    vocabulary = SEMANTIC_CATALOG["planning_policy"]["vocabulary"]
    subjects = vocabulary["subjects"]
    has_connector = _matches_any(normalized, subjects["connector"])
    has_current_connector = _matches_any(normalized, subjects.get("current_connector", [])) or bool(
        re.search(r"\bconnector\s+\d{3}\b", normalized)
    )
    has_error = _matches_any(normalized, subjects["error_signature"])
    # A connector is the requested subject in phrases such as "connector có
    # nhiều lỗi nhất".  The word "lỗi" then describes its incident metric,
    # rather than changing the request into an error-signature ranking.
    subject = (
        "current_connector" if has_current_connector
        else "root_connector" if has_connector
        else "error_signature" if has_error else None
    )
    ranking = "descending" if _matches_any(normalized, vocabulary["ranking"]["descending"]) else None
    limit = _ranking_limit(normalized) if ranking else None
    metric = "healing_log_count" if _matches_any(
        normalized, vocabulary["metrics"].get("healing_log_count", [])
    ) else "incident_count" if (
        ranking is not None
        or _matches_any(normalized, vocabulary["metrics"]["incident_count"])
    ) else None
    time_scope = _explicit_time_scope(question, normalized, vocabulary["time_scopes"])
    return SemanticCueContract(
        subject=subject,
        metric=metric,
        ranking=ranking,
        limit=limit,
        include_ties_requested=bool(ranking and _matches_any(normalized, vocabulary.get("include_ties", []))),
        time_scope=time_scope,
    )


def _explicit_time_scope(
    question: str, normalized_question: str, vocabulary: dict[str, Any]
) -> dict[str, Any] | None:
    """Extract only unambiguous time semantics from user wording.

    This runs before the model plan is validated. It gives the backend—not the
    model—ownership of timestamps and of the number in "N days recently".
    """

    absolute_dates = re.findall(r"\b(20\d{2})[-/](\d{2})[-/](\d{2})\b", question)
    if len(absolute_dates) == 1:
        year, month, day = absolute_dates[0]
        candidate = f"{year}-{month}-{day}"
        try:
            _canonical_time_scope({"kind": "absolute_date", "value": candidate})
        except SemanticPlanError:
            return None
        return {"kind": "absolute_date", "value": candidate}
    if len(absolute_dates) > 1:
        return None

    recent = re.search(
        r"\b(?:(\d{1,3})\s+ngay\s+gan\s+day|(?:last|past)\s+(\d{1,3})\s+days?|(\d{1,3})\s+days?\s+(?:recently|ago))\b",
        normalized_question,
    )
    if recent:
        days = int(next(group for group in recent.groups() if group is not None))
        if 1 <= days <= 366:
            if days == 7:
                return {"kind": "relative", "value": "last_7_days"}
            return {"kind": "relative", "value": "last_n_days", "days": days}

    matched = [
        value
        for value, terms in vocabulary.items()
        if _matches_any(normalized_question, terms)
    ]
    if len(matched) == 1:
        return {"kind": "relative", "value": matched[0]}
    return None


def enforce_semantic_cues(
    question: str,
    plan: SemanticPlan,
    *,
    context: dict[str, Any] | None = None,
) -> SemanticPlan:
    """Reject a valid-looking plan that changes an explicit user constraint."""

    request = plan.data_request
    if request is None:
        return replace(plan, semantic_enforced=True)
    cues = semantic_cue_contract(question)
    if cues.metric == "healing_log_count":
        raise SemanticPlanError("capability_unavailable:healing_log_count")
    if cues.subject is not None and request["subject"] != cues.subject:
        raise SemanticPlanError(
            f"semantic subject mismatch: question requires {cues.subject}; "
            f"plan supplied {request['subject']}"
        )
    if cues.metric is not None and request.get("metric") != cues.metric:
        raise SemanticPlanError(
            f"semantic metric mismatch: question requires {cues.metric}; "
            f"plan supplied {request.get('metric')}"
        )
    if cues.ranking is not None and request.get("ranking") != cues.ranking:
        raise SemanticPlanError(
            f"semantic ranking mismatch: question requires {cues.ranking}; "
            f"plan supplied {request.get('ranking')}"
        )
    if cues.limit is not None and request.get("limit") != cues.limit:
        raise SemanticPlanError(
            f"semantic ranking limit mismatch: question requires top {cues.limit}; "
            f"plan supplied {request.get('limit')}"
        )

    plan_scope = request.get("time_scope")
    origin = request.get("time_scope_origin")
    if cues.time_scope is not None:
        if plan_scope != cues.time_scope or origin != "explicit":
            raise SemanticPlanError(
                "semantic time scope mismatch: an explicit time range must be preserved"
            )
    elif plan_scope is None:
        if origin != "unspecified":
            raise SemanticPlanError(
                "semantic time scope mismatch: no time filter requires unspecified origin"
            )
    elif origin == "inherited" and _valid_inherited_time_scope(plan, context):
        pass
    elif origin == "default" and _catalog_default_time_scope() == plan_scope:
        pass
    else:
        raise SemanticPlanError(
            "semantic time scope mismatch: the question did not establish a time range"
        )
    # Tie behavior is a deterministic business policy, not an LLM choice.
    # Ordinary "top N" means exactly N stable rows.  The broader dense-rank
    # behavior is reserved for an explicit request to include ties.
    request_with_policy = dict(request)
    if request.get("ranking") is not None:
        request_with_policy["tie_policy"] = "include_ties" if cues.include_ties_requested else "exact_limit"
    return replace(plan, data_request=request_with_policy, semantic_enforced=True)


def _valid_inherited_time_scope(plan: SemanticPlan, context: dict[str, Any] | None) -> bool:
    if not ({"filters", "time_scope"} & set(plan.inherited_fields)):
        return False
    prior_request = (context or {}).get("previous_plan", {}).get("data_request")
    if not isinstance(prior_request, dict) or not isinstance(plan.data_request, dict):
        return False
    prior_scope = prior_request.get("time_scope")
    if prior_scope is None:
        prior_scope = (prior_request.get("filters") or {}).get("time_range")
    return prior_scope == plan.data_request.get("time_scope")


def _catalog_default_time_scope() -> dict[str, str] | None:
    value = SEMANTIC_CATALOG["planning_policy"].get("default_time_scope")
    return dict(value) if isinstance(value, dict) else None


def _normalize_question(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value.casefold())
    without_marks = "".join(char for char in decomposed if unicodedata.category(char) != "Mn").replace("đ", "d")
    normalized = re.sub(r"[^a-z0-9]+", " ", without_marks)
    return f" {re.sub(r'\\s+', ' ', normalized).strip()} "


def _ranking_limit(normalized_question: str) -> int:
    for pattern in (r"\btop\s+(\d{1,3})\b", r"\b(\d{1,3})\s+(?:connector|connectors|loi|error)\b"):
        match = re.search(pattern, normalized_question)
        if match:
            value = int(match.group(1))
            if 1 <= value <= 100:
                return value
    # The catalog's superlative vocabulary ("nhiều nhất", "most", etc.)
    # denotes the leading result unless the user explicitly asks for top N.
    return 1


def _matches_any(normalized_question: str, terms: object) -> bool:
    if not isinstance(terms, list):
        return False
    words = normalized_question.split()
    for term in terms:
        if not isinstance(term, str):
            continue
        normalized_term = _normalize_question(term).strip()
        if not normalized_term:
            continue
        if f" {normalized_term} " in normalized_question:
            return True
        term_words = normalized_term.split()
        if len(term_words) == 2 and term_words[0] in words and term_words[1] in words:
            first = words.index(term_words[0])
            second = words.index(term_words[1], first + 1)
            if second - first <= 3:
                return True
        if " " not in normalized_term and len(normalized_term) >= 5:
            if any(_edit_distance_at_most_one(word, normalized_term) for word in words):
                return True
    return False


def _edit_distance_at_most_one(left: str, right: str) -> bool:
    if left == right:
        return True
    if abs(len(left) - len(right)) > 1:
        return False
    if len(left) == len(right):
        return sum(a != b for a, b in zip(left, right, strict=True)) <= 1
    shorter, longer = (left, right) if len(left) < len(right) else (right, left)
    index = offset = 0
    while index < len(shorter):
        if shorter[index] == longer[index + offset]:
            index += 1
            continue
        if offset:
            return False
        offset = 1
    return True


def _parse_guidance(value: object) -> GuidanceRequest:
    if value is None:
        return GuidanceRequest(False)
    if not isinstance(value, dict) or not set(value) <= {"needed", "purpose", "error_codes", "connector_class"}:
        raise SemanticPlanError("guidance_request is unsupported")
    needed = value.get("needed")
    if not isinstance(needed, bool):
        raise SemanticPlanError("guidance_request.needed must be boolean")
    purpose = value.get("purpose")
    if purpose is not None and purpose not in {"remediation", "diagnosis", "meaning"}:
        raise SemanticPlanError("guidance_request.purpose is unsupported")
    codes = value.get("error_codes") or []
    if not isinstance(codes, list) or len(codes) > 10 or not all(
        isinstance(item, str) and 1 <= len(item.strip()) <= 80 for item in codes
    ):
        raise SemanticPlanError("guidance_request.error_codes is unsupported")
    connector_class = value.get("connector_class")
    if connector_class is not None and (not isinstance(connector_class, str) or not connector_class.strip() or len(connector_class) > 80):
        raise SemanticPlanError("guidance_request.connector_class is unsupported")
    if needed and purpose is None:
        raise SemanticPlanError("guidance_request.purpose is required when guidance is needed")
    if not needed and (purpose is not None or codes or connector_class is not None):
        raise SemanticPlanError("guidance_request metadata requires guidance")
    return GuidanceRequest(
        needed=needed,
        purpose=purpose,
        error_codes=tuple(dict.fromkeys(item.strip().upper() for item in codes)),
        connector_class=connector_class.strip().lower() if isinstance(connector_class, str) else None,
    )


def compile_analytics_request(
    plan: SemanticPlan,
    *,
    require_semantic_enforcement: bool = False,
) -> QueryPlan:
    """Compile a validated business request into the existing safe DSL."""

    if require_semantic_enforcement and not plan.semantic_enforced:
        raise SemanticPlanError("semantic plan was not enforced against the user request")
    request = plan.data_request
    if request is None:
        raise SemanticPlanError("semantic plan has no analytics request")
    filters = request["filters"]
    compiled_filters: dict[str, Any] = {}
    if filters.get("time_range") is not None:
        compiled_filters["time_range"] = filters["time_range"]
    if filters.get("event_type") is not None:
        compiled_filters["event_type"] = filters["event_type"]
    if filters.get("outcome") is not None:
        compiled_filters["final_outcome"] = filters["outcome"]
    if filters.get("connector") is not None:
        compiled_filters["connector_name"] = filters["connector"].strip()
    if filters.get("error_code") is not None:
        compiled_filters["error_code"] = filters["error_code"].strip().upper()
    return parse_plan({
        "dataset": "connector_incidents",
        "metrics": [
            {"name": _METRICS[item][0], "aggregation": _METRICS[item][1]}
            for item in request["metrics"]
        ],
        "group_by": [_DIMENSIONS[item] for item in request["dimensions"]],
        "filters": compiled_filters,
        "order_by": [{
            "field": _METRICS[request["sort"]["metric"]][0],
            "direction": request["sort"]["direction"],
        }],
        "limit": request["limit"],
        "comparison": request["comparison"],
        "details": [_DETAILS[item] for item in request["detail_fields"]],
        "tie_policy": request.get("tie_policy", "include_ties"),
    })


class SemanticPlanner:
    """One bounded model boundary with a single correction attempt."""

    def __init__(
        self,
        generate: Callable[..., dict[str, Any]] | None,
        *,
        max_tokens: int = 900,
        enforce_cues: bool = True,
    ):
        self._generate = generate
        self._max_tokens = max_tokens
        self._enforce_cues = enforce_cues

    def plan(self, question: str, *, context: dict[str, Any] | None = None) -> tuple[SemanticPlan, int]:
        if self._generate is None:
            raise SemanticPlanError("semantic_planner_not_configured")
        correction: str | None = None
        diagnostics: dict[str, Any] = {}
        for attempt in range(1, 3):
            messages = _messages(question, context=context, correction=correction)
            try:
                value = self._generate(messages, max_tokens=self._max_tokens)
                diagnostics = _time_scope_diagnostics(value, question)
                value = _apply_explicit_time_scope(value, question)
                plan = parse_semantic_plan(value)
                _validate_inheritance(plan, context)
                if self._enforce_cues:
                    plan = enforce_semantic_cues(question, plan, context=context)
                else:
                    # Dependency-injected planners are used by offline unit
                    # fixtures.  Production construction keeps this guard on.
                    plan = replace(plan, semantic_enforced=True)
                diagnostics["validation_reason"] = None
                return replace(plan, time_scope_resolution=diagnostics), attempt
            except SemanticPlanError as exc:
                diagnostics["validation_reason"] = str(exc)
                correction = _planner_correction_feedback(str(exc))
            except ValueError as exc:
                # A JSON boundary that returns malformed content is a model
                # output problem, not a reason to fall back to phrase rules.
                correction = f"planner_output_invalid:{exc}"
                diagnostics["validation_reason"] = correction
            except httpx.TimeoutException as exc:
                raise SemanticPlanError("semantic_planner_timeout") from exc
            except httpx.HTTPStatusError as exc:
                raise SemanticPlanError(_http_failure_reason(exc)) from exc
            except Exception as exc:
                raise SemanticPlanError(f"semantic_planner_service_failure:{type(exc).__name__}") from exc
        raise SemanticPlanError(
            f"semantic_planner_invalid_after_correction:{correction}", diagnostics=diagnostics
        )


def _apply_explicit_time_scope(value: object, question: str) -> object:
    """Replace a model time shape only when the user supplied an exact scope."""

    cues = semantic_cue_contract(question)
    if cues.time_scope is None or not isinstance(value, dict):
        return value
    request = value.get("data_request")
    if not isinstance(request, dict):
        return value
    filters = request.get("filters")
    if filters is not None and not isinstance(filters, dict):
        return value
    raw_scope = request.get("time_scope")
    if raw_scope is None and isinstance(filters, dict):
        raw_scope = filters.get("time_range")
    # A valid but different semantic period is a meaning error, not a shape
    # error. Keep it so cue enforcement returns correction feedback instead
    # of silently changing "yesterday" into "today".
    if _model_scope_conflicts_with_explicit_scope(raw_scope, cues.time_scope):
        return value
    normalized = dict(value)
    request = dict(request)
    request["filters"] = dict(filters) if isinstance(filters, dict) else {}
    request["filters"]["time_range"] = dict(cues.time_scope)
    request["time_scope"] = dict(cues.time_scope)
    request["time_scope_origin"] = "explicit"
    normalized["data_request"] = request
    return normalized


def _model_scope_conflicts_with_explicit_scope(
    raw_scope: object, expected: dict[str, Any]
) -> bool:
    if not isinstance(raw_scope, dict):
        return False
    value = raw_scope.get("value")
    if not isinstance(value, str):
        return False
    expected_value = expected.get("value")
    if value not in {
        "today", "yesterday", "last_7_days", "this_week", "last_week", "this_month", "last_n_days"
    } and not re.fullmatch(r"20\d{2}-\d{2}-\d{2}", value):
        return False
    if value != expected_value:
        return True
    if value == "last_n_days":
        return raw_scope.get("days") != expected.get("days")
    return False


def _time_scope_diagnostics(value: object, question: str) -> dict[str, Any]:
    """Bounded technical detail; it never retains raw prompts or credentials."""

    request = value.get("data_request") if isinstance(value, dict) else None
    scope = request.get("time_scope") if isinstance(request, dict) else None
    if scope is None and isinstance(request, dict) and isinstance(request.get("filters"), dict):
        scope = request["filters"].get("time_range")
    expected = semantic_cue_contract(question).time_scope
    return {
        "model_time_scope": _safe_time_scope(scope),
        "canonical_time_scope": dict(expected) if expected else None,
    }


def _safe_time_scope(value: object) -> dict[str, Any] | str | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        return "invalid_shape"
    safe: dict[str, Any] = {}
    for key in ("kind", "value", "days"):
        item = value.get(key)
        if isinstance(item, str) and len(item) <= 80:
            safe[key] = item
        elif isinstance(item, int) and not isinstance(item, bool):
            safe[key] = item
    return safe or "invalid_shape"


def _planner_correction_feedback(reason: str) -> str:
    if "time_range" in reason:
        return (
            "data_request.time_scope and filters.time_range must be identical. "
            "Use null when no period was requested, or exactly one semantic scope: "
            '{"kind":"relative","value":"today"}, "yesterday", "last_7_days", '
            '"this_week", "last_week", "this_month", or '
            '{"kind":"relative","value":"last_n_days","days":N}. '
            "Never include from_at, to_at, timezone, or timestamp fields; the backend resolves them."
        )
    return reason


def _validate_inheritance(plan: SemanticPlan, context: dict[str, Any] | None) -> None:
    """Make planner-declared inheritance auditable instead of trusting prose.

    The planner always emits a complete current request.  This check only
    verifies fields it says were inherited; it never copies a prior filter into
    the new plan itself.
    """

    if not plan.inherited_fields:
        return
    previous = (context or {}).get("previous_plan")
    if not isinstance(previous, dict):
        raise SemanticPlanError("inherited_fields require prior conversation context")
    previous_request = previous.get("data_request")
    if not isinstance(previous_request, dict) or not isinstance(plan.data_request, dict):
        raise SemanticPlanError("inherited_fields require a prior data request")
    for field in plan.inherited_fields:
        if plan.data_request.get(field) != previous_request.get(field):
            raise SemanticPlanError(f"inherited field {field} does not match prior context")


def _http_failure_reason(exc: httpx.HTTPStatusError) -> str:
    status = exc.response.status_code
    if status in {401, 403}:
        return "semantic_planner_authentication"
    if status == 402:
        return "semantic_planner_quota_or_billing"
    if status == 429:
        return "semantic_planner_rate_limited"
    if status >= 500:
        return "semantic_planner_service_error"
    return f"semantic_planner_http_error_{status}"


def _messages(question: str, *, context: dict[str, Any] | None, correction: str | None) -> list[dict[str, str]]:
    schema: dict[str, Any] = {
        "version": CATALOG_VERSION,
        "data_request": {
            "intent": "incidents",
            "subject": "root_connector",
            "metric": "incident_count",
            "ranking": "descending",
            "time_scope": None,
            "time_scope_origin": "unspecified",
            "metrics": ["incident_count"],
            "dimensions": ["root_connector"],
            "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
            "sort": {"metric": "incident_count", "direction": "desc"},
            "limit": 20,
            "comparison": None,
            "detail_fields": [],
        },
        "guidance_request": {
            "needed": False,
            "purpose": None,
            "error_codes": [],
            "connector_class": None,
        },
        "clarification": None,
        "conversation_action": "none",
        "inherited_fields": [],
    }
    system = (
        "You are a semantic planner, not an answer writer. Return exactly one JSON object and no prose. "
        "Never return SQL, database tables, procedures, code, credentials, raw logs, counts, or an answer. "
        "Use only business concepts in the catalog. Every data_request must include intent, subject, metric, ranking, time_scope, and time_scope_origin. "
        "subject must be exactly incident, root_connector, current_connector, or error_signature. Generic connector questions use root_connector; use current_connector only when the user explicitly asks for a current/versioned connector. "
        "Use time_scope=null and time_scope_origin=unspecified when the user did not establish a time range. "
        "Do not output route: the backend derives it from whether "
        "data_request and guidance_request are present. A request may include both. Ask clarification only when "
        "a material ambiguity cannot be resolved from supplied conversation context. Preserve an explicit user "
        "filter; do not invent a connector, error, time range, metric, or live status. Return this exact schema "
        "shape, with null where a section is not needed: "
        + json.dumps(schema, ensure_ascii=False)
        + ". Semantic catalog: "
        + json.dumps(catalog_for_model(), ensure_ascii=False)
    )
    payload: dict[str, Any] = {"question": question, "conversation_context": context or {}}
    if correction:
        payload["validation_feedback"] = correction
        payload["instruction"] = "Correct the JSON contract. Do not change the question scope."
    return [{"role": "system", "content": system}, {"role": "user", "content": json.dumps(payload, ensure_ascii=False)}]
