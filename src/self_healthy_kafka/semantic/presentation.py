"""Generic, catalog-driven presentation facts and safe Vietnamese rendering."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from self_healthy_kafka.semantic.catalog import SEMANTIC_CATALOG
from self_healthy_kafka.semantic.outcome import AnalyticsOutcome
from self_healthy_kafka.semantic.planner import SemanticPlan
from self_healthy_kafka.webhook.analytics import QueryPlan

_PRESENTATION = SEMANTIC_CATALOG["presentation"]
_METRIC_VALUE_FIELDS = {
    "incident_count": "failure_count",
    "recovered_incident_count": "recovered_count",
    "open_incident_count": "open_count",
    "average_recovery_minutes": "average_recovery_minutes",
    "recovery_rate": "recovery_rate_percent",
}
_SUBJECT_DIMENSIONS = {"connector", "root_connector", "current_connector", "error", "error_code"}

ResponseAct = Literal[
    "existence",
    "list",
    "count",
    "error_detail",
    "error_code",
    "comparison",
    "runbook_guidance",
]


@dataclass(frozen=True)
class PresentationCondition:
    """A canonical business filter, never a phrase copied from the question."""

    field: str
    operator: str
    value: str

    def to_dict(self) -> dict[str, str]:
        return {"field": self.field, "operator": self.operator, "value": self.value}


@dataclass(frozen=True)
class PresentationFacts:
    """The only semantic input accepted by deterministic outcome rendering."""

    outcome: str
    subject: str
    conditions: tuple[PresentationCondition, ...]
    metric: str | None
    metric_value: Any
    result_count: int
    # Safe evidence packets for verified results. The deterministic renderer
    # and the optional response model consume this single contract rather than
    # a separate question, plan, or raw database row collection.
    rows: tuple[dict[str, Any], ...]
    row_count: int
    time_scope: str
    from_at: str | None
    to_at: str | None
    timezone: str
    source: str
    snapshot_freshness: str | None
    evidence_complete: bool
    query_executed: bool
    clarification_question: str | None
    safe_failure_reason: str | None
    sort_metric: str | None
    ranking: str | None = None
    time_scope_origin: str = "unspecified"
    summary_item_limit: int = 3
    summary_detail_limit: int = 1
    result_total_count: int = 0
    displayed_count: int = 0
    remaining_count: int = 0
    tie_policy: str | None = None
    boundary_tie_count: int | None = None
    boundary_tie_truncated: bool = False
    has_more_verified_results: bool = False
    detail_accessible: bool = False
    response_act: ResponseAct = "list"

    @property
    def condition(self) -> dict[str, str] | None:
        """Compatibility-friendly singular view for simple consumers."""

        return self.conditions[0].to_dict() if len(self.conditions) == 1 else None

    @property
    def permits_empty_claim(self) -> bool:
        return (
            self.outcome == "verified_empty"
            and self.query_executed
            and self.evidence_complete
            and self.row_count == 0
        )

    @property
    def summary_rows(self) -> tuple[dict[str, Any], ...]:
        """Bounded verified facts that are safe to place in response prose."""

        displayed = self.displayed_count
        # Keep direct construction backward compatible for focused renderer
        # tests and trusted internal callers while built contracts always set
        # an explicit displayed_count.
        if displayed == 0 and self.rows:
            displayed = min(len(self.rows), self.summary_item_limit)
        return self.rows[:displayed]

    def summary_metadata(self) -> dict[str, Any]:
        """Public-safe UI state without SQL, prompts, or diagnostics."""

        return {
            "summary_item_limit": self.summary_item_limit,
            "summary_detail_limit": self.summary_detail_limit,
            "result_total_count": self.result_total_count,
            "displayed_count": self.displayed_count,
            "remaining_count": self.remaining_count,
            "ranking": self.ranking,
            "tie_policy": self.tie_policy,
            "boundary_tie_count": self.boundary_tie_count,
            "boundary_tie_truncated": self.boundary_tie_truncated,
            "has_more_verified_results": self.has_more_verified_results,
            "detail_accessible": self.detail_accessible,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome": self.outcome,
            "subject": self.subject,
            "condition": self.condition,
            "conditions": [condition.to_dict() for condition in self.conditions],
            "metric": self.metric,
            "metric_value": self.metric_value,
            "result_count": self.result_count,
            "rows": [dict(row) for row in self.rows],
            "row_count": self.row_count,
            "time_scope": natural_time_scope(self),
            "from_at": self.from_at,
            "to_at": self.to_at,
            "timezone": self.timezone,
            "source": str(_PRESENTATION["sources"].get(self.source, self.source)),
            "snapshot_freshness": self.snapshot_freshness,
            "evidence_complete": self.evidence_complete,
            "query_executed": self.query_executed,
            "clarification_question": self.clarification_question,
            "safe_failure_reason": self.safe_failure_reason,
            "sort_metric": self.sort_metric,
            "ranking": self.ranking,
            "time_scope_origin": self.time_scope_origin,
            "response_act": self.response_act,
            "summary_rows": [dict(row) for row in self.summary_rows],
            "presentation": self.summary_metadata(),
        }


def build_presentation_facts(
    plan: SemanticPlan,
    *,
    query_plan: QueryPlan | None,
    outcome: AnalyticsOutcome,
    source_rows: list[dict[str, Any]],
    facts: list[dict[str, Any]],
    evidence: list[dict[str, Any]],
    from_at: datetime | None,
    to_at: datetime | None,
    timezone_name: str,
) -> PresentationFacts:
    """Build a language-neutral summary from validated plan, facts and outcome."""

    request = plan.data_request or {}
    metrics = tuple(str(metric) for metric in request.get("metrics") or ())
    dimensions = tuple(str(dimension) for dimension in request.get("dimensions") or ())
    metric = metrics[0] if len(metrics) == 1 else None
    metric_value = None
    if metric and len(facts) == 1 and not dimensions:
        metric_value = facts[0].get(_METRIC_VALUE_FIELDS[metric])
    filters = request.get("filters") or {}
    conditions = _conditions(filters)
    sort_metric = None
    if isinstance(request.get("sort"), dict):
        candidate = request["sort"].get("metric")
        sort_metric = str(candidate) if candidate in _METRIC_VALUE_FIELDS else None
    if query_plan is not None and sort_metric is None:
        sort_metric = _semantic_metric_for_query_metric(query_plan.order_by)
    policy = _PRESENTATION["summary_policy"]
    summary_item_limit = int(policy["summary_item_limit"])
    result_total_count = len(evidence)
    displayed_count = min(result_total_count, summary_item_limit)
    boundary = evidence[displayed_count - 1] if displayed_count else None
    boundary_tie_count = boundary.get("tie_count") if isinstance(boundary, dict) else None
    boundary_rank = boundary.get("rank") if isinstance(boundary, dict) else None
    boundary_tie_truncated = bool(
        displayed_count < result_total_count
        and isinstance(boundary_tie_count, int)
        and boundary_tie_count > sum(
            1 for item in evidence[:displayed_count]
            if isinstance(item, dict) and item.get("rank") == boundary_rank
        )
    )
    return PresentationFacts(
        outcome=outcome.outcome,
        subject=_presentation_subject(str(request.get("subject") or "incident"), dimensions),
        conditions=conditions,
        metric=metric,
        metric_value=metric_value,
        result_count=len(facts),
        rows=tuple(dict(item) for item in evidence),
        row_count=len(source_rows),
        time_scope=_time_scope(filters, from_at, to_at, timezone_name),
        from_at=from_at.isoformat() if from_at else None,
        to_at=to_at.isoformat() if to_at else None,
        timezone=timezone_name,
        source=str(SEMANTIC_CATALOG["source"]["kind"]),
        snapshot_freshness=outcome.snapshot_freshness,
        evidence_complete=outcome.evidence_complete,
        query_executed=outcome.query_executed,
        clarification_question=plan.clarification,
        safe_failure_reason=outcome.reason,
        sort_metric=sort_metric,
        ranking=request.get("ranking") if isinstance(request.get("ranking"), str) else None,
        time_scope_origin=str(request.get("time_scope_origin") or "unspecified"),
        summary_item_limit=summary_item_limit,
        summary_detail_limit=int(policy["summary_detail_limit"]),
        result_total_count=result_total_count,
        displayed_count=displayed_count,
        remaining_count=result_total_count - displayed_count,
        tie_policy=(
            query_plan.tie_policy
            if query_plan is not None and query_plan.ranking is not None
            else None
        ),
        boundary_tie_count=boundary_tie_count if isinstance(boundary_tie_count, int) else None,
        boundary_tie_truncated=boundary_tie_truncated,
        has_more_verified_results=(
            outcome.outcome == "verified_results"
            and result_total_count > displayed_count
            and bool(evidence)
        ),
        detail_accessible=outcome.outcome == "verified_results" and bool(evidence),
        response_act=_response_act(request),
    )


def build_fallback_presentation(
    outcome: AnalyticsOutcome,
    *,
    timezone_name: str,
) -> PresentationFacts:
    """Create a source-safe contract when no validated semantic plan exists."""

    return PresentationFacts(
        outcome=outcome.outcome,
        subject="incident",
        conditions=(),
        metric=None,
        metric_value=None,
        result_count=0,
        rows=(),
        row_count=outcome.row_count or 0,
        time_scope="trong phạm vi thời gian chưa thể xác minh",
        from_at=None,
        to_at=None,
        timezone=timezone_name,
        source=str(SEMANTIC_CATALOG["source"]["kind"]),
        snapshot_freshness=outcome.snapshot_freshness,
        evidence_complete=outcome.evidence_complete,
        query_executed=outcome.query_executed,
        clarification_question=None,
        safe_failure_reason=outcome.reason,
        sort_metric=None,
    )


class SemanticResponseRenderer:
    """Render safe outcome statements without semantic-intent branches."""

    def render_outcome(self, facts: PresentationFacts) -> str:
        if facts.permits_empty_claim:
            subject = _subject_label(facts.subject, plural=False)
            condition = _condition_label(facts.conditions)
            suffix = f" {condition}" if condition else ""
            if facts.subject in {"connector", "root_connector", "current_connector"}:
                return f"Không, chưa ghi nhận {subject} nào{suffix} {natural_time_scope(facts)}."
            return f"Chưa ghi nhận {subject} nào{suffix} {natural_time_scope(facts)}."
        if facts.outcome == "verified_results":
            subject = _subject_label(facts.subject, plural=True)
            return f"Có {facts.result_count} {subject} phù hợp {natural_time_scope(facts)}."
        if facts.outcome == "needs_clarification":
            return facts.clarification_question or "Mình cần thêm một thông tin để chọn đúng phạm vi truy vấn."
        if facts.outcome == "out_of_scope":
            return "Câu hỏi này nằm ngoài phạm vi hỗ trợ của chatbot Self Healthy Kafka."
        if facts.outcome == "degraded":
            return _failure_message(
                facts.safe_failure_reason, degraded=True, conditions=facts.conditions
            )
        return _failure_message(
            facts.safe_failure_reason, degraded=False, conditions=facts.conditions
        )


def natural_time_scope(facts: PresentationFacts) -> str:
    """Keep timezone metadata out of ordinary prose while preserving its bounds."""

    return facts.time_scope.split(" theo múi giờ ", 1)[0]


def _response_act(request: dict[str, Any]) -> ResponseAct:
    details = set(request.get("detail_fields") or ())
    dimensions = set(request.get("dimensions") or ())
    if "error_message" in details:
        return "error_detail"
    if "error_code" in dimensions:
        return "error_code"
    if request.get("comparison") is not None:
        return "comparison"
    if request.get("intent") == "failed_connectors":
        return "existence"
    if not dimensions:
        return "count"
    return "list"


def _conditions(filters: dict[str, Any]) -> tuple[PresentationCondition, ...]:
    conditions: list[PresentationCondition] = []
    outcome = filters.get("outcome")
    if isinstance(outcome, list) and len(outcome) == 1 and isinstance(outcome[0], str):
        conditions.append(PresentationCondition("outcome", "equals", outcome[0]))
    for field in ("connector", "error_code"):
        value = filters.get(field)
        if isinstance(value, str) and value.strip():
            conditions.append(PresentationCondition(field, "equals", value.strip()))
    return tuple(conditions)


def _subject(dimensions: tuple[str, ...]) -> str:
    return next((dimension for dimension in dimensions if dimension in _SUBJECT_DIMENSIONS), "incident")


def _presentation_subject(subject: str, dimensions: tuple[str, ...]) -> str:
    if subject in {"incident", "connector", "root_connector", "current_connector"}:
        return subject if subject != "connector" else "connector"
    if subject == "error_signature":
        return _subject(dimensions)
    return _subject(dimensions)


def _subject_label(subject: str, *, plural: bool) -> str:
    descriptor = _PRESENTATION["subjects"].get(subject, _PRESENTATION["subjects"]["incident"])
    return str(descriptor["plural_vi" if plural else "singular_vi"])


def _failure_message(
    reason: str | None,
    *,
    degraded: bool,
    conditions: tuple[PresentationCondition, ...] = (),
) -> str:
    connector = next(
        (condition.value for condition in conditions if condition.field == "connector"),
        None,
    )
    target = f" cho connector {connector}" if connector else ""
    if reason == "snapshot_stale":
        return (
            f"Snapshot incident chưa đủ mới để xác minh yêu cầu{target}. Điều này không có nghĩa là "
            "không có sự cố; hãy thử lại sau khi snapshot được cập nhật."
        )
    if reason in {"incomplete_result_coverage", "analytics_evidence_truncated"}:
        return (
            f"Truy vấn đã chạy nhưng dữ liệu kiểm chứng{target} chưa đầy đủ, nên mình chưa thể xác minh kết quả. "
            "Điều này không có nghĩa là không có sự cố."
        )
    if reason in {"query_failed", "analytics_dbt_query_not_supported"}:
        return (
            f"Truy vấn dữ liệu incident{target} không hoàn tất, nên mình chưa thể xác minh yêu cầu này. "
            "Điều này không có nghĩa là không có sự cố; bạn có thể thử lại sau."
        )
    if degraded or reason in {"source_unavailable", "analytics_source_unavailable"}:
        return (
            f"Nguồn dữ liệu incident hiện không truy cập được, nên mình chưa thể xác minh yêu cầu{target}. "
            "Điều này không có nghĩa là không có sự cố; bạn có thể thử lại sau."
        )
    return (
        f"Mình chưa có đủ dữ liệu đã kiểm chứng để xác minh yêu cầu{target}. "
        "Điều này không có nghĩa là không có sự cố."
    )


def _condition_label(conditions: tuple[PresentationCondition, ...]) -> str:
    rendered: list[str] = []
    for condition in conditions:
        descriptor = _PRESENTATION["fields"].get(condition.field)
        if not isinstance(descriptor, dict) or condition.operator != "equals":
            continue
        value = str(descriptor.get("values", {}).get(condition.value, condition.value))
        template = descriptor.get("equals_template_vi")
        if isinstance(template, str):
            rendered.append(template.format(value=value))
    return " và ".join(rendered)


def _time_scope(
    filters: dict[str, Any],
    from_at: datetime | None,
    to_at: datetime | None,
    timezone_name: str,
) -> str:
    time_range = filters.get("time_range")
    value = time_range.get("value") if isinstance(time_range, dict) else None
    template = _PRESENTATION["time_ranges"].get(value)
    if isinstance(template, str):
        return template.format(
            timezone=timezone_name,
            days=time_range.get("days") if isinstance(time_range, dict) else None,
        )
    if from_at and to_at:
        return f"từ {from_at.isoformat()} đến {to_at.isoformat()}"
    return "trong toàn bộ dữ liệu incident hiện có"


def _semantic_metric_for_query_metric(query_metric: str) -> str | None:
    for semantic_metric, metric_field in _METRIC_VALUE_FIELDS.items():
        if metric_field == query_metric:
            return semantic_metric
    return None
