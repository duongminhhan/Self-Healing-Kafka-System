from __future__ import annotations

import json
import re
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from self_healthy_kafka.config import AnalyticsChatConfig
from self_healthy_kafka.storage.common import json_safe
from self_healthy_kafka.webhook.analytics import (
    ALLOWED_METRICS,
    MAX_LIMIT,
    QueryPlan,
    parse_plan,
    resolve_time_range,
)

CATALOG = {
    "dataset": "connector_incidents",
    "fields": ["job_name", "connector_name", "error_code", "final_outcome", "failure_at"],
    "metrics": ["failure_count", "recovered_count", "open_count", "average_recovery_minutes"],
    "event_types": ["HEALTH_FAILED_CONFIRMED"],
    "outcomes": ["RECOVERED", "FAILED", "ESCALATED", "OPEN"],
}


class AnalyticsChatService:
    """Plans safe incident analysis; neither this class nor its model executes SQL."""

    def __init__(
        self,
        config: AnalyticsChatConfig,
        *,
        incident_facts: Callable[..., list[dict[str, Any]]],
        client: httpx.Client | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ):
        self._config = config
        self._incident_facts = incident_facts
        self._client = client or httpx.Client()
        self._now = now

    @property
    def enabled(self) -> bool:
        return self._config.enabled

    @property
    def path(self) -> str:
        return "/api/v1/chat"

    def validate(self) -> None:
        if not self._config.enabled:
            return
        try:
            ZoneInfo(self._config.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("CHAT_ANALYTICS_TIMEZONE is invalid") from exc
        if bool(self._config.hf_endpoint_url) != bool(self._config.hf_token):
            raise ValueError("HF_CHAT_ENDPOINT_URL and HF_CHAT_TOKEN must be configured together")
        if self._config.hf_endpoint_url and not self._config.hf_model_id:
            raise ValueError("HF_CHAT_MODEL_ID is required with Hugging Face endpoint")

    def ask(self, question: str) -> dict[str, Any]:
        question = question.strip()
        if not question:
            raise ValueError("question is required")
        plan = self._plan(question)
        from_at, to_at = resolve_time_range(
            plan.time_range, now=self._now(), timezone_name=self._config.timezone
        )
        rows = self._incident_facts(
            from_at=from_at,
            to_at=to_at,
            event_type=plan.event_types[0] if plan.event_types else None,
            final_outcome=plan.outcomes[0] if plan.outcomes else None,
            connector_name=plan.connector_name,
            error_code=plan.error_code,
            # Fetch a bounded fact packet before applying group-by/top-N.  Passing
            # the display limit here could make a top-5 aggregate incomplete.
            limit=MAX_LIMIT,
        )
        facts = _aggregate(rows, plan)
        comparison_facts = []
        comparison_rows: list[dict[str, Any]] = []
        if plan.comparison and from_at and to_at:
            interval = to_at - from_at
            comparison_rows = self._incident_facts(
                from_at=from_at - interval,
                to_at=from_at,
                event_type=plan.event_types[0] if plan.event_types else None,
                final_outcome=plan.outcomes[0] if plan.outcomes else None,
                connector_name=plan.connector_name,
                error_code=plan.error_code,
                limit=MAX_LIMIT,
            )
            comparison_facts = _aggregate(comparison_rows, plan)
        sources = [{
            "source": "vConnectorIncidentFacts",
            "count": len(rows),
            "items": [json_safe(row) for row in rows],
        }]
        if comparison_rows:
            sources.append({
                "source": "vConnectorIncidentFacts.previous_period",
                "count": len(comparison_rows),
                "items": [json_safe(row) for row in comparison_rows],
            })
        return {
            "answer": _answer(facts, plan, from_at, to_at, comparison_facts),
            "sources": sources,
            "query_plan": plan.to_dict(),
            "from_at": from_at.isoformat() if from_at else None,
            "to_at": to_at.isoformat() if to_at else None,
            "row_count": len(rows),
            "evidence_ids": [str(row.get("incident_id")) for row in rows + comparison_rows],
        }

    def _plan(self, question: str) -> QueryPlan:
        if not self._config.hf_endpoint_url:
            return _fallback_plan(question)
        response = self._client.post(
            self._config.hf_endpoint_url.rstrip("/") + "/v1/chat/completions",
            headers={"Authorization": f"Bearer {self._config.hf_token}"},
            json={
                "model": self._config.hf_model_id,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": _planner_prompt()},
                    {"role": "user", "content": question},
                ],
            },
            timeout=self._config.hf_request_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        try:
            content = payload["choices"][0]["message"]["content"]
            return parse_plan(json.loads(content))
        except (KeyError, IndexError, TypeError, json.JSONDecodeError, ValueError) as exc:
            raise ValueError("Hugging Face planner did not return a valid query plan") from exc


def _planner_prompt() -> str:
    return (
        "Return JSON only. Never return SQL, a table name, a procedure name, code, "
        "or credentials. Select only from this semantic catalog: "
        + json.dumps(CATALOG, ensure_ascii=False)
        + ". Use relative time values only: today, yesterday, last_7_days, this_month, this_week, last_week. "
        "Use failure_count for confirmed failures and group by job_name for connector ranking."
    )


def _fallback_plan(question: str) -> QueryPlan:
    text = question.lower()
    time_value = next((value for term, value in {
        "hôm nay": "today", "hôm qua": "yesterday", "7 ngày": "last_7_days",
        "tuần trước": "this_week", "tháng này": "this_month",
    }.items() if term in text), None)
    metric = "average_recovery_minutes" if "thời gian" in text else "failure_count"
    outcome = ["OPEN"] if "chưa recovery" in text or "chưa phục hồi" in text else []
    error_match = re.search(r"\bORA-\d{5}\b", question, flags=re.IGNORECASE)
    error_code = error_match.group(0).upper() if error_match else None
    comparison = "previous_period" if "tăng hay giảm" in text or "so với" in text else None
    return parse_plan({
        "dataset": "connector_incidents",
        "metrics": [{
            "name": metric,
            "aggregation": "average_recovery_minutes" if metric == "average_recovery_minutes" else "count_distinct_incident",
        }],
        "group_by": ["job_name"],
        "filters": {**({"time_range": {"kind": "relative", "value": time_value}} if time_value else {}), "event_type": ["HEALTH_FAILED_CONFIRMED"], **({"final_outcome": outcome} if outcome else {}), **({"error_code": error_code} if error_code else {})},
        "order_by": [{"field": metric, "direction": "desc"}], "limit": 5,
        **({"comparison": comparison} if comparison else {}),
    })


def _aggregate(rows: list[dict[str, Any]], plan: QueryPlan) -> list[dict[str, Any]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in rows:
        key = tuple(str(row.get(field) or "—") for field in plan.group_by) or ("Tất cả",)
        groups.setdefault(key, []).append(row)
    facts = []
    for key, items in groups.items():
        fact: dict[str, Any] = dict(zip(plan.group_by or ("label",), key, strict=True))
        fact["evidence_ids"] = [str(item.get("incident_id")) for item in items]
        for metric in plan.metrics:
            if metric.name == "failure_count":
                fact[metric.name] = len({item.get("incident_id") for item in items})
            elif metric.name == "recovered_count":
                fact[metric.name] = sum(item.get("final_outcome") == "RECOVERED" for item in items)
            elif metric.name == "open_count":
                fact[metric.name] = sum(item.get("final_outcome") == "OPEN" for item in items)
            elif metric.name == "average_recovery_minutes":
                durations = []
                for item in items:
                    duration = _duration(item)
                    if duration is not None:
                        durations.append(duration)
                fact[metric.name] = round(sum(durations) / len(durations), 2) if durations else None
        facts.append(fact)
    return sorted(facts, key=lambda item: (item.get(plan.order_by) is None, item.get(plan.order_by, 0)), reverse=plan.direction == "desc")[:plan.limit]


def _duration(item: dict[str, Any]) -> float | None:
    start, end = item.get("failure_at"), item.get("recovered_at")
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return None
    return (end - start).total_seconds() / 60


def _answer(facts: list[dict[str, Any]], plan: QueryPlan, from_at: datetime | None, to_at: datetime | None, comparison_facts: list[dict[str, Any]]) -> str:
    if not facts:
        return "Không có dữ liệu phù hợp trong khoảng thời gian đã truy vấn."
    lines = _summary_lines(facts, plan) + ["Kết quả phân tích dựa trên incident đã truy xuất:"]
    for fact in facts:
        label = ", ".join(f"{key}={value}" for key, value in fact.items() if key not in {"evidence_ids"} and key not in ALLOWED_METRICS)
        metrics = ", ".join(f"{metric.name}={fact.get(metric.name)}" for metric in plan.metrics)
        lines.append(f"- {label}: {metrics}; evidence={', '.join(fact['evidence_ids'])}")
    if from_at and to_at:
        lines.append(f"Khoảng thời gian: {from_at.isoformat()} đến {to_at.isoformat()}.")
    if plan.comparison:
        current = sum(int(fact.get("failure_count") or 0) for fact in facts)
        previous = sum(int(fact.get("failure_count") or 0) for fact in comparison_facts)
        direction = "tăng" if current > previous else "giảm" if current < previous else "không đổi"
        lines.append(f"So với kỳ trước: {direction} ({current} so với {previous}).")
    return "\n".join(lines)


def _summary_lines(facts: list[dict[str, Any]], plan: QueryPlan) -> list[str]:
    """State the ranking conclusion before the detailed, cited evidence."""
    if plan.order_by != "failure_count" or not plan.group_by:
        return []
    highest = max(int(fact.get("failure_count") or 0) for fact in facts)
    leaders = [
        str(fact.get(plan.group_by[0]) or "—")
        for fact in facts
        if int(fact.get("failure_count") or 0) == highest
    ]
    if len(leaders) == 1:
        return [f"{leaders[0]} gặp lỗi nhiều nhất: {highest} incident đã xác nhận."]
    return [
        "Không có connector nào gặp lỗi nhiều hơn; "
        f"{', '.join(leaders)} đồng hạng với {highest} incident đã xác nhận."
    ]
