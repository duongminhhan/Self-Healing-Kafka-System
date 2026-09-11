from __future__ import annotations

import json
import re
import unicodedata
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from self_healthy_kafka.config import AnalyticsChatConfig, RagConfig
from self_healthy_kafka.rag.answer_composer import (
    GroundedAnswerComposer,
    QwenJsonGenerator,
    chat_completions_url,
)
from self_healthy_kafka.rag.qdrant_store import QdrantRunbookStore
from self_healthy_kafka.rag.retriever import RunbookRetriever
from self_healthy_kafka.rag.shadow import ShadowRetrievalCoordinator
from self_healthy_kafka.rag.workflow import RunbookRagWorkflow
from self_healthy_kafka.redaction import redact_text
from self_healthy_kafka.storage.common import json_safe
from self_healthy_kafka.webhook.analytics import (
    MAX_LIMIT,
    QueryPlan,
    TimeRange,
    parse_plan,
    resolve_time_range,
)

CATALOG = {
    "dataset": "connector_incidents",
    "fields": [
        "job_name",
        "connector_name",
        "error_code",
        "failure_code",
        "final_outcome",
        "failure_at",
    ],
    "metrics": ["failure_count", "recovered_count", "open_count", "average_recovery_minutes"],
    "event_types": ["HEALTH_FAILED_CONFIRMED"],
    "outcomes": ["RECOVERED", "FAILED", "ESCALATED", "OPEN"],
}

_CONVERSATION_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_CONTEXT_RESET_PHRASES = {
    "bat dau lai",
    "chuyen chu de moi",
    "reset context",
    "reset conversation",
    "xoa ngu canh",
}
_ORDINAL_WORDS = {"nhat": 1, "hai": 2, "ba": 3, "tu": 4, "nam": 5}
_RELATIVE_TIME_PHRASES = {
    "hom nay": "today",
    "hom qua": "yesterday",
    "7 ngay qua": "last_7_days",
    "tuan nay": "this_week",
    "tuan truoc": "last_week",
    "thang nay": "this_month",
}
_MAX_ERROR_MESSAGE_CHARS = 2_000
_MAX_ANALYTICS_ROWS = 1_000
_FACT_FETCH_LIMIT = _MAX_ANALYTICS_ROWS + 1
_TECHNICAL_CODE = re.compile(
    r"\b(?:ORA-\d{5}|SQLSTATE-?[0-9A-Z]{5}|HTTP-?\d{3}|"
    r"[A-Z][A-Z0-9]+(?:_[A-Z0-9]+){1,6})\b",
    re.IGNORECASE,
)
_EXCEPTION_CLASS = re.compile(
    r"(?:[A-Za-z_$][\w$]*\.)*([A-Z][A-Za-z0-9_$]*(?:Exception|Error))\b"
)


class ChatInputError(ValueError):
    """The browser supplied an invalid chat request."""


class ChatPlanningError(RuntimeError):
    """The model response could not be converted to the safe analytics contract."""


@dataclass(frozen=True)
class _ConversationState:
    """Bounded semantic state; never stores raw chat history or naturalized answers."""

    expires_at: datetime
    route: str
    resolved_entity: str | None
    plan: QueryPlan | None
    facts: tuple[dict[str, Any], ...]
    selected_connector: str | None
    selected_error_code: str | None
    evidence_ids: tuple[str, ...]
    from_at: str | None
    to_at: str | None


@dataclass(frozen=True)
class _FollowUp:
    kind: str
    ordinal: int | None = None
    time_range: str | None = None


class AnalyticsChatService:
    """Plans safe incident analysis; neither this class nor its model executes SQL."""

    def __init__(
        self,
        config: AnalyticsChatConfig,
        *,
        incident_facts: Callable[..., list[dict[str, Any]]],
        client: httpx.Client | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
        rag_config: RagConfig | None = None,
        rag_workflow: RunbookRagWorkflow | None = None,
    ):
        self._config = config
        self._incident_facts = incident_facts
        self._owns_client = client is None
        self._client = client or httpx.Client()
        self._now = now
        self._rag_config = rag_config
        self._qwen = QwenJsonGenerator(config, self._client)
        self._rag_workflow = rag_workflow
        self._closed = False
        self._conversation_states: OrderedDict[str, _ConversationState] = OrderedDict()
        self._conversation_lock = RLock()
        if self._rag_workflow is None and rag_config is not None and rag_config.enabled:
            retrieval_mode = getattr(
                rag_config,
                "effective_retrieval_mode",
                rag_config.search_mode,
            )
            primary_mode = "dense" if retrieval_mode == "shadow" else retrieval_mode
            canonical_mode = bool(
                getattr(rag_config, "retrieval_mode", "")
                or getattr(rag_config, "hybrid_shadow_enabled", False)
            )
            if canonical_mode:
                primary_collection = (
                    getattr(rag_config, "dense_collection", rag_config.collection)
                    if primary_mode == "dense"
                    else getattr(rag_config, "hybrid_collection", rag_config.collection)
                )
            else:
                # RAG_SEARCH_MODE/QDRANT_COLLECTION remain a backward-compatible pair.
                primary_collection = rag_config.collection
            primary_config = _retrieval_config(
                rag_config,
                mode=primary_mode,
                collection=primary_collection,
            )
            retriever: RunbookRetriever | ShadowRetrievalCoordinator = RunbookRetriever(
                primary_config,
                QdrantRunbookStore(primary_config),
            )
            if retrieval_mode == "shadow":
                shadow_config = _retrieval_config(
                    rag_config,
                    mode="hybrid",
                    collection=getattr(rag_config, "hybrid_collection", rag_config.collection),
                    timeout_seconds=float(
                        getattr(
                            rag_config,
                            "shadow_timeout_seconds",
                            rag_config.request_timeout_seconds,
                        )
                    ),
                )
                retriever = ShadowRetrievalCoordinator(
                    retriever,
                    RunbookRetriever(shadow_config, QdrantRunbookStore(shadow_config)),
                    sample_rate=float(getattr(rag_config, "shadow_sample_rate", 0.0)),
                    queue_size=int(getattr(rag_config, "shadow_queue_size", 100)),
                    shutdown_timeout_seconds=float(
                        getattr(
                            rag_config,
                            "shadow_timeout_seconds",
                            rag_config.request_timeout_seconds,
                        )
                    ),
                )
            self._rag_workflow = RunbookRagWorkflow(
                rag_config,
                retriever=retriever,
                composer=GroundedAnswerComposer(self._qwen.generate),
            )

    @property
    def enabled(self) -> bool:
        return self._config.enabled or bool(self._rag_config and self._rag_config.enabled)

    @property
    def path(self) -> str:
        return "/api/v1/chat"

    def validate(self) -> None:
        if not self.enabled:
            return
        try:
            ZoneInfo(self._config.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("CHAT_ANALYTICS_TIMEZONE is invalid") from exc
        if bool(self._config.hf_endpoint_url) != bool(self._config.hf_token):
            raise ValueError("HF_CHAT_ENDPOINT_URL and HF_CHAT_TOKEN must be configured together")
        if self._config.hf_endpoint_url and not self._config.hf_model_id:
            raise ValueError("HF_CHAT_MODEL_ID is required with Hugging Face endpoint")
        if not 1 <= self._config.conversation_ttl_seconds <= 86_400:
            raise ValueError("CHAT_CONVERSATION_TTL_SECONDS must be between 1 and 86400")
        if not 1 <= self._config.conversation_max_entries <= 10_000:
            raise ValueError("CHAT_CONVERSATION_MAX_ENTRIES must be between 1 and 10000")
        if self._rag_config is not None and self._rag_config.enabled:
            self._rag_config.validate()
            missing_hf = [
                name
                for name, value in (
                    ("HF_CHAT_ENDPOINT_URL", self._config.hf_endpoint_url),
                    ("HF_CHAT_TOKEN", self._config.hf_token),
                    ("HF_CHAT_MODEL_ID", self._config.hf_model_id),
                )
                if not value.strip()
            ]
            if missing_hf:
                raise ValueError(
                    "RAG is enabled but Qwen configuration is missing: " + ", ".join(missing_hf)
                )

    def ask(self, question: str, *, conversation_id: str | None = None) -> dict[str, Any]:
        question = question.strip()
        if not question or len(question) > 4_000:
            raise ChatInputError("question must contain between 1 and 4000 characters")
        conversation_id = _validate_conversation_id(conversation_id)
        normalized = _normalize_text(question)
        if conversation_id and normalized in _CONTEXT_RESET_PHRASES:
            self.clear_conversation(conversation_id)
            return {
                "answer": "Mình đã xóa ngữ cảnh của cuộc trò chuyện này. Bạn có thể bắt đầu câu hỏi mới.",
                "route": "conversation",
                "source": "deterministic_fallback",
                "status": "ok",
                "citations": [],
                "conversation": {
                    "id": conversation_id,
                    "context_used": False,
                    "action": "reset",
                },
            }

        state = self._get_conversation(conversation_id) if conversation_id else None
        follow_up = _detect_follow_up(question) if conversation_id else None
        context_used = False
        context_action = "none"
        if follow_up is not None:
            if follow_up.kind == "remediation":
                if state is None or not state.selected_error_code:
                    result = _missing_context_response(follow_up)
                    context_action = "clarification_missing_error_context"
                elif self._rag_workflow is None:
                    result = _missing_context_response(follow_up)
                    context_action = "clarification_runbook_unavailable"
                else:
                    contextual_question = (
                        f"{question.rstrip(' ?')} cho lỗi {state.selected_error_code} là gì?"
                    )
                    result = self._rag_workflow.ask(
                        contextual_question,
                        analytics_ask=self._ask_analytics,
                    )
                    context_used = True
                    context_action = "inherit_verified_error_for_remediation"
            elif state is None or state.plan is None:
                result = _missing_context_response(follow_up)
                context_action = "clarification_missing_context"
            elif follow_up.kind == "ordinal":
                result = _ordinal_follow_up_response(state, follow_up.ordinal or 1)
                context_used = result.get("status") == "ok"
                context_action = "reuse_verified_ranking" if context_used else "clarification"
            else:
                inherited_plan = replace(
                    state.plan,
                    time_range=TimeRange("relative", str(follow_up.time_range)),
                )
                result = self._ask_analytics(question, plan_override=inherited_plan)
                result = _analytics_envelope(result)
                context_used = True
                context_action = "inherit_plan_with_time_override"
        elif self._rag_workflow is not None:
            result = self._rag_workflow.ask(question, analytics_ask=self._ask_analytics)
        else:
            result = self._ask_analytics(question)

        internal_state = result.pop("_conversation_state", None)
        if conversation_id:
            if isinstance(internal_state, dict):
                self._remember_conversation(conversation_id, result, internal_state)
            elif (
                not context_used
                and result.get("route") in {"runbook", "combined"}
                and not result.get("query_plan")
            ):
                self.clear_conversation(conversation_id)
                context_action = "reset_on_topic_change"
            result["conversation"] = {
                "id": conversation_id,
                "context_used": context_used,
                "action": context_action,
            }
        return result

    def clear_conversation(self, conversation_id: str) -> None:
        validated_id = _validate_conversation_id(conversation_id)
        if validated_id is None:
            return
        with self._conversation_lock:
            self._conversation_states.pop(validated_id, None)

    def _get_conversation(self, conversation_id: str) -> _ConversationState | None:
        now = self._now()
        with self._conversation_lock:
            self._evict_expired_conversations(now)
            state = self._conversation_states.get(conversation_id)
            if state is not None:
                self._conversation_states.move_to_end(conversation_id)
            return state

    def _remember_conversation(
        self,
        conversation_id: str,
        result: dict[str, Any],
        internal: dict[str, Any],
    ) -> None:
        plan = internal.get("plan")
        facts = internal.get("facts")
        if not isinstance(plan, QueryPlan) or not isinstance(facts, list):
            return
        selected = internal.get("selected_connector")
        if not isinstance(selected, str):
            selected = _selected_connector(facts[0], plan) if len(facts) == 1 else None
        selected_error = internal.get("selected_error_code")
        if not isinstance(selected_error, str):
            selected_error = _selected_error_code(facts[0]) if facts else None
        state = _ConversationState(
            expires_at=self._now() + timedelta(seconds=self._config.conversation_ttl_seconds),
            route=str(result.get("route") or "analytics"),
            resolved_entity=plan.dataset,
            plan=plan,
            facts=tuple(dict(fact) for fact in facts[:MAX_LIMIT]),
            selected_connector=selected,
            selected_error_code=selected_error,
            evidence_ids=tuple(str(value) for value in result.get("evidence_ids") or ())[:MAX_LIMIT],
            from_at=result.get("from_at"),
            to_at=result.get("to_at"),
        )
        with self._conversation_lock:
            self._evict_expired_conversations(self._now())
            self._conversation_states[conversation_id] = state
            self._conversation_states.move_to_end(conversation_id)
            while len(self._conversation_states) > self._config.conversation_max_entries:
                self._conversation_states.popitem(last=False)

    def _evict_expired_conversations(self, now: datetime) -> None:
        expired = [
            key for key, state in self._conversation_states.items() if state.expires_at <= now
        ]
        for key in expired:
            self._conversation_states.pop(key, None)

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            close = getattr(self._rag_workflow, "close", None)
            if callable(close):
                close()
        finally:
            if self._owns_client:
                self._client.close()

    def _ask_analytics(
        self,
        question: str,
        *,
        plan_override: QueryPlan | None = None,
    ) -> dict[str, Any]:
        intent = _question_intent(question)
        plan = plan_override or self._plan(question)
        from_at, to_at = resolve_time_range(
            plan.time_range, now=self._now(), timezone_name=self._config.timezone
        )
        raw_rows = self._incident_facts(
            from_at=from_at,
            to_at=to_at,
            event_type=plan.event_types[0] if plan.event_types else None,
            final_outcome=plan.outcomes[0] if plan.outcomes else None,
            connector_name=plan.connector_name,
            error_code=_database_error_filter(plan.error_code),
            # Fetch a bounded fact packet before applying group-by/top-N.  Passing
            # the display limit here could make a top-5 aggregate incomplete.
            limit=_FACT_FETCH_LIMIT,
        )
        evidence_truncated = len(raw_rows) > _MAX_ANALYTICS_ROWS
        raw_rows = raw_rows[:_MAX_ANALYTICS_ROWS]
        rows = _prepare_incident_rows(raw_rows)
        if _database_error_filter(plan.error_code) is None:
            rows = _filter_failure_code(rows, plan.error_code)
        facts = _aggregate(rows, plan)
        comparison_facts = []
        comparison_rows: list[dict[str, Any]] = []
        if plan.comparison and from_at and to_at:
            interval = to_at - from_at
            comparison_rows = _prepare_incident_rows(self._incident_facts(
                from_at=from_at - interval,
                to_at=from_at,
                event_type=plan.event_types[0] if plan.event_types else None,
                final_outcome=plan.outcomes[0] if plan.outcomes else None,
                connector_name=plan.connector_name,
                error_code=_database_error_filter(plan.error_code),
                limit=_FACT_FETCH_LIMIT,
            ))
            evidence_truncated = evidence_truncated or len(comparison_rows) > _MAX_ANALYTICS_ROWS
            comparison_rows = comparison_rows[:_MAX_ANALYTICS_ROWS]
            if _database_error_filter(plan.error_code) is None:
                comparison_rows = _filter_failure_code(comparison_rows, plan.error_code)
            comparison_facts = _aggregate(comparison_rows, plan)
        sources = [{
            "source": "vConnectorIncidentFacts",
            "count": len(rows),
            "items": [json_safe(row) for row in rows[:MAX_LIMIT]],
        }]
        if comparison_rows:
            sources.append({
                "source": "vConnectorIncidentFacts.previous_period",
                "count": len(comparison_rows),
                "items": [json_safe(row) for row in comparison_rows[:MAX_LIMIT]],
            })
        answer = _answer(
                facts,
                plan,
                from_at,
                to_at,
                comparison_facts,
                rows=rows,
                intent=intent,
            )
        if evidence_truncated:
            answer = (
                "Mình chưa thể xác nhận kết quả chính xác vì phạm vi truy vấn vượt quá "
                f"{_MAX_ANALYTICS_ROWS} incident. Cần dùng phép tổng hợp tại database "
                "thay vì suy luận từ một tập dữ liệu bị cắt."
            )
        return {
            "answer": answer,
            "sources": sources,
            "query_plan": plan.to_dict(),
            "from_at": from_at.isoformat() if from_at else None,
            "to_at": to_at.isoformat() if to_at else None,
            "row_count": len(rows),
            "evidence_ids": [
                str(row.get("incident_id"))
                for row in (rows + comparison_rows)[:MAX_LIMIT]
            ],
            "status": "no_answer" if evidence_truncated else "ok",
            "reason": "analytics_evidence_truncated" if evidence_truncated else None,
            "fallback_reason": "analytics_evidence_truncated" if evidence_truncated else None,
            "_conversation_state": {
                "plan": plan,
                "facts": facts,
                "selected_error_code": _selected_error_code(facts[0]) if facts else None,
            },
        }

    def _plan(self, question: str) -> QueryPlan:
        if _question_intent(question) in {"top_error", "error_detail"}:
            return _fallback_plan(question)
        if not self._config.hf_endpoint_url:
            return _fallback_plan(question)
        response = self._client.post(
            chat_completions_url(self._config.hf_endpoint_url),
            headers={"Authorization": f"Bearer {self._config.hf_token}"},
            json={
                "model": self._config.hf_model_id,
                "response_format": {"type": "json_object"},
                "messages": [
                    {"role": "system", "content": _planner_prompt()},
                    {"role": "user", "content": question},
                ],
                "temperature": 0,
                "max_tokens": 700,
            },
            timeout=self._config.hf_request_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
        try:
            content = payload["choices"][0]["message"]["content"]
            return _enforce_question_semantics(question, parse_plan(json.loads(content)))
        except (KeyError, IndexError, TypeError, json.JSONDecodeError, ValueError) as exc:
            raise ChatPlanningError(
                "Hugging Face planner did not return a valid query plan"
            ) from exc


def _normalize_text(value: str) -> str:
    decomposed = unicodedata.normalize("NFKD", value.casefold())
    plain = "".join(character for character in decomposed if not unicodedata.combining(character))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", plain).split())


def _question_intent(question: str) -> str:
    """Recognize a small set of business intents that require deterministic semantics."""

    normalized = _normalize_text(question)
    top = any(
        phrase in normalized
        for phrase in ("pho bien nhat", "thuong gap nhat", "nhieu nhat", "top 1")
    )
    error_subject = any(
        phrase in normalized
        for phrase in ("loi", "ma loi", "error", "failure", "nguyen nhan")
    )
    connector_ranking = bool(
        re.search(
            r"\b(?:connector|job)\b.{0,40}\b(?:nhieu nhat|thuong xuyen|hay gap)\b",
            normalized,
        )
    )
    if top and error_subject and not connector_ranking:
        return "top_error"
    if _TECHNICAL_CODE.search(question) and any(
        phrase in normalized
        for phrase in (
            "noi dung loi",
            "thong bao loi",
            "message loi",
            "error message",
            "loi day du",
            "loi gi",
            "y nghia",
        )
    ):
        return "error_detail"
    return "analytics"


def _prepare_incident_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for source in rows:
        row = dict(source)
        message = row.get("error_message")
        if isinstance(message, str) and message.strip():
            row["error_message"] = redact_text(message.strip())[:_MAX_ERROR_MESSAGE_CHARS]
        else:
            row.pop("error_message", None)
        row["failure_code"] = _failure_signature(row)
        prepared.append(row)
    return prepared


def _failure_signature(row: dict[str, Any]) -> str | None:
    explicit = str(row.get("error_code") or "").strip().upper()
    if explicit:
        return explicit
    message = str(row.get("error_message") or "")
    code = _TECHNICAL_CODE.search(message)
    if code:
        return code.group(0).upper()
    error_code = re.search(r"\bError Code:\s*([A-Za-z][A-Za-z0-9_-]{2,80})", message, re.I)
    if error_code:
        return error_code.group(1)
    exceptions = _EXCEPTION_CLASS.findall(message)
    if exceptions:
        return exceptions[-1]
    return None


def _database_error_filter(error_code: str | None) -> str | None:
    """The SQL view currently has an exact indexed projection only for Oracle codes."""

    if error_code and re.fullmatch(r"ORA-\d{5}", error_code, re.IGNORECASE):
        return error_code.upper()
    return None


def _filter_failure_code(
    rows: list[dict[str, Any]],
    requested: str | None,
) -> list[dict[str, Any]]:
    if not requested:
        return rows
    expected = requested.upper()
    return [
        row
        for row in rows
        if str(row.get("failure_code") or "").upper() == expected
    ]


def _enforce_question_semantics(question: str, plan: QueryPlan) -> QueryPlan:
    if _question_intent(question) == "top_error":
        return replace(
            plan,
            metrics=plan.metrics
            if any(metric.name == "failure_count" for metric in plan.metrics)
            else _fallback_plan(question).metrics,
            group_by=("failure_code",),
            order_by="failure_count",
            direction="desc",
            limit=1,
        )
    return plan


def _validate_conversation_id(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not _CONVERSATION_ID.fullmatch(value):
        raise ChatInputError("conversation_id is invalid")
    return value


def _detect_follow_up(question: str) -> _FollowUp | None:
    normalized = _normalize_text(question)
    contextual = any(
        marker in normalized
        for marker in ("con ", "thi sao", "the nao", "vay ", "truong hop do")
    )
    ordinal_match = re.search(r"\bconnector\s+(?:thu\s+)?(\d+|nhat|hai|ba|tu|nam)\b", normalized)
    if ordinal_match:
        raw = ordinal_match.group(1)
        ordinal = int(raw) if raw.isdigit() else _ORDINAL_WORDS[raw]
        return _FollowUp("ordinal", ordinal=ordinal)
    if (
        not _TECHNICAL_CODE.search(question)
        and not re.search(r"\bconnector\s+[a-z0-9._-]{3,}\b", normalized)
        and re.fullmatch(
            r"(?:(?:vay|the|con) )?(?:cach|huong) (?:xu ly|khac phuc)"
            r"(?: (?:loi|su co)(?: nay| do)?)?(?: la gi)?",
            normalized,
        )
    ):
        return _FollowUp("remediation")
    if contextual:
        for phrase, value in _RELATIVE_TIME_PHRASES.items():
            if phrase in normalized:
                return _FollowUp("time", time_range=value)
    return None


def _missing_context_response(follow_up: _FollowUp) -> dict[str, Any]:
    if follow_up.kind == "ordinal":
        answer = (
            "Mình chưa có kết quả xếp hạng trước đó trong cuộc trò chuyện này. "
            "Bạn muốn xếp hạng connector theo số incident, số healing log hay chỉ số nào khác?"
        )
    elif follow_up.kind == "remediation":
        answer = (
            "Mình chưa xác định được lỗi nào từ ngữ cảnh trước. "
            "Bạn hãy cho biết mã lỗi hoặc tên lỗi cần xử lý."
        )
    else:
        answer = (
            "Mình chưa có câu hỏi trước đó để biết cần áp dụng mốc thời gian này cho chỉ số nào. "
            "Bạn muốn xem số incident, tỷ lệ phục hồi hay thời gian xử lý?"
        )
    return {
        "answer": answer,
        "route": "analytics",
        "source": "deterministic_fallback",
        "status": "needs_clarification",
        "reason": "missing_compatible_context",
        "fallback_reason": "missing_compatible_context",
        "citations": [],
        "sources": [],
        "query_plan": None,
        "row_count": 0,
        "evidence_ids": [],
    }


def _ordinal_follow_up_response(state: _ConversationState, ordinal: int) -> dict[str, Any]:
    plan = state.plan
    if (
        plan is None
        or ordinal < 1
        or not set(plan.group_by).intersection({"job_name", "connector_name"})
    ):
        return _missing_context_response(_FollowUp("ordinal", ordinal=ordinal))
    if ordinal > len(state.facts):
        return {
            **_missing_context_response(_FollowUp("ordinal", ordinal=ordinal)),
            "answer": (
                f"Kết quả trước chỉ có {len(state.facts)} connector, nên chưa có vị trí thứ {ordinal}. "
                "Bạn có muốn mình chạy lại với phạm vi rộng hơn không?"
            ),
            "reason": "ordinal_out_of_range",
            "fallback_reason": "ordinal_out_of_range",
        }
    fact = dict(state.facts[ordinal - 1])
    connector = _selected_connector(fact, plan) or "connector chưa xác định"
    metric_text = _fact_metric_text(fact, plan)
    tie_note = ""
    if ordinal > 1:
        previous = state.facts[ordinal - 2].get(plan.order_by)
        current = fact.get(plan.order_by)
        if previous is not None and previous == current:
            tie_note = " Connector này đồng hạng với vị trí ngay trước đó."
    return {
        "answer": f"Ở vị trí thứ {ordinal} là {connector}, với {metric_text}.{tie_note}",
        "route": "analytics",
        "source": "conversation_verified_result",
        "status": "ok",
        "reason": None,
        "fallback_reason": None,
        "citations": [],
        "sources": [{"source": "conversation_verified_result", "count": 1, "items": [fact]}],
        "query_plan": plan.to_dict(),
        "from_at": state.from_at,
        "to_at": state.to_at,
        "row_count": 1,
        "evidence_ids": list(fact.get("evidence_ids") or state.evidence_ids),
        "_conversation_state": {
            "plan": plan,
            "facts": [dict(item) for item in state.facts],
            "selected_connector": connector,
        },
    }


def _selected_connector(fact: dict[str, Any], plan: QueryPlan) -> str | None:
    for field in ("connector_name", "job_name"):
        value = fact.get(field)
        if field in plan.group_by and isinstance(value, str) and value.strip() and value != "—":
            return value.strip()
    return None


def _selected_error_code(fact: dict[str, Any]) -> str | None:
    for field in ("failure_code", "error_code"):
        value = fact.get(field)
        if isinstance(value, str) and value.strip() and value != "—":
            return value.strip()
    return None


def _fact_metric_text(fact: dict[str, Any], plan: QueryPlan) -> str:
    values: list[str] = []
    for metric in plan.metrics:
        value = fact.get(metric.name)
        if metric.name == "failure_count":
            values.append(f"{value} incident đã xác nhận")
        elif metric.name == "recovered_count":
            values.append(f"{value} incident đã phục hồi")
        elif metric.name == "open_count":
            values.append(f"{value} incident chưa có kết quả cuối")
        elif value is None:
            values.append("chưa có thời gian phục hồi hợp lệ để tính trung bình")
        else:
            values.append(f"thời gian phục hồi trung bình {value} phút")
    return ", ".join(values)


def _analytics_envelope(result: dict[str, Any]) -> dict[str, Any]:
    return {
        **result,
        "route": "analytics",
        "source": "analytics",
        "citations": [],
        "fallback_reason": result.get("fallback_reason"),
        "status": result.get("status") or "ok",
        "reason": result.get("reason"),
        "evidence": [],
        "recommended_runbooks": [],
        "candidates": [],
    }


def _retrieval_config(
    config: RagConfig,
    *,
    mode: str,
    collection: str,
    timeout_seconds: float | None = None,
) -> RagConfig:
    """Create one concrete store config while preserving legacy RagConfig callers."""

    available = {item.name for item in fields(config)}
    changes: dict[str, Any] = {
        "search_mode": mode,
        "collection": collection,
    }
    # Newer configuration exposes the orchestration mode separately. Replacing it
    # prevents validation from treating a concrete shadow store as another shadow.
    if "retrieval_mode" in available:
        changes["retrieval_mode"] = mode
    if timeout_seconds is not None:
        changes["request_timeout_seconds"] = timeout_seconds
    return replace(config, **changes)


def _planner_prompt() -> str:
    contract = {
        "dataset": "connector_incidents",
        "metrics": [
            {"name": "failure_count", "aggregation": "count_distinct_incident"}
        ],
        "group_by": ["job_name"],
        "filters": {
            "time_range": {"kind": "relative", "value": "last_7_days"},
            "event_type": ["HEALTH_FAILED_CONFIRMED"],
            "final_outcome": ["RECOVERED"],
            "connector_name": "optional exact connector name",
            "error_code": "optional exact error code",
        },
        "order_by": [{"field": "failure_count", "direction": "desc"}],
        "limit": 5,
        "comparison": "previous_period",
    }
    ranking_example = {
        "dataset": "connector_incidents",
        "metrics": [
            {"name": "failure_count", "aggregation": "count_distinct_incident"}
        ],
        "group_by": ["job_name"],
        "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
        "order_by": [{"field": "failure_count", "direction": "desc"}],
        "limit": 5,
    }
    top_error_example = {
        "dataset": "connector_incidents",
        "metrics": [
            {"name": "failure_count", "aggregation": "count_distinct_incident"}
        ],
        "group_by": ["failure_code"],
        "filters": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
        "order_by": [{"field": "failure_count", "direction": "desc"}],
        "limit": 1,
    }
    incident_example = {
        "dataset": "connector_incidents",
        "metrics": [
            {"name": "failure_count", "aggregation": "count_distinct_incident"}
        ],
        "group_by": ["connector_name", "error_code", "final_outcome"],
        "filters": {
            "event_type": ["HEALTH_FAILED_CONFIRMED"],
            "connector_name": "sample-oracle-orders",
            "error_code": "ORA-01017",
        },
        "order_by": [{"field": "failure_count", "direction": "desc"}],
        "limit": 20,
    }
    return (
        "You are a query planner, not an answer writer. Return exactly one JSON object "
        "and no prose. Never return SQL, database object names, procedures, code, advice, "
        "causes, actions, credentials, or keys outside this contract. The only top-level "
        "keys are dataset, metrics, group_by, filters, order_by, limit, and optional "
        "comparison. metrics must be an array of objects with name and aggregation; "
        "filters must contain every filter. Omit optional filters and comparison when the "
        "question does not request them. Select values only from this semantic catalog: "
        + json.dumps(CATALOG, ensure_ascii=False)
        + ". Valid relative time values are today, yesterday, last_7_days, this_month, "
        "this_week, and last_week. Valid metric-to-aggregation mappings are: failure_count, "
        "recovered_count, and open_count use count_distinct_incident; "
        "average_recovery_minutes uses average_recovery_minutes. Use failure_count and "
        "group_by job_name for connector ranking. Use failure_code, not error_code, for "
        "ranking error types because failure_code also covers non-Oracle failures. A singular "
        "'most common' question uses limit 1. A question that also asks for causes or "
        "actions still requires only an analytics query plan here. Contract shape example "
        "(illustrative optional fields; do not copy filters not asked for): "
        + json.dumps(contract, ensure_ascii=False)
        + ". Example input: Connector nào thường xuyên gặp sự cố nhất? Example output: "
        + json.dumps(ranking_example, ensure_ascii=False)
        + ". Example input: Lỗi phổ biến nhất là gì? Example output: "
        + json.dumps(top_error_example, ensure_ascii=False)
        + ". Example input: Connector sample-oracle-orders đang báo ORA-01017, nguyên nhân "
        "có thể là gì và cần xử lý thế nào? Example output: "
        + json.dumps(incident_example, ensure_ascii=False)
    )


def _fallback_plan(question: str) -> QueryPlan:
    text = question.lower()
    intent = _question_intent(question)
    time_value = next((value for term, value in {
        "hôm nay": "today", "hôm qua": "yesterday", "7 ngày": "last_7_days",
        "tuần trước": "last_week", "tháng này": "this_month",
    }.items() if term in text), None)
    metric = "average_recovery_minutes" if "thời gian" in text else "failure_count"
    outcome = ["OPEN"] if "chưa recovery" in text or "chưa phục hồi" in text else []
    error_match = _TECHNICAL_CODE.search(question)
    error_code = error_match.group(0).upper() if error_match else None
    comparison = "previous_period" if "tăng hay giảm" in text or "so với" in text else None
    group_by = ["failure_code"] if intent == "top_error" else ["job_name"]
    if intent == "error_detail":
        group_by = ["connector_name", "error_code"]
    return parse_plan({
        "dataset": "connector_incidents",
        "metrics": [{
            "name": metric,
            "aggregation": "average_recovery_minutes" if metric == "average_recovery_minutes" else "count_distinct_incident",
        }],
        "group_by": group_by,
        "filters": {**({"time_range": {"kind": "relative", "value": time_value}} if time_value else {}), "event_type": ["HEALTH_FAILED_CONFIRMED"], **({"final_outcome": outcome} if outcome else {}), **({"error_code": error_code} if error_code else {})},
        "order_by": [{"field": metric, "direction": "desc"}],
        "limit": 1 if intent == "top_error" else 20 if intent == "error_detail" else 5,
        **({"comparison": comparison} if comparison else {}),
    })


def _aggregate(rows: list[dict[str, Any]], plan: QueryPlan) -> list[dict[str, Any]]:
    groups: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for row in rows:
        if any(
            field in {"error_code", "failure_code"} and not row.get(field)
            for field in plan.group_by
        ):
            # Unknown errors are not a named error category and must never win a
            # "most common error" ranking merely because many rows are unclassified.
            continue
        key = tuple(str(row.get(field) or "Chưa xác định") for field in plan.group_by) or (
            "Tất cả",
        )
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
                fact["valid_recovery_duration_count"] = len(durations)
                fact["excluded_recovery_duration_count"] = len(items) - len(durations)
        facts.append(fact)
    present = [item for item in facts if item.get(plan.order_by) is not None]
    missing = [item for item in facts if item.get(plan.order_by) is None]
    present.sort(key=lambda item: tuple(str(item.get(field) or "") for field in plan.group_by))
    present.sort(
        key=lambda item: item[plan.order_by],
        reverse=plan.direction == "desc",
    )
    return (present + missing)[: plan.limit]


def _duration(item: dict[str, Any]) -> float | None:
    if item.get("final_outcome") != "RECOVERED" or item.get("queue_status") != "COMPLETED":
        return None
    start, end = item.get("failure_at"), item.get("recovered_at")
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return None
    # MSSQL timestamps are offset-aware. Unknown timezones must not be guessed.
    if start.utcoffset() is None or end.utcoffset() is None:
        return None
    minutes = (end - start).total_seconds() / 60
    return minutes if minutes >= 0 else None


def _answer(
    facts: list[dict[str, Any]],
    plan: QueryPlan,
    from_at: datetime | None,
    to_at: datetime | None,
    comparison_facts: list[dict[str, Any]],
    *,
    rows: list[dict[str, Any]],
    intent: str,
) -> str:
    if not facts:
        return "Không có dữ liệu phù hợp trong khoảng thời gian đã truy vấn."
    if intent == "top_error":
        return _top_error_answer(facts[0], rows)
    if intent == "error_detail":
        return _error_detail_answer(plan.error_code, rows)
    lines = _summary_lines(facts, plan)
    details = facts
    if lines and len(facts) > 1:
        leader_value = facts[0].get(plan.order_by)
        details = [fact for fact in facts if fact.get(plan.order_by) != leader_value]
        if details:
            lines.append("Các kết quả tiếp theo:")
    elif lines:
        details = []
    for fact in details:
        label = ", ".join(
            str(fact.get(key) or "—") for key in plan.group_by
        ) or "Toàn bộ phạm vi"
        metrics = []
        for metric in plan.metrics:
            value = fact.get(metric.name)
            if metric.name == "failure_count":
                metrics.append(f"{value} incident đã xác nhận")
            elif metric.name == "recovered_count":
                metrics.append(f"{value} incident đã phục hồi")
            elif metric.name == "open_count":
                metrics.append(f"{value} incident chưa có kết quả cuối")
            elif value is None:
                metrics.append("chưa có thời gian phục hồi hợp lệ để tính trung bình")
            else:
                metrics.append(f"thời gian phục hồi trung bình {value} phút")
        lines.append(f"{label}: {', '.join(metrics)}.")
    if from_at and to_at:
        lines.append(f"Khoảng thời gian: {from_at.isoformat()} đến {to_at.isoformat()}.")
    if plan.comparison:
        current = sum(int(fact.get("failure_count") or 0) for fact in facts)
        previous = sum(int(fact.get("failure_count") or 0) for fact in comparison_facts)
        direction = "tăng" if current > previous else "giảm" if current < previous else "không đổi"
        lines.append(f"So với kỳ trước: {direction} ({current} so với {previous}).")
    return "\n".join(lines)


def _top_error_answer(leader: dict[str, Any], rows: list[dict[str, Any]]) -> str:
    code = str(leader.get("failure_code") or leader.get("error_code") or "").strip()
    count = int(leader.get("failure_count") or 0)
    matching = [row for row in rows if row.get("failure_code") == code]
    connectors = _unique_text(matching, "job_name")
    messages = _unique_text(matching, "error_message")
    answer = f"Lỗi phổ biến nhất là {code}, xuất hiện trong {count} incident đã xác nhận."
    if connectors:
        answer += " Connector gặp lỗi này: " + ", ".join(connectors) + "."
    if messages:
        answer += " Nội dung lỗi được ghi nhận: " + " | ".join(f"“{item}”" for item in messages[:3])
        answer += "."
    return answer


def _error_detail_answer(error_code: str | None, rows: list[dict[str, Any]]) -> str:
    code = error_code or next(
        (str(row.get("failure_code")) for row in rows if row.get("failure_code")),
        "mã lỗi được hỏi",
    )
    messages = _unique_text(rows, "error_message")
    connectors = _unique_text(rows, "job_name")
    if not messages:
        return f"Mình chưa tìm thấy nội dung log đã xác minh cho {code} trong phạm vi dữ liệu hiện tại."
    answer = f"Nội dung lỗi đầy đủ được ghi nhận cho {code}: "
    answer += " | ".join(f"“{item}”" for item in messages[:3]) + "."
    if connectors:
        answer += " Lỗi xuất hiện trên connector: " + ", ".join(connectors) + "."
    return answer


def _unique_text(rows: list[dict[str, Any]], field: str) -> list[str]:
    result: list[str] = []
    for row in rows:
        value = str(row.get(field) or "").strip()
        if value and value not in result:
            result.append(value)
    return result


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
