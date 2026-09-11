from __future__ import annotations

import re

from self_healthy_kafka.rag.models import Route, RouteDecision

_ANALYTICS_TERMS = {
    "bao nhiêu",
    "tổng",
    "nhiều nhất",
    "ít nhất",
    "xếp hạng",
    "ranking",
    "trung bình",
    "tỷ lệ",
    "xu hướng",
    "tăng",
    "giảm",
    "thống kê",
    "count",
    "thường xuyên",
    "hay gặp",
    "phổ biến nhất",
}
_RUNBOOK_TERMS = {
    "xử lý",
    "khắc phục",
    "làm gì",
    "runbook",
    "hướng dẫn",
    "cách sửa",
    "nguyên nhân",
    "tại sao",
    "diagnose",
    "troubleshoot",
    "resolve",
    "fix",
}
_INCIDENT_TERMS = {
    "đang lỗi",
    "bị lỗi",
    "thất bại",
    "failed",
    "incident",
    "sự cố",
    "timeout",
}
_ERROR_DETAIL_TERMS = {
    "nội dung lỗi",
    "thông báo lỗi",
    "message lỗi",
    "error message",
    "lỗi đầy đủ",
    "lỗi gì",
    "ý nghĩa",
}
_KNOWN_CLASSES = {
    "oracle": "oracle",
    "jdbc": "jdbc",
    "kafka connect": "kafka-connect",
    "schema registry": "schema-registry",
    "network": "network",
}


class RunbookRouter:
    """Schema-free deterministic router for non-technical user wording."""

    def route(self, question: str) -> RouteDecision:
        clean = question.strip()
        text = clean.casefold()
        error_codes = tuple(dict.fromkeys(_technical_codes(clean)))
        matched_classes = tuple(
            dict.fromkeys(value for term, value in _KNOWN_CLASSES.items() if term in text)
        )
        # A multi-domain question is intentionally ambiguous. Applying the first
        # matching class as a server-side payload filter would silently hide valid
        # runbooks from every other class named by the user.
        connector_class = matched_classes[0] if len(matched_classes) == 1 else None
        connector_name = _connector_name(clean)
        analytics = any(term in text for term in _ANALYTICS_TERMS)
        procedural = any(term in text for term in _RUNBOOK_TERMS)
        knowledge_lookup = any(
            term in text for term in ("runbook cho", "runbook nào", "có hướng dẫn")
        )
        incident = bool(
            error_codes
            or connector_name
            or matched_classes
            or any(term in text for term in _INCIDENT_TERMS)
        )
        error_detail = bool(error_codes) and any(term in text for term in _ERROR_DETAIL_TERMS)
        current_incident = bool(
            connector_name
            or analytics
            or any(
                term in text
                for term in (
                    "đang",
                    "bị",
                    "báo",
                    "trả về",
                    "hiện tại",
                    "incident id",
                    "sự cố này",
                )
            )
        )

        if (
            procedural
            and knowledge_lookup
            and not connector_name
            and not any(term in text for term in ("đang", "bị", "báo"))
        ):
            route = Route.RUNBOOK
        elif procedural and error_codes and not current_incident:
            # A known error-code remediation question is a knowledge lookup. Do not
            # make it depend on a separate Text-to-SQL planning call unless the user
            # also asks about a current connector/incident or an aggregate.
            route = Route.RUNBOOK
        elif procedural and incident:
            route = Route.COMBINED
        elif error_detail:
            # A message lookup must keep the verified incident result instead of
            # replacing it with generic runbook advice.
            route = Route.ANALYTICS
        elif procedural:
            route = Route.RUNBOOK
        elif analytics:
            route = Route.ANALYTICS
        elif incident:
            route = Route.COMBINED
        else:
            route = Route.ANALYTICS
        return RouteDecision(
            route=route,
            connector_name=connector_name,
            connector_class=connector_class,
            error_codes=error_codes,
            time_scope=_time_scope(text),
        )


def validate_route(value: object) -> RouteDecision:
    if isinstance(value, RouteDecision):
        return value
    if not isinstance(value, dict):
        raise ValueError("router output must be an object")
    try:
        route = Route(str(value["route"]))
    except (KeyError, ValueError) as exc:
        raise ValueError("router route must be analytics, runbook or combined") from exc
    codes = value.get("error_codes") or []
    if not isinstance(codes, list):
        raise ValueError("router error_codes must be a list")
    return RouteDecision(
        route=route,
        connector_name=_optional_text(value.get("connector_name")),
        connector_class=_optional_text(value.get("connector_class")),
        error_codes=tuple(str(item).upper() for item in codes),
        time_scope=_optional_text(value.get("time_scope")),
        needs_clarification=bool(value.get("needs_clarification", False)),
        clarification_question=_optional_text(value.get("clarification_question")),
    )


def _time_scope(text: str) -> str | None:
    return next(
        (
            value
            for term, value in {
                "hôm nay": "today",
                "hôm qua": "yesterday",
                "tuần này": "this_week",
                "tuần trước": "last_week",
                "tháng này": "this_month",
                "7 ngày": "last_7_days",
            }.items()
            if term in text
        ),
        None,
    )


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _technical_codes(text: str) -> list[str]:
    patterns = (
        r"\bORA-\d{5}\b",
        r"\bSQLSTATE-?[0-9A-Z]{5}\b",
        r"\bHTTP-?\d{3}\b",
        r"\b[A-Z][A-Z0-9]+(?:_[A-Z0-9]+){1,6}\b",
    )
    return [
        match.group(0).upper() for pattern in patterns for match in re.finditer(pattern, text, re.I)
    ]


def _connector_name(text: str) -> str | None:
    match = re.search(r"\bconnector\s+[`'\"]?([A-Za-z0-9][A-Za-z0-9._-]{2,})", text, re.I)
    if not match:
        return None
    value = match.group(1).strip("`'\"")
    if value.casefold() in {
        "này",
        "nào",
        "đang",
        "oracle",
        "jdbc",
        "kafka",
        "sink",
        "source",
        "thường",
        "báo",
        "không",
    }:
        return None
    return value
