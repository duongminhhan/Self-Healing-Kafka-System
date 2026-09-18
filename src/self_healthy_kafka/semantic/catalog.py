"""Versioned business catalog for the chat analytics boundary.

This catalog is intentionally small.  It states what can be calculated from
``vConnectorIncidentFacts`` today and, equally importantly, what cannot.  A
language model receives this contract but never receives table names, stored
procedure names, or SQL.
"""

from __future__ import annotations

from typing import Any

CATALOG_VERSION = "2026-09-11.1"

# Public names are business concepts.  Physical mappings live only in the
# compiler, never in the model prompt.
SEMANTIC_CATALOG: dict[str, Any] = {
    "version": CATALOG_VERSION,
    "source": {
        "kind": "historical_incident_snapshot",
        "display_name_vi": "snapshot incident hiện tại",
        "freshness": "The source records persisted healing incidents. It is not a live Kafka Connect status API.",
        "timezone": "The backend resolves relative dates in the configured business timezone.",
        "truncation": "A result is not asserted when the bounded source packet is truncated.",
        "availability": "No rows, an unavailable source, and a truncated source are distinct outcomes.",
    },
    "entities": {
        "incident": {
            "grain": "one healing queue incident",
            "identity": "incident_id",
            "started_at": "failure_at",
            "outcome": "final_outcome",
            "meaning": "An incident is created after a HEALTH_FAILED_CONFIRMED audit event. It is not a healing-log row.",
            "timestamps": {
                "failure_at": "Timestamp of the confirmed failure; used for incident date filters.",
                "recovered_at": "Queue completion timestamp only when the historical final outcome is RECOVERED.",
            },
        },
        "connector": {
            "dimensions": ["root_connector", "current_connector"],
            "meaning": "root_connector is the logical connector lineage for an incident; current_connector is one deployed connector version. A generic connector ranking uses root_connector unless the user explicitly asks for the current/versioned connector.",
        },
        "error": {
            "dimensions": ["error", "error_code"],
            "meaning": "error is a normalized signature extracted from the confirmed failure; error_code is available only when the source exposes an Oracle code.",
        },
    },
    "metrics": {
        "incident_count": {
            "unit": "incidents",
            "aggregation": "distinct incident identity",
            "meaning": "Count of unique incident_id values, not healing logs and not retry attempts.",
        },
        "recovered_incident_count": {
            "unit": "incidents",
            "aggregation": "distinct incident identity where final_outcome is RECOVERED",
        },
        "open_incident_count": {
            "unit": "incidents",
            "aggregation": "distinct incident identity where final_outcome is OPEN",
        },
        "average_recovery_minutes": {
            "unit": "minutes",
            "aggregation": "mean(recovered_at - failure_at) for RECOVERED incidents whose queue status is COMPLETED and whose timestamps are offset-aware and non-negative",
            "null_behavior": "null when no valid duration exists; never coerce null to zero",
        },
        "recovery_rate": {
            "unit": "percent",
            "aggregation": "recovered distinct incident identity / all distinct incident identities in the same validated scope",
            "numerator": "final_outcome is RECOVERED",
            "denominator": "all incident_id values after the same filters and time range",
            "null_behavior": "null only when the source cannot provide a valid denominator; zero is a valid computed rate when the denominator is positive",
        },
    },
    "dimensions": {
        "root_connector": {"type": "text", "meaning": "Logical connector lineage / original root connector name"},
        "current_connector": {"type": "text", "meaning": "Current deployed connector version, including recreate suffixes such as .001"},
        "error": {"type": "text", "meaning": "Normalized confirmed-failure signature"},
        "error_code": {"type": "text", "meaning": "Source-projected Oracle error code when present"},
        "outcome": {"type": "categorical", "values": ["RECOVERED", "FAILED", "ESCALATED", "OPEN"]},
    },
    # Presentation metadata is intentionally declarative: the renderer uses
    # these values to describe a validated plan without keeping a sentence per
    # intent or a mapping from a user's wording to an answer.
    "presentation": {
        "sources": {
            "historical_incident_snapshot": "snapshot incident hiện tại",
        },
        "subjects": {
            "incident": {"singular_vi": "incident", "plural_vi": "incident"},
            "connector": {"singular_vi": "connector", "plural_vi": "connector"},
            "root_connector": {"singular_vi": "root connector", "plural_vi": "root connector"},
            "current_connector": {"singular_vi": "phiên bản connector", "plural_vi": "phiên bản connector"},
            "error": {"singular_vi": "lỗi", "plural_vi": "lỗi"},
            "error_code": {"singular_vi": "mã lỗi", "plural_vi": "mã lỗi"},
        },
        "fields": {
            "outcome": {
                "label_vi": "trạng thái cuối",
                "equals_template_vi": "có trạng thái {value}",
                "values": {
                    "RECOVERED": "RECOVERED",
                    "FAILED": "FAILED",
                    "ESCALATED": "ESCALATED",
                    "OPEN": "OPEN",
                },
            },
            "connector": {
                "label_vi": "connector",
                "equals_template_vi": "có connector {value}",
            },
            "error_code": {
                "label_vi": "mã lỗi",
                "equals_template_vi": "có mã lỗi {value}",
            },
        },
        "metrics": {
            "incident_count": {"label_vi": "số incident", "unit_vi": "incident"},
            "recovered_incident_count": {"label_vi": "số incident đã phục hồi", "unit_vi": "incident"},
            "open_incident_count": {"label_vi": "số incident chưa có kết quả cuối", "unit_vi": "incident"},
            "average_recovery_minutes": {"label_vi": "thời gian phục hồi trung bình", "unit_vi": "phút"},
            "recovery_rate": {"label_vi": "tỷ lệ phục hồi", "unit_vi": "%"},
        },
        "detail_fields": {
            "error_message": "Nội dung lỗi đã ghi nhận",
            "connector": "Connector",
            "root_connector": "Root connector",
            "outcome": "Kết quả cuối",
            "queue_status": "Trạng thái hàng đợi",
            "recovery_rate_numerator": "Số incident đã phục hồi",
            "recovery_rate_denominator": "Tổng incident trong phạm vi",
        },
        "time_ranges": {
            "today": "trong ngày hôm nay theo múi giờ {timezone}",
            "yesterday": "trong ngày hôm qua theo múi giờ {timezone}",
            "last_7_days": "trong 7 ngày gần đây theo múi giờ {timezone}",
            "last_n_days": "trong {days} ngày gần đây theo múi giờ {timezone}",
            "this_week": "trong tuần này theo múi giờ {timezone}",
            "last_week": "trong tuần trước theo múi giờ {timezone}",
            "this_month": "trong tháng này theo múi giờ {timezone}",
            "absolute_date": "trong ngày đã chỉ định theo múi giờ {timezone}",
        },
        # Presentation limits bound prose only.  They never remove evidence
        # from the verified-result table.
        "summary_policy": {
            "summary_item_limit": 3,
            "summary_detail_limit": 1,
            "more_results_vi": "Còn {count} kết quả đã xác minh khác trong bảng chi tiết.",
            "boundary_tie_vi": "Có thêm {count} kết quả đồng hạng với vị trí thứ {rank}; bảng chi tiết chứa đầy đủ danh sách.",
        },
    },
    "filters": {
        "time_range": {
            "model_contract": "The model selects only kind/value (and days for last_n_days). It must never emit timestamps, timezone, or timestamp_field.",
            "relative_values": ["today", "yesterday", "last_7_days", "last_n_days", "this_week", "last_week", "this_month"],
            "absolute_values": ["absolute_date"],
            "timestamp_field": "failure_at",
        },
        "connector": {"operator": "exact"},
        "error_code": {"operator": "exact"},
        "outcome": {"operator": "in", "values": ["RECOVERED", "FAILED", "ESCALATED", "OPEN"]},
        "event_type": {"operator": "in", "values": ["HEALTH_FAILED_CONFIRMED"]},
    },
    # This vocabulary is a business-level interpretation aid for the planner
    # guardrail.  It intentionally contains reusable terms, rather than
    # question-to-SQL or question-to-answer mappings.  The guardrail uses it
    # only to reject a model plan that changes an explicit user constraint.
    "planning_policy": {
        "unspecified_time_scope": "all_snapshot",
        "default_time_scope": None,
        "time_scope_origins": ["explicit", "inherited", "default", "unspecified"],
        "vocabulary": {
            "subjects": {
                "connector": [
                    "connector", "connectors", "root connector", "root connectors",
                ],
                "current_connector": [
                    "current connector", "connector hien tai", "phien ban hien tai", "connector version",
                ],
                "error_signature": [
                    "loi", "ma loi", "error", "error code", "failure code", "ora",
                ],
            },
            "metrics": {
                "incident_count": ["incident", "su co", "loi", "failure"],
                "healing_log_count": ["healing log", "event log", "retry log", "log healing"],
            },
            "ranking": {
                "descending": [
                    "nhieu nhat", "thuong xuyen nhat", "top", "most", "highest", "worst",
                ],
            },
            "include_ties": [
                "bao gom dong hang", "ke ca dong hang", "cung hang", "dong hang",
                "tat ca o hang", "including ties", "with ties",
            ],
            "time_scopes": {
                "today": ["hom nay", "today"],
                "yesterday": ["hom qua", "yesterday"],
                "last_7_days": ["7 ngay gan day", "last 7 days"],
                "this_week": ["tuan nay", "this week"],
                "last_week": ["tuan truoc", "last week"],
                "this_month": ["thang nay", "this month"],
            },
        },
        "ranking_policy": {
            "default": "exact_limit",
            "include_ties": "Include every entity tied at the Nth dense rank only when the user explicitly asks to include ties.",
            "exact_limit": "Top N means exactly N rows by default; stable ordering is logical connector name ascending after metric order.",
        },
    },
    "intents": {
        "failed_connectors": {
            "meaning": "Connectors whose persisted final incident outcome is FAILED in the requested time range.",
            "required": {"metrics": ["incident_count"], "dimensions_any": ["connector", "root_connector"], "outcome": ["FAILED"]},
        },
        "incidents": {
            "meaning": "Confirmed failure incidents, including incidents that have subsequently RECOVERED.",
            "required": {"event_type": ["HEALTH_FAILED_CONFIRMED"]},
        },
        "top_error_signature": {
            "meaning": "The most frequent normalized failure signature, not the connector with the most incidents.",
            "required": {"metrics": ["incident_count"], "dimensions_any": ["error", "error_code"]},
        },
        "recovery_rate": {
            "meaning": "Recovered incidents divided by all incidents in the same time range and state scope.",
            "required": {"metrics": ["recovery_rate"]},
        },
        "runbook_guidance": {"meaning": "Approved runbook guidance only; it never establishes an incident or live connector state."},
    },
    "relationships": {
        "incident_to_healing_log": {
            "cardinality": "one_to_many",
            "safe_usage": "The chat source projects at most one confirmed failure per incident. It never joins raw healing logs for counting, because that would multiply incident counts.",
        },
    },
    "status_semantics": {
        "RECOVERED": "Historical final outcome only; it does not prove the connector is healthy now.",
        "OPEN": "Persisted queue state with no final outcome in the incident snapshot; it is not a live Kafka Connect probe.",
        "COMPLETED": "Queue processing completed. It is used with RECOVERED for a valid recovery duration.",
    },
    "null_and_duration": {
        "duration": "A recovery duration is valid only for RECOVERED plus COMPLETED with two offset-aware, non-negative timestamps.",
        "missing": "Missing values remain missing. The backend never converts an unavailable aggregate to zero.",
    },
    "detail_fields": {
        "error_message": "Redacted confirmed-failure message. It may be unavailable for an incident.",
        "connector": "Current connector name",
        "root_connector": "Original/root connector name",
        "outcome": "Historical incident outcome, not current live connector health",
        "queue_status": "Persisted queue status",
    },
    "unsupported": {
        "healing_log_count": "The current chat execution source is incident-grain only; it cannot count all healing-log rows without a separately verified log-grain source.",
        "live_connector_status": "Historical RECOVERED/OPEN values cannot prove live Kafka Connect health.",
        "root_cause": "The audit snapshot can show a failure signature and message but cannot prove a root cause without additional evidence.",
    },
}


def catalog_for_model() -> dict[str, Any]:
    """Return only bounded, serializable semantic information for planning."""

    return SEMANTIC_CATALOG
