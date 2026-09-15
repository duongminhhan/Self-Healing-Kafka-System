"""Evaluation-only plans; never imported by the planner or runtime compiler.

Exact holdout questions/plans are excluded from few-shot examples; constituent
operations may be demonstrated. This is not an unseen model-training benchmark.
Live accuracy requires a model
to generate its own plan/SQL; these plans are only offline compiler oracles.
"""

from notebooks.evaluation.evaluate import CASES
from self_healthy_kafka.semantic.catalog import CATALOG_VERSION


def plan(entity="incidents", dimensions=None, metrics=None, **options):
    return {
        "kind": "query",
        "entity": entity,
        "dimensions": ["root"] if dimensions is None else dimensions,
        "metrics": ["incident_count"] if metrics is None else metrics,
        "order_by": [{"field": "root", "direction": "asc"}],
        **options,
    }


PLANS = [
    plan(
        limit=5,
        order_by=[
            {"field": "incident_count", "direction": "desc"},
            {"field": "root", "direction": "asc"},
        ],
    ),
    plan(metrics=["log_count"]),
    plan(
        "events", ["severity"], ["log_count"], order_by=[{"field": "severity", "direction": "asc"}]
    ),
    plan(
        "events",
        ["event_id", "attempt"],
        [],
        filters=[{"field": "attempt", "op": "is_null"}],
        order_by=[{"field": "event_id", "direction": "asc"}],
    ),
    plan(
        filters=[
            {"field": "received_at", "op": "gte", "value": "2026-09-02T00:00:00Z"},
            {"field": "received_at", "op": "lt", "value": "2026-09-03T00:00:00Z"},
        ]
    ),
    plan(having=[{"metric": "incident_count", "op": "lt", "compare_to": "population_mean"}]),
    plan(
        dimensions=[],
        metrics=[
            "avg_duration_minutes",
            "matched_count",
            "valid_duration_count",
            "excluded_duration_count",
        ],
        success_only=True,
        order_by=[],
    ),
]

GOLD = [
    {"id": f"gold-{i + 1}", "split": "gold", "question": question, "reference_sql": sql, "plan": p}
    for i, ((question, sql), p) in enumerate(zip(CASES, PLANS, strict=True))
]

HOLDOUT = [
    {
        "id": "holdout-independent-totals",
        "split": "holdout",
        "question": "Tổng cộng có bao nhiêu incident và healing log?",
        "reference_sql": "SELECT (SELECT COUNT(*) FROM ConnectorHealingQueue), (SELECT COUNT(*) FROM ConnectorHealingLogs)",
        "plan": {
            "kind": "independent",
            "queries": [
                {"kind": "query", "entity": "incidents", "dimensions": [], "metrics": ["incident_count"]},
                {"kind": "query", "entity": "events", "dimensions": [], "metrics": ["log_count"]},
            ],
        },
    },
    {
        "id": "holdout-natural-day-independent-totals",
        "split": "holdout",
        "question": "Ngày 5 tháng 9 năm 2026 có bao nhiêu incident và healing log?",
        "reference_sql": (
            "SELECT "
            "(SELECT COUNT(*) FROM ConnectorHealingQueue WHERE "
            "julianday(ReceivedAt)>=julianday('2026-09-04T17:00:00Z') AND "
            "julianday(ReceivedAt)<julianday('2026-09-05T17:00:00Z')),"
            "(SELECT COUNT(*) FROM ConnectorHealingLogs WHERE "
            "julianday(CreatedAt)>=julianday('2026-09-04T17:00:00Z') AND "
            "julianday(CreatedAt)<julianday('2026-09-05T17:00:00Z'))"
        ),
        "plan": {
            "kind": "independent",
            "queries": [
                {
                    "kind": "query", "entity": "incidents", "dimensions": [],
                    "metrics": ["incident_count"], "filters": [
                        {"field": "received_at", "op": "gte", "value": "2026-09-04T17:00:00Z"},
                        {"field": "received_at", "op": "lt", "value": "2026-09-05T17:00:00Z"},
                    ],
                },
                {
                    "kind": "query", "entity": "events", "dimensions": [],
                    "metrics": ["log_count"], "filters": [
                        {"field": "event_at", "op": "gte", "value": "2026-09-04T17:00:00Z"},
                        {"field": "event_at", "op": "lt", "value": "2026-09-05T17:00:00Z"},
                    ],
                },
            ],
        },
    },
    {
        "id": "holdout-natural-recovery-rate",
        "split": "holdout",
        "question": "Tỷ lệ phục hồi là bao nhiêu?",
        "reference_sql": (
            "SELECT ROUND(100.0*COUNT(CASE WHEN FinalOutcome='RECOVERED' THEN 1 END) "
            "/NULLIF(COUNT(CASE WHEN FinalOutcome IN ('RECOVERED','FAILED','ESCALATED') "
            "THEN 1 END),0),2) FROM ConnectorHealingQueue"
        ),
        "plan": plan(dimensions=[], metrics=["recovery_rate_percent"], order_by=[]),
    },
    {
        "id": "holdout-natural-recovery-duration",
        "split": "holdout",
        "question": "Mất bao lâu để phục hồi?",
        "reference_sql": (
            "WITH durations AS (SELECT (julianday(CompletedAt)-julianday(ReceivedAt))*1440.0 "
            "AS minutes FROM ConnectorHealingQueue WHERE QueueStatus='COMPLETED' "
            "AND FinalOutcome='RECOVERED') SELECT ROUND(AVG(CASE WHEN minutes>=0 THEN minutes END),2),"
            "COUNT(*),COUNT(CASE WHEN minutes>=0 THEN 1 END),COUNT(*)-COUNT(CASE WHEN minutes>=0 THEN 1 END) "
            "FROM durations"
        ),
        "plan": plan(
            dimensions=[],
            metrics=[
                "avg_duration_minutes", "matched_count", "valid_duration_count",
                "excluded_duration_count",
            ],
            success_only=True,
            order_by=[],
        ),
    },
    {
        "id": "holdout-natural-unfinished",
        "split": "holdout",
        "question": "Những connector nào vẫn chưa xử lý xong?",
        "reference_sql": (
            "SELECT RootConnectorName,QueueStatus,COUNT(QueueId) FROM ConnectorHealingQueue "
            "WHERE QueueStatus!='COMPLETED' AND QueueStatus!='ESCALATED' "
            "GROUP BY RootConnectorName,QueueStatus "
            "ORDER BY COUNT(QueueId) DESC,RootConnectorName,QueueStatus"
        ),
        "plan": plan(
            dimensions=["root", "queue_status"],
            metrics=["incident_count"],
            filters=[
                {"field": "queue_status", "op": "ne", "value": "COMPLETED"},
                {"field": "queue_status", "op": "ne", "value": "ESCALATED"},
            ],
            order_by=[
                {"field": "incident_count", "direction": "desc"},
                {"field": "root", "direction": "asc"},
                {"field": "queue_status", "direction": "asc"},
            ],
        ),
    },
    {
        "id": "holdout-ingestion-time-unavailable",
        "split": "holdout",
        "question": "Hôm nay có bao nhiêu incident được insert vào database?",
        "reference_sql": None,
        "plan": None,
        "expected_clarification": True,
    },
    {
        "id": "holdout-grain",
        "split": "holdout",
        "question": "Với mỗi root và QueueStatus, trả root, trạng thái, số incident khác nhau và tổng log. Giữ cả incident không có log. Sắp xếp root rồi trạng thái tăng dần.",
        "reference_sql": "SELECT q.RootConnectorName,q.QueueStatus,COUNT(DISTINCT q.QueueId),COUNT(l.Id) FROM ConnectorHealingQueue q LEFT JOIN ConnectorHealingLogs l ON l.QueueId=q.QueueId GROUP BY q.RootConnectorName,q.QueueStatus ORDER BY q.RootConnectorName,q.QueueStatus",
        "plan": plan(
            dimensions=["root", "queue_status"],
            metrics=["incident_count", "log_count"],
            order_by=[
                {"field": "root", "direction": "asc"},
                {"field": "queue_status", "direction": "asc"},
            ],
        ),
    },
    {
        "id": "holdout-filtered-events",
        "split": "holdout",
        "question": "Theo từng root, có bao nhiêu incident khác nhau có log Severity bằng WARN? Chỉ đếm các log có đúng giá trị WARN, không gộp WARNING. Trả root, số incident; sắp xếp tên root tăng dần.",
        "reference_sql": "SELECT q.RootConnectorName,COUNT(DISTINCT q.QueueId) FROM ConnectorHealingLogs l JOIN ConnectorHealingQueue q ON q.QueueId=l.QueueId WHERE l.Severity='WARN' GROUP BY q.RootConnectorName ORDER BY q.RootConnectorName",
        "plan": plan(entity="events", filters=[{"field": "severity", "op": "eq", "value": "WARN"}]),
    },
    {
        "id": "holdout-latest",
        "split": "holdout",
        "question": "Chỉ lấy root có ít nhất 2 healing log trên toàn bộ incident. Trả tên root, tổng số log, QueueStatus của incident mới nhất theo ReceivedAt rồi QueueId giảm dần. Sắp xếp số log giảm dần rồi tên root tăng dần. Không suy ra trạng thái connector live.",
        "reference_sql": "WITH ranked AS (SELECT RootConnectorName,QueueStatus,ROW_NUMBER() OVER(PARTITION BY RootConnectorName ORDER BY julianday(ReceivedAt) DESC,QueueId DESC) rn FROM ConnectorHealingQueue), counts AS (SELECT q.RootConnectorName,COUNT(l.Id) n FROM ConnectorHealingQueue q LEFT JOIN ConnectorHealingLogs l ON l.QueueId=q.QueueId GROUP BY q.RootConnectorName) SELECT c.RootConnectorName,c.n,r.QueueStatus FROM counts c JOIN ranked r ON r.RootConnectorName=c.RootConnectorName AND r.rn=1 WHERE c.n>=2 ORDER BY c.n DESC,c.RootConnectorName",
        "plan": plan(
            latest_status=True,
            metrics=["log_count"],
            having=[{"metric": "log_count", "op": "gte", "value": 2}],
            order_by=[
                {"field": "log_count", "direction": "desc"},
                {"field": "root", "direction": "asc"},
            ],
        ),
    },
    {
        "id": "holdout-ambiguous",
        "split": "holdout",
        "question": "Connector nào tệ nhất?",
        "reference_sql": None,
        "plan": None,
        "expected_clarification": True,
    },
]

GOLD_EXTENDED = [
    {
        "id": "gold-natural-incident-ranking-typo",
        "split": "gold",
        "question": "Connnector nao thuong xuyen gap su co nhat?",
        "reference_sql": (
            "SELECT RootConnectorName,COUNT(DISTINCT QueueId) FROM ConnectorHealingQueue "
            "GROUP BY RootConnectorName ORDER BY COUNT(DISTINCT QueueId) DESC,"
            "RootConnectorName ASC LIMIT 1"
        ),
        "plan": plan(
            limit=1,
            order_by=[
                {"field": "incident_count", "direction": "desc"},
                {"field": "root", "direction": "asc"},
            ],
        ),
    },
    {
        "id": "gold-status-or-filter",
        "split": "gold",
        "question": "Có bao nhiêu sự cố đang chờ hoặc đã chuyển cấp?",
        "reference_sql": (
            "SELECT COUNT(DISTINCT QueueId) FROM ConnectorHealingQueue "
            "WHERE QueueStatus='WAITING' OR QueueStatus='ESCALATED'"
        ),
        "plan": plan(
            dimensions=[],
            metrics=["incident_count"],
            filters=[
                {"field": "queue_status", "op": "eq", "value": "WAITING"},
                {"field": "queue_status", "op": "eq", "value": "ESCALATED"},
            ],
            filter_logic="or",
            order_by=[],
        ),
    },
    {
        "id": "gold-daily-incident-buckets-vietnam",
        "split": "gold",
        "question": "Mỗi ngày hệ thống nhận bao nhiêu sự cố?",
        "reference_sql": (
            "SELECT strftime('%Y-%m-%d',ReceivedAt,'+7 hours'),"
            "COUNT(DISTINCT QueueId) FROM ConnectorHealingQueue GROUP BY 1 ORDER BY 1"
        ),
        "plan": plan(
            dimensions=[],
            metrics=["incident_count"],
            time_bucket={
                "field": "received_at",
                "unit": "day",
                "timezone": "Asia/Ho_Chi_Minh",
            },
            order_by=[{"field": "time_bucket", "direction": "asc"}],
        ),
    },
    {
        "id": "gold-confirmed-failure-ranking-natural",
        "split": "gold",
        "question": "Ba connector hay báo lỗi được xác nhận nhất là những connector nào?",
        "reference_sql": (
            "SELECT q.RootConnectorName,COUNT(CASE WHEN l.EventType='HEALTH_FAILED_CONFIRMED' "
            "THEN 1 END) FROM ConnectorHealingLogs l LEFT JOIN ConnectorHealingQueue q "
            "ON l.QueueId=q.QueueId GROUP BY q.RootConnectorName ORDER BY 2 DESC,1 ASC LIMIT 3"
        ),
        "plan": plan(
            entity="events",
            metrics=["confirmed_failure_count"],
            limit=3,
            order_by=[
                {"field": "confirmed_failure_count", "direction": "desc"},
                {"field": "root", "direction": "asc"},
            ],
        ),
    },
    {
        "id": "gold-unsupported-owner-question",
        "split": "gold",
        "question": "Ai là người chịu trách nhiệm xử lý connector này?",
        "reference_sql": None,
        "plan": None,
        "expected_clarification": True,
    },
]

for case in GOLD + GOLD_EXTENDED + HOLDOUT:
    case.setdefault("expected_answer_facts", "all ordered reference-result cells")
    case.setdefault(
        "forbidden_claims",
        ["unreferenced numbers", "unreferenced connector names", "live health inference"],
    )
    case.setdefault("snapshot_version", "evaluator runtime SHA-256")

SEMANTIC_CASES = GOLD + GOLD_EXTENDED + HOLDOUT

def _conversation_plan(*, time: str = "today", inherited: list[str] | None = None, clarification: str | None = None):
    return {
        "version": CATALOG_VERSION,
        "data_request": None if clarification else {
            "metrics": ["incident_count"],
            "dimensions": ["root_connector"],
            "filters": {"time_range": {"kind": "relative", "value": time}},
            "sort": {"metric": "incident_count", "direction": "desc"},
            "limit": 5,
            "comparison": None,
            "detail_fields": [],
        },
        "guidance_request": {"needed": False, "purpose": None, "error_codes": [], "connector_class": None},
        "clarification": clarification,
        "conversation_action": "none",
        "inherited_fields": inherited or [],
    }


# Evaluation-only contracts. They inject raw semantic plans into the real
# service to verify context boundaries and compiler behavior; they explicitly
# do not claim a model understands the natural-language phrasing.
MULTI_TURN_CASES = [
    {
        "id": "conversation-time-override",
        "split": "gold",
        "now_utc": "2026-09-03T12:00:00+00:00",
        "facts": [{"incident_id": "a-1", "job_name": "connector-a"}],
        "plans": [
            _conversation_plan(time="today"),
            _conversation_plan(time="yesterday", inherited=["metrics", "dimensions"]),
        ],
        "turns": [
            {"conversation_id": "time-a", "question": "Connector nào lỗi hôm nay?", "expected": {"context_used": False}},
            {"conversation_id": "time-a", "question": "Còn hôm qua thì sao?", "expected": {
                "status": "verified_results", "context_used": True, "action": "semantic_plan",
                "time_range": {"kind": "relative", "value": "yesterday"},
            }},
        ],
        "expected_backend_calls": 2,
    },
    {
        "id": "conversation-isolation",
        "split": "holdout",
        "now_utc": "2026-09-03T12:00:00+00:00",
        "facts": [{"incident_id": "a-1", "job_name": "connector-a"}],
        "plans": [
            _conversation_plan(),
            _conversation_plan(clarification="Bạn muốn connector nào?"),
        ],
        "turns": [
            {"conversation_id": "tenant-a", "question": "Connector nào gặp nhiều sự cố nhất?", "expected": {"context_used": False}},
            {"conversation_id": "tenant-b", "question": "Còn connector đó thì sao?", "expected": {
                "status": "needs_clarification", "context_used": False, "action": "clarification",
            }},
        ],
        "expected_backend_calls": 1,
    },
    {
        "id": "conversation-topic-change",
        "split": "holdout",
        "now_utc": "2026-09-03T12:00:00+00:00",
        "facts": [{"incident_id": "a-1", "job_name": "connector-a"}],
        "plans": [_conversation_plan(), _conversation_plan(time="last_7_days")],
        "turns": [
            {"conversation_id": "topic-a", "question": "Connector nào lỗi hôm nay?", "expected": {"context_used": False}},
            {"conversation_id": "topic-a", "question": "Cho số incident trong 7 ngày gần đây.", "expected": {
                "status": "verified_results", "context_used": False, "action": "semantic_plan",
                "time_range": {"kind": "relative", "value": "last_7_days"},
            }},
        ],
        "expected_backend_calls": 2,
    },
]
