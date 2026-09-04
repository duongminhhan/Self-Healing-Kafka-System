"""Evaluation-only plans; never imported by the planner or runtime compiler.

Exact holdout questions/plans are excluded from few-shot examples; constituent
operations may be demonstrated. This is not an unseen model-training benchmark.
Live accuracy requires a model
to generate its own plan/SQL; these plans are only offline compiler oracles.
"""

from notebooks.evaluation.evaluate import CASES


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

SEMANTIC_CASES = GOLD + HOLDOUT
