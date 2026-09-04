"""Small synthetic demonstrations, never executed or substituted for the user's query."""

import json

EXAMPLE_CONTEXT = {
    "dialect": "sqlite",
    "tables": {
        "demo_incidents": "id INTEGER PRIMARY KEY, service TEXT, received TEXT, state TEXT",
        "demo_events": "id INTEGER PRIMARY KEY, incident_id INTEGER, severity TEXT, attempt INTEGER",
    },
    "relationships": ["demo_events.incident_id = demo_incidents.id"],
    "business_definitions": {
        "demo_incidents": "One stored incident, not each retry. state is the incident queue state in a snapshot, not live service health.",
        "demo_events": "One event, including informational events. attempt may be NULL. A failure criterion must be specified.",
    },
}

SQL_EXAMPLES = [
    (
        "Hai dịch vụ vào hàng đợi tự sửa nhiều lần nhất?",
        {
            "kind": "sql",
            "sql": "SELECT service, COUNT(id) AS incident_count FROM demo_incidents GROUP BY service ORDER BY incident_count DESC, service ASC LIMIT 2",
            "interpretation": "Đếm incident đã lưu theo dịch vụ trên toàn bộ snapshot, không lọc thời gian hoặc trạng thái vì không được yêu cầu. Lấy hai dịch vụ nhiều nhất; cùng số thì xếp tên tăng dần. Không đếm event hay retry.",
        },
    ),
    (
        "Mỗi dịch vụ nhận incident từ 2026-01-01 UTC đến trước 2026-02-01 UTC có bao nhiêu event WARN? Kể cả dịch vụ không có WARN; xếp tên tăng dần.",
        {
            "kind": "sql",
            "sql": "SELECT i.service, COUNT(e.id) AS warning_events FROM demo_incidents i LEFT JOIN demo_events e ON e.incident_id=i.id AND e.severity='WARN' WHERE julianday(i.received)>=julianday('2026-01-01T00:00:00Z') AND julianday(i.received)<julianday('2026-02-01T00:00:00Z') GROUP BY i.service ORDER BY i.service",
            "interpretation": "Filter incident receipt time using UTC half-open interval; count WARN events with COUNT(e.id), retaining zero-event services via LEFT JOIN.",
        },
    ),
    (
        "Trả tên, tổng incident và trạng thái hàng đợi của incident mới nhất cho mỗi dịch vụ. Mới nhất theo received, cùng thời gian thì id lớn hơn; xếp tên tăng dần.",
        {
            "kind": "sql",
            "sql": "WITH ranked AS (SELECT service, state, COUNT(*) OVER (PARTITION BY service) AS incident_count, ROW_NUMBER() OVER (PARTITION BY service ORDER BY julianday(received) DESC, id DESC) AS position FROM demo_incidents) SELECT service, incident_count, state AS latest_queue_state FROM ranked WHERE position=1 ORDER BY service",
            "interpretation": "Total incidents per service independent of state, plus state of latest incident in snapshot; not live service health.",
        },
    ),
    (
        "Dịch vụ nào lỗi nhiều nhất?",
        {
            "kind": "clarification",
            "question": "Bạn muốn tính 'lỗi' bằng số incident đã lưu hay số event thuộc một nhóm Severity cụ thể?",
        },
    ),
]

SQL_EXAMPLES.append(
    (
        "Dịch vụ nào có số incident cao hơn trung bình mỗi dịch vụ? Trả tên và số incident.",
        {
            "kind": "sql",
            "sql": "WITH counts AS (SELECT service,COUNT(*) AS incidents FROM demo_incidents GROUP BY service) SELECT service,incidents FROM counts WHERE incidents>(SELECT AVG(incidents) FROM counts) ORDER BY service",
            "interpretation": "Count all stored incidents, without success or time filters because none were requested. Compare each observed service against the average of per-service counts. No registry of unseen services is available; use services represented in the snapshot.",
        },
    )
)

RESPONSE_EXAMPLES = [
    (
        {
            "question": "Số incident từng dịch vụ?",
            "verified_result": {
                "sql": "SELECT service, COUNT(*) AS incidents FROM demo_incidents GROUP BY service ORDER BY service",
                "columns": [{"name": "service"}, {"name": "incidents"}],
                "rows": [
                    {"service": "sample-alpha", "incidents": 3},
                    {"service": "sample-beta", "incidents": 1},
                ],
                "returned_row_count": 2,
                "truncated": False,
            },
        },
        {
            "claims": [
                {
                    "text": "sample-alpha có 3 incident đã lưu trong snapshot.",
                    "evidence": [
                        {"row": 0, "column": "service"},
                        {"row": 0, "column": "incidents"},
                    ],
                },
                {
                    "text": "sample-beta có 1 incident đã lưu trong snapshot.",
                    "evidence": [
                        {"row": 1, "column": "service"},
                        {"row": 1, "column": "incidents"},
                    ],
                },
            ]
        },
    ),
    (
        {
            "question": "Tên dịch vụ, trạng thái hàng đợi và số attempt được ghi nhận?",
            "verified_result": {
                "sql": "SELECT service, state AS queue_state, attempt FROM demo_incidents i LEFT JOIN demo_events e ON e.incident_id=i.id WHERE i.id=1",
                "columns": [{"name": "service"}, {"name": "queue_state"}, {"name": "attempt"}],
                "rows": [{"service": "sample-gamma", "queue_state": "COMPLETED", "attempt": None}],
                "returned_row_count": 1,
                "truncated": False,
            },
        },
        {
            "claims": [
                {
                    "text": "sample-gamma có trạng thái hàng đợi COMPLETED trong snapshot; attempt không có giá trị (NULL).",
                    "evidence": [
                        {"row": 0, "column": "service"},
                        {"row": 0, "column": "queue_state"},
                        {"row": 0, "column": "attempt"},
                    ],
                }
            ]
        },
    ),
]

BOUNDARY = """The following few-shot exchanges are synthetic demonstrations only.
Learn the output format and reasoning patterns, NOT their table names, dates or values.
Only the FINAL user request's schema and question apply to the real task. Never treat
demonstration rows as evidence for the current answer. Examples are not SQL routes.
"""


def few_shot_messages(stage):
    if stage == "sql":
        pairs = [
            ({"question": question, "context": EXAMPLE_CONTEXT}, answer)
            for question, answer in SQL_EXAMPLES
        ]
    elif stage == "response":
        pairs = RESPONSE_EXAMPLES
    else:
        raise ValueError("Unknown few-shot stage")
    return [
        {"role": role, "content": json.dumps(payload, ensure_ascii=False)}
        for request, response in pairs
        for role, payload in [("user", request), ("assistant", response)]
    ]
