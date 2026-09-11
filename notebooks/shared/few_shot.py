"""Small synthetic demonstrations selected by intent, never used as evidence."""

import json

from notebooks.shared.context_selection import normalize_text

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

PLAN_EXAMPLES = [
    (
        "Hai connector có nhiều incident nhất?",
        {
            "kind": "query",
            "entity": "incidents",
            "dimensions": ["root"],
            "metrics": ["incident_count"],
            "order_by": [{"field": "incident_count", "direction": "desc"}],
            "limit": 2,
        },
    ),
    (
        "Có bao nhiêu incident và healing log?",
        {
            "kind": "independent",
            "queries": [
                {
                    "kind": "query",
                    "entity": "incidents",
                    "dimensions": [],
                    "metrics": ["incident_count"],
                },
                {
                    "kind": "query",
                    "entity": "events",
                    "dimensions": [],
                    "metrics": ["log_count"],
                },
            ],
        },
    ),
    (
        "Thời gian phục hồi trung bình là bao lâu?",
        {
            "kind": "query",
            "entity": "incidents",
            "dimensions": [],
            "metrics": [
                "avg_duration_minutes",
                "matched_count",
                "valid_duration_count",
                "excluded_duration_count",
            ],
            "success_only": True,
        },
    ),
    (
        "Tỷ lệ phục hồi của từng connector?",
        {
            "kind": "query",
            "entity": "incidents",
            "dimensions": ["root"],
            "metrics": ["recovery_rate_percent"],
            "order_by": [{"field": "root", "direction": "asc"}],
        },
    ),
    (
        "Trạng thái gần nhất và tổng incident của từng connector?",
        {
            "kind": "query",
            "entity": "incidents",
            "dimensions": ["root"],
            "metrics": ["incident_count"],
            "latest_status": True,
            "order_by": [{"field": "root", "direction": "asc"}],
        },
    ),
    (
        "Connector nào có nhiều lỗi nhất?",
        {
            "kind": "clarification",
            "question": "Bạn muốn tính lỗi bằng incident hay sự kiện HEALTH_FAILED_CONFIRMED?",
        },
    ),
    (
        "Mỗi ngày có bao nhiêu incident?",
        {
            "kind": "query",
            "entity": "incidents",
            "dimensions": [],
            "metrics": ["incident_count"],
            "time_bucket": {
                "field": "received_at",
                "unit": "day",
                "timezone": "Asia/Ho_Chi_Minh",
            },
            "order_by": [{"field": "time_bucket", "direction": "asc"}],
        },
    ),
    (
        "Có bao nhiêu incident đang chờ hoặc đã chuyển cấp?",
        {
            "kind": "query",
            "entity": "incidents",
            "dimensions": [],
            "metrics": ["incident_count"],
            "filters": [
                {"field": "queue_status", "op": "eq", "value": "WAITING"},
                {"field": "queue_status", "op": "eq", "value": "ESCALATED"},
            ],
            "filter_logic": "or",
        },
    ),
]

_EXAMPLE_METADATA = {
    "sql": [
        ("sql_incident_ranking", {"incident", "ranking", "grouping"}),
        ("sql_temporal_event_join", {"event", "time", "join", "grouping"}),
        ("sql_latest_status", {"incident", "latest", "status", "window"}),
        ("sql_failure_clarification", {"failure", "clarification"}),
        ("sql_population_mean", {"incident", "population_mean", "grouping"}),
    ],
    "plan": [
        ("plan_incident_ranking", {"incident", "ranking", "grouping"}),
        ("plan_independent_totals", {"incident", "event", "independent", "count"}),
        ("plan_duration", {"incident", "duration", "recovery"}),
        ("plan_recovery_rate", {"incident", "rate", "recovery", "grouping"}),
        ("plan_latest_status", {"incident", "latest", "status"}),
        ("plan_failure_clarification", {"failure", "clarification"}),
        ("plan_daily_bucket", {"incident", "time", "grouping", "time_bucket"}),
        ("plan_or_filters", {"incident", "count", "or_filter"}),
    ],
    "response": [
        ("response_grouped_counts", {"grouping", "count"}),
        ("response_null_value", {"null", "status"}),
    ],
}

BOUNDARY = """The following few-shot exchanges are synthetic demonstrations only.
Learn the output format and reasoning patterns, NOT their table names, dates or values.
Only the FINAL user request's schema and question apply to the real task. Never treat
demonstration rows as evidence for the current answer. Examples are not SQL routes.
"""


def _features(question, result=None):
    text = normalize_text(question)
    features = set()
    phrases = {
        "incident": ("incident", "su co", "hang doi"),
        "event": ("healing log", "log", "event", "su kien", "nhat ky"),
        "ranking": ("nhieu nhat", "it nhat", "top", "xep hang", "thuong xuyen"),
        "grouping": ("moi", "tung", "theo", "group"),
        "time": ("ngay", "tuan", "thang", "hom nay", "hom qua", "utc"),
        "time_bucket": ("moi ngay", "tung ngay", "moi tuan", "tung thang"),
        "or_filter": (" hoac ", " hoặc ", "either"),
        "latest": ("moi nhat", "gan nhat", "hien tai", "latest", "current"),
        "status": ("trang thai", "status"),
        "failure": ("loi", "error", "failure", "that bai"),
        "duration": ("mat bao lau", "thoi gian", "duration", "phut"),
        "rate": ("ty le", "rate", "phan tram", "percent"),
        "recovery": ("phuc hoi", "recovery", "recovered", "thanh cong"),
        "population_mean": ("cao hon trung binh", "thap hon trung binh"),
        "count": ("bao nhieu", "tong", "count"),
        "independent": ("incident va", "incident cùng", "incident cung"),
    }
    for name, values in phrases.items():
        if any(value in text for value in values):
            features.add(name)
    if result:
        rows = result.get("rows") or []
        if len(rows) > 1:
            features.add("grouping")
        if any(value is None for row in rows for value in row.values()):
            features.add("null")
    return features


def select_few_shot_messages(stage, question, *, result=None, max_examples=3):
    """Select bounded synthetic examples and return messages plus stable IDs."""
    if type(max_examples) is not int or max_examples < 0:
        raise ValueError("max_examples must be a nonnegative integer")
    if stage == "sql":
        pairs = [
            ({"question": question, "context": EXAMPLE_CONTEXT}, answer)
            for question, answer in SQL_EXAMPLES
        ]
    elif stage == "plan":
        pairs = [({"question": prompt}, answer) for prompt, answer in PLAN_EXAMPLES]
    elif stage == "response":
        pairs = RESPONSE_EXAMPLES
    else:
        raise ValueError("Unknown few-shot stage")
    metadata = _EXAMPLE_METADATA[stage]
    if len(metadata) != len(pairs):
        raise RuntimeError("Few-shot metadata is out of sync with examples")
    wanted = _features(question, result)
    query_tokens = set(normalize_text(question).split())
    ranked = []
    for index, (pair, (example_id, example_features)) in enumerate(zip(pairs, metadata)):
        request = pair[0]
        example_question = request.get("question", "") if isinstance(request, dict) else ""
        lexical_overlap = len(query_tokens & set(normalize_text(example_question).split()))
        feature_overlap = len(wanted & example_features)
        ranked.append((-(feature_overlap * 10 + lexical_overlap), index, example_id, pair))
    selected = sorted(ranked)[: min(max_examples, len(ranked))]
    messages = [
        {"role": role, "content": json.dumps(payload, ensure_ascii=False)}
        for _, _, _, (request, response) in selected
        for role, payload in [("user", request), ("assistant", response)]
    ]
    return messages, [example_id for _, _, example_id, _ in selected]


def few_shot_messages(stage, question=None, *, result=None, max_examples=None):
    """Compatibility wrapper; a supplied question enables dynamic selection."""
    pairs_count = len(_EXAMPLE_METADATA[stage])
    if question is None:
        question = ""
        maximum = pairs_count if max_examples is None else max_examples
    else:
        maximum = 3 if max_examples is None else max_examples
    messages, _ = select_few_shot_messages(
        stage,
        question,
        result=result,
        max_examples=maximum,
    )
    return messages
