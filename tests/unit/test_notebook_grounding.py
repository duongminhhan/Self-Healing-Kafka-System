import sqlite3

import pytest

import notebooks.shared.analytics as notebook_analytics
from notebooks.shared.analytics import Snapshot
from notebooks.shared.diagnostics import diagnose, suspicious_result


@pytest.fixture
def snapshot(tmp_path):
    path = tmp_path / "durations.db"
    with sqlite3.connect(path) as conn:
        conn.executescript("""
            CREATE TABLE ConnectorHealingQueue (
                QueueId INTEGER PRIMARY KEY, QueueStatus TEXT, FinalOutcome TEXT,
                ReceivedAt TEXT, CompletedAt TEXT, Message TEXT);
            INSERT INTO ConnectorHealingQueue VALUES
            (1,'COMPLETED','RECOVERED','2026-09-01T00:00:00Z','2026-09-01T01:00:00Z','secret'),
            (2,'COMPLETED','RECOVERED','2026-09-01T07:00:00+07:00','2026-09-01T00:10:00Z','secret');
        """)
    return Snapshot(path)


AVERAGE = """SELECT ROUND(AVG((julianday(CompletedAt)-julianday(ReceivedAt))*1440),2) AS minutes
FROM ConnectorHealingQueue WHERE QueueStatus='COMPLETED' AND FinalOutcome='RECOVERED'"""
SAFE_AVERAGE = AVERAGE.replace(
    "AVG((julianday(CompletedAt)-julianday(ReceivedAt))*1440)",
    "AVG(CASE WHEN julianday(CompletedAt)>=julianday(ReceivedAt) THEN (julianday(CompletedAt)-julianday(ReceivedAt))*1440 END)",
)


def test_real_values_and_catalog(snapshot):
    context = snapshot.context()
    profiles = context["categorical_observations"]
    assert profiles["ConnectorHealingQueue.FinalOutcome"]["observed_values"] == ["RECOVERED"]
    assert profiles["ConnectorHealingQueue.QueueStatus"]["complete_for_snapshot"]
    assert not any("Message" in key for key in profiles)
    assert "secret" not in str(context)
    assert (
        "receipt_to_completion_minutes"
        in context["semantic_catalog"]["entities"]["ConnectorHealingQueue"]["metrics"]
    )
    assert snapshot.value_profiles() is snapshot.value_profiles()


def test_profiles_invalidate(snapshot):
    snapshot.value_profiles()
    import os

    stat = snapshot.path.stat()
    os.utime(snapshot.path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000))
    with pytest.raises(notebook_analytics.QueryError, match="Snapshot changed"):
        snapshot.context()
    assert snapshot._profile_cache is None


def test_average_fixture_and_offsets(snapshot):
    result = snapshot.execute(AVERAGE)
    assert result["rows"] == [{"minutes": 35.0}]
    assert not suspicious_result(result)
    counts = diagnose(snapshot, result)["counts"]
    assert counts["matched_count"] == 2
    assert counts["duration_0_valid_count"] == 2
    assert counts["duration_0_excluded_count"] == 0


def test_wrong_status_is_no_match_not_missing_timestamps(snapshot):
    result = snapshot.execute(AVERAGE.replace("COMPLETED", "SUCCESS"))
    assert suspicious_result(result)
    counts = diagnose(snapshot, result)["counts"]
    assert counts["matched_count"] == 0
    assert counts["duration_0_missing_count"] == 0


@pytest.mark.parametrize(
    "end,expected",
    [
        (None, "missing_count"),
        ("bad timestamp", "unparseable_count"),
        ("2026-08-01T00:00:00Z", "negative_count"),
    ],
)
def test_duration_diagnostics(snapshot, end, expected):
    with sqlite3.connect(snapshot.path) as conn:
        conn.execute("UPDATE ConnectorHealingQueue SET CompletedAt=?", (end,))
    fresh = Snapshot(snapshot.path)
    counts = diagnose(fresh, fresh.execute(AVERAGE))["counts"]
    assert counts["duration_0_" + expected] == 2
    assert counts["duration_0_valid_count"] == 0
    assert counts["duration_0_excluded_count"] == 2


def test_complex_diagnostic_does_not_invent_cause(snapshot):
    result = snapshot.execute(
        "SELECT QueueStatus,AVG(NULL) n FROM ConnectorHealingQueue GROUP BY QueueStatus"
    )
    assert diagnose(snapshot, result)["status"] == "inconclusive"


def client_with(*outputs):
    import json
    from types import SimpleNamespace as NS

    items = iter(outputs)
    return NS(
        chat_completion=lambda **kwargs: NS(
            choices=[NS(finish_reason="stop", message=NS(content=json.dumps(next(items))))]
        )
    )


def decision(sql):
    return {"kind": "sql", "sql": sql, "interpretation": "Fixture metric and filters"}


def test_review_corrects_sql_within_total_budget(snapshot):
    flow = notebook_analytics.Workflow(
        snapshot,
        client_with(
            decision(SAFE_AVERAGE.replace("COMPLETED", "SUCCESS")),
            decision(SAFE_AVERAGE),
            {"kind": "accept_result"},
        ),
        model_id="fake",
    )
    assert flow.query("Average duration of successful incidents")["rows"] == [{"minutes": 35.0}]
    assert flow.metrics["sql_api_calls"] == 3
    assert flow.metrics["sql_attempts"] == 2
    assert flow.first_result["rows"] == [{"minutes": None}]


def test_legitimate_no_match_preserved(snapshot):
    sql = SAFE_AVERAGE + " AND QueueId=999"
    flow = notebook_analytics.Workflow(
        snapshot, client_with(decision(sql), {"kind": "accept_result"}), model_id="fake"
    )
    result = flow.query("Average successful incident duration for QueueId=999")
    assert result["sql"] == sql
    answer = flow.respond()
    assert "Không có hàng đầu vào khớp" in answer["text"]
    assert flow.metrics["response_api_calls"] == 0


def test_ordinary_success_no_extra_call(snapshot):
    flow = notebook_analytics.Workflow(
        snapshot,
        client_with(decision("SELECT COUNT(*) AS incidents FROM ConnectorHealingQueue")),
        model_id="fake",
    )
    assert flow.query("Incident count")["rows"] == [{"incidents": 2}]
    assert flow.metrics["sql_api_calls"] == 1
    assert flow.metrics["diagnostic_queries"] == 0


def test_diagnostic_deduplicates_duration_expressions(snapshot):
    sql = AVERAGE.replace(
        " AS minutes", ", MAX((julianday(CompletedAt)-julianday(ReceivedAt))*1440) AS maximum"
    )
    counts = diagnose(snapshot, snapshot.execute(sql))["counts"]
    assert "duration_0_valid_count" in counts
    assert "duration_1_valid_count" not in counts


def test_gold_comparison_null_ties_and_rounding():
    from notebooks.evaluation.evaluate import matches

    result = {
        "truncated": False,
        "columns": [{"name": "value"}],
        "rows": [{"value": 35.0}, {"value": None}],
    }
    assert matches(result, [(34.999999,), (None,)])
    assert not matches(result, [(None,), (35.0,)])
    assert not matches(result, [(35.0,), (0,)])
    assert not matches(result, [(35.0,)])


def test_numeric_response_equivalence_is_exact(snapshot):
    result = snapshot.execute(AVERAGE)

    def claim(value):
        return {"claims": [{"text": value, "evidence": [{"row": 0, "column": "minutes"}]}]}

    assert notebook_analytics.validate_claims(claim("Trung bình 35 phút."), result) is None
    assert notebook_analytics.validate_claims(claim("Trung bình 35,00 phút."), result) is None
    assert notebook_analytics.validate_claims(claim("Trung bình 35.1 phút."), result)
    assert notebook_analytics.validate_claims(claim("Trung bình 35 phút, tổng 3535 phút."), result)
    assert notebook_analytics.validate_claims(claim("Trung bình 35 phút trên 9 queue."), result)


def test_safe_average_excludes_negative_but_keeps_zero(snapshot):
    with sqlite3.connect(snapshot.path) as c:
        c.execute("UPDATE ConnectorHealingQueue SET CompletedAt=ReceivedAt WHERE QueueId=1")
        c.execute(
            "UPDATE ConnectorHealingQueue SET CompletedAt='2026-08-01T00:00:00Z' WHERE QueueId=2"
        )
    fresh = Snapshot(snapshot.path)
    result = fresh.execute(SAFE_AVERAGE)
    assert result["rows"] == [{"minutes": 0.0}]
    counts = diagnose(fresh, result)["counts"]
    assert counts["matched_count"] == 2
    assert counts["duration_0_valid_count"] == 1
    assert counts["duration_0_excluded_count"] == 1


def test_profile_limits_are_explicit(snapshot):
    with sqlite3.connect(snapshot.path) as c:
        c.executemany(
            "INSERT INTO ConnectorHealingQueue(QueueStatus) VALUES (?)",
            [(f"STATUS_{i}",) for i in range(40)],
        )
    profile = Snapshot(snapshot.path).value_profiles()["ConnectorHealingQueue.QueueStatus"]
    assert len(profile["observed_values"]) <= 32
    assert not profile["complete_for_snapshot"]
    assert not profile["is_allowed_value_constraint"]


@pytest.mark.parametrize(
    "variant", ["normal", "no_success", "missing", "malformed", "offsets", "negative", "zero"]
)
def test_gold_duration_variants(tmp_path, variant):
    from notebooks.evaluation.evaluate import CASES, matches
    from notebooks.evaluation.fixtures import EXPECTED, create_duration_fixture

    path = tmp_path / "gold.db"
    create_duration_fixture(path, variant)
    result = Snapshot(path).execute(CASES[-1][1])
    assert matches(result, EXPECTED[variant])


def test_wal_changes_invalidate_metadata(tmp_path):
    path = tmp_path / "wal.db"
    with sqlite3.connect(path) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE ConnectorHealingQueue(QueueStatus TEXT)")
        writer.execute("INSERT INTO ConnectorHealingQueue VALUES ('PENDING')")
        writer.commit()
        snapshot = Snapshot(path)
        snapshot.value_profiles()
        writer.execute("UPDATE ConnectorHealingQueue SET QueueStatus='COMPLETED'")
        writer.commit()
        with pytest.raises(notebook_analytics.QueryError, match="Snapshot changed"):
            snapshot.context()


def test_duration_count_disagreement_is_rejected(snapshot):
    from notebooks.shared.diagnostics import diagnostic_count_errors

    result = snapshot.execute(
        SAFE_AVERAGE.replace(" AS minutes", " AS minutes, 99 AS excluded_duration_count")
    )
    result["diagnostics"] = diagnose(snapshot, result)
    errors = diagnostic_count_errors(result)
    assert len(errors) == 1 and "contradicts" in errors[0]


def test_diagnostics_explain_timestamp_failure(snapshot):
    from notebooks.shared.diagnostics import render_diagnostics

    with sqlite3.connect(snapshot.path) as conn:
        conn.execute("UPDATE ConnectorHealingQueue SET CompletedAt='invalid'")
    fresh = Snapshot(snapshot.path)
    explanation = render_diagnostics(diagnose(fresh, fresh.execute(SAFE_AVERAGE)))
    assert "2 không chuyển đổi được timestamp" in explanation
    assert "0 thiếu timestamp" in explanation


def test_date_literal_silent_null_is_rejected(snapshot):
    sql = "SELECT COUNT(*) FROM ConnectorHealingQueue WHERE julianday(ReceivedAt)>=julianday('2026-01-01 00:00 UTC')"
    with pytest.raises(notebook_analytics.QueryError, match="cannot parse"):
        snapshot.validate_date_literals(sql)
    snapshot.validate_date_literals(sql.replace("2026-01-01 00:00 UTC", "2026-01-01T00:00:00Z"))


def test_sql_tokenization_error_is_safe_correction_feedback(snapshot):
    with pytest.raises(notebook_analytics.QueryError, match="parser rejected"):
        snapshot.validate("SELECT 'unterminated")
