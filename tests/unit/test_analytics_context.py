from datetime import datetime, timezone

import pytest

from self_healthy_kafka.analytics_context import resolve_calendar_day


@pytest.mark.parametrize("question", [
    "Ngày 5 tháng 9 có bao nhiêu sự cố?",
    "Có bao nhiêu nhật ký ngày 5/9?",
    "Ngày 05/09/2026 có bao nhiêu incident?",
])
def test_calendar_day_uses_request_year_and_local_timezone(question):
    day = resolve_calendar_day(question, now=datetime(2026, 9, 7, tzinfo=timezone.utc),
                               timezone_name="Asia/Ho_Chi_Minh")
    assert day.local_date == "2026-09-05"
    assert day.start_utc == "2026-09-04T17:00:00+00:00"
    assert day.end_utc == "2026-09-05T17:00:00+00:00"


def test_missing_year_uses_local_clock_not_utc_year_or_snapshot():
    day = resolve_calendar_day("ngày 1 tháng 1", now=datetime(2025, 12, 31, 20, tzinfo=timezone.utc),
                               timezone_name="Asia/Ho_Chi_Minh")
    assert day.local_date == "2026-01-01"
    assert day.year_defaulted


@pytest.mark.parametrize("question", ["ngày 31 tháng 2", "từ 5/9 đến 6/9"])
def test_invalid_or_multiple_dates_are_not_silently_reinterpreted(question):
    with pytest.raises(ValueError):
        resolve_calendar_day(question, now=datetime(2026, 1, 1, tzinfo=timezone.utc),
                             timezone_name="Asia/Ho_Chi_Minh")


def test_no_date_does_not_add_a_time_filter():
    assert resolve_calendar_day("Connector nào hay lỗi nhất?",
                                now=datetime(2026, 1, 1, tzinfo=timezone.utc),
                                timezone_name="Asia/Ho_Chi_Minh") is None


@pytest.mark.parametrize(
    "question,expected", [("Có bao nhiêu sự cố hôm nay?", "2026-01-01"),
                          ("Có bao nhiêu log hôm qua?", "2025-12-31")],
)
def test_relative_day_uses_configured_local_clock(question, expected):
    day = resolve_calendar_day(
        question, now=datetime(2025, 12, 31, 20, tzinfo=timezone.utc),
        timezone_name="Asia/Ho_Chi_Minh",
    )
    assert day.local_date == expected
    assert not day.year_defaulted


def test_relative_and_explicit_days_conflict():
    with pytest.raises(ValueError):
        resolve_calendar_day(
            "Hôm nay, ngày 5 tháng 9", now=datetime(2026, 9, 7, tzinfo=timezone.utc),
            timezone_name="Asia/Ho_Chi_Minh",
        )
