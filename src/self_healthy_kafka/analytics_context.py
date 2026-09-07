"""Deterministic request-time context shared by notebook and application planners.

This module resolves explicit calendar days only. It never infers ingestion time
or chooses a year based on which rows happen to exist in a database.
"""

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class CalendarDay:
    local_date: str
    timezone_name: str
    start_utc: str
    end_utc: str
    year_defaulted: bool


def resolve_calendar_day(
    question: str, *, now: datetime, timezone_name: str,
) -> CalendarDay | None:
    """Resolve a Vietnamese calendar day; fail rather than guess an interval."""
    if now.utcoffset() is None:
        raise ValueError("Request clock must be timezone-aware")
    zone = ZoneInfo(timezone_name)
    text = question.casefold()
    relative_days = [
        offset for term, offset in (("hôm nay", 0), ("today", 0), ("hôm qua", -1), ("yesterday", -1))
        if term in text
    ]
    patterns = (
        r"\bngày\s+(\d{1,2})\s+tháng\s+(\d{1,2})(?:\s+(?:năm\s+)?(\d{4}))?\b",
        r"(?<!\d)(\d{1,2})/(\d{1,2})(?:/(\d{4}))?(?!\d)",
    )
    matches = [m for pattern in patterns for m in re.finditer(pattern, question, re.I)]
    if (relative_days and matches) or len(set(relative_days)) > 1:
        raise ValueError("Conflicting calendar days require clarification")
    if relative_days:
        local_day = now.astimezone(zone).date() + timedelta(days=relative_days[0])
        start = datetime.combine(local_day, datetime.min.time(), tzinfo=zone)
        year_defaulted = False
    elif not matches:
        return None
    else:
        if len(matches) != 1:
            raise ValueError("Multiple calendar days require an explicit interval plan")
        day, month, year = matches[0].groups()
        start = datetime(int(year) if year else now.astimezone(zone).year,
                         int(month), int(day), tzinfo=zone)
        year_defaulted = year is None
    end = start + timedelta(days=1)
    return CalendarDay(
        local_date=start.date().isoformat(), timezone_name=timezone_name,
        start_utc=start.astimezone(timezone.utc).isoformat(),
        end_utc=end.astimezone(timezone.utc).isoformat(), year_defaulted=year_defaulted,
    )
