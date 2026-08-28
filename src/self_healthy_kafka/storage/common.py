from __future__ import annotations

import json
import re
import struct
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

SQL_SS_TIMESTAMPOFFSET = -155


class MssqlConnection:
    def __init__(self, connection_string: str, timeout_seconds: int = 5):
        self._connection_string = connection_string
        self._timeout_seconds = timeout_seconds
        self._conn: Any = None

    def __enter__(self):
        import pyodbc

        self._conn = pyodbc.connect(
            self._connection_string,
            timeout=self._timeout_seconds,
            autocommit=False,
        )
        self._conn.add_output_converter(SQL_SS_TIMESTAMPOFFSET, decode_datetimeoffset)
        return self._conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._conn is None:
            return
        try:
            if exc_type:
                self._conn.rollback()
            else:
                self._conn.commit()
        finally:
            self._conn.close()


def json_value(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return json.dumps(
            json_safe(value),
            ensure_ascii=False,
            indent=2,
        )
    return value


def dict_or_empty(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def row_to_dict(cursor: Any, row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    columns = [_to_snake_case(str(item[0])) for item in (cursor.description or ())]
    return dict(zip(columns, row, strict=False))


def rows_to_dicts(cursor: Any) -> list[dict[str, Any]]:
    return [row_to_dict(cursor, row) for row in cursor.fetchall()]


def decode_datetimeoffset(value: bytes) -> datetime:
    year, month, day, hour, minute, second, fraction, offset_hour, offset_minute = (
        struct.unpack("<6hI2h", value)
    )
    return datetime(
        year,
        month,
        day,
        hour,
        minute,
        second,
        fraction // 1000,
        timezone(timedelta(hours=offset_hour, minutes=offset_minute)),
    )


def _to_snake_case(value: str) -> str:
    value = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", value)
    return value.lower()


def json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_safe(item) for item in value]
    return value


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
