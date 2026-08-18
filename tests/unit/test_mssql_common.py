import json
import struct
import sys
import types
from datetime import datetime, timedelta, timezone

from self_healthy_kafka.storage.common import (
    SQL_SS_TIMESTAMPOFFSET,
    MssqlConnection,
    decode_datetimeoffset,
    dict_or_empty,
    json_value,
    row_to_dict,
)


class _Connection:
    def __init__(self):
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.output_converters = {}

    def add_output_converter(self, sql_type, converter):
        self.output_converters[sql_type] = converter

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True

    def close(self):
        self.closed = True


def test_mssql_connection_commits_and_closes(monkeypatch):
    connection = _Connection()
    module = types.SimpleNamespace(connect=lambda *args, **kwargs: connection)
    monkeypatch.setitem(sys.modules, "pyodbc", module)

    with MssqlConnection("Driver={test};Server=example") as current:
        assert current is connection

    assert connection.committed is True
    assert connection.rolled_back is False
    assert connection.closed is True
    assert connection.output_converters[SQL_SS_TIMESTAMPOFFSET] is decode_datetimeoffset


def test_mssql_connection_rolls_back_on_error(monkeypatch):
    connection = _Connection()
    module = types.SimpleNamespace(connect=lambda *args, **kwargs: connection)
    monkeypatch.setitem(sys.modules, "pyodbc", module)

    try:
        with MssqlConnection("Driver={test};Server=example"):
            raise RuntimeError("failed")
    except RuntimeError:
        pass

    assert connection.committed is False
    assert connection.rolled_back is True
    assert connection.closed is True


def test_json_values_are_serialized_and_parsed():
    encoded = json_value({"task_ids": [1, 2]})

    assert json.loads(encoded) == {"task_ids": [1, 2]}
    assert dict_or_empty(encoded) == {"task_ids": [1, 2]}


def test_row_to_dict_uses_cursor_description():
    cursor = types.SimpleNamespace(
        description=[("Id",), ("ConnectorName",)],
    )

    assert row_to_dict(cursor, ("1", "connector-a")) == {
        "id": "1",
        "connector_name": "connector-a",
    }


def test_decode_datetimeoffset_returns_timezone_aware_datetime():
    raw = struct.pack("<6hI2h", 2026, 6, 16, 14, 30, 15, 123456000, 7, 0)

    assert decode_datetimeoffset(raw) == datetime(
        2026,
        6,
        16,
        14,
        30,
        15,
        123456,
        tzinfo=timezone(timedelta(hours=7)),
    )
