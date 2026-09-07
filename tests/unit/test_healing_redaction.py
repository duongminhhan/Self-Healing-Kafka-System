import io
import json
import logging

from self_healthy_kafka.redaction import redact, redact_text


def test_nested_config_and_credentials_redacted_without_mutating_input():
    source = {"active_config": {"other": "private-setting"}, "nested": [
        {"database.password": "hidden", "message": "ORA-01017 password=hidden; retry"}
    ]}
    safe = json.dumps(redact(source))
    assert "private-setting" not in safe and "hidden" not in safe
    assert "ORA-01017" in safe
    assert source["nested"][0]["database.password"] == "hidden"


def test_formatted_json_and_exception_credentials_redacted():
    text = 'password="with spaces"; PWD=secret; https://user:private@host Authorization: Bearer abc.def'
    safe = redact_text(text)
    for secret in ("with spaces", "secret", "private", "abc.def"):
        assert secret not in safe
    assert "hidden" not in redact_text('{"password": "hidden", "event": "TASK_RESTART"}')


def test_stream_handler_redacts_structured_config():
    from self_healthy_kafka.logging_config import _SafeStreamHandler

    stream = io.StringIO()
    handler = _SafeStreamHandler(stream)
    handler.emit(logging.LogRecord("test", logging.ERROR, "", 0,
                                 '{"active_config":{"arbitrary":"private"},"event":"error"}', (), None))
    assert "private" not in stream.getvalue()
    assert json.loads(stream.getvalue())["event"] == "error"
