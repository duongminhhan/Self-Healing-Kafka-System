"""Shared credential redaction for persisted and formatted diagnostic logs."""

import re
from typing import Any

_KEY = re.compile(r"password|passwd|pwd|secret|token|api.?key|authorization|sasl\.jaas|active_config|connector_config", re.I)
_ASSIGNMENT = re.compile(
    r'''(?i)((?:[\w.-]*(?:password|passwd|pwd|secret|token|api[_-]?key|authorization)[\w.-]*)["']?\s*[:=]\s*)("(?:\\.|[^"\\])*"|'[^']*'|[^\s;,}\]]+)'''
)
_AUTH = re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9+/_.=:-]+")
_URL = re.compile(r"(://)[^\s/@:]+:[^\s/@]+@")


def redact_text(value: str) -> str:
    value = _AUTH.sub(r"\1 [REDACTED]", value)
    value = _ASSIGNMENT.sub(lambda m: m[1] + '"[REDACTED]"', value)
    return _URL.sub(r"\1[REDACTED]@", value)


def redact(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: "[REDACTED]" if _KEY.search(str(key)) else redact(item)
                for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return redact_text(value) if isinstance(value, str) else value
