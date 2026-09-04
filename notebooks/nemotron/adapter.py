"""Bounded Ollama Cloud transport. JSON is validated locally, not API-enforced."""

import json
import math
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace as NS

import httpx

from notebooks.shared.analytics import Snapshot, Workflow

HOST = "https://ollama.com"
DEFAULT_MODEL = "nemotron-3-ultra"


class CloudError(RuntimeError):
    """Safe diagnostics only: never retain response bodies or request headers."""

    def __init__(self, category, status=None):
        self.category = category
        self.response = NS(status_code=status)
        super().__init__(f"Ollama Cloud: {category}" + (f" (HTTP {status})" if status else ""))


def positive_env(name, default, cast=int):
    try:
        value = cast(os.getenv(name, str(default)))
        if not math.isfinite(value) or value <= 0:
            raise ValueError
        return value
    except ValueError:
        raise ValueError(f"{name} must be a positive finite number") from None


def validate_output(content, stage):
    try:
        value = json.loads(content)
    except (ValueError, TypeError):
        return "invalid_json"
    if not isinstance(value, dict):
        return "invalid_schema"
    if stage == "sql":
        fields = {
            "sql": ("sql", "interpretation"),
            "clarification": ("question",),
            "accept_result": (),
        }
        kind = value.get("kind")
        if not isinstance(kind, str) or kind not in fields:
            return "invalid_schema"
        if any(not isinstance(value.get(k), str) or not value[k].strip() for k in fields[kind]):
            return "invalid_schema"
    else:
        claims = value.get("claims")
        if not isinstance(claims, list) or not claims:
            return "invalid_schema"
        for claim in claims:
            if not isinstance(claim, dict) or not isinstance(claim.get("text"), str):
                return "invalid_schema"
            refs = claim.get("evidence")
            if not isinstance(refs, list) or not refs:
                return "invalid_schema"
            for ref in refs:
                if (
                    not isinstance(ref, dict)
                    or type(ref.get("row")) is not int
                    or ref["row"] < 0
                    or not isinstance(ref.get("column"), str)
                ):
                    return "invalid_schema"
    return None


class NemotronClient:
    response_source = "ollama_cloud"
    error_label = "Ollama Cloud"

    def __init__(self, *, transport=None):
        key = os.getenv("OLLAMA_API_KEY", "").strip()
        if not key:
            raise ValueError("Set OLLAMA_API_KEY in the environment or ignored .env.nemotron.")
        self.model_id = os.getenv("NEMOTRON_MODEL_ID", DEFAULT_MODEL).strip()
        if not self.model_id:
            raise ValueError("NEMOTRON_MODEL_ID must not be empty")
        thinking = os.getenv("NEMOTRON_THINKING", "true").strip().lower()
        if thinking not in {"true", "false"}:
            raise ValueError(
                "NEMOTRON_THINKING accepts true/false; named levels are not verified for this model"
            )
        self.thinking = thinking == "true"
        self.stage_thinking = {}
        for stage in ("sql", "response"):
            name = f"NEMOTRON_{stage.upper()}_THINKING"
            value = os.getenv(name, thinking).strip().lower()
            if value not in {"true", "false"}:
                raise ValueError(f"{name} accepts true/false")
            self.stage_thinking[stage] = value == "true"
        self.http = httpx.Client(
            base_url=HOST,
            headers={"Authorization": f"Bearer {key}"},
            timeout=positive_env("NEMOTRON_REQUEST_TIMEOUT_SECONDS", 120, float),
            transport=transport,
            follow_redirects=False,
        )

    def close(self):
        self.http.close()

    def request(self, method, path, **kwargs):
        try:
            response = self.http.request(method, path, **kwargs)
        except httpx.TimeoutException:
            raise CloudError("timeout") from None
        except httpx.RequestError:
            raise CloudError("network") from None
        if not 200 <= response.status_code < 300:
            category = {
                400: "unsupported_or_invalid_request",
                401: "authentication",
                402: "billing",
                403: "permission",
                404: "model_unavailable",
                429: "quota_or_rate_limit",
            }.get(response.status_code, "service_error")
            raise CloudError(category, response.status_code) from None
        try:
            payload = response.json()
        except ValueError:
            raise CloudError("invalid_api_json") from None
        if not isinstance(payload, dict):
            raise CloudError("invalid_api_schema")
        if payload.get("error"):
            raise CloudError("service_error")
        return payload

    def check_model(self):
        payload = self.request("GET", "/api/tags")
        models = payload.get("models", [])
        if not isinstance(models, list) or not any(
            isinstance(m, dict) and self.model_id in (m.get("name"), m.get("model")) for m in models
        ):
            raise CloudError("model_unavailable")
        return {"model": self.model_id, "provider": "ollama_cloud", "model_list_verified": True}

    def complete_stage(self, messages, stage, max_tokens):
        if stage not in {"sql", "response"} or type(max_tokens) is not int or max_tokens <= 0:
            raise ValueError("Invalid stage or output budget")
        normalized = []
        for message in messages:
            if message.get("role") not in {"system", "user", "assistant"} or not isinstance(
                message.get("content"), str
            ):
                raise ValueError("Only role/content text messages are supported")
            normalized.append({"role": message["role"], "content": message["content"]})
        payload = self.request(
            "POST",
            "/api/chat",
            json={
                "model": self.model_id,
                "messages": normalized,
                "stream": False,
                "think": self.stage_thinking[stage],
                "options": {"temperature": 0, "num_predict": max_tokens},
            },
        )
        message = payload.get("message")
        if not isinstance(message, dict):
            message = {}
        content = message.get("content")
        reason = payload.get("done_reason")
        error = None
        if reason == "length":
            error = "output_budget_exhausted"
        elif reason in {"content_filter", "safety"}:
            error = "safety_block"
        elif payload.get("done") is not True or reason != "stop":
            error = "incomplete_output"
        elif not isinstance(content, str) or not content.strip():
            error = "thinking_only" if message.get("thinking") else "empty_content"
        elif payload.get("model") != self.model_id:
            error = "unexpected_model"
        else:
            error = validate_output(content, stage)
        usage = {}
        for name, field in (
            ("input", "prompt_eval_count"),
            ("output", "eval_count"),
            ("cached_tokens", "prompt_eval_cached_count"),
        ):
            value = payload.get(field)
            usage[name] = value if type(value) is int and value >= 0 else None
        return NS(
            choices=[NS(finish_reason=reason, message=NS(content=content))],
            reported_usage=usage,
            output_error=error,
            metadata={
                "model": self.model_id,
                "provider": "ollama_cloud",
                "done_reason": reason
                if reason in {"stop", "length", "safety", "content_filter"}
                else "unknown",
                "thinking_present": bool(message.get("thinking")),
                "thinking_requested": self.stage_thinking[stage],
            },
        )


def make_nemotron_workflow(path, *, transport=None, row_limit=100):
    path = Path(path).resolve(strict=True)
    with closing(sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)) as connection:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
    allowed = tables & {"ConnectorHealingQueue", "ConnectorHealingLogs"}
    snapshot = Snapshot(
        path,
        row_limit=row_limit,
        allowed_tables=allowed,
        blocked_columns=[
            ("ConnectorHealingLogs", "Message"),
            ("ConnectorHealingLogs", "Details"),
            ("ConnectorHealingQueue", "LastError"),
            ("ConnectorHealingQueue", "ErrorMessage"),
        ],
    )
    sql_budget = positive_env("NEMOTRON_SQL_MAX_TOKENS", 4096)
    response_budget = positive_env("NEMOTRON_RESPONSE_MAX_TOKENS", 4096)
    client = NemotronClient(transport=transport)
    return Workflow(
        snapshot,
        client,
        model_id=client.model_id,
        provider="ollama_cloud",
        max_attempts=3,
        sql_max_tokens=sql_budget,
        response_max_tokens=response_budget,
    )
