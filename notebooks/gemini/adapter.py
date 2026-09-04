"""Official Google GenAI transport for the shared, read-only analytics workflow.

No SQL generation/execution logic lives here. No implicit provider fallback or retries.
Docs: https://googleapis.github.io/python-genai/
      https://ai.google.dev/gemini-api/docs/models/gemini-3.5-flash
"""

import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace as NS

import httpx
from google import genai
from google.genai import errors, types

DEFAULT_MODEL = "gemini-3.5-flash"
SQL_SCHEMA = {
    "type": "object",
    "properties": {
        "kind": {"type": "string", "enum": ["sql", "clarification", "accept_result"]},
        "sql": {"type": "string"},
        "interpretation": {"type": "string"},
        "question": {"type": "string"},
    },
    "required": ["kind"],
    "additionalProperties": False,
}
RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "claims": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "evidence": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "row": {"type": "integer", "minimum": 0},
                                "column": {"type": "string"},
                            },
                            "required": ["row", "column"],
                            "additionalProperties": False,
                        },
                    },
                },
                "required": ["text", "evidence"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["claims"],
    "additionalProperties": False,
}

# Raw text is never read into results, including through aliases/expressions/WHERE.
# This is an opt-in policy: existing HF Snapshot defaults are unchanged.
RAW_LOG_COLUMNS = [
    ("ConnectorHealingLogs", "Message"),
    ("ConnectorHealingLogs", "Details"),
]


class GeminiServiceError(RuntimeError):
    """Only safe classifications, never SDK request bodies or credentials."""

    def __init__(self, category, status=None):
        self.category = category
        self.response = NS(status_code=status)
        super().__init__(
            f"Gemini service error: {category}" + (f" (HTTP {status})" if status else "")
        )


def enum_name(value):
    return getattr(value, "value", value) or "UNSPECIFIED"


class GeminiAdapter:
    response_source = "gemini"
    error_label = "Gemini"

    def __init__(
        self,
        *,
        api_key,
        model_id=DEFAULT_MODEL,
        timeout_seconds=60,
        thinking_level="low",
        response_thinking_level=None,
        sdk_client=None,
    ):
        if not api_key or not api_key.strip():
            raise ValueError(
                "Set GEMINI_API_KEY in the environment or ignored .env.gemini; no provider fallback."
            )
        if not model_id or not model_id.strip():
            raise ValueError("GEMINI_MODEL_ID must not be empty.")
        if model_id.strip() == "gemini-flash-3.5":
            raise ValueError("Official model ID is gemini-3.5-flash, not gemini-flash-3.5.")
        self.model_id = model_id.strip()
        self.thinking = {
            "sql": thinking_level,
            "response": response_thinking_level or thinking_level,
        }
        if any(
            level not in {"minimal", "low", "medium", "high", "default"}
            for level in self.thinking.values()
        ):
            raise ValueError(
                "Thinking level must be minimal, low, medium, high or default; availability depends on model."
            )
        if not 0 < float(timeout_seconds) <= 600:
            raise ValueError("GEMINI_REQUEST_TIMEOUT_SECONDS must be in (0, 600].")
        self.client = sdk_client or genai.Client(
            api_key=api_key.strip(),
            vertexai=False,
            http_options=types.HttpOptions(
                base_url="https://generativelanguage.googleapis.com",
                api_version="v1beta",
                timeout=int(float(timeout_seconds) * 1000),
                retry_options=types.HttpRetryOptions(attempts=1),
            ),
        )

    @classmethod
    def from_env(cls):
        return cls(
            api_key=os.getenv("GEMINI_API_KEY", ""),
            model_id=os.getenv("GEMINI_MODEL_ID", DEFAULT_MODEL),
            timeout_seconds=float(os.getenv("GEMINI_REQUEST_TIMEOUT_SECONDS", "60")),
            thinking_level=os.getenv("GEMINI_THINKING_LEVEL", "low").strip().lower(),
            response_thinking_level=os.getenv("GEMINI_RESPONSE_THINKING_LEVEL", "low")
            .strip()
            .lower(),
        )

    def complete_stage(self, messages, stage, max_tokens):
        if stage not in {"sql", "response"}:
            raise ValueError("Unknown analytics stage.")
        if not 1 <= int(max_tokens) <= 65536:
            raise ValueError("Output budget must be between 1 and 65536 tokens.")
        system, contents = [], []
        for message in messages:
            role, text = message["role"], message["content"]
            if not isinstance(text, str):
                raise ValueError("Only text messages are supported.")
            if role == "system":
                system.append(text)
            elif role in {"user", "assistant"}:
                contents.append(
                    types.Content(
                        role="model" if role == "assistant" else "user",
                        parts=[types.Part(text=text)],
                    )
                )
            else:
                raise ValueError("Unsupported analytics message role.")
        thinking = self.thinking[stage]
        config = types.GenerateContentConfig(
            system_instruction="\n\n".join(system),
            response_mime_type="application/json",
            response_json_schema=SQL_SCHEMA if stage == "sql" else RESPONSE_SCHEMA,
            max_output_tokens=int(max_tokens),
            # Keep the model's temperature default; Gemini 3 recommends default 1.0.
            thinking_config=None
            if thinking == "default"
            else types.ThinkingConfig(
                thinking_level=thinking.upper(),
                include_thoughts=False,
            ),
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
        )
        try:
            response = self.client.models.generate_content(
                model=self.model_id,
                contents=contents,
                config=config,
            )
        except (httpx.TimeoutException, TimeoutError):
            raise GeminiServiceError("timeout") from None
        except errors.APIError as exc:
            status = exc.code
            # Inspect locally, never retain or emit the remote error body.
            billing = any(
                marker in str(exc).lower()
                for marker in (
                    "billing disabled",
                    "billing is disabled",
                    "billing not enabled",
                    "billing_not_enabled",
                )
            )
            category = (
                "billing"
                if status == 402 or (billing and status != 429)
                else "quota_or_rate_limit"
                if status == 429
                else "authentication_or_permission"
                if status in {401, 403}
                else "model_unavailable"
                if status == 404
                else "invalid_request"
                if status == 400
                else "service_unavailable"
            )
            raise GeminiServiceError(category, status) from None
        except httpx.TransportError:
            raise GeminiServiceError("connection_error") from None
        usage = response.usage_metadata
        candidates = response.candidates or []
        candidate = candidates[0] if candidates else None
        finish = enum_name(candidate.finish_reason) if candidate else "NO_CANDIDATE"
        blocked = enum_name(getattr(response.prompt_feedback, "block_reason", None))
        parts = getattr(getattr(candidate, "content", None), "parts", None) or []
        content = "".join(p.text for p in parts if p.text and not p.thought)
        output_error = None
        if blocked not in {"UNSPECIFIED", "BLOCK_REASON_UNSPECIFIED"} or finish in {
            "SAFETY",
            "BLOCKLIST",
            "PROHIBITED_CONTENT",
            "SPII",
            "IMAGE_SAFETY",
        }:
            output_error = "Gemini output blocked by safety policy."
        elif finish == "MAX_TOKENS":
            output_error = (
                "Gemini output budget exhausted (MAX_TOKENS); no partial result accepted."
            )
        elif finish != "STOP":
            output_error = "Gemini output incomplete: " + finish
        else:
            try:
                if not isinstance(json.loads(content), dict):
                    raise ValueError
            except (ValueError, TypeError):
                output_error = "Gemini returned invalid JSON; no result accepted."
        return NS(
            choices=[
                NS(
                    message=NS(content=content),
                    finish_reason="stop" if finish == "STOP" else finish,
                )
            ],
            usage=NS(
                prompt_tokens=getattr(usage, "prompt_token_count", None),
                completion_tokens=getattr(usage, "candidates_token_count", None),
                thinking_tokens=getattr(usage, "thoughts_token_count", None),
                cached_tokens=getattr(usage, "cached_content_token_count", None),
                total_tokens=getattr(usage, "total_token_count", None),
            ),
            metadata={
                "model_version": response.model_version,
                "finish_reason": finish,
                "block_reason": blocked,
                "thinking_level": thinking,
            },
            output_error=output_error,
        )

    def close(self):
        self.client.close()


def make_gemini_workflow(snapshot_path, *, row_limit=None):
    """Same shared workflow, privacy-restricted snapshot, environment configuration."""
    from notebooks.shared.analytics import Snapshot, Workflow

    allowed = os.getenv("GEMINI_SQL_ALLOWED_TABLES", "").strip()
    if allowed:
        tables = [name.strip() for name in allowed.split(",") if name.strip()]
    else:
        with closing(
            sqlite3.connect(
                Path(snapshot_path).resolve(strict=True).as_uri() + "?mode=ro", uri=True
            )
        ) as conn:
            tables = [
                r[0]
                for r in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('ConnectorHealingQueue','ConnectorHealingLogs')"
                )
            ]
    snapshot = Snapshot(
        snapshot_path,
        allowed_tables=tables,
        blocked_columns=RAW_LOG_COLUMNS,
        row_limit=row_limit or int(os.getenv("GEMINI_SQL_RESULT_ROW_LIMIT", "100")),
        byte_limit=int(os.getenv("GEMINI_SQL_RESULT_BYTE_LIMIT", "64000")),
        timeout_seconds=float(os.getenv("GEMINI_SQL_TIMEOUT_SECONDS", "3")),
    )
    client = GeminiAdapter.from_env()
    return Workflow(
        snapshot,
        client,
        model_id=client.model_id,
        provider="google",
        max_attempts=3,
        sql_max_tokens=int(os.getenv("GEMINI_SQL_MAX_TOKENS", "4096")),
        response_max_tokens=int(os.getenv("GEMINI_RESPONSE_MAX_TOKENS", "4096")),
        few_shot=os.getenv("GEMINI_FEW_SHOT", "true").lower() in {"true", "1", "yes"},
    )
