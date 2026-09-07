"""Stage-aware HF transport; no provider substitution or application retries.

InferenceClient 1.29's chat POST and default HTTPX transport do not retry.
SDK upgrades must retain the transport regression tests before adoption.
JSON/schema enforcement remains local even when a provider accepts a format.
"""

import json
import math
from types import SimpleNamespace

import httpx
from huggingface_hub import InferenceClient

from notebooks.qwen.output_schema import (
    SCHEMAS,
    contract_error,
    contract_instruction,
    response_format,
)


class HFServiceError(RuntimeError):
    def __init__(self, category, status=None):
        self.category = category
        self.response = SimpleNamespace(status_code=status)
        super().__init__(f"Hugging Face service error: {category}; HTTP {status}")


class QwenClient:
    response_source = "huggingface"

    def __init__(
        self,
        *,
        model,
        provider,
        api_key,
        sql_timeout=30,
        response_timeout=30,
        output_byte_limit=16000,
        client_factory=None,
        response_formats=None,
        structured_output="auto",
    ):
        if not api_key or not api_key.strip():
            raise ValueError("Set HF_TOKEN before calling Hugging Face")
        self.model, self.provider = model, provider
        self._api_key = api_key
        self.timeouts = {"sql": float(sql_timeout), "response": float(response_timeout)}
        if any(not math.isfinite(t) or t <= 0 for t in self.timeouts.values()):
            raise ValueError("HF stage timeouts must be finite positive seconds")
        if not isinstance(output_byte_limit, int) or not 1 <= output_byte_limit <= 64000:
            raise ValueError("HF output byte limit must be between 1 and 64000")
        self.output_byte_limit = output_byte_limit
        self.factory = client_factory or InferenceClient
        if structured_output not in {"auto", "json_schema", "local"}:
            raise ValueError("HF_STRUCTURED_OUTPUT must be auto, json_schema or local")
        self.structured_output = structured_output
        self.format_rejected = set()
        self.response_formats = (
            response_formats
            if response_formats is not None
            else {stage: response_format(stage) for stage in SCHEMAS}
        )
        for name, value in self.response_formats.items():
            if name not in SCHEMAS or value != response_format(name):
                raise ValueError("Custom response formats must match the local contract exactly")

    def complete_stage(self, messages, stage, max_tokens):
        return self.complete_contract(
            messages,
            stage,
            max_tokens,
            contract="legacy_generation" if stage == "sql" else "response",
        )

    def complete_contract(self, messages, stage, max_tokens, *, contract):
        if stage not in self.timeouts:
            raise ValueError("Unknown HF generation stage")
        if contract not in SCHEMAS or (stage == "response") != (contract == "response"):
            raise ValueError("Contract does not match HF stage")
        messages = [dict(message) for message in messages]
        instruction = contract_instruction(contract)
        if messages and messages[0].get("role") == "system":
            messages[0]["content"] += "\n" + instruction
        else:
            messages.insert(0, {"role": "system", "content": instruction})
        kwargs = {"messages": messages, "max_tokens": max_tokens, "temperature": 0}
        format_requested = (
            self.structured_output != "local"
            and contract not in self.format_rejected
            and contract in self.response_formats
        )
        if format_requested:
            kwargs["response_format"] = self.response_formats[contract]
        try:
            with self.factory(
                model=self.model,
                provider=self.provider,
                api_key=self._api_key,
                timeout=self.timeouts[stage],
            ) as client:
                completion = client.chat_completion(**kwargs)
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            # This failed request is already counted by Workflow.call. Do NOT
            # retry here: correction must spend the remaining SQL-call budget.
            # Only an explicit format incompatibility permits local-JSON mode.
            detail = str(exc).lower()
            if (
                self.structured_output == "auto"
                and format_requested
                and status in {400, 422}
                and any(word in detail for word in ("response_format", "json_schema"))
                and any(
                    word in detail for word in ("not supported", "unsupported", "not available")
                )
            ):
                self.format_rejected.update(SCHEMAS)
                return SimpleNamespace(
                    choices=[],
                    output_error="structured_output_unsupported_use_local_json",
                    reported_usage={"input": None, "output": None},
                    metadata={
                        "model": self.model,
                        "provider": self.provider,
                        "format_requested": True,
                        "format_status": "explicitly_rejected",
                        "http_status": status,
                        "generation_requests": 1,
                    },
                )
            if isinstance(exc, (TimeoutError, httpx.TimeoutException)):
                category = "timeout"
            elif status in {401, 403}:
                category = "authentication"
            elif status == 402:
                category = "billing"
            elif status == 429:
                category = "quota"
            elif status in {400, 404, 422} or isinstance(exc, ValueError):
                category = "unsupported_model_provider_or_parameters"
            else:
                category = "service_unavailable"
            raise HFServiceError(category, status) from None
        choices = getattr(completion, "choices", None) or []
        choice = choices[0] if choices else None
        reason = getattr(choice, "finish_reason", None)
        message = getattr(choice, "message", None)
        content = getattr(message, "content", None)
        error = None
        validation_detail = None
        output_kind = None
        if reason == "length":
            error = "output_truncated"
        elif reason in {"content_filter", "safety"}:
            error = "output_blocked"
        elif not isinstance(content, str) or not content.strip():
            error = "empty_content"
        elif reason != "stop":
            error = "unexpected_finish_reason"
        elif len(content.encode("utf-8")) > self.output_byte_limit:
            error = "output_size_exceeded"
        else:
            # Retain legacy's fenced JSON compatibility; never use thinking.
            candidate = content.strip()
            if candidate.startswith("```json") and candidate.endswith("```"):
                candidate = candidate[7:-3].strip()
            elif candidate.startswith("```") and candidate.endswith("```"):
                candidate = candidate[3:-3].strip()
            try:
                parsed = json.loads(
                    candidate, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value))
                )
                if not isinstance(parsed, dict):
                    error = "invalid_json_object"
                else:
                    kind = parsed.get("kind")
                    output_kind = (
                        kind
                        if isinstance(kind, str)
                        and kind in {"sql", "query", "clarification", "accept_result"}
                        else "other_or_missing"
                    )
                    validation_detail = contract_error(parsed, contract)
                    if validation_detail:
                        error = "invalid_output_schema"
            except (ValueError, TypeError, RecursionError):
                error = "invalid_json"
        usage = getattr(completion, "usage", None)
        return SimpleNamespace(
            choices=choices,
            output_error=error,
            validation_detail=validation_detail,
            reported_usage={
                "input": getattr(usage, "prompt_tokens", None),
                "output": getattr(usage, "completion_tokens", None),
            },
            metadata={
                "model": getattr(completion, "model", None) or self.model,
                "provider": self.provider,
                "finish_reason": reason,
                "output_error": error,
                "contract": contract,
                "output_kind": output_kind,
                "validation_error": validation_detail,
                "timeout_seconds": self.timeouts[stage],
                "max_tokens": max_tokens,
                "temperature": 0,
                "token_usage": {
                    "input": getattr(usage, "prompt_tokens", None),
                    "output": getattr(usage, "completion_tokens", None),
                },
                "format_requested": format_requested,
                "format_status": (
                    "accepted_and_locally_valid"
                    if format_requested and not error
                    else "requested_not_validated"
                    if format_requested
                    else "local_validation_only"
                ),
                "generation_requests": 1,
            },
        )
