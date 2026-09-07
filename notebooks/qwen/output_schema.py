"""State-specific wire contracts; not a substitute for business validation."""

from jsonschema import Draft202012Validator


def obj(properties, required):
    # Some constrained decoders need a typed enum discriminator rather than
    # const alone. Keep const for local branch selection, with identical meaning.
    if "kind" in properties and "const" in properties["kind"]:
        properties = dict(properties)
        value = properties["kind"]["const"]
        properties["kind"] = {"type": "string", "enum": [value], "const": value}
    return {
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": False,
    }


def array(items, maximum):
    return {"type": "array", "items": items, "maxItems": maximum}


TEXT = {"type": "string", "maxLength": 300}
ID = {"type": "string", "maxLength": 80}
OP = {"type": "string", "enum": ["eq", "ne", "lt", "lte", "gt", "gte"]}
FILTER = obj(
    {
        "field": ID,
        "op": {"type": "string", "enum": OP["enum"] + ["is_null", "not_null"]},
        "value": {"type": ["string", "integer"], "maxLength": 255},
    },
    ["field", "op"],
)
HAVING = obj(
    {
        "metric": ID,
        "op": OP,
        "value": {"type": "number"},
        "compare_to": {"type": "string", "enum": ["population_mean"]},
    },
    ["metric", "op"],
)
ORDER = obj(
    {"field": ID, "direction": {"type": "string", "enum": ["asc", "desc"]}}, ["field", "direction"]
)
PLAN = obj(
    {
        "kind": {"const": "query"},
        "entity": {"type": "string", "enum": ["incidents", "events"]},
        "dimensions": array(ID, 8),
        "metrics": array(ID, 6),
        "filters": array(FILTER, 20),
        "having": array(HAVING, 8),
        "order_by": array(ORDER, 12),
        "success_only": {"type": "boolean"},
        "latest_status": {"type": "boolean"},
        "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
        "assumptions": array(TEXT, 8),
    },
    ["kind", "entity", "dimensions", "metrics"],
)
CLARIFICATION = obj(
    {
        "kind": {"const": "clarification"},
        "question": {
            "type": "string",
            "minLength": 1,
            "maxLength": 500,
            "description": "Only a genuinely unresolved metric or denominator. Never ask about omitted optional time/status filters or confirm documented definitions.",
        },
    },
    ["kind", "question"],
)
SQL = obj(
    {
        "kind": {"const": "sql"},
        "sql": {"type": "string", "maxLength": 12000},
        "interpretation": {"type": "string", "maxLength": 2000},
    },
    ["kind", "sql", "interpretation"],
)
REVIEW = obj({"kind": {"const": "accept_result"}}, ["kind"])
EVIDENCE = obj(
    {"row": {"type": "integer", "minimum": 0}, "column": {"type": "string", "maxLength": 255}},
    ["row", "column"],
)
CLAIM = obj(
    {"text": {"type": "string", "maxLength": 2000}, "evidence": array(EVIDENCE, 100)},
    ["text", "evidence"],
)
RESPONSE_SCHEMA = obj({"claims": {**array(CLAIM, 200), "minItems": 1}}, ["claims"])


def alternatives(*branches):
    properties = {k: v for b in branches for k, v in b["properties"].items()}
    properties["kind"] = {
        "type": "string",
        "enum": [b["properties"]["kind"]["const"] for b in branches],
    }
    return {
        "type": "object",
        "properties": properties,
        "required": ["kind"],
        "additionalProperties": False,
        "allOf": [
            {
                "if": {"properties": {"kind": b["properties"]["kind"]}, "required": ["kind"]},
                "then": b,
            }
            for b in branches
        ],
    }


SCALAR_PLAN = obj(
    {key: value for key, value in PLAN["properties"].items()
     if key in {"kind", "entity", "dimensions", "metrics", "filters", "success_only"}},
    ["kind", "entity", "dimensions", "metrics"],
)
SCALAR_PLAN["properties"]["dimensions"] = array(ID, 0)
SCALAR_PLAN["properties"]["metrics"] = {**array(ID, 6), "minItems": 1}
INDEPENDENT = obj(
    {"kind": {"const": "independent"},
     "queries": {**array(SCALAR_PLAN, 4), "minItems": 2}},
    ["kind", "queries"],
)

SCHEMAS = {
    "legacy_generation": alternatives(SQL, CLARIFICATION),
    "legacy_review": alternatives(SQL, CLARIFICATION, REVIEW),
    "strict_planning": alternatives(PLAN, INDEPENDENT, CLARIFICATION),
    "response": RESPONSE_SCHEMA,
}


def contract_error(value, contract):
    """Return bounded schema-owned details, never echo model values or unknown keys."""
    schema = SCHEMAS[contract]
    if not isinstance(value, dict):
        return "Expected a JSON object"
    if "allOf" in schema:
        branches = [rule["then"] for rule in schema["allOf"]]
        allowed = [b["properties"]["kind"]["const"] for b in branches]
        branch = next(
            (b for b in branches if value.get("kind") == b["properties"]["kind"]["const"]), None
        )
        if branch is None:
            return "kind must be one of: " + ", ".join(allowed)
        schema = branch
    missing = [name for name in schema["required"] if name not in value]
    if missing:
        return "Missing required fields: " + ", ".join(missing)
    error = next(Draft202012Validator(schema).iter_errors(value), None)
    if error:
        # JSONSchema messages can contain sensitive instance values. Use only
        # validator keywords and schema-owned top-level field names.
        field = next(iter(error.path), None)
        field = field if field in schema.get("properties", {}) else "object"
        detail = f"Invalid {field}: {error.validator} constraint"
        if error.validator == "enum":
            detail += "; expected one of " + ", ".join(map(str, error.validator_value))
        elif error.validator == "type":
            detail += "; expected " + str(error.validator_value)
        elif error.validator == "required":
            missing = [key for key in error.schema["required"] if key not in error.instance]
            detail += "; missing " + ", ".join(missing)
        return detail
    return None


def contract_instruction(contract):
    schema = SCHEMAS[contract]
    branches = [rule["then"] for rule in schema["allOf"]] if "allOf" in schema else [schema]
    shapes = [
        {"kind": b["properties"].get("kind", {}).get("const"), "required": b["required"]}
        for b in branches
    ]
    return (
        "Return one compact JSON object only, no prose or reasoning outside JSON. "
        f"Current contract {contract}: {shapes}. "
        "Preserve the original question, metric, filters and time scope."
    )


def response_format(stage):
    return {
        "type": "json_schema",
        "json_schema": {"name": "qwen_" + stage, "schema": SCHEMAS[stage], "strict": True},
    }
