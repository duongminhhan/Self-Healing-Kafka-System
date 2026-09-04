"""Wire schemas shared across Qwen modes; not a substitute for business validation."""


def obj(properties, required):
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
        "entity": {"enum": ["incidents", "events"]},
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
        "question": {"type": "string", "minLength": 1, "maxLength": 500},
    },
    ["kind", "question"],
)
SQL = obj(
    {
        "kind": {"const": "sql"},
        "sql": {"type": "string", "maxLength": 12000},
        "interpretation": {"type": "string", "maxLength": 2000},
    },
    ["kind", "sql"],
)
REVIEW = obj({"kind": {"const": "accept_result"}}, ["kind"])
SQL_STAGE_SCHEMA = {"anyOf": [PLAN, CLARIFICATION, SQL, REVIEW]}
EVIDENCE = obj(
    {"row": {"type": "integer", "minimum": 0}, "column": {"type": "string", "maxLength": 255}},
    ["row", "column"],
)
CLAIM = obj(
    {"text": {"type": "string", "maxLength": 2000}, "evidence": array(EVIDENCE, 100)},
    ["text", "evidence"],
)
RESPONSE_SCHEMA = obj({"claims": {**array(CLAIM, 200), "minItems": 1}}, ["claims"])
SCHEMAS = {"sql": SQL_STAGE_SCHEMA, "response": RESPONSE_SCHEMA}


def response_format(stage):
    return {
        "type": "json_schema",
        "json_schema": {"name": "qwen_" + stage, "schema": SCHEMAS[stage], "strict": True},
    }
