"""Deterministic, bounded schema selection for notebook model prompts.

The selector is deliberately lexical and catalog-backed.  It never selects a
query or result and therefore cannot leak a gold answer into evaluation.
"""

from __future__ import annotations

import copy
import re
import unicodedata
from collections.abc import Mapping
from typing import Any


def normalize_text(value: object) -> str:
    """Return a stable accent-insensitive form for Vietnamese matching."""
    text = unicodedata.normalize("NFKD", str(value).casefold())
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(re.findall(r"[a-z0-9_]+", text.replace("đ", "d")))


def _tokens(value: object) -> set[str]:
    return {token for token in normalize_text(value).split() if len(token) > 1}


def _question_hints(question: str) -> set[str]:
    text = normalize_text(question)
    hints: set[str] = set()
    groups = {
        "incident": ("incident", "su co", "hang doi", "queue", "ca healing"),
        "event": ("healing log", "log healing", "nhat ky", "event", "su kien"),
        "recovery": ("phuc hoi", "recovered", "recovery", "thanh cong"),
        "status": ("trang thai", "status", "hien tai", "gan nhat", "moi nhat"),
        "duration": ("mat bao lau", "thoi gian", "duration", "bao nhieu phut"),
        "failure": ("loi", "that bai", "failure", "error"),
        "time": ("ngay", "tuan", "thang", "hom nay", "hom qua", "time"),
        "connector": ("connector", "ket noi"),
    }
    for name, phrases in groups.items():
        if any(phrase in text for phrase in phrases):
            hints.add(name)
    return hints


def _table_score(
    table: str,
    columns: list[dict[str, Any]],
    definition: Mapping[str, Any] | None,
    question_tokens: set[str],
    hints: set[str],
) -> int:
    table_text = normalize_text(table)
    column_text = " ".join(normalize_text(column.get("name", "")) for column in columns)
    definition_text = normalize_text(definition or {})
    score = 8 * len(question_tokens & _tokens(table_text))
    score += 4 * len(question_tokens & _tokens(column_text))
    score += 2 * len(question_tokens & _tokens(definition_text))
    lowered = table_text.replace("_", "")
    if "incident" in hints and ("queue" in lowered or "incident" in lowered):
        score += 18
    if "event" in hints and ("log" in lowered or "event" in lowered):
        score += 18
    if hints & {"recovery", "status", "duration"} and (
        "queue" in lowered or "incident" in lowered
    ):
        score += 12
    if "failure" in hints and ("log" in lowered or "event" in lowered):
        score += 8
    return score


def _referenced_columns(value: object, available: set[str]) -> set[str]:
    result: set[str] = set()
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in available:
                result.add(str(key))
            result.update(_referenced_columns(child, available))
    elif isinstance(value, (list, tuple, set)):
        for child in value:
            result.update(_referenced_columns(child, available))
    elif isinstance(value, str) and value in available:
        result.add(value)
    return result


def _select_columns(
    table: str,
    columns: list[dict[str, Any]],
    *,
    question: str,
    semantic_entity: Mapping[str, Any] | None,
    relationships: list[dict[str, Any]],
    maximum: int,
) -> list[dict[str, Any]]:
    if len(columns) <= maximum:
        return copy.deepcopy(columns)
    available = {str(column.get("name")) for column in columns}
    required = {
        str(column.get("name"))
        for column in columns
        if column.get("primary_key_position")
    }
    for relationship in relationships:
        if relationship.get("from_table") == table:
            required.add(str(relationship.get("from_column")))
        if relationship.get("to_table") == table:
            required.add(str(relationship.get("to_column")))
    required.update(_referenced_columns(semantic_entity or {}, available))
    question_tokens = _tokens(question)

    def score(column: dict[str, Any]) -> tuple[int, int]:
        name = str(column.get("name", ""))
        normalized = normalize_text(name)
        value = 20 if name in required else 0
        value += 5 * len(question_tokens & _tokens(normalized))
        return value, -columns.index(column)

    ranked = sorted(columns, key=score, reverse=True)
    selected_names = {str(column.get("name")) for column in ranked[:maximum]}
    return [copy.deepcopy(column) for column in columns if str(column.get("name")) in selected_names]


def select_context_for_question(
    context: Mapping[str, Any],
    question: str,
    *,
    max_tables: int = 4,
    max_columns_per_table: int = 24,
) -> dict[str, Any]:
    """Return a safe prompt context plus auditable selection metadata."""
    if max_tables < 1 or max_columns_per_table < 1:
        raise ValueError("Context selection limits must be positive")
    tables = context.get("tables") or {}
    if not isinstance(tables, Mapping) or not tables:
        raise ValueError("Context must contain at least one schema table")
    definitions = context.get("business_definitions") or {}
    question_tokens = _tokens(question)
    hints = _question_hints(question)
    ranked = sorted(
        tables,
        key=lambda table: (
            -_table_score(
                str(table),
                list(tables[table]),
                definitions.get(table) if isinstance(definitions, Mapping) else None,
                question_tokens,
                hints,
            ),
            str(table).casefold(),
        ),
    )
    selected = set(ranked[:max_tables])
    relationships = [
        dict(item)
        for item in context.get("relationships", [])
        if item.get("from_table") in selected and item.get("to_table") in selected
    ]
    semantic_catalog = copy.deepcopy(context.get("semantic_catalog") or {})
    entities = semantic_catalog.get("entities")
    if isinstance(entities, dict):
        semantic_catalog["entities"] = {
            key: value for key, value in entities.items() if key in selected
        }
    catalog_relationships = semantic_catalog.get("relationships")
    if isinstance(catalog_relationships, list):
        semantic_catalog["relationships"] = [
            item
            for item in catalog_relationships
            if item.get("from_table") in selected and item.get("to_table") in selected
        ]
    output_tables: dict[str, list[dict[str, Any]]] = {}
    selected_schema_ids: list[str] = []
    for table in ranked:
        if table not in selected:
            continue
        entity = semantic_catalog.get("entities", {}).get(table, {})
        chosen_columns = _select_columns(
            str(table),
            list(tables[table]),
            question=question,
            semantic_entity=entity,
            relationships=relationships,
            maximum=max_columns_per_table,
        )
        output_tables[str(table)] = chosen_columns
        selected_schema_ids.extend(
            f"{table}.{column['name']}" for column in chosen_columns
        )
    observations = {
        key: copy.deepcopy(value)
        for key, value in (context.get("categorical_observations") or {}).items()
        if key.split(".", 1)[0] in selected
    }
    return {
        **{
            key: copy.deepcopy(value)
            for key, value in context.items()
            if key
            not in {
                "tables",
                "relationships",
                "business_definitions",
                "semantic_catalog",
                "categorical_observations",
                "context_selection",
            }
        },
        "tables": output_tables,
        "relationships": relationships,
        "business_definitions": {
            key: copy.deepcopy(value)
            for key, value in definitions.items()
            if key in selected
        },
        "semantic_catalog": semantic_catalog,
        "categorical_observations": observations,
        "context_selection": {
            "strategy": "deterministic_catalog_lexical_v1",
            "selected_tables": [table for table in ranked if table in selected],
            "selected_schema_ids": selected_schema_ids,
            "available_table_count": len(tables),
            "max_tables": max_tables,
            "max_columns_per_table": max_columns_per_table,
            "question_hints": sorted(hints),
        },
    }
