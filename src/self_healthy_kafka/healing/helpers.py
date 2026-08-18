"""Shared helper functions for healing modules.

This module consolidates utility functions that were previously duplicated
across db_state_machine.py and actions.py to improve maintainability.
"""
from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from self_healthy_kafka.domain.models import HealthResult
from self_healthy_kafka.healing.recovery_summary import format_healing_steps


def utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(timezone.utc)


def incident_id(job: Mapping[str, Any]) -> str | None:
    """Extract active incident ID from job record."""
    value = job.get("active_incident_id")
    return str(value) if value else None


def failure_message(result: HealthResult) -> str:
    """Extract failure message from health result."""
    return result.trace or result.reason


def failure_details(result: HealthResult, **extra: Any) -> dict[str, Any]:
    """Build failure details dictionary with common fields."""
    details = {
        "reason": result.reason,
        "checked_at": result.checked_at.isoformat(),
    }
    if result.trace:
        details["trace"] = result.trace
    details.update(extra)
    return details


def latest_event_at(job: Mapping[str, Any]) -> datetime | None:
    """Parse latest_event_at field from job record."""
    from self_healthy_kafka.storage.common import parse_datetime
    return parse_datetime(job.get("latest_event_at"))


def level(job: Mapping[str, Any]) -> int:
    """Get healing level from job record."""
    return int(job.get("level") or 1)


def recovery_details(
    job: Mapping[str, Any],
    result: HealthResult,
) -> dict[str, Any]:
    """Build recovery details dictionary."""
    latest_details = job.get("latest_event_details") or {}
    current_name = job["connector_name"]
    original_name = (
        latest_details.get("original_connector_name")
        or latest_details.get("old_connector_name")
        or current_name
    )
    old_name = latest_details.get("old_connector_name")
    step_counts = {
        "TASK_RESTART": int(job.get("task_restart_count") or 0),
        "CONNECTOR_RESTART": int(job.get("connector_restart_count") or 0),
        "CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT": int(
            job.get("recreate_with_offset_timeout_count") or 0
        ),
        "CONNECTOR_RECREATE_WITH_OFFSET": int(job.get("recreate_with_offset_count") or 0),
        "CONNECTOR_RECREATE_WITHOUT_OFFSET": int(
            job.get("recreate_without_offset_count") or 0
        ),
    }
    healing_steps = format_healing_steps(
        step_counts,
        final_connector_name=current_name,
    )
    return {
        "previous_event": job.get("latest_event_type"),
        "checked_at": result.checked_at.isoformat(),
        "original_connector_name": original_name,
        "old_connector_name": old_name,
        "final_connector_name": current_name,
        "healing_steps": healing_steps,
        "healing_step_counts": step_counts,
    }


def recovery_message(details: dict[str, Any]) -> str:
    """Format recovery message from details."""
    return (
        "Connector recovered. "
        f"Original connector: {details['original_connector_name']}; "
        f"current connector: {details['final_connector_name']}; "
        f"healing steps: {details['healing_steps']}"
    )


