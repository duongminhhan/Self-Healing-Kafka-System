from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

# ─── Kafka Connect states ──────────────────────────────────────────────────────

class ConnectorState(str, Enum):
    RUNNING    = "RUNNING"
    FAILED     = "FAILED"
    PAUSED     = "PAUSED"
    STOPPED    = "STOPPED"
    UNASSIGNED = "UNASSIGNED"


class HealthStatus(str, Enum):
    HEALTHY     = "HEALTHY"
    UNHEALTHY   = "UNHEALTHY"
    NOT_CREATED = "NOT_CREATED"
    STOPPED     = "STOPPED"


# ─── Kafka Connect API response models ────────────────────────────────────────

@dataclass
class TaskStatus:
    id: int
    state: str
    worker_id: str = ""
    trace: Optional[str] = None


@dataclass
class ConnectorStatus:
    name: str
    state: ConnectorState
    worker_id: str
    tasks: list[TaskStatus]
    connector_type: str = "source"
    trace: Optional[str] = None
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @classmethod
    def from_api_response(cls, data: dict) -> ConnectorStatus:
        tasks = [
            TaskStatus(
                id=t["id"],
                state=t["state"],
                worker_id=t.get("worker_id", ""),
                trace=t.get("trace"),
            )
            for t in data.get("tasks", [])
        ]
        return cls(
            name=data["name"],
            state=ConnectorState(data["connector"]["state"]),
            worker_id=data["connector"].get("worker_id", ""),
            tasks=tasks,
            connector_type=data.get("type", "source"),
            trace=data["connector"].get("trace"),
        )


# ─── Health evaluation result ──────────────────────────────────────────────────

@dataclass
class HealthResult:
    connector_name: str
    status: HealthStatus
    reason: str
    failed_task_ids: list[int] = field(default_factory=list)
    trace: Optional[str] = None
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def is_healthy(self) -> bool:
        return self.status == HealthStatus.HEALTHY

    @property
    def is_unhealthy(self) -> bool:
        return self.status == HealthStatus.UNHEALTHY

    @property
    def is_not_created(self) -> bool:
        return self.status == HealthStatus.NOT_CREATED
