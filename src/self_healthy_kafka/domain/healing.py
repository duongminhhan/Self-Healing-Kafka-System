from __future__ import annotations

from collections.abc import Iterator, Mapping, MutableMapping
from dataclasses import dataclass, field
from datetime import datetime
from enum import IntEnum, StrEnum
from typing import Any


class HealingStep(IntEnum):
    DETECT = 1
    RESTART = 2
    RECREATE_WITH_OFFSET = 3
    RECREATE_WITHOUT_OFFSET = 4


class RecoveryAction(StrEnum):
    WAIT = "WAIT"
    MARK_UNREACHABLE = "MARK_UNREACHABLE"
    MARK_NOT_CREATED = "MARK_NOT_CREATED"
    HANDLE_HEALTHY = "HANDLE_HEALTHY"
    DEBOUNCE = "DEBOUNCE"
    RESTART_TASKS = "RESTART_TASKS"
    RESTART_CONNECTOR = "RESTART_CONNECTOR"
    RECREATE_WITH_OFFSET = "RECREATE_WITH_OFFSET"
    RETRY_RECREATE_WITH_OFFSET = "RETRY_RECREATE_WITH_OFFSET"
    RECREATE_WITHOUT_OFFSET = "RECREATE_WITHOUT_OFFSET"
    ESCALATE = "ESCALATE"
    STOP = "STOP"


@dataclass(frozen=True)
class RecoveryDecision:
    action: RecoveryAction
    required_level: HealingStep | None = None
    attempted_event: str | None = None
    reason: str = ""


@dataclass(frozen=True)
class RecoveryPolicy:
    failure_confirm_checks: int
    task_restart_max_attempts: int
    connector_restart_max_attempts: int
    recreate_with_offset_timeout_max_attempts: int = 2
    recreate_without_offset_max_attempts: int = 1

    def __post_init__(self) -> None:
        numeric_values = {
            "failure_confirm_checks": self.failure_confirm_checks,
            "task_restart_max_attempts": self.task_restart_max_attempts,
            "connector_restart_max_attempts": self.connector_restart_max_attempts,
            "recreate_with_offset_timeout_max_attempts": (
                self.recreate_with_offset_timeout_max_attempts
            ),
            "recreate_without_offset_max_attempts": (
                self.recreate_without_offset_max_attempts
            ),
        }
        invalid = [name for name, value in numeric_values.items() if value < 1]
        if invalid:
            raise ValueError(f"Recovery policy values must be positive: {invalid}")

    @property
    def max_failed_count(self) -> int:
        return (
            self.failure_confirm_checks
            + self.task_restart_max_attempts
            + self.connector_restart_max_attempts
        )


@dataclass
class ConnectorJob(MutableMapping[str, Any]):
    """Typed healing aggregate with a mapping compatibility boundary."""

    id: Any
    connector_name: str
    job_name: str | None = None
    connector_type: str = "source"
    is_active: bool = True
    level: int = 1
    active_config: dict[str, Any] | None = None
    failed_count: int = 0
    failed_connector: bool = False
    failed_task: bool = False
    active_incident_id: str | None = None
    latest_event_type: str | None = None
    latest_event_at: datetime | str | None = None
    latest_event_details: dict[str, Any] = field(default_factory=dict)
    latest_has_next_step: bool | None = None
    current_phase: str = "HEALTHY"
    task_restart_count: int = 0
    connector_restart_count: int = 0
    recreate_with_offset_count: int = 0
    recreate_with_offset_timeout_count: int = 0
    recreate_without_offset_count: int = 0
    last_failed_task_ids: list[int] = field(default_factory=list)
    last_checked_at: datetime | None = None
    last_error: str | None = None
    _extra: dict[str, Any] = field(default_factory=dict, repr=False)

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | ConnectorJob) -> ConnectorJob:
        if isinstance(value, cls):
            return value
        data = dict(value)
        known = {
            item.name
            for item in cls.__dataclass_fields__.values()
            if item.init and not item.name.startswith("_")
        }
        kwargs = {key: data.pop(key) for key in list(data) if key in known}
        job = cls(**kwargs)
        job._extra.update(data)
        return job

    def copy(self, **updates: Any) -> ConnectorJob:
        data = self.to_dict()
        data.update(updates)
        return ConnectorJob.from_mapping(data)

    def to_dict(self) -> dict[str, Any]:
        result = {
            name: getattr(self, name)
            for name, item in self.__dataclass_fields__.items()
            if item.init and not name.startswith("_")
        }
        result.update(self._extra)
        return result

    @property
    def incident_id(self) -> str | None:
        return str(self.active_incident_id) if self.active_incident_id else None

    @property
    def has_failed_tasks(self) -> bool:
        return bool(self.failed_task or self.last_failed_task_ids)

    def __getitem__(self, key: str) -> Any:
        if key in self.__dataclass_fields__ and not key.startswith("_"):
            return getattr(self, key)
        return self._extra[key]

    def __setitem__(self, key: str, value: Any) -> None:
        if key in self.__dataclass_fields__ and not key.startswith("_"):
            setattr(self, key, value)
        else:
            self._extra[key] = value

    def __delitem__(self, key: str) -> None:
        if key in self.__dataclass_fields__ and not key.startswith("_"):
            raise KeyError(f"Cannot delete required ConnectorJob field {key!r}")
        del self._extra[key]

    def __iter__(self) -> Iterator[str]:
        yield from (
            name
            for name in self.__dataclass_fields__
            if not name.startswith("_")
        )
        yield from self._extra

    def __len__(self) -> int:
        return sum(1 for _ in self)

