from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from unittest.mock import MagicMock

import pytest

from self_healthy_kafka.domain.models import (
    ConnectorState,
    ConnectorStatus,
    TaskStatus,
)

# A clean worktree has no private env/dev.env. Keep tests runnable from the
# checked-in example while preserving an explicitly configured private file.
_ROOT = Path(__file__).resolve().parents[1]
_app_env = os.getenv("APP_ENV", "dev").strip().lower() or "dev"
if not os.getenv("SELF_HEALTHY_KAFKA_ENV_FILE") and not (_ROOT / "env" / f"{_app_env}.env").is_file():
    os.environ["SELF_HEALTHY_KAFKA_ENV_FILE"] = str(_ROOT / "env" / "dev.env.example")


def make_status(
    name: str = "conn-x",
    state: ConnectorState = ConnectorState.RUNNING,
    task_states: list[str] | None = None,
    trace: Optional[str] = None,
) -> ConnectorStatus:
    task_states = task_states or ["RUNNING"]
    tasks = [
        TaskStatus(
            id=i,
            state=s,
            worker_id="w1",
            trace=None if s == "RUNNING" else "task boom",
        )
        for i, s in enumerate(task_states)
    ]
    return ConnectorStatus(
        name=name,
        state=state,
        worker_id="w1",
        tasks=tasks,
        trace=trace,
    )


@pytest.fixture
def healthy_status():
    return make_status(
        state=ConnectorState.RUNNING,
        task_states=["RUNNING", "RUNNING"],
    )


@pytest.fixture
def failed_status():
    return make_status(state=ConnectorState.FAILED, trace="connector boom")


@pytest.fixture
def running_with_failed_task_status():
    return make_status(
        state=ConnectorState.RUNNING,
        task_states=["RUNNING", "FAILED"],
    )


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def mock_checker():
    return MagicMock()


@pytest.fixture
def mock_db():
    return MagicMock()
