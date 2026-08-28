from __future__ import annotations

from typing import Optional
from unittest.mock import MagicMock

import pytest

from self_healthy_kafka.domain.models import (
    ConnectorState,
    ConnectorStatus,
    TaskStatus,
)


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
