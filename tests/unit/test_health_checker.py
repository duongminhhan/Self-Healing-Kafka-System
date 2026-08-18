from unittest.mock import MagicMock

from self_healthy_kafka.domain.models import ConnectorState, HealthStatus
from self_healthy_kafka.health.checker import HealthChecker
from tests.conftest import make_status


def test_healthy_when_running_and_all_tasks_running():
    client = MagicMock()
    client.get_status.return_value = make_status(
        state=ConnectorState.RUNNING, task_states=["RUNNING", "RUNNING"]
    )
    result = HealthChecker(client).check("conn-x")
    assert result.status == HealthStatus.HEALTHY


def test_unhealthy_when_connector_failed():
    client = MagicMock()
    client.get_status.return_value = make_status(
        state=ConnectorState.FAILED, trace="db gone"
    )
    result = HealthChecker(client).check("conn-x")
    assert result.status == HealthStatus.UNHEALTHY
    assert result.trace == "db gone"
    assert "FAILED" in result.reason


def test_unhealthy_when_task_failed_on_running_connector():
    client = MagicMock()
    client.get_status.return_value = make_status(
        state=ConnectorState.RUNNING, task_states=["RUNNING", "FAILED"]
    )
    result = HealthChecker(client).check("conn-x")
    assert result.status == HealthStatus.UNHEALTHY
    assert result.failed_task_ids == [1]
    assert result.trace == "task boom"


def test_paused_returns_healthy_and_emits_alert_only(caplog):
    client = MagicMock()
    client.get_status.return_value = make_status(state=ConnectorState.PAUSED)
    with caplog.at_level("WARNING"):
        result = HealthChecker(client).check("conn-x")
    assert result.status == HealthStatus.HEALTHY
    assert "alert only" in result.reason.lower()
    assert any(
        getattr(r, "event", None) == "connector_alert_only"
        for r in caplog.records
    )


def test_unassigned_returns_healthy_and_emits_alert_only(caplog):
    client = MagicMock()
    client.get_status.return_value = make_status(state=ConnectorState.UNASSIGNED)
    with caplog.at_level("WARNING"):
        result = HealthChecker(client).check("conn-x")
    assert result.status == HealthStatus.HEALTHY
    assert any(
        getattr(r, "event", None) == "connector_alert_only"
        for r in caplog.records
    )


def test_stopped_returns_context_neutral_status():
    client = MagicMock()
    client.get_status.return_value = make_status(state=ConnectorState.STOPPED)

    result = HealthChecker(client).check("conn-x")

    assert result.status == HealthStatus.STOPPED
    assert "STOPPED" in result.reason


def test_404_returns_not_created_with_waiting_reason(caplog):
    client = MagicMock()
    client.get_status.return_value = None
    with caplog.at_level("INFO"):
        result = HealthChecker(client).check("ghost")
    assert result.status == HealthStatus.NOT_CREATED
    assert "waiting for initial creation" in result.reason
    assert any(
        getattr(r, "event", None) == "connector_not_created"
        for r in caplog.records
    )


def test_unreachable_returns_none():
    client = MagicMock()
    client.get_status.side_effect = RuntimeError("connection refused")
    result = HealthChecker(client).check("conn-x")
    assert result is None
