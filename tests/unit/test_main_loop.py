from unittest.mock import MagicMock, call

from self_healthy_kafka.main import run_health_checks


def test_run_health_checks_delegates_to_state_machine():
    state_machine = MagicMock()
    state_machine.tick.return_value = []

    run_health_checks(state_machine)

    state_machine.tick.assert_called_once_with()


def test_run_health_checks_schedules_recovery_followups():
    state_machine = MagicMock()
    state_machine.tick.return_value = ["CDC.001", "CDC.002"]
    schedule_followup = MagicMock()

    run_health_checks(state_machine, schedule_followup)

    assert schedule_followup.call_args_list == [call("CDC.001"), call("CDC.002")]
