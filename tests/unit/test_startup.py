from types import SimpleNamespace

from self_healthy_kafka import startup


def test_chat_only_startup_checks_database_but_not_kafka_connect(monkeypatch):
    calls = []
    monkeypatch.setattr(
        startup,
        "cfg",
        SimpleNamespace(
            polling=SimpleNamespace(enabled=False),
            grafana_webhook=SimpleNamespace(enabled=False),
        ),
    )
    monkeypatch.setattr(startup, "_check_mssql", lambda: calls.append("mssql"))
    monkeypatch.setattr(startup, "_check_kafka_connect", lambda: calls.append("connect"))

    startup.validate_environment()

    assert calls == ["mssql"]


def test_healing_startup_still_checks_kafka_connect(monkeypatch):
    calls = []
    monkeypatch.setattr(
        startup,
        "cfg",
        SimpleNamespace(
            polling=SimpleNamespace(enabled=True),
            grafana_webhook=SimpleNamespace(enabled=False),
        ),
    )
    monkeypatch.setattr(startup, "_check_mssql", lambda: calls.append("mssql"))
    monkeypatch.setattr(startup, "_check_kafka_connect", lambda: calls.append("connect"))

    startup.validate_environment()

    assert calls == ["mssql", "connect"]
