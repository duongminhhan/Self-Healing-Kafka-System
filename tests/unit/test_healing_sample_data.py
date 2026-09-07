from copy import deepcopy
from datetime import datetime, timezone

import pytest

from scripts.seed_healing_samples import (
    LOGS,
    QUEUE,
    canonical,
    generate,
    validate,
)


@pytest.fixture(scope="module")
def generated():
    return generate(datetime(2026, 9, 5, 9, tzinfo=timezone.utc))


def test_deterministic_runtime_generated_histories(generated):
    tables, cases, summary = generated
    second, _, _ = generate(datetime(2026, 9, 5, 9, tzinfo=timezone.utc))
    assert canonical(tables) == canonical(second)
    assert len(cases) == summary["queues"] == 27
    assert set(summary["statuses"]) == {"PENDING", "PROCESSING", "WAITING", "COMPLETED", "ESCALATED"}
    assert len({c["error"] for c in cases}) == 14
    assert summary["events"]["CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT"] == 1
    assert summary["events"]["OFFSET_PATCH_FAILED"] == 1
    assert all("sample-" in q["RootConnectorName"] for q in tables[QUEUE])


def test_spontaneous_and_unprocessed_rows_do_not_invent_logs(generated):
    tables, cases, _ = generated
    for case in cases:
        logs = [r for r in tables[LOGS] if r["QueueId"] == case["queue_id"]]
        if case["route"] in {"pending", "processing"}:
            assert logs == []
        if case["route"] == "spontaneous":
            assert case["outcome"] == "RECOVERED"
            assert [r["EventType"] for r in logs] == ["HEALTH_FAILURE_OBSERVED"]


@pytest.mark.parametrize("mutation", ["orphan", "fake_event", "duplicate_open", "wrong_mode", "bad_time", "fake_severity"])
def test_invalid_data_rejected_before_database_write(generated, mutation):
    tables = deepcopy(generated[0])
    if mutation == "orphan":
        tables[LOGS][0]["QueueId"] = "unknown"
    elif mutation == "fake_event":
        tables[LOGS][0]["EventType"] = "S3_ACCESS_DENIED"
    elif mutation == "duplicate_open":
        opened = [r for r in tables[QUEUE] if r["QueueStatus"] == "PENDING"][0]
        tables[QUEUE].append({**opened, "QueueId": "duplicate"})
    elif mutation == "wrong_mode":
        tables[QUEUE][0]["HealingMode"] = "RESTART_ONLY"
    elif mutation == "bad_time":
        tables[QUEUE][0]["CompletedAt"] = "2000-01-01T00:00:00.000+00:00"
    else:
        tables[LOGS][0]["Severity"] = "WARN"
    with pytest.raises(AssertionError):
        validate(tables)
