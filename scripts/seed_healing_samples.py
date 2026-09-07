"""Generate synthetic histories through the real healing engine; opt-in local DB reset.

No Kafka client is constructed. External health/actions are simulated. Queue hydration
mirrors spGetConnectorHealingQueue; log payload normalization uses the real repository.
Default is offline generation. --apply-local replaces ONLY the two local PoC tables.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import subprocess
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import NAMESPACE_URL, uuid4, uuid5

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from self_healthy_kafka.domain.models import HealthResult, HealthStatus  # noqa: E402
from self_healthy_kafka.healing.db_state_machine import ConnectorStateMachine  # noqa: E402
from self_healthy_kafka.healing.phases import EventType  # noqa: E402
from self_healthy_kafka.storage.common import decode_datetimeoffset, rows_to_dicts  # noqa: E402
from self_healthy_kafka.storage.connector_repository import MssqlConnectorRepository  # noqa: E402
from self_healthy_kafka.storage.log_repository import _connector_log_details  # noqa: E402

QUEUE = "ConnectorHealingQueue"
LOGS = "ConnectorHealingLogs"
TABLES = (QUEUE, LOGS)
ORACLE = "io.debezium.connector.oracle.OracleConnector"
JDBC = "io.confluent.connect.jdbc.JdbcSinkConnector"
POSTGRES = "io.debezium.connector.postgresql.PostgresConnector"
S3 = "io.confluent.connect.s3.S3SinkConnector"
OPEN = {"PENDING", "PROCESSING", "WAITING"}
COUNTERS = {
    "failed_count": {"HEALTH_FAILURE_OBSERVED"},
    "task_restart_count": {"TASK_RESTART"},
    "connector_restart_count": {"CONNECTOR_RESTART"},
    "recreate_with_offset_count": {
        "CONNECTOR_RECREATE_WITH_OFFSET", "CONNECTOR_RECREATE_WITH_OFFSET_FAILED"
    },
    "recreate_with_offset_timeout_count": {"CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT"},
    "recreate_without_offset_count": {
        "CONNECTOR_RECREATE_WITHOUT_OFFSET", "CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED"
    },
}
# Representative synthetic exception excerpts, NOT captured production stack traces.
ERRORS = {
    "oracle-timeout": (ORACLE, "java.sql.SQLTimeoutException: ORA-01013: user requested cancel of current operation"),
    "oracle-redo": (ORACLE, "java.sql.SQLException: ORA-01291: missing log file"),
    "oracle-undo": (ORACLE, "java.sql.SQLException: ORA-01555: snapshot too old"),
    "oracle-login": (ORACLE, "java.sql.SQLException: ORA-01017: invalid username/password; logon denied"),
    "jdbc-network": (JDBC, "org.postgresql.util.PSQLException: The connection attempt failed.\nCaused by: java.net.ConnectException: Connection refused"),
    "jdbc-duplicate": (JDBC, "org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"sample_orders_pkey\""),
    "json-format": (JDBC, "org.apache.kafka.connect.errors.DataException: JsonConverter could not decode the record with the configured schema envelope"),
    "schema-registry": (JDBC, "org.apache.kafka.common.errors.SerializationException: Error retrieving Avro schema\nCaused by: java.net.SocketTimeoutException: Read timed out"),
    "topic-acl": (POSTGRES, "org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [sample.orders]"),
    "record-size": (POSTGRES, "org.apache.kafka.common.errors.RecordTooLargeException: Serialized record exceeds max.request.size"),
    "kafka-sasl": (JDBC, "org.apache.kafka.common.errors.SaslAuthenticationException: Authentication failed: invalid credentials"),
    "kafka-tls": (POSTGRES, "org.apache.kafka.common.errors.SslAuthenticationException: SSL handshake failed\nCaused by: javax.net.ssl.SSLHandshakeException: PKIX path building failed"),
    "s3-access": (S3, "com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied)"),
    "postgres-wal": (POSTGRES, "org.postgresql.util.PSQLException: ERROR: requested WAL segment 000000010000000000000001 has already been removed"),
}
SCENARIOS = [
    # root, error, route; repeats are distinct historical incidents, not duplicate logs.
    ("oracle-orders", "oracle-timeout", "task1"),
    ("oracle-orders", "oracle-timeout", "task2"),
    ("oracle-orders", "oracle-timeout", "offset"),
    ("oracle-orders", "oracle-timeout", "connector"),
    ("oracle-redo", "oracle-redo", "without"),
    ("oracle-undo", "oracle-undo", "without"),
    ("oracle-login", "oracle-login", "escalated"),
    ("oracle-create-timeout", "oracle-timeout", "timeout"),
    ("oracle-patch", "oracle-redo", "patch"),
    ("oracle-empty-offset", "oracle-redo", "empty_offset"),
    ("jdbc-orders", "jdbc-network", "task1"),
    ("jdbc-orders", "jdbc-network", "connector"),
    ("jdbc-orders", "jdbc-duplicate", "escalated"),
    ("jdbc-json", "json-format", "escalated"),
    ("jdbc-avro", "schema-registry", "task2"),
    ("pg-acl", "topic-acl", "escalated"),
    ("pg-large-record", "record-size", "escalated"),
    ("jdbc-auth", "kafka-sasl", "escalated"),
    ("pg-tls", "kafka-tls", "escalated"),
    ("s3-lake", "s3-access", "escalated"),
    ("pg-wal", "postgres-wal", "escalated"),
    ("jdbc-transient", "jdbc-network", "spontaneous"),
    ("oracle-waiting", "oracle-timeout", "waiting"),
    ("jdbc-pending", "jdbc-network", "pending"),
    ("s3-processing", "s3-access", "processing"),
    ("jdbc-connector-failure", "jdbc-network", "connector_only"),
    ("jdbc-multiple-tasks", "jdbc-network", "multi_task"),
]


def iso(value):
    return value.isoformat(timespec="milliseconds") if isinstance(value, datetime) else value


class HistoryStore:
    """In-memory persistence boundary; no calls to the real database during simulation."""

    def __init__(self, index, root, connector_class, received):
        self.now = received
        self.logs = []
        self.queue = dict(
            QueueId=str(uuid5(NAMESPACE_URL, f"healing-sample-v1/{index}")),
            RootConnectorName="sample-" + root, CurrentConnectorName="sample-" + root,
            ConnectorClass=connector_class,
            HealingMode="RECOVERY" if connector_class == ORACLE else "RESTART_ONLY",
            QueueStatus="PENDING", FinalOutcome=None, ReceivedAt=iso(received),
            StartedAt=None, CompletedAt=None, NextAttemptAt=None,
        )

    def get_connector_by_id(self, queue_id):
        if self.queue["QueueStatus"] not in OPEN:
            return None  # spGetConnectorHealingQueue defaults to OpenOnly=1.
        latest = self.logs[-1] if self.logs else {}
        counts = Counter(row["EventType"] for row in self.logs)
        return MssqlConnectorRepository._hydrate_queue_row(dict(
            id=queue_id, active_incident_id=queue_id,
            connector_name=self.queue["CurrentConnectorName"],
            root_connector_name=self.queue["RootConnectorName"],
            healing_mode=self.queue["HealingMode"], queue_status=self.queue["QueueStatus"],
            level=4 if self.queue["HealingMode"] == "RECOVERY" else 2,
            latest_event_type=latest.get("EventType"),
            latest_event_at=latest.get("CreatedAt"),
            latest_event_details=latest.get("Details", "{}"), latest_has_next_step=True,
            failure_confirmed=counts["HEALTH_FAILED_CONFIRMED"] > 0,
            **{key: sum(counts[event] for event in events) for key, events in COUNTERS.items()},
        ))

    def start_processing(self, queue_id):
        self.queue["QueueStatus"] = "PROCESSING"
        self.queue["StartedAt"] = self.queue["StartedAt"] or iso(self.now)

    def wait_for_next_attempt(self, queue_id, next_attempt_at):
        self.queue.update(QueueStatus="WAITING", NextAttemptAt=iso(next_attempt_at))

    def complete(self, queue_id, outcome):
        self.queue.update(
            QueueStatus="ESCALATED" if outcome == "ESCALATED" else "COMPLETED",
            FinalOutcome=outcome, CompletedAt=iso(self.now), NextAttemptAt=None,
        )

    def update_queue_fields(self, queue_id, **fields):
        assert set(fields) == {"current_connector_name"}
        self.queue["CurrentConnectorName"] = fields["current_connector_name"]

    def ensure_active_incident(self, queue_id):
        return queue_id

    def record_connector_log(self, **values):
        if values["event_type"] == "HEALTH_FAILED_CONFIRMED" and any(
            row["EventType"] == "HEALTH_FAILED_CONFIRMED" for row in self.logs
        ):
            return  # Mirrors the stored procedure's idempotent confirmation insert.
        details = _connector_log_details(
            details=values.get("details"), severity=values.get("severity", "INFO"),
            task_id=values.get("task_id"), scn=values.get("scn"),
            commit_scn=values.get("commit_scn"),
        )
        self.logs.append(dict(
            Id=str(uuid5(NAMESPACE_URL, self.queue["QueueId"] + f"/{len(self.logs)}")),
            QueueId=values["connector_id"], ConnectorName=values["connector_name"],
            EventType=values["event_type"], AttemptNo=values.get("attempt_no"),
            HealingStep=values.get("healing_step"), Severity=values.get("severity", "INFO"),
            Message=values["message"], Details=json.dumps(details, ensure_ascii=False),
            CreatedAt=iso(self.now),
        ))
        self.now += timedelta(milliseconds=1)  # Stable SQL latest-event ordering.


def simulate(index, spec, anchor):
    root, error, route = spec
    connector_class, trace = ERRORS[error]
    store = HistoryStore(index, root, connector_class, anchor - timedelta(days=index % 10, hours=3))
    client = MagicMock()  # No KafkaConnectClient instance, no network or real connector actions.
    client.get_config.return_value = {
        "connector.class": connector_class,
        "schema.history.internal.kafka.topic": "sample-history." + root,
    }
    client.get_offsets.return_value = {"offsets": [
        {"partition": {"server": "sample-oracle"}, "offset": {"scn": "100000", "commit_scn": "99990"}}
    ]} if route != "empty_offset" else {"offsets": []}
    client.stop_connector.return_value = True
    client.resume_connector.return_value = True
    client.patch_offsets.return_value = route != "patch"
    client.connector_exists.return_value = False
    if route == "timeout":
        client.create_connector.side_effect = [TimeoutError("Synthetic REST create timeout"), {}]
    checker = MagicMock()
    machine = ConnectorStateMachine(
        client=client, checker=checker, db=store, failure_confirm_checks=3,
        task_restart_max_attempts=3, connector_restart_max_attempts=1,
        post_restart_wait_seconds=30, recovery_healthy_confirm_seconds=60,
        recreate_verify_wait_seconds=30, scn_poll_interval_seconds=60,
    )
    healthy = False
    if route == "processing":
        store.now += timedelta(seconds=5)
        store.start_processing(store.queue["QueueId"])
    elif route != "pending":
        for tick in range(30):
            store.now += timedelta(seconds=60 * (1 + index % 3))
            job = store.get_connector_by_id(store.queue["QueueId"])
            if job is None:
                break
            checker.check.return_value = HealthResult(
                connector_name=job.connector_name,
                status=HealthStatus.HEALTHY if healthy else HealthStatus.UNHEALTHY,
                reason="Connector and all tasks RUNNING" if healthy else (
                    "Connector is FAILED" if route == "connector_only" else "Failed connector tasks"
                ),
                failed_task_ids=[] if healthy or route == "connector_only" else (
                    [0, 2] if route == "multi_task" else [0]
                ), trace=None if healthy else trace,
                checked_at=store.now,
            )
            with patch("self_healthy_kafka.healing.db_state_machine.utc_now", lambda: store.now):
                machine._process_job_safely(job)
            events = Counter(row["EventType"] for row in store.logs)
            if events["STATE_MACHINE_ERROR"]:
                raise AssertionError("Simulation hit an unexpected state-machine exception")
            if route == "waiting" and events["TASK_RESTART"]:
                break
            healthy |= (
                (route == "spontaneous" and tick == 0)
                or (route == "task1" and events["TASK_RESTART"] >= 1)
                or (route == "multi_task" and events["TASK_RESTART"] >= 1)
                or (route == "task2" and events["TASK_RESTART"] >= 2)
                or (route == "connector" and events["CONNECTOR_RESTART"] >= 1)
                or (route == "connector_only" and events["CONNECTOR_RESTART"] >= 1)
                or (route in {"offset", "timeout"} and events["CONNECTOR_RECREATE_WITH_OFFSET"] >= 1)
                or (route in {"without", "patch", "empty_offset"} and events["CONNECTOR_RECREATE_WITHOUT_OFFSET"] >= 1)
            )
        else:
            raise AssertionError(f"Simulation exceeded bounded tick budget: {route}")
    expected = {"pending": "PENDING", "processing": "PROCESSING", "waiting": "WAITING",
                "escalated": "ESCALATED"}.get(route, "COMPLETED")
    assert store.queue["QueueStatus"] == expected, (spec, store.queue)
    return store


def validate(tables):
    if not __debug__:
        raise RuntimeError("Do not run this destructive fixture utility with Python -O")
    queues = {q["QueueId"]: q for q in tables[QUEUE]}
    assert len(queues) == len(tables[QUEUE])
    assert len({r["Id"] for r in tables[LOGS]}) == len(tables[LOGS])
    valid_events = {v for k, v in vars(EventType).items() if k.isupper()}
    roots = [q["RootConnectorName"].lower() for q in queues.values() if q["QueueStatus"] in OPEN]
    assert len(roots) == len(set(roots)), "Duplicate open logical root"
    for q in queues.values():
        assert (q["HealingMode"] == "RECOVERY") == (q["ConnectorClass"] == ORACLE)
        logs = [r for r in tables[LOGS] if r["QueueId"] == q["QueueId"]]
        events = Counter(r["EventType"] for r in logs)
        terminal = q["QueueStatus"] not in OPEN
        assert bool(q["CompletedAt"]) == terminal
        assert bool(q["FinalOutcome"]) == terminal
        if terminal:
            assert q["StartedAt"] and q["ReceivedAt"] <= q["StartedAt"] <= q["CompletedAt"]
            assert q["NextAttemptAt"] is None
            assert q["FinalOutcome"] == ("RECOVERED" if q["QueueStatus"] == "COMPLETED" else "ESCALATED")
        assert events["HEALTH_FAILED_CONFIRMED"] <= 1
        if events["TASK_RESTART"]:
            assert events["HEALTH_FAILED_CONFIRMED"] == 1
        for event in ("TASK_RESTART", "CONNECTOR_RESTART"):
            attempts = [r["AttemptNo"] for r in logs if r["EventType"] == event]
            assert attempts == list(range(1, len(attempts) + 1))
        assert events["TASK_RESTART"] <= 3 and events["CONNECTOR_RESTART"] <= 1
        if q["HealingMode"] == "RESTART_ONLY":
            assert not any("RECREATE" in r["EventType"] for r in logs)
        if q["QueueStatus"] == "ESCALATED":
            assert logs[-1]["EventType"] in {"HEALING_ESCALATED", "HEALING_LEVEL_LIMIT_REACHED"}
            assert not events["HEALING_RECOVERED"]
        if q["FinalOutcome"] == "RECOVERED" and events["TASK_RESTART"] + events["CONNECTOR_RESTART"]:
            assert logs[-1]["EventType"] == "HEALING_RECOVERED"
        observations = [r for r in logs if r["EventType"] == "HEALTH_FAILURE_OBSERVED"]
        assert [r["AttemptNo"] for r in observations] == [min(i + 1, 7) for i in range(len(observations))]
        for row in logs:
            assert q["ReceivedAt"] <= row["CreatedAt"] <= (q["CompletedAt"] or "9999")
    for row in tables[LOGS]:
        assert row["QueueId"] in queues and row["EventType"] in valid_events
        assert row["Severity"] in {"INFO", "WARNING", "ERROR", "CRITICAL"}
        assert json.loads(row["Details"])["severity"] == row["Severity"]
    return {"queues": len(queues), "logs": len(tables[LOGS]),
            "statuses": dict(Counter(q["QueueStatus"] for q in queues.values())),
            "events": dict(Counter(r["EventType"] for r in tables[LOGS]))}


def generate(anchor):
    tables = {table: [] for table in TABLES}
    cases = []
    for index, spec in enumerate(SCENARIOS):
        store = simulate(index, spec, anchor)
        tables[QUEUE].append(store.queue)
        tables[LOGS].extend(store.logs)
        cases.append(dict(queue_id=store.queue["QueueId"], root=store.queue["RootConnectorName"],
                          error=spec[1], route=spec[2], outcome=store.queue["FinalOutcome"],
                          log_count=len(store.logs)))
    return tables, cases, validate(tables)


def local_connection():
    import pyodbc
    result = subprocess.run(["docker", "inspect", "poc-mssql"], check=True,
                            capture_output=True, text=True, timeout=15)
    info = json.loads(result.stdout)[0]
    assert info["Name"] == "/poc-mssql" and info["State"]["Running"]
    assert info["Config"]["Image"].startswith("mcr.microsoft.com/mssql/server:")
    assert any(p["HostPort"] == "14330" for p in info["NetworkSettings"]["Ports"]["1433/tcp"])
    variables = dict(v.split("=", 1) for v in info["Config"]["Env"] if "=" in v)
    password = variables.get("MSSQL_SA_PASSWORD") or variables["SA_PASSWORD"]
    driver = next(d for d in ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
                  if d in pyodbc.drivers())
    conn = pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER=127.0.0.1,14330;DATABASE=ingest_reference;"
        "UID=sa;PWD={" + password.replace("}", "}}") + "};TrustServerCertificate=yes;"
        "APP=healing-sample-seed", autocommit=False, timeout=5,
    )
    conn.add_output_converter(-155, decode_datetimeoffset)
    conn.timeout = 20
    return conn, info["Id"]


def fetch_tables(cursor):
    tables = {}
    for table in TABLES:
        cursor.execute(f"SELECT * FROM dbo.[{table}]")
        names = [d[0] for d in cursor.description]
        tables[table] = [dict(zip(names, (iso(v) for v in row), strict=True)) for row in cursor.fetchall()]
    return tables


def canonical(tables):
    normalized = {t: [{k: str(v).lower() if k in {"Id", "QueueId"} else v
                       for k, v in row.items()} for row in rows] for t, rows in tables.items()}
    return json.dumps({t: sorted(rows, key=lambda r: r.get("Id", r.get("QueueId")))
                       for t, rows in normalized.items()}, sort_keys=True, ensure_ascii=False)


def check_runtime_hydration(cursor, tables):
    cursor.execute("EXEC dbo.spGetConnectorHealingQueue @OpenOnly=0, @DueOnly=0")
    hydrated = {str(r["id"]).lower(): r for r in rows_to_dicts(cursor)}
    assert len(hydrated) == len(tables[QUEUE])
    for queue in tables[QUEUE]:
        row = hydrated[queue["QueueId"].lower()]
        logs = [r for r in tables[LOGS] if r["QueueId"] == queue["QueueId"]]
        events = Counter(r["EventType"] for r in logs)
        for key, names in COUNTERS.items():
            assert row[key] == sum(events[name] for name in names), key
        assert row["connector_name"] == queue["CurrentConnectorName"]
        assert row["latest_event_type"] == (logs[-1]["EventType"] if logs else None)
    return len(hydrated)


def replace_local(tables):
    validate(tables)
    conn, container_id = local_connection()
    backup_path = ROOT / "backups" / "healing" / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex + ".json.bak")
    try:
        cur = conn.cursor()
        assert cur.execute("SELECT DB_NAME()").fetchval() == "ingest_reference"
        # Refuse trigger side effects and references from unrelated tables.
        assert cur.execute("SELECT COUNT(*) FROM sys.triggers WHERE is_disabled=0 AND parent_id IN (OBJECT_ID('dbo.ConnectorHealingQueue'),OBJECT_ID('dbo.ConnectorHealingLogs'))").fetchval() == 0
        assert cur.execute("SELECT COUNT(*) FROM sys.foreign_keys WHERE referenced_object_id IN (OBJECT_ID('dbo.ConnectorHealingQueue'),OBJECT_ID('dbo.ConnectorHealingLogs')) AND parent_object_id NOT IN (OBJECT_ID('dbo.ConnectorHealingQueue'),OBJECT_ID('dbo.ConnectorHealingLogs'))").fetchval() == 0
        cur.execute("SET XACT_ABORT ON; SET LOCK_TIMEOUT 10000;")
        for table in TABLES:
            cur.execute(f"SELECT COUNT(*) FROM dbo.[{table}] WITH (TABLOCKX,HOLDLOCK)").fetchval()
        old = fetch_tables(cur)
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        with backup_path.open("x", encoding="utf-8") as handle:
            json.dump({"database": "ingest_reference", "container_id": container_id,
                       "tables": old}, handle, ensure_ascii=False, indent=2)
        assert canonical(json.loads(backup_path.read_text(encoding="utf-8"))["tables"]) == canonical(old)
        cur.execute("DELETE FROM dbo.ConnectorHealingLogs")
        cur.execute("DELETE FROM dbo.ConnectorHealingQueue")
        for table in TABLES:
            rows = tables[table]
            columns = list(rows[0])
            sql = f"INSERT INTO dbo.[{table}] (" + ",".join(f"[{c}]" for c in columns) + ") VALUES (" + ",".join("?" for _ in columns) + ")"
            cur.executemany(sql, [tuple(row[c] for c in columns) for row in rows])
        inserted = fetch_tables(cur)
        assert canonical(inserted) == canonical(tables), "Readback mismatch: rollback"
        checked = check_runtime_hydration(cur, tables)
        conn.commit()
        return {"target": "poc-mssql / 127.0.0.1:14330 / ingest_reference",
                "deleted": {t: len(old[t]) for t in TABLES},
                "inserted": {t: len(tables[t]) for t in TABLES},
                "backup": str(backup_path), "readback_matches": True,
                "stored_procedure_hydration_checked": checked}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply-local", action="store_true", help="Back up and DELETE old data in the two local PoC tables, then insert samples")
    parser.add_argument("--anchor", help="Timezone-aware ISO timestamp for reproducible generation")
    args = parser.parse_args()
    anchor = datetime.fromisoformat(args.anchor) if args.anchor else datetime.now(timezone.utc)
    if anchor.utcoffset() is None:
        raise ValueError("Anchor requires timezone")
    anchor = anchor.astimezone(timezone.utc).replace(microsecond=0)
    tables, cases, summary = generate(anchor)
    output = ROOT / "sql" / "test-data" / "generated-healing-samples.json"
    payload = {"synthetic": True, "anchor": iso(anchor), "summary": summary,
               "scenarios": cases, "tables": tables}
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    snapshot = ROOT / "self_healthy_kafka_snapshot.db"
    before = hashlib.sha256(snapshot.read_bytes()).hexdigest() if snapshot.exists() else None
    result = replace_local(tables) if args.apply_local else {"applied": False}
    assert before == (hashlib.sha256(snapshot.read_bytes()).hexdigest() if snapshot.exists() else None)
    result.update(summary=summary, sample_file=str(output), snapshot_unchanged=True)
    report = output.with_name("healing-samples-validation.json")
    report.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.ERROR)
    main()
