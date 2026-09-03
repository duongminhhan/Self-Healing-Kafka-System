# self-healthy-kafka Business Context

This file is the durable business-context reference for future changes. Update
it whenever connector healing behavior, database contracts, runtime wiring, or
connector configuration handling changes.

## Purpose

`self-healthy-kafka` discovers Kafka Connect connectors and performs
bounded recovery when a connector or task becomes unhealthy. It does not
monitor Kafka topic traffic, record latency, end offsets, or topic idleness, and
it does not publish application-owned Prometheus metrics.

The service runs as one process. Kafka Connect REST is the source of live
connector names, status, and runtime configuration. Microsoft SQL Server
persists only queue incidents and healing history; it does not store connector
configuration or credentials.

## Runtime Flow

1. Startup validates SQL Server and Kafka Connect connectivity.
2. `GET /connectors` discovers the current Kafka Connect inventory.
3. The state machine checks each connector status. Healthy connectors do not
   create database records.
4. An unhealthy connector creates one open queue item per root connector.
5. `GET /connectors/{name}/config` is called only when discovery needs its
   connector class or a `RECOVERY` action needs the runtime config. The config
   is memory-only and is never persisted.
6. Every action and terminal result is persisted through
   `spInsertConnectorHealingLog`.
7. Queue state changes are persisted through `spUpdateConnectorHealingQueue`.
8. Grafana may trigger the same connector processing path through the secured
   webhook. Periodic reconciliation remains the fallback for missed alerts.

The application exposes `GET /health` on the webhook HTTP server. There is no
`/metrics` endpoint.

## Chatbot Context API

When `CHAT_API_ENABLED=true`, the existing HTTP server additionally exposes a
read-only, bearer-token-protected API under `CHAT_API_PATH_PREFIX`:

- `GET /api/v1/incidents`: queue history; `status` can be `all`, `open`,
  `completed`, or `escalated`.
- `GET /api/v1/incidents/{queue_id}`: one queue item and its healing logs.
- `GET /api/v1/healing-logs`: healing logs filtered by `queue_id`,
  `connector_name`, `from`, `to`, and `limit`.

This API is the LLM tool boundary. It never exposes arbitrary SQL, Kafka
Connect runtime configuration, or credentials. Sensitive keys found in log
details are redacted before the response is returned. Ollama cannot perform
healing actions.

When `OLLAMA_ENABLED=true`, `POST /api/v1/chat` accepts `{"question":"..."}`
with the same bearer token. The local Ollama model can call only the read-only
tools above plus `dbo.spGetConnectorFailureRanking`. For a question such as
"liệt kê top connector chết nhiều nhất", the procedure groups actual
`ConnectorHealingQueue` incidents by `RootConnectorName`; the returned count
is therefore not hardcoded and is not affected by versioned recreate names.
After that tool is called, the API formats the ranking from the SP result
itself, rather than relying on the model to reproduce numeric values.
For local CPU deployments, bound each model turn with `OLLAMA_MAX_TOKENS`.
Tool-capable reasoning models may need `OLLAMA_THINK=true` to emit a valid tool
call; these settings control response latency only and do not change the
SQL-backed answer contract.
Routine start-of-day queries are routed directly to the relevant read tool:
failure ranking, open/escalated/recovered incidents, and healing history. This
is intent routing only; all returned values remain database results. It keeps
the answer accurate when a local model does not emit a tool call.

## Healing Flow

Healing mode is selected when an incident is queued. Generic connectors use
`RESTART_ONLY`; Oracle Debezium connectors use `RECOVERY`. Attempt limits bound
the actions within the selected mode:

1. Restart failed tasks up to `TASK_RESTART_MAX_ATTEMPTS`.
2. Restart the connector up to `CONNECTOR_RESTART_MAX_ATTEMPTS`.
3. Recreate a versioned connector while preserving the previous offset.
4. Continue with the configured final recreate/recovery behavior when the
   connector level permits it.
5. Escalate and deactivate automatic healing after the bounded flow is
   exhausted or the connector level blocks the next action.

Recovery preserves queue identity, attempt counts, current connector name, and
the action audit trail. Config and offset/SCN values used by a recovery action
are kept only in action memory or the relevant log details, never as a runtime
connector registry.

## Connector Configuration

The service never stores a connector config, database password, JDBC URL, or
Kafka bootstrap server. For `RECOVERY`, it curls
`GET /connectors/{name}/config` at execution time and uses the response only in
memory. If Kafka Connect no longer returns that config, the queue is escalated
instead of guessing or reconstructing secrets.

## Database Contract

The application owns two tables:

- `ConnectorHealingQueue`: one active or completed incident per detected
  connector failure. It records root/current name, mode, lifecycle timestamps,
  queue status, final outcome, and the next eligible attempt time.
- `ConnectorHealingLogs`: append-only actions and observations belonging to a
  queue item through `QueueId`.

The runtime calls six stored procedures:

- `dbo.spEnqueueConnectorHealing`
- `dbo.spGetConnectorHealingQueue`
- `dbo.spGetConnectorHealingLogs`
- `dbo.spGetConnectorFailureRanking`
- `dbo.spInsertConnectorHealingLog`
- `dbo.spUpdateConnectorHealingQueue`

Files under `sql/init-table` are first-install definitions. Files under
`sql/ingest_reference/stored-procedures` are standalone DBeaver-compatible
procedure scripts and do not contain `GO`.

`drop-legacy-procedures.sql` removes retired metric and topic-lag procedures
from an existing database. It intentionally does not drop historical topic-lag
tables automatically; dropping tables is a separate operator decision because
it destroys data.

Timestamps use `DATETIMEOFFSET(3)` and `SYSDATETIMEOFFSET()`. Do not force a
fixed `+07:00` offset or rewrite timestamps supplied by Kafka Connect or the
database.

## Observability

Operational evidence comes from structured application logs,
`ConnectorHealingLogs`, Kafka Connect worker logs, and Kafka Connect native
metrics. Dashboards and alerts must not depend on retired `kc_shs_*` custom
metrics or topic-lag tables/procedures.

## Deployment

The app runs as a single Linux/systemd process through `scripts/run.sh`. The
runtime environment file supplies Kafka Connect, SQL Server, webhook, healing,
and logging settings. A code or dependency update requires restarting the
systemd service.
