# self-healthy-kafka Business Context

This file is the durable business-context reference for future changes. Update
it whenever connector healing behavior, database contracts, runtime wiring, or
connector configuration handling changes.

## Purpose

`self-healthy-kafka` monitors configured Kafka Connect connectors and performs
bounded recovery when a connector or task becomes unhealthy. It does not
monitor Kafka topic traffic, record latency, end offsets, or topic idleness, and
it does not publish application-owned Prometheus metrics.

The service runs as one process. Microsoft SQL Server is the source of managed
connector configuration and persisted healing state. Kafka Connect REST is the
source of live connector and task status.

## Runtime Flow

1. Startup validates SQL Server and Kafka Connect connectivity.
2. `spGetConnectorContext` returns active connectors without runtime secrets.
3. The state machine requests `GET /connectors/{name}/status` only for managed
   connectors.
4. Healthy connectors remain unchanged. A confirmed connector/task failure
   enters the bounded healing flow.
5. Every action and terminal result is persisted through
   `spInsertConnectorHealingLog`.
6. Connector state changes are persisted through `spUpdateConnector`.
7. Grafana may trigger the same connector processing path through the secured
   webhook. Periodic reconciliation remains the fallback for missed alerts.

The application exposes `GET /health` on the webhook HTTP server. There is no
`/metrics` endpoint.

## Healing Flow

Healing is controlled by the connector `Level` and the configured attempt
limits:

1. Restart failed tasks up to `TASK_RESTART_MAX_ATTEMPTS`.
2. Restart the connector up to `CONNECTOR_RESTART_MAX_ATTEMPTS`.
3. Recreate a versioned connector while preserving the previous offset.
4. Continue with the configured final recreate/recovery behavior when the
   connector level permits it.
5. Escalate and deactivate automatic healing after the bounded flow is
   exhausted or the connector level blocks the next action.

Recovery must preserve incident identity, attempt counts, connector name
versioning, schema-history configuration, and the latest SCN/commit SCN used by
the recovery flow. Removing topic monitoring must not change these rules.

## Connector Configuration

`Connectors.ConfigTemplate` stores JSON in pretty format. Sensitive runtime
values are represented by these placeholders:

- `database.url`: `{url}`
- `database.password`: `{pwd}`
- `schema.history.internal.kafka.bootstrap.servers`: `{kafka_server}`

`Connectors.ConfigId` identifies the credential record in
`ingest_reference.dbo.ETLConfiguration`. `spGetConnectorContext` resolves the
credential value and Kafka bootstrap server only when
`@IncludeRuntimeConfig = 1`. Runtime values must not be written back into
`ConfigTemplate`.

## Database Contract

The application owns two tables:

- `Connectors`: managed connector configuration and current healing state.
- `ConnectorHealingLogs`: append-only incident and healing action history.

The runtime calls exactly three stored procedures:

- `dbo.spGetConnectorContext`
- `dbo.spUpdateConnector`
- `dbo.spInsertConnectorHealingLog`

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
