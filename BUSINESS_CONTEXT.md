# self-healthy-kafka Business Context

This file is the durable business-context reference for future prompt runs.
Whenever business logic, database contracts, healing flow, monitoring semantics,
or connector config behavior changes, update this file in the same change.

## Purpose

`self-healthy-kafka` monitors Kafka Connect connectors, detects unhealthy
connector/task/topic conditions, performs bounded self-healing actions, and
persists operational state to Microsoft SQL Server for audit, metrics, and
Grafana/Text-to-SQL style querying.

UAT CI/CD deploys the single application instance directly to its Linux
systemd service. The GitLab pipeline validates the Python project, creates a
self-contained release with runtime dependencies, transfers it over SSH, and
restarts `self-healthy-kafka`. This deployment path does not depend on Docker,
Kubernetes, or k3s, and it must preserve the host-managed `env/uat.env` file.
Runtime dependencies are installed into project-local `lib/python` through a
temporary installer virtualenv; system Python must remain protected by PEP 668
and must not require `--break-system-packages`.

The application must preserve the existing operational shape:

- Kafka Connect remains the source of live connector/task status.
- Kafka Connect HTTPS certificate verification is controlled by
  `KAFKA_CONNECT_TLS_VERIFY`. Keep it enabled by default. UAT may set it to
  `false` only as a temporary workaround for an expired/internal certificate;
  both startup checks and runtime status/healing requests must use the same
  setting.
- MSSQL remains the source of configured monitored connectors, healing state,
  topic lag jobs, and audit logs.
- Stored procedures are part of the runtime contract and must be updated with
  Python changes that depend on DB fields or DB-backed behavior.
- After any behavior change, review the related MSSQL stored procedures even
  when the change initially appears to be Python-only, dashboard-only, or
  alert-only. If code, dashboard metrics, alert semantics, connector config
  handling, topic lag state, or healing logs depend on DB data, update
  `sql/ingest_reference/stored-procedures/` and the matching
  tests in the same change.
- The application does not auto-migrate SQL in runtime startup. SQL files must
  be applied manually to the target DB environment.

## Runtime Performance Contract

- Connector reconciliation, topic lag polling, and DB metric synchronization
  run in independent non-overlapping workers. Their intervals are controlled by
  `POLL_INTERVAL_SECONDS`, `TOPIC_IDLE_POLL_INTERVAL_SECONDS`, and
  `METRICS_SYNC_INTERVAL_SECONDS`; one slow workload must not delay the others.
- Kafka topic polling fetches end offsets in a batch and reads the latest record
  from all required partitions under one bounded poll deadline. The decoded
  latest-record snapshot is reused for `LastMessageAt`, event capture lag, and
  `kc_shs_topic_records_lag`, and retained while offsets remain unchanged.
- The webhook `/metrics` endpoint serves the last successful in-memory
  monitoring snapshot. DB, Kafka Connect status, and Kafka metadata are
  refreshed in a background worker every
  `MONITORING_INVENTORY_REFRESH_SECONDS`; refresh failure keeps the stale
  snapshot available. Inventory uses `source_server` from DB and must not call
  Kafka Connect `GET /config` per connector.
- Regular reconciliation loads connector context with
  `@IncludeRuntimeConfig = 0`. This excludes `ConfigTemplate`, credential, and
  Kafka runtime values from the result. They are loaded only immediately before
  a recreate action. Healthy connectors do not require a post-processing
  `get_connector_by_id` query.
- Topic monitoring reads only topic job state from `spGetMonitoredTopics`; it
  must not return connector `ConfigTemplate`. Monitoring inventory indexes
  broker topics by matching source prefix in one pass instead of scanning every
  broker topic once per connector.
- Kafka Connect status polling opens a process-level circuit after a connection
  error or timeout. The remaining connectors in that reconciliation cycle are
  skipped, and a half-open retry is allowed after
  `KC_CIRCUIT_BREAKER_COOLDOWN_SECONDS`. Connector-specific HTTP responses do
  not open this circuit.
- Repository connection context managers own commit/rollback. Repository
  methods must not issue a second explicit commit. Per-cycle `topic_lag_state`
  logs are DEBUG; transition, warning, and error logs remain visible at their
  existing levels.
- `kc_shs_connector_failed` is reconciled from `Connectors.FailedCount` during
  every DB metrics synchronization. Runtime healing events may update the gauge
  immediately, but the next synchronization is authoritative and clears stale
  failure values after DB recovery state has been reset.

## Production Grafana Alloy Contract

The production observability setup is documented in
`deploy/grafana-alloy/README.md`.

- Alloy on the `self-healthy-kafka` host must scrape both metrics endpoints:
  the webhook/DB inventory endpoint on `GRAFANA_WEBHOOK_PORT` and the runtime
  Prometheus endpoint on `PROMETHEUS_METRICS_PORT`. Scraping only one endpoint
  leaves either inventory/last-message metrics or runtime lag/healing metrics
  missing.
- Preserve `job="self-healthy-kafka"`; distinguish the two targets with the
  `metrics_endpoint` label and add stable `environment`, `service`, and
  `instance` labels.
- App JSON stdout logs are collected from `self-healthy-kafka.service` through
  systemd journal. Do not drop all INFO logs because state and healing events
  are emitted at INFO.
- Use only low-cardinality Loki labels such as `job`, `service`, `environment`,
  `host`, and `level`. Keep `connector_name`, `topic`, `event`, and `logger` as
  structured metadata.
- The `Connector Warning/Error Logs` dashboard requires Kafka Connect worker
  logs, not only app logs. Alloy must tail the Kafka Connect log files on every
  worker and forward WARN/ERROR records to Loki with `job="kafka-connect"`.
- Production secrets must remain outside the repository and production TLS
  verification must stay enabled. Use the internal CA file instead of
  `insecure_skip_verify = true`.

## Connector Config Business Rules

`Connectors.ConfigTemplate` stores the durable connector template used when a
connector must be recreated.

Sensitive or environment-owned values must not be stored directly in
`ConfigTemplate`:

- `database.url`
- `database.password`
- `schema.history.internal.kafka.bootstrap.servers`

Instead, `Connectors.ConfigTemplate` keeps placeholders:

```json
{
  "database.url": "{url}",
  "database.password": "{pwd}",
  "schema.history.internal.kafka.bootstrap.servers": "{kafka_server}"
}
```

`Connectors.ConfigId varchar(100)` stores the external credential identifier
for the connector, for example `318`.

At runtime the repository resolves a separate `active_config`:

- `dbo.spGetConnectorContext` returns the credential string shaped as
  `<database.url>;<database.password>` together with the connector context.
- The credential is read from `ingest_reference.dbo.ETLConfiguration` where
  `ETLConfigurationID` matches `Connectors.ConfigId`.
- The code splits this value at the last semicolon so JDBC URLs may contain
  semicolons.
- `dbo.spGetConnectorContext` also reads the Kafka bootstrap server from
  `ingest_reference.dbo.ETLConfiguration` where `ETLConfigurationID = 27`.
- Calls that only need connector activity set `@IncludeRuntimeConfig = 0` so
  `ConfigTemplate` and credential values are not returned to status,
  dashboard, or inventory paths.
- These resolved values are inserted only into runtime config passed to Kafka
  Connect.

When the app updates `ConfigTemplate` after recreate/versioning, it must write
the placeholder values back into DB, not the resolved URL/password/server.
This prevents recreated connector config from leaking secrets back into MSSQL.

JSON persisted through repository update paths should remain pretty formatted,
not compacted to a single line.

## Current SQL Contract

`sql/init-table/` owns table and index shape. Each KC-SHS table is stored in its
own same-named SQL file: `Connectors.sql`, `ConnectorHealingLogs.sql`,
`TopicLagJobs.sql`, and `TopicLagLogs.sql`. These files are first-install table
scripts and include the indexes owned by their respective tables. Identifiers
in this directory use unquoted PascalCase names without square brackets. There
is no aggregate `mssql-schema.sql` or separate
`mssql-performance-indexes.sql`.
Apply future changes to an existing database through a separate reviewed
migration.
The `Connectors` table includes:

- `ConfigTemplate nvarchar(max) not null default N'{}'`
- `ConfigId varchar(100) null`

`sql/ingest_reference/stored-procedures/` owns stored procedure shape. Each
runtime procedure is stored in its own same-named `.sql` file. The current
runtime contract contains twelve procedures:

- `dbo.spGetConnectorContext`
- `dbo.spUpdateConnector`
- `dbo.spInsertConnectorHealingLog`
- `dbo.spGetHealingAttemptMetrics`
- `dbo.spGetHealingRecoveredMetrics`
- `dbo.spGetHealingEscalatedMetrics`
- `dbo.spGetTopicLagStateMetrics`
- `dbo.spGetTopicLagEventMetrics`
- `dbo.spGetOperationalMetrics`
- `dbo.spGetMonitoredTopics`
- `dbo.spUpdateTopicLagState`
- `dbo.spInsertTopicLagLog`

`spGetConnectorContext` replaces separate connector list/by-name/by-id,
activity, healing-state, credential, and Kafka-server reads. Runtime credential
columns must be removed from intermediate connector mappings after
`active_config` is resolved so secrets cannot leak into logs or persisted
details.

The application calls the five focused metric procedures directly. Each
procedure owns one result shape and one business purpose, so a failure in one
metric group does not block or clear the other groups. Each successful call
logs its procedure name, metric group, duration, and row count.
Failed calls log the same operational context plus the error and leave the
previous Prometheus labels for that group unchanged.
`spGetOperationalMetrics` is a compatibility wrapper only. It delegates to the
five focused procedures and preserves the old normalized `MetricGroup` result
for staged UAT rollout; new application code must not call it.

`spGetHealingRecoveredMetrics` joins `ConnectorHealingLogs` to
`Connectors` by stable `ConnectorId`, returns only active connectors under their
current `ConnectorName`, and applies `@WindowMinutes` to `HEALING_RECOVERED`
logs. It must not group directly by historical log connector names or return
old/inactive connector versions.

Each `sp*.sql` file is an independently executable direct
`CREATE OR ALTER PROCEDURE` script. Apply all twelve procedure files first,
deploy the application code that calls the focused metric procedures, and only
then apply the transaction-wrapped `drop-legacy-procedures.sql` cleanup to
remove the twelve unrelated legacy procedures. In DBeaver, select the
target `ingest_reference` database and execute each complete `sp*.sql` file as
one script. The procedure declaration must be the first statement; these files
therefore contain neither `USE`, `GO`, nor dynamic `EXEC(N'...')` wrappers.
Unicode T-SQL string literals use the uppercase `N'...'` prefix so SQL Server
and DBeaver parse them consistently. Stored `ConnectorHealingLogs.EventType`
filters must also use the exact uppercase `EventType` values defined by the
application so the procedures work under case-sensitive database collations.
Epoch timestamps are constructed with `DATETIMEOFFSETFROMPARTS` rather than
locale-sensitive date strings, and aggregate counters use explicit zero-valued
`CASE` branches so DBeaver execution does not emit null-elimination warnings.

## Healing Flow

Healing is intentionally bounded and stateful.

The normal escalation path is:

1. Detect unhealthy connector/task.
2. Debounce for the configured confirmation checks.
3. Restart failed tasks.
4. Restart connector.
5. Recreate connector with offset restore.
6. Retry one timed-out recreate-with-offset if applicable.
7. Recreate connector without offset only when policy allows.
8. Escalate/stop when the level or max failed count is reached.

Recreate behavior:

- Replacement connector names are versioned: `NAME` -> `NAME.001`,
  `NAME.001` -> `NAME.002`.
- A failed recreate step reserves its attempted version for healing-flow
  sequencing. The next recreate step derives its name from the previous
  `new_connector_name`, so a failed attempt for `NAME.002` is followed by
  `NAME.003`, not another attempt to create `NAME.002`.
- Recreate with offset preserves the existing schema history topic.
- Recreate without offset creates a new schema history topic for the new
  connector name.
- `RECREATE_KEEP_BASE_CONNECTOR=true` preserves the connector that started the
  active incident, even when that original name already has a numeric suffix
  such as `.002`. Intermediate failed replacement versions may still be
  deleted after a later replacement succeeds.
- After successful recreate, stale topic lag state for the old connector is
  reset/superseded.
- Versioned failed connectors may be deleted after successful replacement.

Planned level-4 SCN gap recovery:

- Level 4 will replace the current one-shot recreate-without-offset behavior
  with an Oracle SCN gap recovery workflow. The implementation plan is kept in
  the local, gitignored `STEP4_SCN_RECOVERY_PLAN.md` file until delivery.
- The workflow captures the old connector's final offset SCN, creates a fresh
  replacement, waits for its first confirmed output message, pauses it, captures
  Oracle `CURRENT_SCN`, republishes committed Oracle changes in that SCN range,
  and resumes the replacement only after the backfill succeeds.
- Recovery uses at-least-once semantics. A small number of duplicate Kafka
  messages around the pause and SCN boundaries is explicitly acceptable; the
  workflow prioritizes avoiding missing messages.
- Recovery phase and progress checkpoints must be persisted so an app restart
  continues the same incident without creating another replacement connector.
- The old connector and DB connector identity must not be finalized or removed
  before backfill and replacement resume both succeed.
- This behavior remains feature-disabled until the Oracle/LogMiner reader,
  Kafka recovery publisher, durable state-machine phases, SQL contract updates,
  and integration tests are implemented and accepted.

Timeout behavior:

- Kafka Connect REST requests use a 30-second default timeout via
  `KC_REQUEST_TIMEOUT`; environments may override it when worker response time
  requires a larger bound.
- If Kafka Connect create times out during recreate-with-offset, the app records
  retry state with offsets and retries once.
- Before retrying the POST, the app checks whether the timed-out replacement
  connector already exists. An existing replacement is stopped and reused for
  offset patching; it is not created again. A 404 while stopping a superseded
  connector is treated as idempotent success so it cannot block the next fresh
  recreate version.
- If that bounded retry also fails, the next permitted step is recreate without
  offset using the next connector version and a matching new schema history
  topic; the timeout path does not skip directly to escalation while a fresh
  recreate attempt remains available.
- For backward compatibility, old timeout logs may contain a runtime `config`.
  New logs should avoid storing resolved secret config and should rely on
  `active_config` rehydrated from DB + SPs.

## Topic Lag Business Rules

Topic lag monitoring is per active topic job, not only per connector.

- `TopicLagJobs` defines monitored connector/topic pairs.
- Only active connectors and active topic jobs are monitored.
- Operational audit columns (`CreatedAt` and `UpdatedAt`) use
  `DATETIMEOFFSET(3) NOT NULL` with an inline `SYSDATETIMEOFFSET()` default. The
  inline form intentionally omits explicit `DF_*` names to keep the
  first-install schema compact without changing timestamp behavior. Source/event
  timestamps such as `LastMessageAt`, `LastFailedAt`, and `FinishedAt` also
  remain `DATETIMEOFFSET(3)` so their original event offsets
  can be preserved. Prometheus timestamp metrics convert stored values to Unix
  epoch seconds for Grafana/PromQL math.
- Event timestamps must preserve the original event meaning. In particular,
  `TopicLagJobs.LastMessageAt` represents the Debezium message timestamp
  extracted from the payload and must be stored exactly as received by the app;
  `spUpdateTopicLagState` must not rewrite its timezone offset,
  schema migration must not bulk-rewrite it, and metric SPs must not hide
  offset mistakes behind `DATEADD(HOUR, -7, ...)` corrections. If a row contains
  a bad future timestamp, fix that row explicitly from the source payload
  timestamp.
- Topic lag jobs for connectors with `FailedCount > 0` are not treated as new
  normal monitoring work, but jobs already marked `IsOverThreshold = 1` must
  still be returned by `spGetMonitoredTopics` so the app can detect a new Kafka
  message and write the topic `RECOVERED` transition.
- Over-threshold topic state is persisted and exposed through metrics.
- `TopicLagJobs.IsOverThreshold` represents the persisted topic-idle state only.
  It must be cleared when a new Kafka message makes `topic_idle` healthy again,
  even if `event_capture_time` still reports source-capture lag for that latest
  message. Source-capture lag remains a separate runtime metric/alert condition.
- Topic lag logs are transition logs. `OVER_THRESHOLD` should be written when a
  condition first enters over-threshold, not on every poll while the condition
  remains true. This is especially important for `event_capture_time`, which is
  runtime state and must not spam `TopicLagLogs` while source lag remains high.
- Topic recovered/over-threshold event alerting should prefer DB-backed recent
  event counts from `TopicLagLogs`. The app reads these through
  `dbo.spGetTopicLagEventMetrics @WindowMinutes = 15` and exposes
  `kc_shs_topic_lag_events_stored_count{event_status,condition}`. The older
  runtime counter `kc_shs_topic_lag_events_total` can remain for compatibility,
  but it is reset on app restart and should not be the primary recovered-alert
  signal.
- When a connector is recreated successfully, active over-threshold topic jobs
  for that connector are reset and a superseded log is written.
- When the app starts after downtime and sees Kafka end offset increased, topic
  idle state must be evaluated from the latest Kafka record timestamp. A newly
  discovered record only clears idle state when that latest timestamp is still
  within the configured idle threshold; if the latest timestamp is already older
  than the threshold, `TopicLagJobs.IsOverThreshold` must be set immediately.
- If the topic was already over-threshold before downtime and the app later
  discovers a newer Kafka record, write a `RECOVERED` topic lag log for that
  newly observed message. If that latest record is already older than the idle
  threshold by the time the app checks it, write a new `OVER_THRESHOLD` log in
  the same tick and keep the persisted final state over-threshold.
- A topic is only considered recovered by a new message when the app confirms a
  newer Kafka record timestamp. Do not use the app poll time as a fallback
  recovery timestamp, because an offset/read ambiguity after restart can
  otherwise create a false `RECOVERED` log for a topic that did not receive a
  new message. For an already over-threshold topic, the confirmed Kafka record
  timestamp must also be newer than the topic state `UpdatedAt`; an old latest
  record discovered after restart must not clear or recover an idle alert that
  was raised after that record existed.
- For Debezium JSON records, `TopicLagJobs.LastMessageAt` should use the
  payload capture timestamp `ts_ms` from the message body. Kafka consumer
  `record.timestamp` is only a fallback when the payload cannot be decoded or
  lacks `ts_ms`, because Kafka record timestamps may differ from the Debezium
  event/capture timestamp and can falsely reset Topics Over Threshold.
- `RECOVERED` is a confirmed-new-message event, not a generic state-correction
  event. If `TopicLagJobs.IsOverThreshold = 1` but the current calculation is
  healthy without a confirmed newer Kafka record, the app may clear persisted
  state only when it is part of a confirmed recovery. It must not silently set
  `TopicLagJobs.IsOverThreshold = 0` without a `RECOVERED` log/new message,
  because that breaks Topics Over Threshold tracking for the topic. This
  prevents repeated recovered notifications and false lag-job clears when DB
  state remains stale or the same topic row is returned again.

Dashboard semantics previously agreed:

- Topic Records Lag must show, over time, the source capture lag of the latest
  Debezium record for each monitored topic: how long it took from the database
  event timestamp to the Kafka/capture timestamp. It uses
  `kc_shs_topic_records_lag` grouped by `connector_name, topic`, with unit
  minutes. The metric is derived from Debezium `source.ts_ms` and record
  `ts_ms` (`event_capture_lag_ms / 60000`). It is not Kafka end-offset delta
  and not the duration since the topic last received a message; that separate
  no-message duration belongs to Topics Over Threshold. The dashboard query
  must zero-fill from `kc_shs_topic_active * 0` so every currently monitored
  topic is visible even before it has a `kc_shs_topic_records_lag` sample. Do
  not persist Grafana `hideSeriesFrom` overrides on this panel; every series
  matching the connector and topic filters must remain visible as a time-series
  line.
- Healing Attempts should count every configured connector that entered a
  healing cycle, including connectors that are currently inactive. It should
  respect both the selected dashboard time range and connector filter, and use
  the DB-backed cumulative gauge
  `kc_shs_healing_attempts_stored_count`, with
  `event_type="HEALTH_FAILED_CONFIRMED"` representing cycle entry count. Plot
  the cumulative gauge directly across the selected range; do not subtract the
  value at PromQL `start()`, because that would turn an unchanged persisted
  total such as 12 into zero. Dashboard PromQL must intersect these historical
  counts with `kc_shs_connector_active` evaluated at `end()` and separately add
  zero-valued current connector series. The metric value may be either 0 or 1;
  its presence defines the current configured connector scope. This excludes
  deleted connector series while retaining inactive connectors and connectors
  with zero healing attempts.
- `spGetHealingAttemptMetrics` must aggregate Healing Attempts by
  `ConnectorHealingLogs.ConnectorId = Connectors.Id`, not by `ConnectorName`.
  Connector names can change when a recreate advances the numeric suffix;
  `ConnectorId` is the stable relationship that keeps each connector's
  persisted healing history attached to the correct dashboard series. The SP
  must not filter `Connectors.IsActive`. It returns the current physical
  `ConnectorName` for dashboard filtering and
  `COALESCE(NULLIF(JobName, ''), ConnectorName)` as `BaseConnectorName` for
  grouping and display. The exported
  `kc_shs_healing_attempts_stored_count` metric therefore carries both
  `connector_name` and `base_connector_name`. Healing Attempts must calculate
  the cumulative value by `base_connector_name`, then join that value to the
  current metric mapping and display the latest `connector_name`. A recreated
  physical connector name must not reset or split the base connector's count.
- The runtime dashboard does not include a Healing Step % panel. Persisted
  healing-step rows remain available through `spGetHealingAttemptMetrics` and
  `kc_shs_healing_attempts_stored_count` for operational analysis, but they are
  not visualized as a percentage distribution.
- Every runtime dashboard panel must honor the dashboard time picker. Range
  data panels (Topic Records Lag, Healing Attempts, and connector logs) query
  samples inside the selected interval. Event-summary panels use
  `increase(...[$__range])`. Current-state panels (App Up, Active Connectors,
  Failed Connectors, Topics Over Threshold, and Connector Status) represent the
  latest state at the selected range end rather than summing state samples over
  the whole interval. Active Connectors and Connector Status must also apply the
  `$connector_name` filter.
- Active Connectors and Failed Connectors are number-only stat panels. Keep
  `graphMode="none"` and `textMode="value"`; do not render a sparkline or label
  inside either panel.
- Connector Status must list every connector configured in the application,
  including inactive rows. Active connectors use `kc_shs_connector_inventory`
  for their Kafka Connect runtime state; inactive rows use
  `kc_shs_connector_active == 0` and render `state="INACTIVE"`. Do not call
  Kafka Connect for inactive connector status solely to populate this panel.
  The table exposes only Connector Name, Source, and Status. Status text is
  color mapped: running is green, failed or missing is red, inactive is gray,
  and paused, unassigned, or unknown states use warning colors.
- The runtime dashboard Connector filter must list every connector configured
  in the application, including inactive rows. Build it from
  `max by (connector_name) (kc_shs_connector_active)` to deduplicate the two
  metrics endpoints, and keep the textbox search variable available so
  operators can narrow the dropdown without losing the default All view.
- Monitoring inventory should not scan every connector on the Kafka Connect
  worker. To reduce REST load and log volume, it calls `GET /status` and
  `GET /config` only for active connectors returned from DB connector activity.
  This means runtime inventory no longer detects unmanaged connectors or
  replacement connectors by scanning `/connectors`; those need a separate
  diagnostic path if operators require them later.
- Topics Over Threshold should not group or display by `condition` when the
  user wants one row/series per topic. For the live no-message duration view,
  use `clamp_min((time() - (kc_shs_topic_last_message_timestamp_seconds > 0))
  / 60, 0)`, grouped by `connector_name, topic`, and display it as a
  horizontal bar gauge for quick operator scanning of the longest-idle topics.
  Filtering timestamp samples to `> 0` is required because startup/unknown
  samples at Unix epoch 0 otherwise makes Grafana show idle durations around 56
  years. Clamping at zero prevents future timestamps from rendering negative
  idle durations while a DB timezone correction is being applied.
- Alert priority for topic lag: when a topic's last message had source capture
  lag and the topic later becomes idle, repeat the idle alert only. The
  source-capture-lag alert must suppress any topic that currently has active
  `topic_idle` over-threshold state for the same `connector_name, topic`.
  Source-capture-lag alert repeats only while messages are still arriving and
  idle alert is not firing for that topic. On app restart, if
  `TopicLagJobs.IsOverThreshold = 1` and the topic is still idle over-threshold,
  the app must not write a new `OVER_THRESHOLD` log just because
  `event_capture_time` in-memory transition state was empty.
- Topic recovered alert should use
  `kc_shs_topic_lag_events_stored_count{event_status="RECOVERED"} > 0` instead
  of `increase(kc_shs_topic_lag_events_total[15m]) > 0`, because the stored
  count is rebuilt from `TopicLagLogs` by SP and survives app restarts inside
  the configured window.
- Connector Status keeps existing behavior but should not show the constant
  `value = 1` column.
- A live connector log panel should show warning/error logs for monitored
  connectors, filtered by the dashboard connector variable and backed by Loki
  when the Grafana environment has the app logs available.
- Grafana Telegram notification template `kc-shs-template` renders KC-SHS alert
  messages. When multiple topic alerts are grouped into one notification, each
  alert block must be separated by a visible divider and blank lines. Avoid
  aggressive Go-template whitespace trimming such as `{{- end }}` around block
  boundaries because it causes consecutive topic messages to run together.

## Engineering Rules

When changing this project:

- Read current code paths before refactor; do not assume behavior from tests
  alone.
- If Python changes depend on DB columns, procedures, or query result shape,
  update the matching files under `sql/init-db` and tests in
  the same change.
- Keep unrelated worktree changes untouched.
- Keep connector seed configs faithful to real Kafka Connect config. Do not
  reduce full connector configs to minimal examples when the seed is intended
  for runtime use.
- Keep seed responsibilities separate. The folder
  `sql/connector-inserts/connector` contains one SQL file per source system with
  its plain `Connectors` inserts; these files do not upsert or query existing
  data. Files under `sql/connector-inserts/topic-lag-job-inserts` manage
  `TopicLagJobs`; run the connector insert before its topic-lag-job script.
- Group connector suffixes from the same source-system folder into one file.
  For example, `TOPO-TCH.sql` contains the inserts for `G025`, `G026`, and
  `G027`, while `TOPO-CLI.sql` contains its three connector inserts.
- Connector seeds use `{config_id}`, `{url}`, `{pwd}`, and `{kafka_server}` as
  replacement placeholders. Replace only `{config_id}` before executing the
  seed; runtime credential resolution replaces the three config placeholders.
- External credential inserts live in the single file
  `sql/ingest_reference/others.sql`, grouped by source-system region. Connector
  `ConfigId` values must match the resulting `ETLConfigurationID` records. The
  shared Kafka server remains `ETLConfigurationID = 27`.
- Do not persist resolved connector secrets in DB fields or logs.
- Add/update tests for repository contracts and healing behavior when changing
  DB-backed business logic.
- Runtime scheduling is implemented by `PeriodicWorker`; the project does not
  depend on the legacy `schedule` package.
- Keep Kafka Connect REST and repository interfaces limited to production
  callers. Do not reintroduce unwired persistence callbacks, direct config
  get/update helpers, dead healing branches, or metric-specific repository
  wrappers without a runtime use case.
