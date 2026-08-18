# kc_shs

dbt project for the Kafka Connect self-healing support database.

## Configuration seeds

Human-maintained connector JSON files live under `config/connectors`; topic and
policy YAML files live under `config/topics`.
The seed CSV files under `seeds/generated` are generated artifacts:

- `connector_configs.csv`: desired connector definitions.
- `topic_lag_job_configs.csv`: explicit connector/topic mappings.

`ConnectorReference` may match `ConnectorName`, `JobName`, or the deployed
Kafka Connect name in `properties.name`.

When generating `connector_configs.csv`, these connector properties are always
replaced with runtime placeholders; their source JSON values are left unchanged:

- `database.url` -> `{url}`
- `database.user` -> `{user}`
- `database.password` -> `{pwd}`
- `schema.history.internal.kafka.bootstrap.servers` -> `{kafka_server}`

For connectors using `{user}`, the `ETLConfiguration.ConfiguredValue` selected
by `ConfigId` must use this format:

```text
<database.url>;<database.user>;<database.password>
```

Connectors without `{user}` remain compatible with the existing
`<database.url>;<database.password>` format.

Regenerate the seeds after changing a connector JSON or topic YAML configuration:

```bash
python scripts/generate_seeds.py
```

Connector and topic configurations are independent. When one configuration
directory is empty, its generated CSV contains only the header and the other
configuration type is still generated and inserted normally. Topic-only loads
resolve `ConnectorReference` against connectors already stored in the database.

Load the seed tables into the configured `dbt_seed` schema:

```bash
dbt seed --full-refresh
dbt test --select resource_type:seed
```

The active dbt profile must target SQL Server before running `dbt seed`.

Preview connector updates/inserts and new topic-lag records:

```bash
dbt run-operation insert_kafka_config_seeds
```

Upsert connectors by `JobName`, then insert missing records into
`ingest_reference.dbo.TopicLagJobs`:

```bash
dbt run-operation insert_kafka_config_seeds --args '{dry_run: false}'
```

For an existing `JobName`, the connector keeps its current `Id` while its
configuration fields are overwritten from the seed. A new connector is
inserted only when its `JobName` does not exist. Existing topic-lag records are
not updated or deleted, and the seed tables remain available for validation
and audit.

## Stored procedures

Install or update all stored procedure definitions:

```bash
dbt run-operation run_all_sps
```
