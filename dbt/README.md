# dbt Analytics

This directory holds the SQL Server analytics transformation layer:

- `models/staging`: source-aligned queue and healing-log models.
- `models/marts`: canonical incident facts and verified aggregates.
- `tests`: data-quality and parity tests before any semantic-layer cutover.

`fct_connector_incidents` is the incident-grain canonical mart. The semantic
runtime does not query it directly: `vSemanticConnectorIncidentFacts` is the
compatibility view that preserves the approved legacy column contract.

Use the repository's SQL Server dbt environment, not a globally installed dbt
with a different adapter. Run these commands from the checkout that contains
`.venv-dbt`:

```powershell
& .\.venv-dbt\Scripts\dbt.exe debug
& .\.venv-dbt\Scripts\dbt.exe run-operation preflight_analytics_permissions
```

The preflight is read-only. It verifies source `SELECT` access and either
`ALTER` on the existing target schema or `CREATE SCHEMA` when it does not yet
exist, plus the database permissions needed for future view/table
materializations. It does not create, alter, or delete SQL Server objects.

`DBT_ANALYTICS_SOURCE_DATABASE` optionally selects a disposable integration
database; when unset, dbt uses the target database. This keeps source names
portable without putting connection details in the project.

## Deployment target

The dbt profile remains outside Git. Before an operator builds a durable
target, the profile owner must explicitly approve a target such as
`analytics` whose `schema` is `analytics`. The corresponding runtime setting
is `CHAT_ANALYTICS_DBT_SCHEMA=analytics`; it is an allowlisted source name,
not a user or model input.

```powershell
$env:DBT_ANALYTICS_TARGET = "analytics"
& .\.venv-dbt\Scripts\dbt.exe debug --target $env:DBT_ANALYTICS_TARGET
& .\.venv-dbt\Scripts\dbt.exe run-operation preflight_analytics_permissions --target $env:DBT_ANALYTICS_TARGET
```

The preflight reports SQL Server snapshot-isolation availability but does not
claim that a snapshot is consistent. Do not run `build` against a durable
schema until the target, schema, and deployment window have been approved.

## Runtime rollout

`CHAT_ANALYTICS_FACT_SOURCE=legacy` is the default and rollback setting.
`shadow` serves the legacy answer while a separately bounded dbt comparison
runs in the background. It reports `inconclusive` unless the operator has
verified a shared snapshot. `dbt` serves only the compatibility view and
returns a source failure instead of silently falling back to legacy.

Freshness remains `unknown` until an owner provides an explicit watermark and
SLA. A successful dbt build or parity test is deployment evidence, not source
freshness evidence.

For a CI system added later, use `dbt parse` as the database-free PR check and
run `dbt build` only against an isolated SQL Server database/schema. Source
freshness remains a scheduled concern until its business SLA is approved.

`fixtures/ci_source_contract.sql` is only for the disposable CI database. Its
small deterministic incident set ensures the parity tests exercise non-empty
source data; it is never applied to a developer or production database.
