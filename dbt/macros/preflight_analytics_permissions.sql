{% macro preflight_analytics_permissions() %}
  {# Verify only the permissions needed by the first dbt analytics phase. #}
  {% if not execute %}
    {{ return({"status": "not_executed"}) }}
  {% endif %}

  {% set target_schema = target.schema | replace("'", "''") %}
  {% set permission_sql %}
    SELECT
      COALESCE(HAS_PERMS_BY_NAME(N'dbo.ConnectorHealingQueue', 'OBJECT', 'SELECT'), 0) AS can_select_queue,
      COALESCE(HAS_PERMS_BY_NAME(N'dbo.ConnectorHealingLogs', 'OBJECT', 'SELECT'), 0) AS can_select_logs,
      COALESCE(HAS_PERMS_BY_NAME(N'dbo.vConnectorIncidentFacts', 'OBJECT', 'SELECT'), 0) AS can_select_legacy_facts,
      COALESCE(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE TABLE'), 0) AS can_create_table,
      COALESCE(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE VIEW'), 0) AS can_create_view,
      DATABASEPROPERTYEX(DB_NAME(), 'IsSnapshotIsolationOn') AS snapshot_isolation_state,
      CASE WHEN SCHEMA_ID(N'{{ target_schema }}') IS NULL THEN 0 ELSE 1 END AS target_schema_exists,
      COALESCE(HAS_PERMS_BY_NAME(N'{{ target_schema }}', 'SCHEMA', 'ALTER'), 0) AS can_alter_target_schema,
      COALESCE(HAS_PERMS_BY_NAME(DB_NAME(), 'DATABASE', 'CREATE SCHEMA'), 0) AS can_create_schema
  {% endset %}
  {% set result = run_query(permission_sql) %}
  {% if result is none or result.rows | length != 1 %}
    {{ exceptions.raise_compiler_error("dbt analytics preflight could not read SQL Server permissions") }}
  {% endif %}

  {% set row = result.rows[0] %}
  {% set checks = {
    "select_queue": row[0] | int,
    "select_logs": row[1] | int,
    "select_legacy_facts": row[2] | int,
    "create_table": row[3] | int,
    "create_view": row[4] | int,
    "snapshot_isolation_state": row[5] | int,
    "target_schema_exists": row[6] | int,
    "alter_target_schema": row[7] | int,
    "create_schema": row[8] | int
  } %}
  {% set required_checks = {
    "select_queue": checks["select_queue"],
    "select_logs": checks["select_logs"],
    "select_legacy_facts": checks["select_legacy_facts"],
    "create_table": checks["create_table"],
    "create_view": checks["create_view"]
  } %}
  {% if checks["target_schema_exists"] == 1 %}
    {% do required_checks.update({"alter_target_schema": checks["alter_target_schema"]}) %}
  {% else %}
    {% do required_checks.update({"create_schema": checks["create_schema"]}) %}
  {% endif %}

  {% set missing = [] %}
  {% for permission, granted in required_checks.items() %}
    {% if granted != 1 %}
      {% do missing.append(permission) %}
    {% endif %}
  {% endfor %}

  {% if missing %}
    {{ exceptions.raise_compiler_error(
      "dbt analytics preflight failed for target schema '" ~ target.schema ~ "'; missing permissions: " ~ (missing | join(", "))
    ) }}
  {% endif %}

  {{ log("dbt analytics permission preflight passed for the configured target schema; snapshot isolation state=" ~ checks["snapshot_isolation_state"], info=True) }}
  {{ return(checks) }}
{% endmacro %}
