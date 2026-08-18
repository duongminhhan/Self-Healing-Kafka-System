{% macro insert_kafka_config_seeds(dry_run=true) %}
    {% set connector_seed = ref('connector_configs') %}
    {% set topic_seed = ref('topic_lag_job_configs') %}

    {% if execute %}
        {% set connector_relation = adapter.get_relation(
            database=connector_seed.database,
            schema=connector_seed.schema,
            identifier=connector_seed.identifier
        ) %}
        {% set topic_relation = adapter.get_relation(
            database=topic_seed.database,
            schema=topic_seed.schema,
            identifier=topic_seed.identifier
        ) %}
    {% else %}
        {% set connector_relation = connector_seed %}
        {% set topic_relation = topic_seed %}
    {% endif %}

    {% if connector_relation is none %}
        {% do log('Skipping connector configs: seed table is not available', info=True) %}
    {% endif %}
    {% if topic_relation is none %}
        {% do log('Skipping topic lag configs: seed table is not available', info=True) %}
    {% endif %}
    {% if connector_relation is none and topic_relation is none %}
        {% do log("No seed tables are available. Run 'python scripts/generate_seeds.py' and 'dbt seed --full-refresh'.", info=True) %}
        {{ return(none) }}
    {% endif %}

    {% if dry_run %}
        {% set sql %}
            {% if connector_relation is not none %}
            SELECT
                'Connectors' AS [TargetTable],
                'UPDATE' AS [Action],
                COUNT_BIG(*) AS [RowsAffected]
            FROM {{ connector_relation }} AS seed
            INNER JOIN [ingest_reference].[dbo].[Connectors] AS target
                ON target.[JobName] = seed.[JobName]

            UNION ALL

            SELECT
                'Connectors' AS [TargetTable],
                'INSERT' AS [Action],
                COUNT_BIG(*) AS [RowsAffected]
            FROM {{ connector_relation }} AS seed
            WHERE NOT EXISTS (
                SELECT 1
                FROM [ingest_reference].[dbo].[Connectors] AS target
                WHERE target.[JobName] = seed.[JobName]
            )
            {% endif %}

            {% if connector_relation is not none and topic_relation is not none %}
            UNION ALL
            {% endif %}

            {% if topic_relation is not none %}
            SELECT
                'TopicLagJobs' AS [TargetTable],
                'INSERT' AS [Action],
                COUNT_BIG(*) AS [RowsAffected]
            FROM {{ topic_relation }} AS seed
            INNER JOIN [ingest_reference].[dbo].[Connectors] AS connector
                ON seed.[ConnectorReference] = connector.[ConnectorName]
                OR seed.[ConnectorReference] = connector.[JobName]
                OR seed.[ConnectorReference] = JSON_VALUE(
                    connector.[ConfigTemplate], '$.name'
                )
                OR seed.[ConnectorReference] = JSON_VALUE(
                    connector.[ConfigTemplate], '$.config.name'
                )
            WHERE NOT EXISTS (
                SELECT 1
                FROM [ingest_reference].[dbo].[TopicLagJobs] AS target
                WHERE target.[ConnectorId] = connector.[Id]
                  AND target.[TopicName] = seed.[TopicName]
            );
            {% endif %}
        {% endset %}

        {% set result = run_query(sql) %}
        {% if execute and result is not none %}
            {% for row in result.rows %}
                {% do log(row[0] ~ ' ' ~ row[1] ~ ': ' ~ row[2] ~ ' row(s)', info=True) %}
            {% endfor %}
        {% endif %}
        {{ return(result) }}
    {% endif %}

    {% set sql %}
        SET NOCOUNT ON;
        SET XACT_ABORT ON;

        BEGIN TRY
            BEGIN TRANSACTION;

            DECLARE @UpdatedConnectors INT = 0;
            DECLARE @InsertedConnectors INT = 0;
            DECLARE @InsertedTopics INT = 0;

            {% if connector_relation is not none %}
            IF EXISTS (
                SELECT seed.[JobName]
                FROM {{ connector_relation }} AS seed
                GROUP BY seed.[JobName]
                HAVING COUNT_BIG(*) > 1
            )
                THROW 50001,
                    'Connector seed must contain exactly one row per JobName.',
                    1;

            IF EXISTS (
                SELECT seed.[JobName]
                FROM {{ connector_relation }} AS seed
                INNER JOIN [ingest_reference].[dbo].[Connectors] AS target
                    ON target.[JobName] = seed.[JobName]
                GROUP BY seed.[JobName]
                HAVING COUNT(DISTINCT target.[Id]) > 1
            )
                THROW 50001,
                    'A seeded JobName matches multiple existing connectors.',
                    1;

            IF EXISTS (
                SELECT 1
                FROM {{ connector_relation }} AS seed
                INNER JOIN [ingest_reference].[dbo].[Connectors] AS target
                    ON target.[ConnectorName] = seed.[ConnectorName]
                   AND target.[JobName] <> seed.[JobName]
            )
                THROW 50001,
                    'ConnectorName already belongs to another JobName.',
                    1;

            UPDATE target
            SET
                target.[ConnectorName] = seed.[ConnectorName],
                target.[ConnectorType] = seed.[ConnectorType],
                target.[IsActive] = seed.[IsActive],
                target.[Level] = seed.[Level],
                target.[ConfigTemplate] = seed.[ConfigTemplate],
                target.[ConfigId] = seed.[ConfigId]
            FROM [ingest_reference].[dbo].[Connectors] AS target
            INNER JOIN {{ connector_relation }} AS seed
                ON target.[JobName] = seed.[JobName];

            SET @UpdatedConnectors = @@ROWCOUNT;

            INSERT INTO [ingest_reference].[dbo].[Connectors] (
                [JobName],
                [ConnectorName],
                [ConnectorType],
                [IsActive],
                [Level],
                [ConfigTemplate],
                [ConfigId]
            )
            SELECT
                seed.[JobName],
                seed.[ConnectorName],
                seed.[ConnectorType],
                seed.[IsActive],
                seed.[Level],
                seed.[ConfigTemplate],
                seed.[ConfigId]
            FROM {{ connector_relation }} AS seed
            WHERE NOT EXISTS (
                SELECT 1
                FROM [ingest_reference].[dbo].[Connectors] AS target
                WHERE target.[JobName] = seed.[JobName]
            );

            SET @InsertedConnectors = @@ROWCOUNT;
            {% endif %}

            {% if topic_relation is not none %}
            IF EXISTS (
                SELECT seed.[ConnectorReference]
                FROM {{ topic_relation }} AS seed
                LEFT JOIN [ingest_reference].[dbo].[Connectors] AS connector
                    ON seed.[ConnectorReference] = connector.[ConnectorName]
                    OR seed.[ConnectorReference] = connector.[JobName]
                    OR seed.[ConnectorReference] = JSON_VALUE(
                        connector.[ConfigTemplate], '$.name'
                    )
                    OR seed.[ConnectorReference] = JSON_VALUE(
                        connector.[ConfigTemplate], '$.config.name'
                    )
                GROUP BY seed.[ConnectorReference]
                HAVING COUNT(DISTINCT connector.[Id]) <> 1
            )
                THROW 50001,
                    'Each ConnectorReference must match exactly one connector.',
                    1;

            INSERT INTO [ingest_reference].[dbo].[TopicLagJobs] (
                [ConnectorId],
                [JobName],
                [TopicName],
                [IsActive]
            )
            SELECT
                connector.[Id],
                connector.[JobName],
                seed.[TopicName],
                seed.[IsActive]
            FROM {{ topic_relation }} AS seed
            INNER JOIN [ingest_reference].[dbo].[Connectors] AS connector
                ON seed.[ConnectorReference] = connector.[ConnectorName]
                OR seed.[ConnectorReference] = connector.[JobName]
                OR seed.[ConnectorReference] = JSON_VALUE(
                    connector.[ConfigTemplate], '$.name'
                )
                OR seed.[ConnectorReference] = JSON_VALUE(
                    connector.[ConfigTemplate], '$.config.name'
                )
            WHERE NOT EXISTS (
                SELECT 1
                FROM [ingest_reference].[dbo].[TopicLagJobs] AS target
                WHERE target.[ConnectorId] = connector.[Id]
                  AND target.[TopicName] = seed.[TopicName]
            );

            SET @InsertedTopics = @@ROWCOUNT;
            {% endif %}

            COMMIT TRANSACTION;

            SELECT
                @UpdatedConnectors AS [UpdatedConnectors],
                @InsertedConnectors AS [InsertedConnectors],
                @InsertedTopics AS [InsertedTopics];
        END TRY
        BEGIN CATCH
            IF XACT_STATE() <> 0
                ROLLBACK TRANSACTION;
            THROW;
        END CATCH;
    {% endset %}

    {% set result = run_query(sql) %}
    {% if execute and result is not none and result.rows | length > 0 %}
        {% set row = result.rows[0] %}
        {% do log('Updated connectors: ' ~ row[0], info=True) %}
        {% do log('Inserted connectors: ' ~ row[1], info=True) %}
        {% do log('Inserted topic lag jobs: ' ~ row[2], info=True) %}
    {% endif %}
    {{ return(result) }}
{% endmacro %}
