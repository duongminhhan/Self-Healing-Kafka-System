{% macro create_sp_get_healing_attempt_metrics() %}

    {% set sql %}
        CREATE OR ALTER PROCEDURE dbo.spGetHealingAttemptMetrics
        as
        begin
            set nocount on;

            declare @healingevents table (
                [EventType] varchar(80) not null primary key,
                [HealingStep] smallint not null
            );

            insert into @healingevents ([EventType], [HealingStep])
            values
                ('HEALTH_FAILED_CONFIRMED', 1),
                ('TASK_RESTART', 2),
                ('CONNECTOR_RESTART', 2),
                ('CONNECTOR_RECREATE_WITH_OFFSET', 3),
                ('CONNECTOR_RECREATE_WITHOUT_OFFSET', 4);

            select
                c.[ConnectorName],
                coalesce(nullif(c.[JobName], ''), c.[ConnectorName])
                    as [BaseConnectorName],
                e.[EventType],
                e.[HealingStep],
                sum(convert(bigint, case when l.[Id] is null then 0 else 1 end))
                    as [TotalAttempts]
            from [dbo].[Connectors] as c
            cross join @healingevents as e
            left join [dbo].[ConnectorHealingLogs] as l
              on l.[ConnectorId] = c.[Id]
             and l.[EventType] = e.[EventType]
            group by
                c.[Id],
                c.[ConnectorName],
                c.[JobName],
                e.[EventType],
                e.[HealingStep]
            order by
                c.[ConnectorName],
                e.[HealingStep],
                e.[EventType];
        END;
    {% endset %}

    {% do run_query(sql) %}
    {% do log("--> Đã tạo/cập nhật dbo.spGetHealingAttemptMetrics", info=True) %}

{% endmacro %}
