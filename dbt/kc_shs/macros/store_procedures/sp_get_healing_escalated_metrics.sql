{% macro create_sp_get_healing_escalated_metrics() %}

    {% set sql %}
        CREATE OR ALTER PROCEDURE dbo.spGetHealingEscalatedMetrics
            @windowminutes int = 15
        as
        begin
            set nocount on;

            declare @effectivewindowminutes int = case
                when @windowminutes is null or @windowminutes <= 0 then 15
                else @windowminutes
            end;

            declare @unixepoch datetimeoffset(3) = datetimeoffsetfromparts(
                1970, 1, 1, 0, 0, 0, 0, 0, 0, 3
            );

            select
                c.[ConnectorName],
                l.[EventType],
                datediff_big(
                    second,
                    @unixepoch,
                    max(switchoffset(l.[CreatedAt], '+00:00'))
                ) as [LastHealingEscalatedTimestampSeconds]
            from [dbo].[Connectors] as c
            join [dbo].[ConnectorHealingLogs] as l
              on l.[ConnectorId] = c.[Id]
            where c.[IsActive] = 1
              and l.[EventType] in (
                    'HEALING_ESCALATED',
                    'HEALING_LEVEL_LIMIT_REACHED',
                    'STATE_MACHINE_ERROR'
                )
              and l.[CreatedAt] is not null
              and l.[CreatedAt] >= dateadd(
                    minute,
                    -@effectivewindowminutes,
                    sysdatetimeoffset()
                )
            group by
                c.[Id],
                c.[ConnectorName],
                l.[EventType]
            order by
                c.[ConnectorName],
                l.[EventType];
        END;
    {% endset %}

    {% do run_query(sql) %}
    {% do log("--> Đã tạo/cập nhật dbo.spGetHealingEscalatedMetrics", info=True) %}

{% endmacro %}
