{% macro create_sp_get_topic_lag_event_metrics() %}

    {% set sql %}
        CREATE OR ALTER PROCEDURE dbo.spGetTopicLagEventMetrics
            @windowminutes int = 15
        as
        begin
            set nocount on;

            declare @effectivewindowminutes int = case
                when @windowminutes is null or @windowminutes <= 0 then 15
                else @windowminutes
            end;

            select
                c.[ConnectorName],
                t.[TopicName],
                l.[EventStatus],
                coalesce(
                    nullif(json_value(l.[Details], '$.lag_condition'), ''),
                    N'topic_idle'
                ) as [Condition],
                count_big(1) as [TotalEvents]
            from [dbo].[TopicLagLogs] as l
            join [dbo].[Connectors] as c
              on c.[Id] = l.[ConnectorId]
            join [dbo].[TopicLagJobs] as t
              on t.[Id] = l.[TopicLagJobId]
             and t.[ConnectorId] = c.[Id]
            where c.[IsActive] = 1
              and t.[IsActive] = 1
              and l.[CreatedAt] >= dateadd(
                    minute,
                    -@effectivewindowminutes,
                    sysdatetimeoffset()
                )
              and l.[EventStatus] is not null
            group by
                c.[Id],
                c.[ConnectorName],
                t.[Id],
                t.[TopicName],
                l.[EventStatus],
                coalesce(
                    nullif(json_value(l.[Details], '$.lag_condition'), ''),
                    N'topic_idle'
                )
            order by
                c.[ConnectorName],
                t.[TopicName],
                l.[EventStatus],
                [Condition];
        END;
    {% endset %}

    {% do run_query(sql) %}
    {% do log("--> Đã tạo/cập nhật dbo.spGetTopicLagEventMetrics", info=True) %}

{% endmacro %}
