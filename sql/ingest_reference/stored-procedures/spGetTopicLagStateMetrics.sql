CREATE OR ALTER PROCEDURE dbo.spGetTopicLagStateMetrics
as
begin
    set nocount on;

    declare @unixepoch datetimeoffset(3) = datetimeoffsetfromparts(
        1970, 1, 1, 0, 0, 0, 0, 0, 0, 3
    );

    select
        c.[ConnectorName],
        t.[TopicName],
        t.[IsOverThreshold],
        case
            when t.[LastMessageAt] is null then 0
            else datediff_big(
                second,
                @unixepoch,
                switchoffset(t.[LastMessageAt], '+00:00')
            )
        end as [LastMessageTimestampSeconds]
    from [dbo].[TopicLagJobs] as t
    join [dbo].[Connectors] as c
      on c.[Id] = t.[ConnectorId]
    where t.[IsActive] = 1
      and c.[IsActive] = 1
      and c.[FailedCount] = 0
    order by
        c.[ConnectorName],
        t.[TopicName];
END;
