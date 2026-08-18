CREATE OR ALTER PROCEDURE dbo.spGetMonitoredTopics
    @connectorid uniqueidentifier = null,
    @overthresholdonly bit = 0
as
begin
    set nocount on;

    select
        t.[Id] as [TopicLagJobId],
        t.[ConnectorId],
        t.[JobName],
        c.[ConnectorName],
        t.[TopicName],
        t.[IsOverThreshold],
        t.[LastEndOffset],
        t.[LastMessageAt],
        t.[UpdatedAt]
    from [dbo].[TopicLagJobs] as t
    join [dbo].[Connectors] as c on c.[Id] = t.[ConnectorId]
    where t.[IsActive] = 1
      and c.[IsActive] = 1
      and (@connectorid is null or t.[ConnectorId] = @connectorid)
      and (@overthresholdonly = 0 or t.[IsOverThreshold] = 1)
      and (c.[FailedCount] = 0 or t.[IsOverThreshold] = 1)
    order by t.[CreatedAt], t.[TopicName];
END;
