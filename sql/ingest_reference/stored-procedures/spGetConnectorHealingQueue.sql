CREATE OR ALTER PROCEDURE dbo.spGetConnectorHealingQueue
    @queueid uniqueidentifier = null,
    @connectorname varchar(255) = null,
    @openonly bit = 1,
    @dueonly bit = 0
as
begin
    set nocount on;

    select
        q.[QueueId] as [Id],
        q.[QueueId] as [ActiveIncidentId],
        q.[RootConnectorName],
        q.[CurrentConnectorName] as [ConnectorName],
        q.[ConnectorClass],
        q.[HealingMode],
        case when q.[HealingMode] = 'RECOVERY' then 4 else 2 end as [Level],
        q.[QueueStatus],
        q.[FinalOutcome],
        q.[ReceivedAt],
        q.[StartedAt],
        q.[CompletedAt],
        q.[NextAttemptAt],
        latest.[EventType] as [LatestEventType],
        latest.[AttemptNo] as [LatestAttemptNo],
        latest.[Message] as [LatestMessage],
        latest.[Details] as [LatestEventDetails],
        latest.[CreatedAt] as [LatestEventAt],
        case when q.[QueueStatus] in ('PENDING', 'PROCESSING', 'WAITING')
            then cast(1 as bit) else cast(0 as bit) end as [LatestHasNextStep],
        coalesce(counts.[FailedCount], 0) as [FailedCount],
        coalesce(counts.[TaskRestartCount], 0) as [TaskRestartCount],
        coalesce(counts.[ConnectorRestartCount], 0) as [ConnectorRestartCount],
        coalesce(counts.[RecreateWithOffsetCount], 0) as [RecreateWithOffsetCount],
        coalesce(counts.[RecreateWithOffsetTimeoutCount], 0)
            as [RecreateWithOffsetTimeoutCount],
        coalesce(counts.[RecreateWithoutOffsetCount], 0)
            as [RecreateWithoutOffsetCount]
    from [dbo].[ConnectorHealingQueue] as q
    outer apply (
        select top (1)
            l.[EventType],
            l.[AttemptNo],
            l.[Message],
            l.[Details],
            l.[CreatedAt]
        from [dbo].[ConnectorHealingLogs] as l
        where l.[QueueId] = q.[QueueId]
        order by l.[CreatedAt] desc, l.[Id] desc
    ) as latest
    outer apply (
        select
            sum(case when l.[EventType] = 'HEALTH_FAILURE_OBSERVED' then 1 else 0 end)
                as [FailedCount],
            sum(case when l.[EventType] = 'TASK_RESTART' then 1 else 0 end)
                as [TaskRestartCount],
            sum(case when l.[EventType] = 'CONNECTOR_RESTART' then 1 else 0 end)
                as [ConnectorRestartCount],
            sum(case when l.[EventType] in (
                'CONNECTOR_RECREATE_WITH_OFFSET',
                'CONNECTOR_RECREATE_WITH_OFFSET_FAILED'
            ) then 1 else 0 end) as [RecreateWithOffsetCount],
            sum(case when l.[EventType] = 'CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT'
                then 1 else 0 end) as [RecreateWithOffsetTimeoutCount],
            sum(case when l.[EventType] in (
                'CONNECTOR_RECREATE_WITHOUT_OFFSET',
                'CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED'
            ) then 1 else 0 end) as [RecreateWithoutOffsetCount]
        from [dbo].[ConnectorHealingLogs] as l
        where l.[QueueId] = q.[QueueId]
    ) as counts
    where (@queueid is null or q.[QueueId] = @queueid)
      and (@connectorname is null or q.[CurrentConnectorName] = @connectorname
           or q.[RootConnectorName] = @connectorname)
      and (@openonly = 0 or q.[QueueStatus] in ('PENDING', 'PROCESSING', 'WAITING'))
      and (
          @dueonly = 0
          or q.[NextAttemptAt] is null
          or q.[NextAttemptAt] <= sysdatetimeoffset()
      )
    order by q.[ReceivedAt];
END;
