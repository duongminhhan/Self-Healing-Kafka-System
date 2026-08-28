CREATE OR ALTER PROCEDURE dbo.spGetConnectorContext
    @connectorid uniqueidentifier = null,
    @connectorname varchar(255) = null,
    @activeonly bit = 1,
    @includeruntimeconfig bit = 1,
    @includehealingstate bit = 1
as
begin
    set nocount on;

    select
        c.[Id],
        c.[JobName],
        c.[ConnectorName],
        c.[ConnectorType],
        c.[IsActive],
        c.[Level],
        case when @includeruntimeconfig = 1
            then c.[ConfigTemplate]
        end as [ConfigTemplate],
        c.[ConfigId],
        c.[FailedCount],
        c.[FailedConnector],
        c.[FailedTask],
        c.[LastScn],
        c.[LastCommitScn],
        c.[LastFailedAt],
        case
            when @includehealingstate = 1
             and latest_incident.[HasNextStep] = 1
            then latest_incident.[IncidentId]
        end as [ActiveIncidentId],
        latest_log.[EventType] as [LatestEventType],
        latest_log.[AttemptNo] as [LatestAttemptNo],
        latest_log.[HasNextStep] as [LatestHasNextStep],
        latest_log.[Message] as [LatestMessage],
        latest_log.[Details] as [LatestEventDetails],
        latest_log.[CreatedAt] as [LatestEventAt],
        coalesce(attempts.[TaskRestartCount], 0) as [TaskRestartCount],
        coalesce(attempts.[ConnectorRestartCount], 0) as [ConnectorRestartCount],
        coalesce(attempts.[RecreateWithOffsetCount], 0)
            as [RecreateWithOffsetCount],
        coalesce(attempts.[RecreateWithOffsetTimeoutCount], 0)
            as [RecreateWithOffsetTimeoutCount],
        coalesce(attempts.[RecreateWithoutOffsetCount], 0)
            as [RecreateWithoutOffsetCount],
        case when @includeruntimeconfig = 1
            then credential.[ConfiguredValue]
        end as [Credential],
        case when @includeruntimeconfig = 1
            then kafka_server.[ConfiguredValue]
        end as [KafkaServer]
    from [dbo].[Connectors] as c
    outer apply (
        select top (1)
            h.[IncidentId],
            h.[HasNextStep]
        from [dbo].[ConnectorHealingLogs] as h
        where @includehealingstate = 1
          and h.[ConnectorId] = c.[Id]
          and h.[IncidentId] is not null
        order by h.[CreatedAt] desc, h.[Id] desc
    ) as latest_incident
    outer apply (
        select top (1)
            h.[EventType],
            h.[AttemptNo],
            h.[HasNextStep],
            h.[Message],
            h.[Details],
            h.[CreatedAt]
        from [dbo].[ConnectorHealingLogs] as h
        where @includehealingstate = 1
          and latest_incident.[HasNextStep] = 1
          and h.[ConnectorId] = c.[Id]
          and h.[IncidentId] = latest_incident.[IncidentId]
        order by h.[CreatedAt] desc, h.[Id] desc
    ) as latest_log
    outer apply (
        select
            sum(convert(bigint, case
                when h.[EventType] = 'TASK_RESTART' then 1 else 0
            end))
                as [TaskRestartCount],
            sum(convert(bigint, case
                when h.[EventType] = 'CONNECTOR_RESTART' then 1 else 0
            end))
                as [ConnectorRestartCount],
            sum(convert(bigint, case when h.[EventType] in (
                'CONNECTOR_RECREATE_WITH_OFFSET',
                'CONNECTOR_RECREATE_WITH_OFFSET_FAILED'
            ) then 1 else 0 end)) as [RecreateWithOffsetCount],
            sum(convert(bigint, case when h.[EventType] =
                'CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT' then 1 else 0 end))
                as [RecreateWithOffsetTimeoutCount],
            sum(convert(bigint, case when h.[EventType] in (
                'CONNECTOR_RECREATE_WITHOUT_OFFSET',
                'CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED'
            ) then 1 else 0 end)) as [RecreateWithoutOffsetCount]
        from [dbo].[ConnectorHealingLogs] as h
        where @includehealingstate = 1
          and latest_incident.[HasNextStep] = 1
          and h.[ConnectorId] = c.[Id]
          and h.[IncidentId] = latest_incident.[IncidentId]
    ) as attempts
    outer apply (
        select top (1) ec.[ConfiguredValue]
        from ingest_reference.dbo.ETLConfiguration as ec
        where @includeruntimeconfig = 1
          and ec.[ETLConfigurationID] = try_convert(int, c.[ConfigId])
    ) as credential
    outer apply (
        select top (1) ec.[ConfiguredValue]
        from ingest_reference.dbo.ETLConfiguration as ec
        where @includeruntimeconfig = 1
          and ec.[ETLConfigurationID] = 27
    ) as kafka_server
    where (@connectorid is null or c.[Id] = @connectorid)
      and (@connectorname is null or c.[ConnectorName] = @connectorname)
      and (@activeonly = 0 or c.[IsActive] = 1)
    order by c.[ConnectorName];
END;
