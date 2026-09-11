-- Manual deployment bundle for healing correctness fixes.
-- Apply to the intended database with workers stopped, BEFORE deploying Python.
-- Canonical sources are named below. No data/schema-table mutation; no GO needed.

-- Source: sql/ingest_reference/stored-procedures/spInsertConnectorHealingLog.sql
EXEC(N'CREATE OR ALTER PROCEDURE dbo.spInsertConnectorHealingLog
    @queueid uniqueidentifier,
    @connectorname varchar(255),
    @eventtype varchar(80),
    @attemptno int = null,
    @healingstep smallint = null,
    @severity varchar(20) = ''INFO'',
    @message nvarchar(max) = null,
    @details nvarchar(max) = N''{}''
as
begin
    set nocount on;
    set xact_abort on;

    -- Serialize confirmation writers on the parent row, even when no log exists.
    -- Other event types intentionally retain their append-only semantics.
    begin transaction;
    if @eventtype = ''HEALTH_FAILED_CONFIRMED''
    begin
        declare @existingqueue uniqueidentifier;
        select @existingqueue = [QueueId]
        from dbo.ConnectorHealingQueue with (updlock, holdlock)
        where [QueueId] = @queueid;
        if @existingqueue is null
        begin
            rollback transaction;
            throw 50002, ''Queue does not exist.'', 1;
        end;
        if exists (
            select 1 from dbo.ConnectorHealingLogs
            where [QueueId] = @queueid and [EventType] = ''HEALTH_FAILED_CONFIRMED''
        )
        begin
            commit transaction;
            return;
        end;
    end;

    insert into [dbo].[ConnectorHealingLogs] (
        [QueueId],
        [ConnectorName],
        [EventType],
        [AttemptNo],
        [HealingStep],
        [Severity],
        [Message],
        [Details]
    )
    values (
        @queueid,
        @connectorname,
        @eventtype,
        @attemptno,
        @healingstep,
        @severity,
        @message,
        @details
    );
    commit transaction;
END;');

-- Source: sql/ingest_reference/stored-procedures/spGetConnectorHealingQueue.sql
EXEC(N'CREATE OR ALTER PROCEDURE dbo.spGetConnectorHealingQueue
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
        case when q.[HealingMode] = ''RECOVERY'' then 4 else 2 end as [Level],
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
        case when q.[QueueStatus] in (''PENDING'', ''PROCESSING'', ''WAITING'')
            then cast(1 as bit) else cast(0 as bit) end as [LatestHasNextStep],
        coalesce(counts.[FailedCount], 0) as [FailedCount],
        cast(case when counts.[ConfirmationCount] > 0 then 1 else 0 end as bit)
            as [FailureConfirmed],
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
            sum(case when l.[EventType] = ''HEALTH_FAILED_CONFIRMED'' then 1 else 0 end)
                as [ConfirmationCount],
            sum(case when l.[EventType] = ''HEALTH_FAILURE_OBSERVED'' then 1 else 0 end)
                as [FailedCount],
            sum(case when l.[EventType] = ''TASK_RESTART'' then 1 else 0 end)
                as [TaskRestartCount],
            sum(case when l.[EventType] = ''CONNECTOR_RESTART'' then 1 else 0 end)
                as [ConnectorRestartCount],
            sum(case when l.[EventType] in (
                ''CONNECTOR_RECREATE_WITH_OFFSET'',
                ''CONNECTOR_RECREATE_WITH_OFFSET_FAILED''
            ) then 1 else 0 end) as [RecreateWithOffsetCount],
            sum(case when l.[EventType] = ''CONNECTOR_RECREATE_WITH_OFFSET_TIMEOUT''
                then 1 else 0 end) as [RecreateWithOffsetTimeoutCount],
            sum(case when l.[EventType] in (
                ''CONNECTOR_RECREATE_WITHOUT_OFFSET'',
                ''CONNECTOR_RECREATE_WITHOUT_OFFSET_FAILED''
            ) then 1 else 0 end) as [RecreateWithoutOffsetCount]
        from [dbo].[ConnectorHealingLogs] as l
        where l.[QueueId] = q.[QueueId]
    ) as counts
    where (@queueid is null or q.[QueueId] = @queueid)
      and (@connectorname is null or q.[CurrentConnectorName] = @connectorname
           or q.[RootConnectorName] = @connectorname)
      and (@openonly = 0 or q.[QueueStatus] in (''PENDING'', ''PROCESSING'', ''WAITING''))
      and (
          @dueonly = 0
          or q.[NextAttemptAt] is null
          or q.[NextAttemptAt] <= sysdatetimeoffset()
      )
    order by q.[ReceivedAt];
END;');

-- Source: sql/ingest_reference/views/vConnectorIncidentFacts.sql
EXEC(N'CREATE OR ALTER VIEW dbo.vConnectorIncidentFacts
AS
SELECT
    q.[QueueId] AS [IncidentId],
    q.[RootConnectorName] AS [JobName],
    q.[CurrentConnectorName] AS [ConnectorName],
    failure.[CreatedAt] AS [FailureAt],
    q.[CompletedAt] AS [CompletedAt],
    q.[QueueStatus] AS [QueueStatus],
    CASE WHEN q.[QueueStatus] = ''COMPLETED'' AND q.[FinalOutcome] = ''RECOVERED''
        THEN q.[CompletedAt] END AS [RecoveredAt],
    CASE
        WHEN q.[QueueStatus] IN (''PENDING'', ''PROCESSING'', ''WAITING'') THEN ''OPEN''
        ELSE q.[FinalOutcome]
    END AS [FinalOutcome],
    failure.[EventType] AS [EventType],
    failure.[Severity] AS [Severity],
    failure.[Message] AS [ErrorMessage],
    CASE WHEN failure.[Message] LIKE ''%ORA-[0-9][0-9][0-9][0-9][0-9]%''
        THEN SUBSTRING(failure.[Message], CHARINDEX(''ORA-'', failure.[Message]), 9)
    END AS [ErrorCode]
FROM [dbo].[ConnectorHealingQueue] AS q
OUTER APPLY (
    SELECT TOP (1) l.[CreatedAt], l.[EventType], l.[Severity], l.[Message]
    FROM [dbo].[ConnectorHealingLogs] AS l
    WHERE l.[QueueId] = q.[QueueId]
      AND l.[EventType] = ''HEALTH_FAILED_CONFIRMED''
    ORDER BY l.[CreatedAt], l.[Id]
) AS failure;');

-- Source: sql/ingest_reference/stored-procedures/spGetConnectorIncidentFacts.sql
EXEC(N'CREATE OR ALTER PROCEDURE dbo.spGetConnectorIncidentFacts
    @FromAt datetimeoffset(3) = NULL,
    @ToAt datetimeoffset(3) = NULL,
    @EventType varchar(80) = NULL,
    @FinalOutcome varchar(20) = NULL,
    @ConnectorName varchar(255) = NULL,
    @ErrorCode varchar(20) = NULL,
    @Limit int = 100
AS
BEGIN
    SET NOCOUNT ON;

    IF @Limit < 1 OR @Limit > 1001
        SET @Limit = 1001;

    SELECT TOP (@Limit)
        [IncidentId], [JobName], [ConnectorName], [FailureAt], [RecoveredAt],
        [FinalOutcome], [EventType], [Severity], [ErrorCode], [ErrorMessage],
        [CompletedAt], [QueueStatus]
    FROM [dbo].[vConnectorIncidentFacts]
    WHERE (@FromAt IS NULL OR [FailureAt] >= @FromAt)
      AND (@ToAt IS NULL OR [FailureAt] < @ToAt)
      AND (@EventType IS NULL OR [EventType] = @EventType)
      AND (@FinalOutcome IS NULL OR [FinalOutcome] = @FinalOutcome)
      AND (@ConnectorName IS NULL OR [JobName] = @ConnectorName OR [ConnectorName] = @ConnectorName)
      AND (@ErrorCode IS NULL OR [ErrorCode] = @ErrorCode)
    ORDER BY [FailureAt] DESC, [IncidentId] DESC;
END;');
