CREATE OR ALTER PROCEDURE dbo.spGetConnectorHealingLogs
    @queueid uniqueidentifier = null,
    @connectorname varchar(255) = null,
    @fromat datetimeoffset(3) = null,
    @toat datetimeoffset(3) = null,
    @limit int = 100
as
begin
    set nocount on;

    if @limit < 1 or @limit > 500
        set @limit = 100;

    select top (@limit)
        [Id],
        [QueueId],
        [ConnectorName],
        [EventType],
        [AttemptNo],
        [HealingStep],
        [Severity],
        [Message],
        [Details],
        [CreatedAt]
    from [dbo].[ConnectorHealingLogs]
    where (@queueid is null or [QueueId] = @queueid)
      and (@connectorname is null or [ConnectorName] = @connectorname)
      and (@fromat is null or [CreatedAt] >= @fromat)
      and (@toat is null or [CreatedAt] <= @toat)
    order by [CreatedAt] desc, [Id] desc;
end;
