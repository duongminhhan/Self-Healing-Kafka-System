CREATE OR ALTER PROCEDURE dbo.spEnqueueConnectorHealing
    @rootconnectorname varchar(255),
    @currentconnectorname varchar(255),
    @connectorclass varchar(255) = null,
    @healingmode varchar(20) = 'RESTART_ONLY'
as
begin
    set nocount on;
    set xact_abort on;

    begin transaction;

    declare @queueid uniqueidentifier;

    select @queueid = [QueueId]
    from [dbo].[ConnectorHealingQueue] with (updlock, holdlock)
    where [RootConnectorName] = @rootconnectorname
      and [QueueStatus] in ('PENDING', 'PROCESSING', 'WAITING');

    if @queueid is null
    begin
        set @queueid = newid();

        insert into [dbo].[ConnectorHealingQueue] (
            [QueueId],
            [RootConnectorName],
            [CurrentConnectorName],
            [ConnectorClass],
            [HealingMode],
            [QueueStatus]
        )
        values (
            @queueid,
            @rootconnectorname,
            @currentconnectorname,
            @connectorclass,
            @healingmode,
            'PENDING'
        );
    end;

    commit transaction;

    select
        [QueueId] as [Id],
        [QueueId] as [ActiveIncidentId],
        [RootConnectorName],
        [CurrentConnectorName] as [ConnectorName],
        [ConnectorClass],
        [HealingMode],
        [QueueStatus],
        [FinalOutcome],
        [ReceivedAt],
        [StartedAt],
        [CompletedAt],
        [NextAttemptAt]
    from [dbo].[ConnectorHealingQueue]
    where [QueueId] = @queueid;
END;
