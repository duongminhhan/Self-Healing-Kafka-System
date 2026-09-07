CREATE OR ALTER PROCEDURE dbo.spInsertConnectorHealingLog
    @queueid uniqueidentifier,
    @connectorname varchar(255),
    @eventtype varchar(80),
    @attemptno int = null,
    @healingstep smallint = null,
    @severity varchar(20) = 'INFO',
    @message nvarchar(max) = null,
    @details nvarchar(max) = N'{}'
as
begin
    set nocount on;
    set xact_abort on;

    -- Serialize confirmation writers on the parent row, even when no log exists.
    -- Other event types intentionally retain their append-only semantics.
    begin transaction;
    if @eventtype = 'HEALTH_FAILED_CONFIRMED'
    begin
        declare @existingqueue uniqueidentifier;
        select @existingqueue = [QueueId]
        from dbo.ConnectorHealingQueue with (updlock, holdlock)
        where [QueueId] = @queueid;
        if @existingqueue is null
        begin
            rollback transaction;
            throw 50002, 'Queue does not exist.', 1;
        end;
        if exists (
            select 1 from dbo.ConnectorHealingLogs
            where [QueueId] = @queueid and [EventType] = 'HEALTH_FAILED_CONFIRMED'
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
END;
