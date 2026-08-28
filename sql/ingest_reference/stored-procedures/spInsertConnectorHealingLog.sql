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
END;
