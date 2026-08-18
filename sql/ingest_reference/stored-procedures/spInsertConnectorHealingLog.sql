CREATE OR ALTER PROCEDURE dbo.spInsertConnectorHealingLog
    @connectorid uniqueidentifier = null,
    @jobname varchar(255) = null,
    @connectorname varchar(255),
    @eventtype varchar(80),
    @attemptno int = null,
    @incidentid uniqueidentifier = null,
    @healingstep smallint = null,
    @hasnextstep bit,
    @message nvarchar(max),
    @details nvarchar(max)
as
begin
    set nocount on;

    insert into [dbo].[ConnectorHealingLogs] (
        [ConnectorId],
        [JobName],
        [ConnectorName],
        [EventType],
        [AttemptNo],
        [IncidentId],
        [HealingStep],
        [HasNextStep],
        [Message],
        [Details]
    )
    values (
        @connectorid,
        @jobname,
        @connectorname,
        @eventtype,
        @attemptno,
        @incidentid,
        @healingstep,
        @hasnextstep,
        @message,
        @details
    );
END;
