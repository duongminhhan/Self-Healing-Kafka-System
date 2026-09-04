CREATE OR ALTER PROCEDURE dbo.spGetConnectorIncidentFacts
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

    IF @Limit < 1 OR @Limit > 100
        SET @Limit = 100;

    SELECT TOP (@Limit)
        [IncidentId], [JobName], [ConnectorName], [FailureAt], [RecoveredAt],
        [FinalOutcome], [EventType], [Severity], [ErrorCode]
    FROM [dbo].[vConnectorIncidentFacts]
    WHERE (@FromAt IS NULL OR [FailureAt] >= @FromAt)
      AND (@ToAt IS NULL OR [FailureAt] < @ToAt)
      AND (@EventType IS NULL OR [EventType] = @EventType)
      AND (@FinalOutcome IS NULL OR [FinalOutcome] = @FinalOutcome)
      AND (@ConnectorName IS NULL OR [JobName] = @ConnectorName OR [ConnectorName] = @ConnectorName)
      AND (@ErrorCode IS NULL OR [ErrorCode] = @ErrorCode)
    ORDER BY [FailureAt] DESC, [IncidentId] DESC;
END;
