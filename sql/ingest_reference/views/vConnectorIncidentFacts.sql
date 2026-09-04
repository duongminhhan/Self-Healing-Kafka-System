CREATE OR ALTER VIEW dbo.vConnectorIncidentFacts
AS
SELECT
    q.[QueueId] AS [IncidentId],
    q.[RootConnectorName] AS [JobName],
    q.[CurrentConnectorName] AS [ConnectorName],
    failure.[CreatedAt] AS [FailureAt],
    q.[CompletedAt] AS [RecoveredAt],
    CASE
        WHEN q.[QueueStatus] IN ('PENDING', 'PROCESSING', 'WAITING') THEN 'OPEN'
        ELSE q.[FinalOutcome]
    END AS [FinalOutcome],
    failure.[EventType] AS [EventType],
    failure.[Severity] AS [Severity],
    CASE WHEN failure.[Message] LIKE '%ORA-[0-9][0-9][0-9][0-9][0-9]%'
        THEN SUBSTRING(failure.[Message], CHARINDEX('ORA-', failure.[Message]), 9)
    END AS [ErrorCode]
FROM [dbo].[ConnectorHealingQueue] AS q
OUTER APPLY (
    SELECT TOP (1) l.[CreatedAt], l.[EventType], l.[Severity], l.[Message]
    FROM [dbo].[ConnectorHealingLogs] AS l
    WHERE l.[QueueId] = q.[QueueId]
      AND l.[EventType] = 'HEALTH_FAILED_CONFIRMED'
    ORDER BY l.[CreatedAt], l.[Id]
) AS failure;
