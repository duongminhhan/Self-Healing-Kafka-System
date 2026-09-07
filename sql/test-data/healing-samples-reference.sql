-- Read-only reference queries for the generated local MSSQL sample dataset.
-- Run against ingest_reference. No snapshot refresh or data mutation.
SELECT QueueStatus, COUNT(*) AS incident_count
FROM dbo.ConnectorHealingQueue
GROUP BY QueueStatus ORDER BY QueueStatus;

-- Log volume is NOT the number of failures or incidents.
SELECT q.RootConnectorName, COUNT(l.Id) AS log_count
FROM dbo.ConnectorHealingQueue AS q
LEFT JOIN dbo.ConnectorHealingLogs AS l ON l.QueueId = q.QueueId
GROUP BY q.RootConnectorName
ORDER BY log_count DESC, q.RootConnectorName;

SELECT RootConnectorName, COUNT(*) AS incident_count
FROM dbo.ConnectorHealingQueue
GROUP BY RootConnectorName
ORDER BY incident_count DESC, RootConnectorName;

-- A confirmed failure is counted once per incident, not once per log.
SELECT q.RootConnectorName, COUNT(*) AS confirmed_incident_count
FROM dbo.ConnectorHealingQueue AS q
WHERE EXISTS (
    SELECT 1 FROM dbo.ConnectorHealingLogs AS l
    WHERE l.QueueId = q.QueueId AND l.EventType = 'HEALTH_FAILED_CONFIRMED'
)
GROUP BY q.RootConnectorName
ORDER BY confirmed_incident_count DESC, q.RootConnectorName;

-- Successful end-to-end duration; invalid/missing durations remain excluded,
-- and an empty valid population yields NULL rather than a fabricated zero.
SELECT COUNT(*) AS successful_incidents,
       COUNT(CASE WHEN CompletedAt >= ReceivedAt THEN 1 END) AS valid_durations,
       COUNT(*) - COUNT(CASE WHEN CompletedAt >= ReceivedAt THEN 1 END) AS excluded_durations,
       AVG(CASE WHEN CompletedAt >= ReceivedAt
                THEN DATEDIFF_BIG(MILLISECOND, ReceivedAt, CompletedAt) / 60000.0
           END) AS average_minutes
FROM dbo.ConnectorHealingQueue
WHERE QueueStatus = 'COMPLETED' AND FinalOutcome = 'RECOVERED';
