CREATE OR ALTER PROCEDURE dbo.spSearchConnectorHealingLogs
    @SearchText nvarchar(4000),
    @Limit int = 20
AS
BEGIN
    SET NOCOUNT ON;

    IF @Limit < 1 OR @Limit > 100
        SET @Limit = 20;

    DECLARE @NormalizedSearchText nvarchar(4000) = LOWER(LTRIM(RTRIM(@SearchText)));

    ;WITH SearchTerms AS (
        SELECT DISTINCT
            REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(value)), N'%', N'\%'), N'_', N'\_'), N'[', N'\[') AS [Term]
        FROM STRING_SPLIT(@NormalizedSearchText, N' ')
        WHERE LEN(LTRIM(RTRIM(value))) >= 2
    ),
    RankedLogs AS (
        SELECT
            l.[Id], l.[QueueId], l.[ConnectorName], l.[EventType], l.[AttemptNo],
            l.[HealingStep], l.[Severity], l.[Message], l.[Details], l.[CreatedAt],
            SUM(CASE
                WHEN l.[ConnectorName] LIKE N'%' + t.[Term] + N'%' ESCAPE N'\' THEN 4
                WHEN l.[EventType] LIKE N'%' + t.[Term] + N'%' ESCAPE N'\' THEN 3
                WHEN l.[Message] LIKE N'%' + t.[Term] + N'%' ESCAPE N'\' THEN 2
                WHEN l.[Details] LIKE N'%' + t.[Term] + N'%' ESCAPE N'\' THEN 1
                ELSE 0 END) AS [RelevanceScore]
        FROM [dbo].[ConnectorHealingLogs] AS l
        CROSS JOIN SearchTerms AS t
        GROUP BY
            l.[Id], l.[QueueId], l.[ConnectorName], l.[EventType], l.[AttemptNo],
            l.[HealingStep], l.[Severity], l.[Message], l.[Details], l.[CreatedAt]
    )
    SELECT TOP (@Limit)
        [Id], [QueueId], [ConnectorName], [EventType], [AttemptNo], [HealingStep],
        [Severity], [Message], [Details], [CreatedAt]
    FROM RankedLogs
    WHERE [RelevanceScore] > 0
    ORDER BY [RelevanceScore] DESC, [CreatedAt] DESC, [Id] DESC;
END;
