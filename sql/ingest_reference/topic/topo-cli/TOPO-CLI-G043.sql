SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @ConnectorName VARCHAR(255) = 'TOPO-CLI-G038';
DECLARE @ConnectorId UNIQUEIDENTIFIER;
DECLARE @JobName VARCHAR(100);

DECLARE @Topics TABLE (
    [TopicName] VARCHAR(255) NOT NULL PRIMARY KEY
);

INSERT INTO @Topics ([TopicName])
VALUES
    ('CDC.TOPO-CLI.TOPOVN.DISCHARGE_LIST');

SELECT
    @ConnectorId = [Id],
    @JobName = [JobName]
FROM [dbo].[Connectors]
WHERE [ConnectorName] = @ConnectorName;

IF @ConnectorId IS NULL
    THROW 50001, 'Insert connector TOPO-CLI-G038 before creating its topic lag jobs.', 1;

BEGIN TRY
    BEGIN TRANSACTION;

    INSERT INTO [dbo].[TopicLagJobs] ([ConnectorId], [JobName], [TopicName], [IsActive])
    SELECT @ConnectorId, @JobName, topics.[TopicName], 1
    FROM @Topics AS topics
    WHERE NOT EXISTS (
        SELECT 1
        FROM [dbo].[TopicLagJobs] AS existing
        WHERE existing.[ConnectorId] = @ConnectorId
          AND existing.[TopicName] = topics.[TopicName]
    );

    UPDATE jobs
    SET
        jobs.[JobName] = @JobName,
        jobs.[IsActive] = 1,
        jobs.[UpdatedAt] = SYSDATETIMEOFFSET()
    FROM [dbo].[TopicLagJobs] AS jobs
    INNER JOIN @Topics AS topics ON topics.[TopicName] = jobs.[TopicName]
    WHERE jobs.[ConnectorId] = @ConnectorId;

    COMMIT TRANSACTION;

    SELECT [Id], [JobName], [TopicName], [IsActive]
    FROM [dbo].[TopicLagJobs]
    WHERE [ConnectorId] = @ConnectorId
    ORDER BY [TopicName];
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
