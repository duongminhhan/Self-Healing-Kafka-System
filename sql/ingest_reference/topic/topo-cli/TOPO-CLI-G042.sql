SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @ConnectorName VARCHAR(255) = 'TOPO-CLI-G042';
DECLARE @ConnectorId UNIQUEIDENTIFIER;

SELECT @ConnectorId = [Id]
FROM [dbo].[Connectors]
WHERE [ConnectorName] = @ConnectorName;

IF @ConnectorId IS NULL
    THROW 50001, 'Insert connector TOPO-CLI-G042 before updating its topic lag jobs.', 1;

BEGIN TRY
    BEGIN TRANSACTION;

    UPDATE [dbo].[TopicLagJobs]
    SET
        [IsActive] = 0,
        [IsOverThreshold] = 0,
        [UpdatedAt] = SYSDATETIMEOFFSET()
    WHERE [ConnectorId] = @ConnectorId
      AND ([IsActive] = 1 OR [IsOverThreshold] = 1);

    COMMIT TRANSACTION;

    SELECT [Id], [JobName], [TopicName], [IsActive], [IsOverThreshold]
    FROM [dbo].[TopicLagJobs]
    WHERE [ConnectorId] = @ConnectorId
    ORDER BY [TopicName];
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
