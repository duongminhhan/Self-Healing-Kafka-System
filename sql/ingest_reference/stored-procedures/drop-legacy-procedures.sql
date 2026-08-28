SET NOCOUNT ON;
SET XACT_ABORT ON;

BEGIN TRY
    BEGIN TRANSACTION;

    DROP PROCEDURE IF EXISTS dbo.spGetConnectors;
    DROP PROCEDURE IF EXISTS dbo.spGetConnectorActivity;
    DROP PROCEDURE IF EXISTS dbo.spGetConnectorByName;
    DROP PROCEDURE IF EXISTS dbo.spGetConnectorById;
    DROP PROCEDURE IF EXISTS dbo.spGetLatestConnectorIncident;
    DROP PROCEDURE IF EXISTS dbo.spGetLatestConnectorHealingLog;
    DROP PROCEDURE IF EXISTS dbo.spGetConnectorHealingAttemptCounts;
    DROP PROCEDURE IF EXISTS dbo.spGetTopicLagMetrics;
    DROP PROCEDURE IF EXISTS dbo.spGetActiveTopicLagJobs;
    DROP PROCEDURE IF EXISTS dbo.spUpdateTopicLagJobs;
    DROP PROCEDURE IF EXISTS dbo.spGetCredentialConnector;
    DROP PROCEDURE IF EXISTS dbo.spGetKafkaServer;
    DROP PROCEDURE IF EXISTS dbo.spGetOperationalMetrics;
    DROP PROCEDURE IF EXISTS dbo.spGetHealingAttemptMetrics;
    DROP PROCEDURE IF EXISTS dbo.spGetHealingRecoveredMetrics;
    DROP PROCEDURE IF EXISTS dbo.spGetHealingEscalatedMetrics;
    DROP PROCEDURE IF EXISTS dbo.spGetTopicLagStateMetrics;
    DROP PROCEDURE IF EXISTS dbo.spGetTopicLagEventMetrics;
    DROP PROCEDURE IF EXISTS dbo.spGetMonitoredTopics;
    DROP PROCEDURE IF EXISTS dbo.spUpdateTopicLagState;
    DROP PROCEDURE IF EXISTS dbo.spInsertTopicLagLog;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;
