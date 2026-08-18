SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @ConnectorName VARCHAR(255) = 'OTM_TMS-G007';
DECLARE @ConnectorId UNIQUEIDENTIFIER;
DECLARE @JobName VARCHAR(100);

DECLARE @Topics TABLE (
    [TopicName] VARCHAR(255) NOT NULL PRIMARY KEY
);

INSERT INTO @Topics ([TopicName])
VALUES
    ('OTM_TMS.GLOGOWNER.CURRENCY'),
    ('OTM_TMS.GLOGOWNER.LOCATION'),
    ('OTM_TMS.GLOGOWNER.LOCATION_ADDRESS'),
    ('OTM_TMS.GLOGOWNER.LOCATION_REFNUM'),
    ('OTM_TMS.GLOGOWNER.LOCATION_ROLE_PROFILE'),
    ('OTM_TMS.GLOGOWNER.INVOICE'),
    ('OTM_TMS.GLOGOWNER.INVOICE_STATUS'),
    ('OTM_TMS.GLOGOWNER.LOCATION_PROFILE_DETAIL'),
    ('OTM_TMS.GLOGOWNER.VAT_RATE'),
    ('OTM_TMS.GLOGOWNER.STATUS_VALUE'),
    ('OTM_TMS.GLOGOWNER.SHIP_UNIT_SPEC'),
    ('OTM_TMS.GLOGOWNER.SERVPROV'),
    ('OTM_TMS.GLOGOWNER.OB_SHIP_UNIT'),
    ('OTM_TMS.GLOGOWNER.SHIP_UNIT'),
    ('OTM_TMS.GLOGOWNER.SHIPMENT_COST'),
    ('OTM_TMS.GLOGOWNER.INVOICE_INVOLVED_PARTY'),
    ('OTM_TMS.GLOGOWNER.VAT_ANALYSIS'),
    ('OTM_TMS.GLOGOWNER.INVOICE_LINEITEM'),
    ('OTM_TMS.GLOGOWNER.SHIPMENT_BILL'),
    ('OTM_TMS.GLOGOWNER.SHIPMENT_S_EQUIPMENT_JOIN'),
    ('OTM_TMS.GLOGOWNER.S_EQUIPMENT_S_SHIP_UNIT_JOIN'),
    ('OTM_TMS.GLOGOWNER.S_SHIP_UNIT_LINE'),
    ('OTM_TMS.GLOGOWNER.INVOICE_SHIPMENT'),
    ('OTM_TMS.GLOGOWNER.SHIPMENT_INVOLVED_PARTY'),
    ('OTM_TMS.GLOGOWNER.CURRENCY_EXCHANGE_RATE'),
    ('OTM_TMS.GLOGOWNER.SHIPMENT'),
    ('OTM_TMS.GLOGOWNER.OB_ORDER_BASE'),
    ('OTM_TMS.GLOGOWNER.ORDER_MOVEMENT'),
    ('OTM_TMS.GLOGOWNER.ORDER_RELEASE'),
    ('OTM_TMS.GLOGOWNER.SHIPMENT_REFNUM'),
    ('OTM_TMS.GLOGOWNER.SHIPMENT_STATUS'),
    ('OTM_TMS.GLOGOWNER.ORDER_RELEASE_REFNUM'),
    ('OTM_TMS.GLOGOWNER.SHIPMENT_STOP'),
    ('OTM_TMS.GLOGOWNER.IE_SHIPMENTSTATUS'),
    ('OTM_TMS.GLOGOWNER.ORDER_MOVEMENT_D'),
    ('OTM_TMS.GLOGOWNER.RATE_GEO'),
    ('OTM_TMS.GLOGOWNER.RATE_OFFERING'),
    ('OTM_TMS.GLOGOWNER.OB_REFNUM');

SELECT
    @ConnectorId = [Id],
    @JobName = [JobName]
FROM [dbo].[Connectors]
WHERE [ConnectorName] = @ConnectorName;

IF @ConnectorId IS NULL
    THROW 50001, 'Insert connector OTM_TMS-G007 before creating its topic lag jobs.', 1;

BEGIN TRY
    BEGIN TRANSACTION;

    INSERT INTO [dbo].[TopicLagJobs] (
        [ConnectorId],
        [JobName],
        [TopicName],
        [IsActive]
    )
    SELECT
        @ConnectorId,
        @JobName,
        topics.[TopicName],
        1
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
    INNER JOIN @Topics AS topics
        ON topics.[TopicName] = jobs.[TopicName]
    WHERE jobs.[ConnectorId] = @ConnectorId;

    COMMIT TRANSACTION;

    SELECT
        jobs.[Id],
        jobs.[JobName],
        jobs.[TopicName],
        jobs.[IsActive]
    FROM [dbo].[TopicLagJobs] AS jobs
    WHERE jobs.[ConnectorId] = @ConnectorId
    ORDER BY jobs.[TopicName];
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0
        ROLLBACK TRANSACTION;

    THROW;
END CATCH;
