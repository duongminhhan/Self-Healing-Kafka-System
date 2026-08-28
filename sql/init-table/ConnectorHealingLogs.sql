CREATE TABLE [ingest_reference].[dbo].[ConnectorHealingLogs] (
  [Id] UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
  [ConnectorId] UNIQUEIDENTIFIER NULL,
  [JobName] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
  [ConnectorName] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [EventType] VARCHAR(80) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [AttemptNo] INT NULL,
  [IncidentId] UNIQUEIDENTIFIER NULL,
  [HealingStep] SMALLINT NULL,
  [HasNextStep] BIT NOT NULL DEFAULT 1,
  [Message] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
  [Details] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL DEFAULT N'{}',
  [CreatedAt] DATETIMEOFFSET(3) NOT NULL DEFAULT SYSDATETIMEOFFSET(),
  [FinishedAt] DATETIMEOFFSET(3) NULL,
  CONSTRAINT [PK_ConnectorHealingLogs] PRIMARY KEY ([Id])
);

CREATE NONCLUSTERED INDEX [ConnectorHealingLogs.IDX_01]
ON [ingest_reference].[dbo].[ConnectorHealingLogs] ([ConnectorId] ASC, [CreatedAt] DESC)
INCLUDE ([HasNextStep], [IncidentId])
WITH
  (
    PAD_INDEX = OFF,
    FILLFACTOR = 100,
    SORT_IN_TEMPDB = OFF,
    IGNORE_DUP_KEY = OFF,
    STATISTICS_NORECOMPUTE = OFF,
    ONLINE = OFF,
    ALLOW_ROW_LOCKS = ON,
    ALLOW_PAGE_LOCKS = ON
  ) ON [PRIMARY];
CREATE NONCLUSTERED INDEX [ConnectorHealingLogs.IDX_02]
ON [ingest_reference].[dbo].[ConnectorHealingLogs] (
  [ConnectorId] ASC,
  [IncidentId] ASC,
  [CreatedAt] DESC
) INCLUDE (
  [AttemptNo],
  [Details],
  [EventType],
  [HasNextStep],
  [Message]
)
WITH
  (
    PAD_INDEX = OFF,
    FILLFACTOR = 100,
    SORT_IN_TEMPDB = OFF,
    IGNORE_DUP_KEY = OFF,
    STATISTICS_NORECOMPUTE = OFF,
    ONLINE = OFF,
    ALLOW_ROW_LOCKS = ON,
    ALLOW_PAGE_LOCKS = ON
  ) ON [PRIMARY];
