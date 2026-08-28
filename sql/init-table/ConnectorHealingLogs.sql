CREATE TABLE [ingest_reference].[dbo].[ConnectorHealingLogs] (
  [Id] UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
  [QueueId] UNIQUEIDENTIFIER NOT NULL,
  [ConnectorName] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [EventType] VARCHAR(80) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [AttemptNo] INT NULL,
  [HealingStep] SMALLINT NULL,
  [Severity] VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL DEFAULT 'INFO',
  [Message] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
  [Details] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL DEFAULT N'{}',
  [CreatedAt] DATETIMEOFFSET(3) NOT NULL DEFAULT SYSDATETIMEOFFSET(),
  CONSTRAINT [PK_ConnectorHealingLogs] PRIMARY KEY ([Id]),
  CONSTRAINT [FK_ConnectorHealingLogs_Queue]
    FOREIGN KEY ([QueueId])
    REFERENCES [ingest_reference].[dbo].[ConnectorHealingQueue] ([QueueId])
);

CREATE NONCLUSTERED INDEX [ConnectorHealingLogs.IDX_01]
ON [ingest_reference].[dbo].[ConnectorHealingLogs] ([QueueId] ASC, [CreatedAt] DESC)
INCLUDE ([EventType], [HealingStep], [Severity])
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
  [QueueId] ASC,
  [CreatedAt] DESC
) INCLUDE (
  [AttemptNo],
  [Details],
  [EventType],
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
