CREATE TABLE [ingest_reference].[dbo].[Connectors] (
  [Id] UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
  [JobName] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
  [ConnectorName] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [ConnectorType] VARCHAR(50) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [IsActive] BIT NOT NULL DEFAULT 1,
  [Level] SMALLINT NOT NULL,
  [ConfigTemplate] NVARCHAR(MAX) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL DEFAULT N'{}',
  [ConfigId] VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
  [FailedCount] INT NOT NULL DEFAULT 0,
  [FailedConnector] BIT NOT NULL DEFAULT 0,
  [FailedTask] BIT NOT NULL DEFAULT 0,
  [LastScn] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
  [LastCommitScn] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
  [LastFailedAt] DATETIMEOFFSET(3) NULL,
  CONSTRAINT [PK_Connectors] PRIMARY KEY ([Id])
);

CREATE NONCLUSTERED INDEX [Connectors.IDX_01]
ON [ingest_reference].[dbo].[Connectors] ([IsActive] ASC, [ConnectorName] ASC)
INCLUDE (
  [JobName],
  [ConnectorType],
  [Level],
  [ConfigId],
  [FailedCount],
  [FailedConnector],
  [FailedTask],
  [LastScn],
  [LastCommitScn],
  [LastFailedAt]
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
