CREATE TABLE [ingest_reference].[dbo].[TopicLagJobs] (
  [Id] UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
  [JobName] VARCHAR(100) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [ConnectorId] UNIQUEIDENTIFIER NOT NULL,
  [TopicName] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [IsActive] BIT NOT NULL DEFAULT 1,
  [IsOverThreshold] BIT NOT NULL DEFAULT 0,
  [LastEndOffset] BIGINT NULL,
  [LastMessageAt] DATETIMEOFFSET(3) NULL,
  [CreatedAt] DATETIMEOFFSET(3) NOT NULL DEFAULT SYSDATETIMEOFFSET(),
  [UpdatedAt] DATETIMEOFFSET(3) NOT NULL DEFAULT SYSDATETIMEOFFSET(),
  CONSTRAINT [PK_TopicLagJobs] PRIMARY KEY ([Id]),
  CONSTRAINT [UQ_TopicLagJobs_ConnectorId_TopicName] UNIQUE ([ConnectorId], [TopicName])
);

CREATE NONCLUSTERED INDEX [TopicLagJobs.IDX_01]
ON [ingest_reference].[dbo].[TopicLagJobs] (
  [IsActive] ASC,
  [ConnectorId] ASC,
  [CreatedAt] ASC,
  [TopicName] ASC
) INCLUDE (
  [IsOverThreshold],
  [JobName],
  [LastEndOffset],
  [LastMessageAt]
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

CREATE NONCLUSTERED INDEX [TopicLagJobs.IDX_02]
ON [ingest_reference].[dbo].[TopicLagJobs] ([ConnectorId] ASC, [IsOverThreshold] ASC)
INCLUDE ([JobName], [LastEndOffset], [TopicName])
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
