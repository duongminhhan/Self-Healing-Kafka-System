CREATE TABLE [ingest_reference].[dbo].[ConnectorHealingQueue] (
  [QueueId] UNIQUEIDENTIFIER NOT NULL DEFAULT NEWSEQUENTIALID(),
  [RootConnectorName] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [CurrentConnectorName] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [ConnectorClass] VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
  [HealingMode] VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL,
  [QueueStatus] VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS NOT NULL DEFAULT 'PENDING',
  [FinalOutcome] VARCHAR(20) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
  [ReceivedAt] DATETIMEOFFSET(3) NOT NULL DEFAULT SYSDATETIMEOFFSET(),
  [StartedAt] DATETIMEOFFSET(3) NULL,
  [CompletedAt] DATETIMEOFFSET(3) NULL,
  [NextAttemptAt] DATETIMEOFFSET(3) NULL,
  CONSTRAINT [PK_ConnectorHealingQueue] PRIMARY KEY ([QueueId]),
  CONSTRAINT [CK_ConnectorHealingQueue_HealingMode]
    CHECK ([HealingMode] IN ('RESTART_ONLY', 'RECOVERY')),
  CONSTRAINT [CK_ConnectorHealingQueue_QueueStatus]
    CHECK ([QueueStatus] IN ('PENDING', 'PROCESSING', 'WAITING', 'COMPLETED', 'ESCALATED')),
  CONSTRAINT [CK_ConnectorHealingQueue_FinalOutcome]
    CHECK ([FinalOutcome] IS NULL OR [FinalOutcome] IN ('RECOVERED', 'FAILED', 'ESCALATED'))
);

CREATE UNIQUE NONCLUSTERED INDEX [UX_ConnectorHealingQueue_OpenRoot]
ON [ingest_reference].[dbo].[ConnectorHealingQueue] ([RootConnectorName])
WHERE [QueueStatus] IN ('PENDING', 'PROCESSING', 'WAITING');

CREATE NONCLUSTERED INDEX [IX_ConnectorHealingQueue_Ready]
ON [ingest_reference].[dbo].[ConnectorHealingQueue] ([QueueStatus], [NextAttemptAt], [ReceivedAt])
INCLUDE ([CurrentConnectorName], [HealingMode], [ConnectorClass]);
