CREATE OR ALTER PROCEDURE dbo.spUpdateConnectorHealingQueue
    @queueid uniqueidentifier,
    @fields nvarchar(max)
as
begin
    set nocount on;

    if isjson(@fields) <> 1
        throw 50001, 'fields must be a valid json object.', 1;

    update [dbo].[ConnectorHealingQueue]
    set
        [CurrentConnectorName] = case when exists (
            select 1 from openjson(@fields) where [key] = 'current_connector_name'
        ) then json_value(@fields, '$.current_connector_name') else [CurrentConnectorName] end,
        [QueueStatus] = case when exists (
            select 1 from openjson(@fields) where [key] = 'queue_status'
        ) then json_value(@fields, '$.queue_status') else [QueueStatus] end,
        [FinalOutcome] = case when exists (
            select 1 from openjson(@fields) where [key] = 'final_outcome'
        ) then json_value(@fields, '$.final_outcome') else [FinalOutcome] end,
        [StartedAt] = case when exists (
            select 1 from openjson(@fields) where [key] = 'started_at'
        ) then coalesce([StartedAt], try_convert(
            datetimeoffset(3),
            json_value(@fields, '$.started_at'),
            127
        ))
            else [StartedAt] end,
        [CompletedAt] = case when exists (
            select 1 from openjson(@fields) where [key] = 'completed_at'
        ) then try_convert(datetimeoffset(3), json_value(@fields, '$.completed_at'), 127)
            else [CompletedAt] end,
        [NextAttemptAt] = case when exists (
            select 1 from openjson(@fields) where [key] = 'next_attempt_at'
        ) then try_convert(datetimeoffset(3), json_value(@fields, '$.next_attempt_at'), 127)
            else [NextAttemptAt] end
    where [QueueId] = @queueid;
END;
