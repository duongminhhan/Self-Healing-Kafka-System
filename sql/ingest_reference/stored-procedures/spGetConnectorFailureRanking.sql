CREATE OR ALTER PROCEDURE dbo.spGetConnectorFailureRanking
    @fromat datetimeoffset(3) = null,
    @toat datetimeoffset(3) = null,
    @limit int = 10
as
begin
    set nocount on;

    if @limit < 1 or @limit > 100
        set @limit = 10;

    select top (@limit)
        q.[RootConnectorName],
        count(*) as [FailureIncidentCount],
        sum(case when q.[QueueStatus] in ('PENDING', 'PROCESSING', 'WAITING')
            then 1 else 0 end) as [OpenIncidentCount],
        sum(case when q.[FinalOutcome] = 'ESCALATED' then 1 else 0 end)
            as [EscalatedIncidentCount],
        sum(case when q.[FinalOutcome] = 'FAILED' then 1 else 0 end)
            as [FailedIncidentCount],
        sum(case when q.[FinalOutcome] = 'RECOVERED' then 1 else 0 end)
            as [RecoveredIncidentCount],
        max(q.[ReceivedAt]) as [LastFailureAt]
    from [dbo].[ConnectorHealingQueue] as q
    where (@fromat is null or q.[ReceivedAt] >= @fromat)
      and (@toat is null or q.[ReceivedAt] <= @toat)
    group by q.[RootConnectorName]
    order by
        count(*) desc,
        sum(case when q.[QueueStatus] in ('PENDING', 'PROCESSING', 'WAITING')
            then 1 else 0 end) desc,
        max(q.[ReceivedAt]) desc,
        q.[RootConnectorName];
end;
