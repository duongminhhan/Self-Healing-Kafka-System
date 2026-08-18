CREATE OR ALTER PROCEDURE dbo.spGetOperationalMetrics
    @windowminutes int = 15,
    @metricgroup varchar(40) = null
as
begin
    set nocount on;

    if @metricgroup is not null
       and @metricgroup not in (
            'healing_attempt',
            'healing_recovered',
            'healing_escalated',
            'topic_lag',
            'topic_lag_event'
       )
        throw 50004, 'unsupported operational metric group.', 1;

    declare @metrics table (
        [MetricGroup] varchar(40) not null,
        [ConnectorName] varchar(255) null,
        [TopicName] varchar(255) null,
        [EventStatus] varchar(30) null,
        [Condition] nvarchar(100) null,
        [EventType] varchar(80) null,
        [HealingStep] smallint null,
        [TotalAttempts] bigint null,
        [LastHealingRecoveredTimestampSeconds] bigint null,
        [LastHealingEscalatedTimestampSeconds] bigint null,
        [IsOverThreshold] bit null,
        [LastMessageTimestampSeconds] bigint null,
        [TotalEvents] bigint null
    );

    if @metricgroup is null or @metricgroup = 'healing_attempt'
    begin
        declare @healingattempt table (
            [ConnectorName] varchar(255),
            [EventType] varchar(80),
            [HealingStep] smallint,
            [TotalAttempts] bigint
        );

        insert into @healingattempt
        execute dbo.spGetHealingAttemptMetrics;

        insert into @metrics (
            [MetricGroup],
            [ConnectorName],
            [EventType],
            [HealingStep],
            [TotalAttempts]
        )
        select
            'healing_attempt',
            [ConnectorName],
            [EventType],
            [HealingStep],
            [TotalAttempts]
        from @healingattempt;
    end;

    if @metricgroup is null or @metricgroup = 'healing_recovered'
    begin
        declare @healingrecovered table (
            [ConnectorName] varchar(255),
            [LastHealingRecoveredTimestampSeconds] bigint
        );

        insert into @healingrecovered
        execute dbo.spGetHealingRecoveredMetrics
            @windowminutes = @windowminutes;

        insert into @metrics (
            [MetricGroup],
            [ConnectorName],
            [LastHealingRecoveredTimestampSeconds]
        )
        select
            'healing_recovered',
            [ConnectorName],
            [LastHealingRecoveredTimestampSeconds]
        from @healingrecovered;
    end;

    if @metricgroup is null or @metricgroup = 'healing_escalated'
    begin
        declare @healingescalated table (
            [ConnectorName] varchar(255),
            [EventType] varchar(80),
            [LastHealingEscalatedTimestampSeconds] bigint
        );

        insert into @healingescalated
        execute dbo.spGetHealingEscalatedMetrics
            @windowminutes = @windowminutes;

        insert into @metrics (
            [MetricGroup],
            [ConnectorName],
            [EventType],
            [LastHealingEscalatedTimestampSeconds]
        )
        select
            'healing_escalated',
            [ConnectorName],
            [EventType],
            [LastHealingEscalatedTimestampSeconds]
        from @healingescalated;
    end;

    if @metricgroup is null or @metricgroup = 'topic_lag'
    begin
        declare @topiclagstate table (
            [ConnectorName] varchar(255),
            [TopicName] varchar(255),
            [IsOverThreshold] bit,
            [LastMessageTimestampSeconds] bigint
        );

        insert into @topiclagstate
        execute dbo.spGetTopicLagStateMetrics;

        insert into @metrics (
            [MetricGroup],
            [ConnectorName],
            [TopicName],
            [IsOverThreshold],
            [LastMessageTimestampSeconds]
        )
        select
            'topic_lag',
            [ConnectorName],
            [TopicName],
            [IsOverThreshold],
            [LastMessageTimestampSeconds]
        from @topiclagstate;
    end;

    if @metricgroup is null or @metricgroup = 'topic_lag_event'
    begin
        declare @topiclagevent table (
            [ConnectorName] varchar(255),
            [TopicName] varchar(255),
            [EventStatus] varchar(30),
            [Condition] nvarchar(100),
            [TotalEvents] bigint
        );

        insert into @topiclagevent
        execute dbo.spGetTopicLagEventMetrics
            @windowminutes = @windowminutes;

        insert into @metrics (
            [MetricGroup],
            [ConnectorName],
            [TopicName],
            [EventStatus],
            [Condition],
            [TotalEvents]
        )
        select
            'topic_lag_event',
            [ConnectorName],
            [TopicName],
            [EventStatus],
            [Condition],
            [TotalEvents]
        from @topiclagevent;
    end;

    select
        [MetricGroup],
        [ConnectorName],
        [TopicName],
        [EventStatus],
        [Condition],
        [EventType],
        [HealingStep],
        [TotalAttempts],
        [LastHealingRecoveredTimestampSeconds],
        [LastHealingEscalatedTimestampSeconds],
        [IsOverThreshold],
        [LastMessageTimestampSeconds],
        [TotalEvents]
    from @metrics
    order by
        [MetricGroup],
        [ConnectorName],
        [TopicName],
        [EventStatus],
        [EventType];
END;
