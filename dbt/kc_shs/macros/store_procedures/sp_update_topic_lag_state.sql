{% macro create_sp_update_topic_lag_state() %}

    {% set sql %}
        CREATE OR ALTER PROCEDURE dbo.spUpdateTopicLagState
            @topiclagjobid uniqueidentifier = null,
            @lastendoffset bigint = null,
            @lastmessageat datetimeoffset(3) = null,
            @isoverthreshold bit = null,
            @connectorid uniqueidentifier = null,
            @resetconnectortopics bit = 0
        as
        begin
            set nocount on;

            if @resetconnectortopics = 1
            begin
                if @connectorid is null
                    throw 50002, 'connectorid is required when resetting topic lag state.', 1;

                update [dbo].[TopicLagJobs]
                set [IsOverThreshold] = 0,
                    [LastEndOffset] = null,
                    [LastMessageAt] = null,
                    [UpdatedAt] = sysdatetimeoffset()
                where [ConnectorId] = @connectorid;

                return;
            end;

            if @topiclagjobid is null or @isoverthreshold is null
                throw 50003, 'topiclagjobid and isoverthreshold are required.', 1;

            update [dbo].[TopicLagJobs]
            set [LastEndOffset] = @lastendoffset,
                [LastMessageAt] = @lastmessageat,
                [IsOverThreshold] = @isoverthreshold,
                [UpdatedAt] = sysdatetimeoffset()
            where [Id] = @topiclagjobid;
        END;
    {% endset %}

    {% do run_query(sql) %}
    {% do log("--> Đã tạo/cập nhật dbo.spUpdateTopicLagState", info=True) %}

{% endmacro %}
