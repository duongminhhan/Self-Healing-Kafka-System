{% macro create_sp_insert_topic_lag_log() %}

    {% set sql %}
        CREATE OR ALTER PROCEDURE dbo.spInsertTopicLagLog
            @topiclagjobid uniqueidentifier = null,
            @connectorid uniqueidentifier = null,
            @jobname varchar(255),
            @connectorname varchar(255) = null,
            @topicname varchar(255),
            @endoffset bigint = null,
            @eventstatus varchar(30),
            @message nvarchar(max),
            @details nvarchar(max)
        as
        begin
            set nocount on;

            insert into [dbo].[TopicLagLogs] (
                [TopicLagJobId],
                [ConnectorId],
                [JobName],
                [ConnectorName],
                [TopicName],
                [EndOffset],
                [EventStatus],
                [Message],
                [Details]
            )
            values (
                @topiclagjobid,
                @connectorid,
                @jobname,
                @connectorname,
                @topicname,
                @endoffset,
                @eventstatus,
                @message,
                @details
            );
        END;
    {% endset %}

    {% do run_query(sql) %}
    {% do log("--> Đã tạo/cập nhật dbo.spInsertTopicLagLog", info=True) %}

{% endmacro %}
