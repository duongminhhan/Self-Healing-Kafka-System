{% macro create_sp_insert_connector_healing_log() %}

    {% set sql %}
        create OR ALTER PROCEDURE dbo.spInsertConnectorHealingLog
            @connectorid uniqueidentifier = null,
            @jobname varchar(255) = null,
            @connectorname varchar(255),
            @eventtype varchar(80),
            @attemptno int = null,
            @incidentid uniqueidentifier = null,
            @healingstep smallint = null,
            @hasnextstep bit,
            @message nvarchar(max),
            @details nvarchar(max)
        as
        begin
            set nocount on;

            insert into [dbo].[ConnectorHealingLogs] (
                [ConnectorId],
                [JobName],
                [ConnectorName],
                [EventType],
                [AttemptNo],
                [IncidentId],
                [HealingStep],
                [HasNextStep],
                [Message],
                [Details]
            )
            values (
                @connectorid,
                @jobname,
                @connectorname,
                @eventtype,
                @attemptno,
                @incidentid,
                @healingstep,
                @hasnextstep,
                @message,
                @details
            );
        END;
    {% endset %}

    {% do run_query(sql) %}
    {% do log("--> Đã tạo/cập nhật dbo.spInsertConnectorHealingLog", info=True) %}

{% endmacro %}
