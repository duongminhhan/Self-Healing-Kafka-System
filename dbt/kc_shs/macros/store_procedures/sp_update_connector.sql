{% macro create_sp_update_connector() %}

    {% set sql %}
        CREATE OR ALTER PROCEDURE dbo.spUpdateConnector
            @connectorid uniqueidentifier,
            @fields nvarchar(max)
        as
        begin
            set nocount on;

            if isjson(@fields) <> 1
                throw 50001, 'fields must be a valid json object.', 1;

            update [dbo].[Connectors]
            set
                [JobName] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'job_name'
                ) then json_value(@fields, '$.job_name') else [JobName] end,
                [ConnectorName] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'connector_name'
                ) then json_value(@fields, '$.connector_name') else [ConnectorName] end,
                [ConnectorType] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'connector_type'
                ) then json_value(@fields, '$.connector_type') else [ConnectorType] end,
                [IsActive] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'is_active'
                ) then case json_value(@fields, '$.is_active')
                    when 'true' then 1 when 'false' then 0
                    else try_convert(bit, json_value(@fields, '$.is_active'))
                end else [IsActive] end,
                [Level] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'level'
                ) then try_convert(smallint, json_value(@fields, '$.level')) else [Level] end,
                [ConfigTemplate] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'config_template'
                ) then json_query(@fields, '$.config_template') else [ConfigTemplate] end,
                [ConfigId] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'config_id'
                ) then json_value(@fields, '$.config_id') else [ConfigId] end,
                [FailedCount] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'failed_count'
                ) then try_convert(int, json_value(@fields, '$.failed_count'))
                    else [FailedCount] end,
                [FailedConnector] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'failed_connector'
                ) then case json_value(@fields, '$.failed_connector')
                    when 'true' then 1 when 'false' then 0
                    else try_convert(bit, json_value(@fields, '$.failed_connector'))
                end else [FailedConnector] end,
                [FailedTask] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'failed_task'
                ) then case json_value(@fields, '$.failed_task')
                    when 'true' then 1 when 'false' then 0
                    else try_convert(bit, json_value(@fields, '$.failed_task'))
                end else [FailedTask] end,
                [LastScn] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'last_scn'
                ) then json_value(@fields, '$.last_scn') else [LastScn] end,
                [LastCommitScn] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'last_commit_scn'
                ) then json_value(@fields, '$.last_commit_scn')
                    else [LastCommitScn] end,
                [LastFailedAt] = case when exists (
                    select 1 from openjson(@fields) where [key] = 'last_failed_at'
                ) then try_convert(
                    datetimeoffset(3),
                    json_value(@fields, '$.last_failed_at'),
                    127
                ) else [LastFailedAt] end
            where [Id] = @connectorid;

            if exists (select 1 from openjson(@fields) where [key] = 'is_active')
               and case json_value(@fields, '$.is_active')
                    when 'true' then 1 when 'false' then 0
                    else try_convert(bit, json_value(@fields, '$.is_active'))
                end = 0
            begin
                update [dbo].[TopicLagJobs]
                set [IsActive] = 0,
                    [IsOverThreshold] = 0,
                    [UpdatedAt] = sysdatetimeoffset()
                where [ConnectorId] = @connectorid
                  and ([IsActive] = 1 or [IsOverThreshold] = 1);
            end;
        END;
    {% endset %}

    {% do run_query(sql) %}
    {% do log("--> Đã tạo/cập nhật dbo.spUpdateConnector", info=True) %}

{% endmacro %}
