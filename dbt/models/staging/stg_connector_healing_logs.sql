{{ config(materialized="view") }}

select
    cast([Id] as uniqueidentifier) as log_id,
    cast([QueueId] as uniqueidentifier) as incident_id,
    cast([ConnectorName] as varchar(255)) as connector_name,
    cast([EventType] as varchar(80)) as event_type,
    cast([AttemptNo] as int) as attempt_number,
    cast([HealingStep] as smallint) as healing_step,
    cast([Severity] as varchar(20)) as severity,
    cast([Message] as nvarchar(max)) as failure_message,
    cast([CreatedAt] as datetimeoffset(3)) as created_at
from {{ source("ingest_reference", "connector_healing_logs") }}
