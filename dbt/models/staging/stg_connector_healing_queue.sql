{{ config(materialized="view") }}

select
    cast([QueueId] as uniqueidentifier) as incident_id,
    cast([RootConnectorName] as varchar(255)) as root_connector_name,
    cast([CurrentConnectorName] as varchar(255)) as current_connector_name,
    cast([ConnectorClass] as varchar(255)) as connector_class,
    cast([HealingMode] as varchar(20)) as healing_mode,
    cast([QueueStatus] as varchar(20)) as queue_status,
    cast([FinalOutcome] as varchar(20)) as final_outcome,
    cast([ReceivedAt] as datetimeoffset(3)) as received_at,
    cast([StartedAt] as datetimeoffset(3)) as started_at,
    cast([CompletedAt] as datetimeoffset(3)) as completed_at,
    cast([NextAttemptAt] as datetimeoffset(3)) as next_attempt_at
from {{ source("ingest_reference", "connector_healing_queue") }}
