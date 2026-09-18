-- Bounded parity validates the public semantic contract rather than the mart's
-- internal snake_case schema. This query is a dbt data test: it must return no
-- rows when the compatibility view matches the legacy incident view.
with legacy_incidents as (
    select top (1001)
        [IncidentId], [JobName], [ConnectorName], [FailureAt], [RecoveredAt],
        [CompletedAt], [QueueStatus], [FinalOutcome], [EventType], [Severity],
        {{ redact_incident_error_message("[ErrorMessage]") }} as [ErrorMessage], [ErrorCode]
    from {{ source("ingest_reference", "connector_incident_facts") }}
    where [EventType] = 'HEALTH_FAILED_CONFIRMED'
    order by [FailureAt] desc, [IncidentId] desc
),

semantic_incidents as (
    select top (1001)
        [IncidentId], [JobName], [ConnectorName], [FailureAt], [RecoveredAt],
        [CompletedAt], [QueueStatus], [FinalOutcome], [EventType], [Severity],
        [ErrorMessage], [ErrorCode]
    from {{ ref("v_semantic_connector_incident_facts") }}
    order by [FailureAt] desc, [IncidentId] desc
),

semantic_only as (
    select * from semantic_incidents
    except
    select * from legacy_incidents
),

legacy_only as (
    select * from legacy_incidents
    except
    select * from semantic_incidents
)

select [IncidentId] from semantic_only
union all
select [IncidentId] from legacy_only
