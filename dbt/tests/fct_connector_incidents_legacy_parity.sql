with legacy_incidents as (
    select top (1001)
        [IncidentId] as incident_id,
        [JobName] as root_connector_name,
        [ConnectorName] as current_connector_name,
        [FailureAt] as failure_at,
        [RecoveredAt] as recovered_at,
        [CompletedAt] as completed_at,
        [QueueStatus] as queue_status,
        [FinalOutcome] as final_outcome,
        [EventType] as event_type,
        [Severity] as severity,
        [ErrorMessage] as error_message,
        [ErrorCode] as error_code
    from {{ source("ingest_reference", "connector_incident_facts") }}
    where [EventType] = 'HEALTH_FAILED_CONFIRMED'
    order by [FailureAt] desc, [IncidentId] desc
),

mart_incidents as (
    select top (1001)
        incident_id,
        root_connector_name,
        current_connector_name,
        failure_at,
        recovered_at,
        completed_at,
        queue_status,
        final_outcome,
        event_type,
        severity,
        error_message,
        error_code
    from {{ ref("fct_connector_incidents") }}
    order by failure_at desc, incident_id desc
),

mart_only as (
    select
        incident_id,
        root_connector_name,
        current_connector_name,
        failure_at,
        recovered_at,
        completed_at,
        queue_status,
        final_outcome,
        event_type,
        severity,
        error_message,
        error_code
    from mart_incidents
    except
    select
        incident_id,
        root_connector_name,
        current_connector_name,
        failure_at,
        recovered_at,
        completed_at,
        queue_status,
        final_outcome,
        event_type,
        severity,
        error_message,
        error_code
    from legacy_incidents
),

legacy_only as (
    select
        incident_id,
        root_connector_name,
        current_connector_name,
        failure_at,
        recovered_at,
        completed_at,
        queue_status,
        final_outcome,
        event_type,
        severity,
        error_message,
        error_code
    from legacy_incidents
    except
    select
        incident_id,
        root_connector_name,
        current_connector_name,
        failure_at,
        recovered_at,
        completed_at,
        queue_status,
        final_outcome,
        event_type,
        severity,
        error_message,
        error_code
    from mart_incidents
)

select incident_id from mart_only
union all
select incident_id from legacy_only
