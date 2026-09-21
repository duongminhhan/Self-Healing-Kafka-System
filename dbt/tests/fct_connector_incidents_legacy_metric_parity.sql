with legacy_incidents as (
    select top (1001)
        [FailureAt] as failure_at,
        [RecoveredAt] as recovered_at,
        [QueueStatus] as queue_status,
        [FinalOutcome] as final_outcome,
        [EventType] as event_type
    from {{ source("ingest_reference", "connector_incident_facts") }}
    where [EventType] = 'HEALTH_FAILED_CONFIRMED'
    order by [FailureAt] desc, [IncidentId] desc
),

mart_incidents as (
    select top (1001)
        failure_at,
        recovered_at,
        queue_status,
        final_outcome,
        event_type
    from {{ ref("fct_connector_incidents") }}
    order by failure_at desc, incident_id desc
),

mart_metrics as (
    select
        count(*) as incident_count,
        sum(case when final_outcome = 'RECOVERED' then 1 else 0 end) as recovered_incident_count,
        sum(case when final_outcome = 'FAILED' then 1 else 0 end) as failed_incident_count,
        cast(avg(case
            when final_outcome = 'RECOVERED'
             and queue_status = 'COMPLETED'
             and recovered_at >= failure_at
                then cast(datediff_big(millisecond, failure_at, recovered_at) as decimal(38, 3)) / 60000.0
        end) as decimal(18, 3)) as average_recovery_minutes
    from mart_incidents
),

legacy_metrics as (
    select
        count(*) as incident_count,
        sum(case when final_outcome = 'RECOVERED' then 1 else 0 end) as recovered_incident_count,
        sum(case when final_outcome = 'FAILED' then 1 else 0 end) as failed_incident_count,
        cast(avg(case
            when final_outcome = 'RECOVERED'
             and queue_status = 'COMPLETED'
             and recovered_at >= failure_at
                then cast(datediff_big(millisecond, failure_at, recovered_at) as decimal(38, 3)) / 60000.0
        end) as decimal(18, 3)) as average_recovery_minutes
    from legacy_incidents
)

select 'incident_count' as metric_name
from mart_metrics
cross join legacy_metrics
where mart_metrics.incident_count <> legacy_metrics.incident_count

union all

select 'recovered_incident_count' as metric_name
from mart_metrics
cross join legacy_metrics
where mart_metrics.recovered_incident_count <> legacy_metrics.recovered_incident_count

union all

select 'failed_incident_count' as metric_name
from mart_metrics
cross join legacy_metrics
where mart_metrics.failed_incident_count <> legacy_metrics.failed_incident_count

union all

select 'average_recovery_minutes' as metric_name
from mart_metrics
cross join legacy_metrics
where mart_metrics.average_recovery_minutes <> legacy_metrics.average_recovery_minutes
   or (
       mart_metrics.average_recovery_minutes is null
       and legacy_metrics.average_recovery_minutes is not null
   )
   or (
       mart_metrics.average_recovery_minutes is not null
       and legacy_metrics.average_recovery_minutes is null
   )
