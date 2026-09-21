with expected_incidents as (
    select incident_id
    from {{ ref("stg_connector_healing_logs") }}
    where event_type = 'HEALTH_FAILED_CONFIRMED'
    group by incident_id
),

actual_counts as (
    select incident_id, count(*) as row_count
    from {{ ref("fct_connector_incidents") }}
    group by incident_id
)

select
    coalesce(expected_incidents.incident_id, actual_counts.incident_id) as incident_id
from expected_incidents
full outer join actual_counts
    on expected_incidents.incident_id = actual_counts.incident_id
where actual_counts.row_count is null or actual_counts.row_count <> 1
