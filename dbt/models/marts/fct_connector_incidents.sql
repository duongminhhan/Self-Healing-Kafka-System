{{ config(materialized="view") }}

with confirmed_failures_ranked as (
    select
        incident_id,
        event_type,
        severity,
        failure_message,
        created_at as failure_at,
        row_number() over (
            partition by incident_id
            order by created_at asc, log_id asc
        ) as confirmed_failure_order
    from {{ ref("stg_connector_healing_logs") }}
    where event_type = 'HEALTH_FAILED_CONFIRMED'
),

confirmed_failures as (
    select
        incident_id,
        event_type,
        severity,
        failure_message,
        failure_at
    from confirmed_failures_ranked
    where confirmed_failure_order = 1
)

select
    queue.incident_id,
    queue.root_connector_name,
    queue.current_connector_name,
    queue.connector_class,
    queue.healing_mode,
    queue.received_at,
    failure.failure_at,
    queue.completed_at,
    case
        when queue.queue_status = 'COMPLETED' and queue.final_outcome = 'RECOVERED'
            then queue.completed_at
    end as recovered_at,
    queue.queue_status,
    case
        when queue.queue_status in ('PENDING', 'PROCESSING', 'WAITING') then 'OPEN'
        else queue.final_outcome
    end as final_outcome,
    failure.event_type,
    failure.severity,
    failure.failure_message as error_message,
    case
        when failure.failure_message like '%ORA-[0-9][0-9][0-9][0-9][0-9]%'
            then substring(failure.failure_message, charindex('ORA-', failure.failure_message), 9)
    end as error_code
from {{ ref("stg_connector_healing_queue") }} as queue
inner join confirmed_failures as failure
    on queue.incident_id = failure.incident_id
