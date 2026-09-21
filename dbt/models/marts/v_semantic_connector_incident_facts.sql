{{ config(materialized="view", alias="vSemanticConnectorIncidentFacts") }}

-- This is the stable compatibility contract consumed by the semantic compiler.
-- Keep physical mart column names behind this view so the runtime never needs
-- to know the dbt-internal naming convention.
select
    incident_id as [IncidentId],
    root_connector_name as [JobName],
    current_connector_name as [ConnectorName],
    failure_at as [FailureAt],
    recovered_at as [RecoveredAt],
    completed_at as [CompletedAt],
    queue_status as [QueueStatus],
    final_outcome as [FinalOutcome],
    event_type as [EventType],
    severity as [Severity],
    {{ redact_incident_error_message("error_message") }} as [ErrorMessage],
    error_code as [ErrorCode]
from {{ ref("fct_connector_incidents") }}
