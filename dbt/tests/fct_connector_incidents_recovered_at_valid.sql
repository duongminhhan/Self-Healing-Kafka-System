select incident_id
from {{ ref("fct_connector_incidents") }}
where recovered_at is not null
  and (
      final_outcome <> 'RECOVERED'
      or queue_status <> 'COMPLETED'
      or recovered_at <> completed_at
  )
