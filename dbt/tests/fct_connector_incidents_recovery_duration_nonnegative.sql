select incident_id
from {{ ref("fct_connector_incidents") }}
where recovered_at is not null
  and recovered_at < failure_at
