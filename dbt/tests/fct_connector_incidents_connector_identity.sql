select incident_id
from {{ ref("fct_connector_incidents") }}
where nullif(ltrim(rtrim(root_connector_name)), '') is null
   or nullif(ltrim(rtrim(current_connector_name)), '') is null
