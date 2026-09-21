select incident_id
from {{ ref("fct_connector_incidents") }}
where error_code is not null
  and (
      error_message not like '%ORA-[0-9][0-9][0-9][0-9][0-9]%'
      or error_code <> substring(error_message, charindex('ORA-', error_message), 9)
  )
