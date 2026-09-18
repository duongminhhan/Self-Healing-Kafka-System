{% macro redact_incident_error_message(column_name) %}
  cast(
    case
      when {{ column_name }} is null or ltrim(rtrim(cast({{ column_name }} as nvarchar(max)))) = '' then null
      -- Redact the complete diagnostic whenever it includes a credential-shaped
      -- value. The Python presentation boundary applies its stricter shared
      -- redactor again before any detail can reach a response.
      when lower(cast({{ column_name }} as nvarchar(max))) like '%password%'
        or lower(cast({{ column_name }} as nvarchar(max))) like '%passwd%'
        or lower(cast({{ column_name }} as nvarchar(max))) like '%pwd%'
        or lower(cast({{ column_name }} as nvarchar(max))) like '%secret%'
        or lower(cast({{ column_name }} as nvarchar(max))) like '%token%'
        or lower(cast({{ column_name }} as nvarchar(max))) like '%api_key%'
        or lower(cast({{ column_name }} as nvarchar(max))) like '%apikey%'
        or lower(cast({{ column_name }} as nvarchar(max))) like '%authorization%'
        or lower(cast({{ column_name }} as nvarchar(max))) like '%bearer %'
        or lower(cast({{ column_name }} as nvarchar(max))) like '%basic %'
        or cast({{ column_name }} as nvarchar(max)) like '%://%:%@%'
        then N'[REDACTED]'
      else left(cast({{ column_name }} as nvarchar(max)), 2000)
    end as nvarchar(2000)
  )
{% endmacro %}
