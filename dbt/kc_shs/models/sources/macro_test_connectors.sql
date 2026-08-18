select
    id,
    ConnectorName,
    IsActive,
    -- Gọi Macro ở đây:
    {{ get_status_label('IsActive') }} as status
from from {{ source('sqlserver_raw', 'Connectors') }}