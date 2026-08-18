select
    id,
    ConnectorName,
    FailedCount,
    IsActive,
    Level
from {{ source('sqlserver_raw', 'Connectors') }}

