select
    ConnectorReference,
    TopicName,
    count(*) as duplicate_count
from {{ ref('topic_lag_job_configs') }}
group by
    ConnectorReference,
    TopicName
having count(*) > 1
