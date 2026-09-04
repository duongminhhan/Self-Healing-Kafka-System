"""Reviewed business metadata, not question routing or production SQL templates."""

CATALOG = {
    "version": 1,
    "entities": {
        "ConnectorHealingQueue": {
            "grain": "One persisted incident per QueueId; active enqueue requests reuse it.",
            "dimensions": ["RootConnectorName", "QueueStatus", "FinalOutcome", "HealingMode"],
            "timestamps": {
                "ReceivedAt": "Incident receipt time, not worker start time.",
                "StartedAt": "Processing start time; nullable.",
                "CompletedAt": "Terminal incident time; includes non-success outcomes.",
            },
            "metrics": {
                "incident_count": {"aggregation": "COUNT", "grain_key": "QueueId"},
                "successful_healing": {
                    "filters": {"QueueStatus": "COMPLETED", "FinalOutcome": "RECOVERED"},
                    "meaning": "Recovered incident, not proof of current live health.",
                },
                "receipt_to_completion_minutes": {
                    "start": "ReceivedAt",
                    "end": "CompletedAt",
                    "unit": "minutes",
                    "expression": "(julianday(CompletedAt)-julianday(ReceivedAt))*1440.0",
                    "policy": "Exclude missing, unparseable and negative durations from averages; report matched, valid and excluded counts. Zero duration is valid. Round reported averages to 2 decimals. Apply success filters only when success is requested.",
                },
            },
            "caveats": "COMPLETED alone is not success: repository.complete marks any non-ESCALATED outcome COMPLETED. Latest incident status requires per-root timestamp ordering, not GROUP BY status.",
            "sources": [
                "src/self_healthy_kafka/storage/connector_repository.py:complete",
                "src/self_healthy_kafka/healing/db_state_machine.py",
                "sql/ingest_reference/stored-procedures/spEnqueueConnectorHealing.sql",
                "sql/ingest_reference/stored-procedures/spGetConnectorFailureRanking.sql",
            ],
        },
        "ConnectorHealingLogs": {
            "grain": "One recorded event per Id, not necessarily a failure or incident.",
            "dimensions": ["ConnectorName", "Severity", "EventType"],
            "timestamps": {"CreatedAt": "Recorded event time."},
            "metrics": {"event_count": {"aggregation": "COUNT", "grain_key": "Id"}},
            "caveats": "Informational actions are included. Error-event criteria must be defined; AttemptNo is not an event count.",
            "sources": ["sql/ingest_reference/stored-procedures/spGetConnectorHealingQueue.sql"],
        },
    },
    "relationships": [
        {
            "from": "ConnectorHealingLogs.QueueId",
            "to": "ConnectorHealingQueue.QueueId",
            "cardinality": "many-to-one",
            "warning": "Joining logs multiplies incident rows; aggregate at requested grain.",
        }
    ],
    "defaults": {
        "time_scope": "All snapshot rows unless specified.",
        "filters": "No optional filters unless specified or required by the requested metric definition.",
        "ties": "Metric then entity/key for deterministic ranking.",
        "clarification": "Only materially unresolved metric or denominator, not optional filters.",
    },
}

# Explicit privacy boundary: no free text, identifiers, names, messages or details.
PROFILE_COLUMNS = {
    "ConnectorHealingQueue": ("QueueStatus", "FinalOutcome", "HealingMode"),
    "ConnectorHealingLogs": ("Severity",),
}


def catalog_for(schema):
    """Do not advertise absent tables or metrics whose required columns are missing."""
    import copy

    catalog = copy.deepcopy(CATALOG)
    catalog["entities"] = {k: v for k, v in catalog["entities"].items() if k in schema}
    for table, entity in catalog["entities"].items():
        columns = {c["name"] for c in schema[table]}
        entity["dimensions"] = [c for c in entity["dimensions"] if c in columns]
        entity["timestamps"] = {c: v for c, v in entity["timestamps"].items() if c in columns}
        for name, metric in list(entity["metrics"].items()):
            required = set(metric.get("filters", {}))
            required.update(metric[k] for k in ("start", "end", "grain_key") if k in metric)
            if not required <= columns:
                del entity["metrics"][name]
    catalog["relationships"] = [
        r
        for r in catalog["relationships"]
        if all(
            ref.split(".")[0] in schema
            and ref.split(".")[1] in {c["name"] for c in schema[ref.split(".")[0]]}
            for ref in (r["from"], r["to"])
        )
    ]
    return catalog
