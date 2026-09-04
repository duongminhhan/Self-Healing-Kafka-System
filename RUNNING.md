# Running self-healthy-kafka

Create a Python 3.12 virtual environment and install the package:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
cp env/prod.env.example env/prod.env
```

Install Microsoft ODBC Driver 18 for SQL Server on the host. Update the chosen
environment file with the real Kafka Connect, SQL Server, webhook, and healing
settings.

For a first database installation, execute these table scripts in DBeaver:

```text
sql/init-table/ConnectorHealingQueue.sql
sql/init-table/ConnectorHealingLogs.sql
sql/ingest_reference/views/vConnectorIncidentFacts.sql
```

Then execute the runtime stored procedures:

```text
sql/ingest_reference/stored-procedures/spEnqueueConnectorHealing.sql
sql/ingest_reference/stored-procedures/spGetConnectorHealingQueue.sql
sql/ingest_reference/stored-procedures/spGetConnectorHealingLogs.sql
sql/ingest_reference/stored-procedures/spSearchConnectorHealingLogs.sql
sql/ingest_reference/stored-procedures/spGetConnectorIncidentFacts.sql
sql/ingest_reference/stored-procedures/spGetConnectorFailureRanking.sql
sql/ingest_reference/stored-procedures/spInsertConnectorHealingLog.sql
sql/ingest_reference/stored-procedures/spUpdateConnectorHealingQueue.sql
```

For an existing database, also execute
`sql/ingest_reference/stored-procedures/drop-legacy-procedures.sql` to remove
retired metric and topic-lag procedures. It does not drop historical tables.

Start the app:

```bash
bash scripts/run.sh prod
```

Other environments:

```bash
bash scripts/run.sh uat
bash scripts/run.sh dev
```

Run one connector reconciliation pass:

```bash
APP_ENV=uat SELF_HEALTHY_KAFKA_ENV_FILE=env/uat.env \
  python -m self_healthy_kafka.main --health-check-once
```

The webhook server exposes `GET /health` and the configured Grafana POST path.
The application does not expose a custom metrics endpoint.

## Local chatbot UI

When `CHAT_API_ENABLED=true` and `OLLAMA_ENABLED=true`, open the same-origin UI
at [http://127.0.0.1:8080/](http://127.0.0.1:8080/) (or `/chat`). Enter the
private `CHAT_API_TOKEN` in the browser field; it is retained only in that
browser tab's session storage and is sent as a Bearer token to
`POST /api/v1/chat`. The UI polls `GET /health`, displays request failures, and
shows only the `ConnectorHealingLogs` rows supplied by the API as evidence.

## Optional Hugging Face analytics planner

For bounded Vietnamese analysis (time filters, rankings, grouping, recovery
state), apply `sql/ingest_reference/views/vConnectorIncidentFacts.sql` and
`sql/ingest_reference/stored-procedures/spGetConnectorIncidentFacts.sql`, then
set `CHAT_ANALYTICS_ENABLED=true`. Set `CHAT_ANALYTICS_TIMEZONE` explicitly.
To use a Hugging Face Dedicated Endpoint, set `HF_CHAT_ENDPOINT_URL`,
`HF_CHAT_TOKEN`, and `HF_CHAT_MODEL_ID`; these values stay in the backend and
are never returned to the browser. The planner can return only a validated JSON
query plan; the app calls the fixed read-only procedure with bound parameters.
UAT/Prod DBAs must apply both SQL scripts manually before enabling the flag.

## Local chatbot context test

Set `CHAT_API_ENABLED=true` and a private `CHAT_API_TOKEN` in the selected
`env/<environment>.env` file. Execute the runtime procedure scripts above,
then restart the application. The API shares the webhook port and is read-only:

```bash
curl -sS \
  -H "Authorization: Bearer $CHAT_API_TOKEN" \
  "http://127.0.0.1:8080/api/v1/incidents?status=all&limit=20"

curl -sS \
  -H "Authorization: Bearer $CHAT_API_TOKEN" \
  "http://127.0.0.1:8080/api/v1/healing-logs?limit=20"
```

To ask the app in normal language, install a local Ollama model, set
`OLLAMA_ENABLED=true`, `OLLAMA_MODEL`, and `OLLAMA_CONTEXT_LOG_LIMIT` (for
example `3`), then restart the app. For every question the app performs one
parameterized retrieval from `ConnectorHealingLogs`, redacts the retrieved rows,
and sends only that evidence plus the original question to Ollama. Ollama never
receives SQL Server credentials, arbitrary SQL, or a Kafka Connect write endpoint:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $CHAT_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"question":"liệt kê top connector chết nhiều nhất"}' \
  "http://127.0.0.1:8080/api/v1/chat"
```

For a local CPU-only Ollama container, bound generation with
`OLLAMA_MAX_TOKENS=256`. The response contains `sources` with the exact
redacted rows retrieved from the DB; use their log IDs to verify the answer.
See [CHATBOT_TEST_SCENARIOS.md](CHATBOT_TEST_SCENARIOS.md) for the full local
validation procedure.
