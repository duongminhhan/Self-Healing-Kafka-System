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
```

Then execute the runtime stored procedures:

```text
sql/ingest_reference/stored-procedures/spEnqueueConnectorHealing.sql
sql/ingest_reference/stored-procedures/spGetConnectorHealingQueue.sql
sql/ingest_reference/stored-procedures/spGetConnectorHealingLogs.sql
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

## Local chatbot context test

Set `CHAT_API_ENABLED=true` and a private `CHAT_API_TOKEN` in the selected
`env/<environment>.env` file. Execute the six runtime procedure scripts above,
then restart the application. The API shares the webhook port and is read-only:

```bash
curl -sS \
  -H "Authorization: Bearer $CHAT_API_TOKEN" \
  "http://127.0.0.1:8080/api/v1/incidents?status=all&limit=20"

curl -sS \
  -H "Authorization: Bearer $CHAT_API_TOKEN" \
  "http://127.0.0.1:8080/api/v1/healing-logs?limit=20"
```

To ask the app in normal language, install a tool-capable model locally, set
`OLLAMA_ENABLED=true` and `OLLAMA_MODEL` to that installed model, then restart
the app. Ollama must not receive direct SQL Server credentials or a Kafka
Connect write endpoint:

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $CHAT_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"question":"liệt kê top connector chết nhiều nhất"}' \
  "http://127.0.0.1:8080/api/v1/chat"
```

For a local CPU-only Ollama container, bound generation with
`OLLAMA_MAX_TOKENS=1024`. For Qwen3 tool calling, set `OLLAMA_THINK=true`.
Routine operational questions are routed by the application to their exact
read-only tool before the model is used, so an LLM cannot bypass the SQL-backed
result. See [CHATBOT_TEST_SCENARIOS.md](CHATBOT_TEST_SCENARIOS.md) for the full
local validation procedure.
