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
