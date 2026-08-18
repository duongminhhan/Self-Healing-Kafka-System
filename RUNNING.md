# Running self-healthy-kafka

Install Python 3.12, then run from the project directory:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install .
cp env/prod.env.example env/prod.env
```

If you need project-local Python libraries instead of installing into the
environment, install them into `lib/python`:

```bash
PYTHON_BIN=python3.12 bash scripts/install-lib.sh
```

Install Microsoft ODBC Driver 18 for SQL Server on the remote host.

Update `env/prod.env` with the real Kafka Connect, Kafka, SQL Server, and
webhook values. Start the service with:

Create the required SQL Server tables once with an administrative account:

```bash
sqlcmd -S mssql.example.internal,1433 -d self_healthy_kafka \
  -U self_healthy_kafka -P 'replace-me' \
  -i sql/mssql-schema.sql

sqlcmd -S mssql.example.internal,1433 -d self_healthy_kafka \
  -U self_healthy_kafka -P 'replace-me' \
  -i sql/mssql-stored-procedures.sql
```

```bash
bash scripts/run.sh prod
```

For UAT or development:

```bash
bash scripts/run.sh uat
bash scripts/run.sh dev
```

When `PROMETHEUS_METRICS_ENABLED=true`, the service also exposes app-owned
metrics at:

```text
http://<self-healthy-kafka-host>:<PROMETHEUS_METRICS_PORT>/metrics
```

For UAT with the default example values:

```bash
curl http://10.1.253.74:9108/metrics
```

Scrape this endpoint from Prometheus or Grafana Alloy:

```yaml
scrape_configs:
  - job_name: self-healthy-kafka
    static_configs:
      - targets:
          - 10.1.253.74:9108
```

Useful alert metrics:

```promql
kc_shs_topic_over_threshold == 1
kc_shs_topic_lag_seconds > 180
kc_shs_connector_failed == 1
increase(kc_shs_healing_escalated_total[5m]) > 0
```

Check all external connections before starting or after changing env values:

```bash
PYTHON_BIN=python3.12 bash scripts/check-connections.sh uat
```

The check reads `env/<environment>.env`, falls back to `.env.example`, and tests
Kafka Connect, Kafka brokers, SQL Server ODBC, the local Grafana webhook health
endpoint, and database hosts found in `sql/seed-*.sql`.

Check manually created Grafana alert rules through the Grafana API:

```bash
GRAFANA_URL=http://grafana-stg.snp.com.vn:3000 \
GRAFANA_API_TOKEN='<service-account-token>' \
python3 scripts/check-grafana-alert-rules.py
```

The script verifies the SELF-HEALTHY-KAFKA alert rules exist, checks their
labels/states, and evaluates Prometheus/Loki query expressions through Grafana's
datasource proxy. This catches issues such as broken datasource DNS, missing
rules, and alert queries that do not return the expected labels.

Update the KC-SHS runtime dashboard panels through the Grafana API:

```bash
GRAFANA_URL=https://grafana-stg.snp.com.vn \
GRAFANA_API_TOKEN='<service-account-token>' \
python3 scripts/update-grafana-runtime-dashboard.py
```

The updater changes `Healing Attempts` to DB-backed healing log counts, changes
the detailed `Topics Over Threshold` panel to a per-topic line chart of minutes
without incoming records, and adds a Loki warning/error log panel when a Loki
datasource exists.

The equivalent direct command is:

```bash
APP_ENV=prod SELF_HEALTHY_KAFKA_ENV_FILE=env/prod.env \
  python -m self_healthy_kafka.main
```
