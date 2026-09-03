# Chatbot Test Scenarios

## Purpose

The chatbot answers read-only Kafka Connect operations questions from the
healing queue and healing logs. It never executes arbitrary SQL, Kafka Connect
actions, or exposes connector credentials. For routine status questions, the
application deterministically selects a read tool and formats the tool result
itself. The model does not supply counts, connector names, or statuses.

## One-Time Local Setup

1. Start the local containers:

   ```powershell
   docker start poc-mssql poc-ollama
   docker exec poc-ollama ollama list
   ```

   Confirm that `qwen3:4b` is listed. Pull it only when it is absent:

   ```powershell
   docker exec poc-ollama ollama pull qwen3:4b
   ```

2. In DBeaver, connect to database `ingest_reference` on `localhost,14330`.
   Execute the two files under `sql/init-table`, then execute the six files
   under `sql/ingest_reference/stored-procedures`. Apply the provided test-data
   script when the database has no queue/log data.

3. Configure `env/dev.env` for the local SQL Server and Ollama container. Keep
   `CHAT_API_TOKEN` private. The relevant values are:

   ```dotenv
   CHAT_API_ENABLED=true
   CHAT_API_PATH_PREFIX=/api/v1
   OLLAMA_ENABLED=true
   OLLAMA_BASE_URL=http://127.0.0.1:11434
   OLLAMA_MODEL=qwen3:4b
   OLLAMA_THINK=true
   OLLAMA_MAX_TOKENS=1024
   ```

4. Start the application from the repository root. On Windows, ensure the
   current checkout is imported by setting `PYTHONPATH` first:

   ```powershell
   $env:PYTHONPATH = "$PWD\src"
   $env:APP_ENV = "dev"
   $env:SELF_HEALTHY_KAFKA_ENV_FILE = "$PWD\env\dev.env"
   python -m self_healthy_kafka.main
   ```

## Scenario 1: Top Failed Connectors

Ask the chatbot:

```powershell
$headers = @{ Authorization = "Bearer $env:CHAT_API_TOKEN" }
$body = @{ question = "liệt kê top connector chết nhiều nhất" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8080/api/v1/chat" `
  -Headers $headers -ContentType "application/json" -Body $body
```

Expected result:

- HTTP `200`.
- `sources[0].tool` is `get_connector_failure_ranking`.
- `answer` lists `RootConnectorName` and `FailureIncidentCount`.
- Every displayed connector and count matches this direct database check:

  ```sql
  EXEC dbo.spGetConnectorFailureRanking @limit = 10;
  ```

The phrase `hôm nay top connector lỗi nhiều nhất` applies a time window from
00:00 in `Asia/Ho_Chi_Minh` to the current time. Without a time phrase, the
ranking uses all available queue history.

## Scenario 2: Open Incidents

Ask `liệt kê connector đang lỗi cần xử lý`.

Expected result: `sources[0].tool` is `list_incidents`; the answer is derived
from queue rows whose `QueueStatus` is `PENDING`, `PROCESSING`, or `WAITING`.
Validate with `GET /api/v1/incidents?status=open` using the same bearer token.

## Scenario 3: Escalated Incidents

Ask `connector nào đã escalated`.

Expected result: `sources[0].tool` is `list_incidents` and only queue rows with
`QueueStatus=ESCALATED` are shown. Validate with
`GET /api/v1/incidents?status=escalated`.

## Scenario 4: Connector Healing History

Ask `cho tôi log healing của TEST-TOPO-CLI-G042`.

Expected result: `sources[0].tool` is `list_healing_logs`; the result is
filtered by that connector name and contains redacted details. Validate with:

```powershell
Invoke-RestMethod -Uri "http://127.0.0.1:8080/api/v1/healing-logs?connector_name=TEST-TOPO-CLI-G042" `
  -Headers $headers
```

## Negative Checks

- Omit or change the bearer token: the API returns HTTP `401`.
- Stop `poc-ollama` and ask an unsupported free-form question: the API returns
  HTTP `503`; no queue row or log is changed.
- Verify every request is read-only by confirming queue/log row counts do not
  change before and after the tests.
