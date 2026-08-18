#!/usr/bin/env bash
set -u -o pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ENV="${1:-${APP_ENV:-prod}}"
ENV_FILE="${SELF_HEALTHY_KAFKA_ENV_FILE:-env/${APP_ENV}.env}"
DEFAULT_ENV_FILE=".env.example"
TIMEOUT_SECONDS="${CHECK_TIMEOUT_SECONDS:-5}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

cd "${PROJECT_ROOT}"

PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

info() {
  printf '\n== %s ==\n' "$1"
}

pass() {
  PASS_COUNT=$((PASS_COUNT + 1))
  printf '[PASS] %s\n' "$1"
}

fail() {
  FAIL_COUNT=$((FAIL_COUNT + 1))
  printf '[FAIL] %s\n' "$1"
}

warn() {
  WARN_COUNT=$((WARN_COUNT + 1))
  printf '[WARN] %s\n' "$1"
}

env_value() {
  local key="$1"
  local value

  value="${!key-}"
  if [[ -n "${value}" ]]; then
    printf '%s' "${value}"
    return 0
  fi

  for file in "${ENV_FILE}" "${DEFAULT_ENV_FILE}"; do
    if [[ -f "${file}" ]]; then
      value="$(
        awk -F= -v key="${key}" '
          $0 !~ /^[[:space:]]*#/ && $1 == key {
            sub(/^[^=]*=/, "", $0)
            print $0
          }
        ' "${file}" | tail -n 1
      )"
      if [[ -n "${value}" ]]; then
        printf '%s' "${value}"
        return 0
      fi
    fi
  done

  return 1
}

tcp_check() {
  local host="$1"
  local port="$2"
  local label="$3"

  if [[ -z "${host}" || -z "${port}" ]]; then
    warn "${label}: missing host or port"
    return 0
  fi

  if command -v nc >/dev/null 2>&1; then
    if nc -vz -w "${TIMEOUT_SECONDS}" "${host}" "${port}" >/tmp/check-conn-nc.out 2>&1; then
      pass "${label}: TCP ${host}:${port}"
    else
      fail "${label}: TCP ${host}:${port} ($(tr '\n' ' ' </tmp/check-conn-nc.out))"
    fi
    rm -f /tmp/check-conn-nc.out
    return 0
  fi

  if timeout "${TIMEOUT_SECONDS}" bash -c "cat < /dev/null > /dev/tcp/${host}/${port}" >/dev/null 2>&1; then
    pass "${label}: TCP ${host}:${port}"
  else
    fail "${label}: TCP ${host}:${port}"
  fi
}

http_check() {
  local url="$1"
  local label="$2"
  local expected="${3:-}"

  if ! command -v curl >/dev/null 2>&1; then
    warn "${label}: curl is not installed"
    return 0
  fi

  local body_file status
  body_file="$(mktemp)"
  status="$(
    curl -sS --max-time "${TIMEOUT_SECONDS}" \
      -o "${body_file}" \
      -w '%{http_code}' \
      "${url}" 2>/tmp/check-conn-curl.err
  )"
  local curl_status=$?

  if [[ ${curl_status} -ne 0 ]]; then
    if [[ "${url}" == https://* ]]; then
      local insecure_body_file insecure_status insecure_error_file
      insecure_body_file="$(mktemp)"
      insecure_error_file="$(mktemp)"
      insecure_status="$(
        curl -k -sS --max-time "${TIMEOUT_SECONDS}" \
          -o "${insecure_body_file}" \
          -w '%{http_code}' \
          "${url}" 2>"${insecure_error_file}"
      )"
      local insecure_curl_status=$?
      if [[ ${insecure_curl_status} -eq 0 && "${insecure_status}" =~ ^[23] ]]; then
        warn "${label}: ${url} is reachable with insecure TLS (-k) but certificate verification failed; install/trust the CA certificate for the app runtime. strict_error=$(tr '\n' ' ' </tmp/check-conn-curl.err)"
      else
        fail "${label}: ${url} strict TLS failed ($(tr '\n' ' ' </tmp/check-conn-curl.err)); insecure retry returned curl=${insecure_curl_status} HTTP=${insecure_status:-000} $(tr '\n' ' ' <"${insecure_error_file}")"
      fi
      rm -f "${insecure_body_file}" "${insecure_error_file}"
    else
      fail "${label}: ${url} ($(tr '\n' ' ' </tmp/check-conn-curl.err))"
    fi
  elif [[ -n "${expected}" && "${status}" != "${expected}" ]]; then
    fail "${label}: ${url} returned HTTP ${status}, expected ${expected}; body=$(head -c 160 "${body_file}")"
  elif [[ "${status}" =~ ^[23] ]]; then
    pass "${label}: ${url} returned HTTP ${status}"
  else
    fail "${label}: ${url} returned HTTP ${status}; body=$(head -c 160 "${body_file}")"
  fi

  rm -f "${body_file}" /tmp/check-conn-curl.err
}

url_host_port() {
  "${PYTHON_BIN}" - "$1" <<'PY'
import sys
from urllib.parse import urlsplit

url = sys.argv[1]
parsed = urlsplit(url)
port = parsed.port or (443 if parsed.scheme == "https" else 80)
print(parsed.hostname or "")
print(port)
PY
}

mssql_server_host_port() {
  "${PYTHON_BIN}" - "$1" <<'PY'
import re
import sys

conn = sys.argv[1]
match = re.search(r"(?:^|;)Server=([^;]+)", conn, re.I)
if not match:
    print("")
    print("")
    raise SystemExit

server = match.group(1).strip()
if "\\" in server:
    host, _instance = server.split("\\", 1)
    print(host)
    print("")
elif "," in server:
    host, port = server.rsplit(",", 1)
    print(host.strip())
    print(port.strip())
else:
    print(server)
    print("1433")
PY
}

check_python_import() {
  local module="$1"
  local label="$2"
  if "${PYTHON_BIN}" - <<PY >/dev/null 2>&1
import ${module}
PY
  then
    pass "${label}: python import ${module}"
  else
    fail "${label}: python import ${module}"
  fi
}

check_seed_database_endpoints() {
  if [[ ! -d sql ]]; then
    return 0
  fi

  while IFS='|' read -r label host port; do
    [[ -n "${label}" ]] || continue
    tcp_check "${host}" "${port}" "${label}"
  done < <("${PYTHON_BIN}" - <<'PY'
import json
import re
from pathlib import Path

for path in sorted(Path("sql").glob("seed-*.sql")):
    text = path.read_text(encoding="utf-8")
    match = re.search(r"DECLARE @ConfigTemplate NVARCHAR\(MAX\) = N'(.*?)';", text, re.S)
    if not match:
        continue
    raw = match.group(1).replace("''", "'")
    try:
        config = json.loads(raw)
    except json.JSONDecodeError:
        continue

    name = config.get("name") or path.stem
    hostname = config.get("database.hostname")
    port = config.get("database.port")
    if hostname and port:
        print(f"{name} database.hostname|{hostname}|{port}")

    url = config.get("database.url") or ""
    for host, port in re.findall(r"HOST=([^)]+)\)\(PORT=([0-9]+)\)", url, re.I):
        print(f"{name} database.url|{host}|{port}")
PY
  )
}

info "Environment"
if [[ -f "${ENV_FILE}" ]]; then
  pass "environment file exists: ${ENV_FILE}"
else
  warn "environment file not found: ${ENV_FILE}; falling back to ${DEFAULT_ENV_FILE}"
fi

printf 'APP_ENV=%s\n' "${APP_ENV}"

info "Python libraries"
export PYTHONPATH="${PROJECT_ROOT}/lib/python:${PROJECT_ROOT}/src:${PYTHONPATH:-}"
check_python_import httpx "HTTP client"
check_python_import pyodbc "MSSQL client"
check_python_import kafka "Kafka client"
check_python_import prometheus_client "Prometheus metrics client"

info "ODBC driver"
if command -v odbcinst >/dev/null 2>&1; then
  if odbcinst -q -d | grep -Fq "[ODBC Driver 18 for SQL Server]"; then
    pass "ODBC Driver 18 for SQL Server is installed"
  else
    fail "ODBC Driver 18 for SQL Server is not installed"
  fi
else
  fail "odbcinst is not installed"
fi

info "Kafka Connect REST"
KAFKA_CONNECT_URL="$(env_value KAFKA_CONNECT_URL || true)"
if [[ -n "${KAFKA_CONNECT_URL}" ]]; then
  readarray -t connect_parts < <(url_host_port "${KAFKA_CONNECT_URL}")
  if [[ "${KAFKA_CONNECT_URL}" == https://* ]]; then
    pass "Kafka Connect REST scheme: HTTPS"
  elif [[ "${KAFKA_CONNECT_URL}" == http://* ]]; then
    warn "Kafka Connect REST scheme: HTTP; update KAFKA_CONNECT_URL if UAT now requires HTTPS"
  else
    warn "Kafka Connect REST scheme is not explicit: ${KAFKA_CONNECT_URL}"
  fi
  tcp_check "${connect_parts[0]}" "${connect_parts[1]}" "Kafka Connect REST TCP"
  http_check "${KAFKA_CONNECT_URL%/}/connectors" "Kafka Connect REST /connectors"
else
  fail "KAFKA_CONNECT_URL is missing"
fi

info "Kafka bootstrap servers"
KAFKA_BOOTSTRAP_SERVERS="$(env_value KAFKA_BOOTSTRAP_SERVERS || true)"
if [[ -n "${KAFKA_BOOTSTRAP_SERVERS}" ]]; then
  IFS=',' read -ra brokers <<< "${KAFKA_BOOTSTRAP_SERVERS}"
  for broker in "${brokers[@]}"; do
    broker="$(echo "${broker}" | xargs)"
    host="${broker%:*}"
    port="${broker##*:}"
    tcp_check "${host}" "${port}" "Kafka broker"
  done
else
  fail "KAFKA_BOOTSTRAP_SERVERS is missing"
fi

KAFKA_SECURITY_PROTOCOL="$(env_value KAFKA_SECURITY_PROTOCOL || printf 'PLAINTEXT')"
case "${KAFKA_SECURITY_PROTOCOL^^}" in
  PLAINTEXT)
    pass "Kafka broker protocol configured as PLAINTEXT"
    ;;
  SSL|SASL_SSL)
    warn "Kafka broker protocol ${KAFKA_SECURITY_PROTOCOL} is set, but this script currently only verifies broker TCP reachability. Ensure app code/runtime supports Kafka SSL settings before relying on topic lag checks."
    ;;
  *)
    warn "Kafka broker protocol ${KAFKA_SECURITY_PROTOCOL} is not recognized by this script"
    ;;
esac

info "MSSQL"
MSSQL_CONNECTION_STRING="$(env_value MSSQL_CONNECTION_STRING || true)"
if [[ -n "${MSSQL_CONNECTION_STRING}" ]]; then
  readarray -t mssql_parts < <(mssql_server_host_port "${MSSQL_CONNECTION_STRING}")
  if [[ -n "${mssql_parts[1]-}" ]]; then
    tcp_check "${mssql_parts[0]}" "${mssql_parts[1]}" "MSSQL TCP"
  else
    warn "MSSQL TCP skipped: named instance or missing port in Server=${mssql_parts[0]-}"
  fi
"${PYTHON_BIN}" - "${MSSQL_CONNECTION_STRING}" "${TIMEOUT_SECONDS}" <<'PY'
import sys

conn_str = sys.argv[1]
timeout = int(float(sys.argv[2]))
try:
    import pyodbc
    conn = pyodbc.connect(conn_str, timeout=timeout)
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    conn.close()
except Exception as exc:
    print(f"[FAIL] MSSQL login/query: {exc}")
    raise SystemExit(1)
else:
    print("[PASS] MSSQL login/query: SELECT 1")
PY
  if [[ $? -eq 0 ]]; then
    PASS_COUNT=$((PASS_COUNT + 1))
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
else
  fail "MSSQL_CONNECTION_STRING is missing"
fi

info "Grafana webhook receiver"
GRAFANA_WEBHOOK_ENABLED="$(env_value GRAFANA_WEBHOOK_ENABLED || printf 'false')"
GRAFANA_WEBHOOK_PORT="$(env_value GRAFANA_WEBHOOK_PORT || true)"
if [[ "${GRAFANA_WEBHOOK_ENABLED,,}" == "true" ]]; then
  if [[ -n "${GRAFANA_WEBHOOK_PORT}" ]]; then
    http_check "http://127.0.0.1:${GRAFANA_WEBHOOK_PORT}/health" "Local Grafana webhook /health" "200"
  else
    fail "GRAFANA_WEBHOOK_PORT is missing"
  fi
else
  warn "Grafana webhook is disabled"
fi

info "Prometheus metrics endpoint"
PROMETHEUS_METRICS_ENABLED="$(env_value PROMETHEUS_METRICS_ENABLED || printf 'false')"
PROMETHEUS_METRICS_PORT="$(env_value PROMETHEUS_METRICS_PORT || true)"
if [[ "${PROMETHEUS_METRICS_ENABLED,,}" == "true" ]]; then
  if [[ -n "${PROMETHEUS_METRICS_PORT}" ]]; then
    http_check "http://127.0.0.1:${PROMETHEUS_METRICS_PORT}/metrics" "Local Prometheus /metrics" "200"
  else
    fail "PROMETHEUS_METRICS_PORT is missing"
  fi
else
  warn "Prometheus metrics endpoint is disabled"
fi

info "Database endpoints from seed scripts"
check_seed_database_endpoints

info "Summary"
printf 'PASS=%s WARN=%s FAIL=%s\n' "${PASS_COUNT}" "${WARN_COUNT}" "${FAIL_COUNT}"

if [[ "${FAIL_COUNT}" -gt 0 ]]; then
  exit 1
fi
