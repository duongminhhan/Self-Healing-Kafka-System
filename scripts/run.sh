#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ENV="${1:-prod}"
ENV_FILE="${SELF_HEALTHY_KAFKA_ENV_FILE:-env/${APP_ENV}.env}"

cd "${PROJECT_ROOT}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing environment file: ${ENV_FILE}" >&2
  exit 1
fi

export APP_ENV
export SELF_HEALTHY_KAFKA_ENV_FILE="${ENV_FILE}"
export PYTHONPATH="${PROJECT_ROOT}/lib/python:${PROJECT_ROOT}/src:${PYTHONPATH:-}"
exec python -m self_healthy_kafka.main
