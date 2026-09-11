#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_ENV="${1:-prod}"
RUN_MODE="${2:-run}"
ENV_FILE="${SELF_HEALTHY_KAFKA_ENV_FILE:-env/${APP_ENV}.env}"

if [[ "${RUN_MODE}" != "run" && "${RUN_MODE}" != "--check-only" ]]; then
  echo "Unknown mode: ${RUN_MODE}. Expected --check-only or no second argument." >&2
  exit 2
fi

cd "${PROJECT_ROOT}"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "Missing environment file: ${ENV_FILE}" >&2
  exit 1
fi

export APP_ENV
export SELF_HEALTHY_KAFKA_ENV_FILE="${ENV_FILE}"

if [[ -n "${SELF_HEALTHY_KAFKA_PYTHON:-}" ]]; then
  PYTHON_BIN="${SELF_HEALTHY_KAFKA_PYTHON}"
elif [[ -x "${PROJECT_ROOT}/.venv/bin/python" ]]; then
  PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
elif [[ -x "${PROJECT_ROOT}/.venv/Scripts/python.exe" ]]; then
  PYTHON_BIN="${PROJECT_ROOT}/.venv/Scripts/python.exe"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
elif command -v python >/dev/null 2>&1; then
  PYTHON_BIN="python"
else
  PYTHON_BIN="python"
fi

IS_WSL=false
if command -v wslpath >/dev/null 2>&1 && grep -qi microsoft /proc/version 2>/dev/null; then
  IS_WSL=true
fi
if [[ "${IS_WSL}" == "true" && "${PYTHON_BIN}" =~ ^[A-Za-z]:[\\/] ]]; then
  PYTHON_BIN="$(wslpath -u "${PYTHON_BIN}")"
fi
if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Python executable not found: ${PYTHON_BIN}" >&2
  echo "Activate the project environment or use scripts/run.ps1 from PowerShell." >&2
  exit 1
fi

if [[ "${IS_WSL}" == "true" && "${PYTHON_BIN}" == *.exe ]]; then
  EXPECTED_SRC="$(wslpath -w "${PROJECT_ROOT}/src")"
  VENDORED_PATH="$(wslpath -w "${PROJECT_ROOT}/lib/python")"
  BOOTSTRAP_SCRIPT="$(wslpath -w "${PROJECT_ROOT}/scripts/_repo_bootstrap.py")"
  export PYTHONPATH="${EXPECTED_SRC};${VENDORED_PATH};${PYTHONPATH:-}"
else
  EXPECTED_SRC="${PROJECT_ROOT}/src"
  BOOTSTRAP_SCRIPT="${PROJECT_ROOT}/scripts/_repo_bootstrap.py"
  export PYTHONPATH="${EXPECTED_SRC}:${PROJECT_ROOT}/lib/python:${PYTHONPATH:-}"
fi

RESOLVED_PACKAGE="$("${PYTHON_BIN}" "${BOOTSTRAP_SCRIPT}" --check)"
echo "Using package: ${RESOLVED_PACKAGE}"

if [[ "${RUN_MODE}" == "--check-only" ]]; then
  echo "Runtime identity check passed."
  exit 0
fi

exec "${PYTHON_BIN}" "${BOOTSTRAP_SCRIPT}" --run-module self_healthy_kafka.main
