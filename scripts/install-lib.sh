#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
LIB_DIR="${LIB_DIR:-${PROJECT_ROOT}/lib/python}"
INSTALLER_VENV=""

cleanup() {
  if [[ -n "${INSTALLER_VENV}" && -d "${INSTALLER_VENV}" ]]; then
    rm -rf "${INSTALLER_VENV}"
  fi
}
trap cleanup EXIT

cd "${PROJECT_ROOT}"

INSTALLER_VENV="$(mktemp -d "${TMPDIR:-/tmp}/self-healthy-kafka-install.XXXXXX")"
if ! "${PYTHON_BIN}" -m venv "${INSTALLER_VENV}"; then
  echo "Failed to create installer virtualenv with ${PYTHON_BIN}." >&2
  echo "Install the matching python3-venv package, then retry." >&2
  exit 1
fi

"${INSTALLER_VENV}/bin/python" -m pip install --upgrade pip
"${INSTALLER_VENV}/bin/python" -m pip install \
  --upgrade \
  --target "${LIB_DIR}" \
  -r requirements.txt

echo "Installed Python dependencies into ${LIB_DIR}"

if command -v odbcinst >/dev/null 2>&1; then
  if odbcinst -q -d | grep -Fq "[ODBC Driver 18 for SQL Server]"; then
    echo "Found ODBC Driver 18 for SQL Server"
  else
    echo "WARNING: ODBC Driver 18 for SQL Server is not installed on this host" >&2
    echo "Install msodbcsql18 at OS level before connecting to MSSQL" >&2
  fi
else
  echo "WARNING: odbcinst is not available; cannot verify ODBC Driver 18" >&2
fi
