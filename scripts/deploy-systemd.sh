#!/usr/bin/env bash
set -euo pipefail

APP_DIR="${1:?application directory is required}"
SERVICE_NAME="${2:?systemd service name is required}"
ARCHIVE="${3:?release archive is required}"
RELEASE_ID="${4:?release id is required}"

case "${APP_DIR}" in
  */self-healthy-kafka) ;;
  *)
    echo "APP_DIR must be an absolute self-healthy-kafka path: ${APP_DIR}" >&2
    exit 2
    ;;
esac

if [[ ! "${SERVICE_NAME}" =~ ^[A-Za-z0-9_.@-]+$ ]]; then
  echo "Invalid systemd service name: ${SERVICE_NAME}" >&2
  exit 2
fi

for command in tar rsync sudo systemctl curl; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Missing required command on UAT host: ${command}" >&2
    exit 2
  fi
done

if [[ ! -f "${ARCHIVE}" ]]; then
  echo "Release archive does not exist: ${ARCHIVE}" >&2
  exit 2
fi

if [[ ! -f "${APP_DIR}/env/uat.env" ]]; then
  echo "Existing UAT environment file is required: ${APP_DIR}/env/uat.env" >&2
  exit 2
fi

DEPLOY_ROOT="${APP_DIR}/.deploy"
STAGE_DIR="${DEPLOY_ROOT}/staging/${RELEASE_ID}"
BACKUP_DIR="${DEPLOY_ROOT}/backups/${RELEASE_ID}"

rm -rf "${STAGE_DIR}" "${BACKUP_DIR}"
mkdir -p "${STAGE_DIR}" "${BACKUP_DIR}"
tar -xzf "${ARCHIVE}" -C "${STAGE_DIR}"

for path in src scripts sql lib/python .env.example pyproject.toml requirements.txt; do
  if [[ ! -e "${STAGE_DIR}/${path}" ]]; then
    echo "Invalid release archive; missing ${path}" >&2
    exit 2
  fi
done

for path in src scripts sql lib .env.example pyproject.toml requirements.txt BUSINESS_CONTEXT.md; do
  if [[ -e "${APP_DIR}/${path}" ]]; then
    cp -a "${APP_DIR}/${path}" "${BACKUP_DIR}/"
  fi
done

rollback() {
  local exit_code=$?
  trap - ERR
  echo "Deployment failed; restoring ${RELEASE_ID}" >&2
  for path in src scripts sql lib .env.example pyproject.toml requirements.txt BUSINESS_CONTEXT.md; do
    rm -rf "${APP_DIR:?}/${path}"
    if [[ -e "${BACKUP_DIR}/${path}" ]]; then
      cp -a "${BACKUP_DIR}/${path}" "${APP_DIR}/"
    fi
  done
  sudo systemctl restart "${SERVICE_NAME}" || true
  rm -f "${ARCHIVE}"
  exit "${exit_code}"
}
trap rollback ERR

mkdir -p "${APP_DIR}/src" "${APP_DIR}/scripts" "${APP_DIR}/sql" "${APP_DIR}/lib/python"
rsync -a --delete "${STAGE_DIR}/src/" "${APP_DIR}/src/"
rsync -a --delete "${STAGE_DIR}/scripts/" "${APP_DIR}/scripts/"
rsync -a --delete "${STAGE_DIR}/sql/" "${APP_DIR}/sql/"
rsync -a --delete "${STAGE_DIR}/lib/python/" "${APP_DIR}/lib/python/"
install -m 0644 "${STAGE_DIR}/.env.example" "${APP_DIR}/.env.example"
install -m 0644 "${STAGE_DIR}/pyproject.toml" "${APP_DIR}/pyproject.toml"
install -m 0644 "${STAGE_DIR}/requirements.txt" "${APP_DIR}/requirements.txt"
install -m 0644 "${STAGE_DIR}/BUSINESS_CONTEXT.md" "${APP_DIR}/BUSINESS_CONTEXT.md"
chmod +x "${APP_DIR}"/scripts/*.sh

PYTHONPATH="${APP_DIR}/lib/python:${APP_DIR}/src" python3.12 -m compileall -q "${APP_DIR}/src"
sudo systemctl restart "${SERVICE_NAME}"

for _ in $(seq 1 30); do
  if sudo systemctl is-active --quiet "${SERVICE_NAME}" \
    && curl --fail --silent --show-error --max-time 3 \
      http://127.0.0.1:9108/metrics >/dev/null; then
    trap - ERR
    rm -f "${ARCHIVE}"
    rm -rf "${STAGE_DIR}"
    echo "Deployed ${RELEASE_ID}; ${SERVICE_NAME} is active and metrics are reachable"
    exit 0
  fi
  sleep 2
done

sudo systemctl status "${SERVICE_NAME}" --no-pager || true
echo "Service health verification timed out" >&2
false
