#!/usr/bin/env bash
set -euo pipefail

STRATEGY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${STRATEGY_DIR}/.." && pwd)"

usage() {
  cat <<'EOF'
Usage:
  scripts/start-bare-runtime-debugpy.sh [USER_ID] [PLATFORM_HOST]
  scripts/start-bare-runtime-debugpy.sh --user-id 6 --platform-host 192.168.88.6
  scripts/start-bare-runtime-debugpy.sh --user-id 6 \
    --control-panel-addr 192.168.88.6:50054 \
    --runtime-channel-addr 192.168.88.6:50055

Defaults:
  USER_ID=6
  PLATFORM_HOST=127.0.0.1
  control-panel     PLATFORM_HOST:50054 (certificate bootstrap only)
  runtime-channel   PLATFORM_HOST:50055

Useful debug env:
  DEBUG_WAIT=0      start immediately without waiting for VS Code attach
  DEBUG_PORT=5679   change debugpy listen port
  RUNTIME_AGENT_CONTROL_ADDR=127.0.0.1:5706
                    local-only control endpoint used by restart-bare-worker-session
EOF
}

USER_ID="${USER_ID:-6}"
PLATFORM_HOST="${PLATFORM_HOST:-127.0.0.1}"

positional=()
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --user-id|--user_id)
      USER_ID="${2:?missing value for $1}"
      shift 2
      ;;
    --platform-host|--host)
      PLATFORM_HOST="${2:?missing value for $1}"
      shift 2
      ;;
    --control-panel-addr|--control-addr)
      CONTROL_PANEL_ADDR="${2:?missing value for $1}"
      shift 2
      ;;
    --runtime-channel-addr|--runtime-addr)
      RUNTIME_CHANNEL_ADDR="${2:?missing value for $1}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      positional+=("$@")
      break
      ;;
    -*)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      positional+=("$1")
      shift
      ;;
  esac
done

if [[ "${#positional[@]}" -gt 0 ]]; then
  USER_ID="${positional[0]}"
fi
if [[ "${#positional[@]}" -gt 1 ]]; then
  PLATFORM_HOST="${positional[1]}"
fi
if [[ "${#positional[@]}" -gt 2 ]]; then
  echo "too many positional arguments: ${positional[*]}" >&2
  usage >&2
  exit 2
fi

DEBUG_HOST="${DEBUG_HOST:-127.0.0.1}"
DEBUG_PORT="${DEBUG_PORT:-5678}"
DEBUG_WAIT="${DEBUG_WAIT:-1}"

CONTROL_PANEL_ADDR="${CONTROL_PANEL_ADDR:-${PLATFORM_HOST}:50054}"
RUNTIME_CHANNEL_ADDR="${RUNTIME_CHANNEL_ADDR:-${RUNTIME_CHANNEL_GRPC_ADDR:-${PLATFORM_HOST}:50055}}"

if [[ -z "${CONFIG_PATH:-}" ]]; then
  CONFIG_PATH="./config.yaml"
fi
if [[ ! -f "${STRATEGY_DIR}/${CONFIG_PATH#./}" && ! -f "${CONFIG_PATH}" ]]; then
  echo "config file not found: ${CONFIG_PATH}" >&2
  echo "Set CONFIG_PATH or run from a checkout that contains config.yaml." >&2
  exit 1
fi

WORKER_PYTHON="${HUSHINE_WORKER_PYTHON:-}"
if [[ -z "${WORKER_PYTHON}" ]]; then
  if [[ -x "${STRATEGY_DIR}/.venv/bin/python" ]]; then
    WORKER_PYTHON="${STRATEGY_DIR}/.venv/bin/python"
  elif [[ -f "${STRATEGY_DIR}/.venv/Scripts/python.exe" ]]; then
    WORKER_PYTHON="${STRATEGY_DIR}/.venv/Scripts/python.exe"
  fi
fi
if [[ -z "${WORKER_PYTHON}" || "${WORKER_PYTHON}" != /* || ! -f "${WORKER_PYTHON}" ]]; then
  echo "guarded worker virtualenv Python is unavailable" >&2
  echo "Repair with: cd ${STRATEGY_DIR} && uv sync --frozen --extra dev" >&2
  exit 1
fi
WORKER_PYTHON_DIR="$(dirname "${WORKER_PYTHON}")"
case "$(basename "${WORKER_PYTHON_DIR}")/$(basename "${WORKER_PYTHON}")" in
  bin/python|Scripts/python.exe) ;;
  *)
    echo "guarded worker Python must use the virtualenv launcher layout" >&2
    exit 1
    ;;
esac
WORKER_VENV_ROOT="$(cd "${WORKER_PYTHON_DIR}/.." && pwd -P)"
if [[ ! -f "${WORKER_VENV_ROOT}/pyvenv.cfg" ]]; then
  echo "guarded worker virtualenv marker is unavailable" >&2
  exit 1
fi
if [[ -n "${HUSHINE_WORKER_PYTHON_ARGS:-}" && "${HUSHINE_WORKER_PYTHON_ARGS}" != "-Xfrozen_modules=off" ]]; then
  echo "HUSHINE_WORKER_PYTHON_ARGS must be exactly -Xfrozen_modules=off when set" >&2
  exit 1
fi
if ! "${WORKER_PYTHON}" -I - <<'PY'
from importlib import metadata
from pathlib import Path
import sys

if Path(sys.prefix).resolve() == Path(sys.base_prefix).resolve():
    raise SystemExit("guarded worker Python did not activate a virtual environment")

for distribution in ("hushine-strategy-service", "hushine-strategy-library"):
    metadata.distribution(distribution)
import hushine_runtime_import_probe
import hushine_strategy
import strategy_service
PY
then
  echo "guarded worker virtualenv does not contain the installed Hushine packages" >&2
  echo "Repair with: cd ${STRATEGY_DIR} && uv sync --frozen --extra dev" >&2
  exit 1
fi

PROFILE_VALUES="$("${WORKER_PYTHON}" -I - <<'PY'
from hushine_strategy.runtime_dependencies import load_runtime_dependency_profile
from strategy_service.runtime_profile import current_runtime_profile

manifest = load_runtime_dependency_profile()
profile = current_runtime_profile()
if (
    profile.strategy_service_commit != "local-dev"
    or profile.strategy_library_commit != "local-dev"
    or profile.image_build_id != "local-dev"
):
    raise SystemExit("bare worker profile must use the local-dev build identity")
print(profile.name)
print(profile.version)
print(profile.contract_sha256)
print(profile.hosted_python)
print(",".join(manifest.public_import_roots))
print(profile.strategy_service_commit)
print(profile.strategy_library_commit)
print(profile.image_build_id)
PY
)"
HUSHINE_RUNTIME_PROFILE_NAME="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '1p')"
HUSHINE_RUNTIME_PROFILE_VERSION="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '2p')"
HUSHINE_RUNTIME_CONTRACT_SHA256="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '3p')"
HUSHINE_RUNTIME_HOSTED_PYTHON="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '4p')"
HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '5p')"
HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '6p')"
HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '7p')"
HUSHINE_RUNTIME_IMAGE_BUILD_ID="$(printf '%s\n' "${PROFILE_VALUES}" | sed -n '8p')"

random_suffix() {
  "${WORKER_PYTHON}" -I -c 'import sys, uuid; print(uuid.uuid4().hex[:int(sys.argv[1])])' "$1"
}

BOOTSTRAP_DIR="${RUNTIME_BARE_BOOTSTRAP_DIR:-/tmp/hushine-bare-debugpy-user-${USER_ID}}"
RUNTIME_BARE_STATE_FILE="${RUNTIME_BARE_STATE_FILE:-${BOOTSTRAP_DIR}/runtime.env}"

read_state_value() {
  local key="$1"
  if [[ ! -f "${RUNTIME_BARE_STATE_FILE}" ]]; then
    return 0
  fi
  sed -n "s/^export ${key}=\"\\(.*\\)\"$/\\1/p" "${RUNTIME_BARE_STATE_FILE}" | tail -n 1
}

previous_runtime_id="$(read_state_value RUNTIME_RUNTIME_ID)"
previous_runtime_name="$(read_state_value RUNTIME_NAME)"
RUNTIME_ID="${RUNTIME_RUNTIME_ID:-${previous_runtime_id:-bare-${USER_ID}-$(random_suffix 8)}}"
RUNTIME_NAME="${RUNTIME_NAME:-${previous_runtime_name:-bare-debug-${USER_ID}-$(random_suffix 6)}}"
if [[ -z "${RUNTIME_AGENT_CONTROL_ADDR:-}" ]]; then
  RUNTIME_AGENT_CONTROL_PORT="$("${WORKER_PYTHON}" -I -c 'import sys; print(5700 + (int(sys.argv[1]) % 1000))' "${USER_ID}")"
  RUNTIME_AGENT_CONTROL_ADDR="127.0.0.1:${RUNTIME_AGENT_CONTROL_PORT}"
fi
RUNTIME_AGENT_CONTROL_URL="http://${RUNTIME_AGENT_CONTROL_ADDR}"

RUNTIME_CHANNEL_TLS_ENABLED="${RUNTIME_CHANNEL_TLS_ENABLED:-true}"
RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE="${RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE:-${REPO_ROOT}/hushine-deploy/certs/runtime-channel-server.pem}"
RUNTIME_CHANNEL_TLS_SERVER_NAME="${RUNTIME_CHANNEL_TLS_SERVER_NAME:-runtime-channel.local}"
BOOTSTRAP_ROOT_CERT_FILE="${RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE}"
export RUNTIME_BARE_BOOTSTRAP_DIR="${BOOTSTRAP_DIR}"
export RUNTIME_BARE_STATE_FILE="${RUNTIME_BARE_STATE_FILE}"

wait_args=()
debug_wait_normalized="$(printf '%s' "${DEBUG_WAIT}" | tr '[:upper:]' '[:lower:]')"
debug_wait_normalized="${debug_wait_normalized#"${debug_wait_normalized%%[![:space:]]*}"}"
debug_wait_normalized="${debug_wait_normalized%"${debug_wait_normalized##*[![:space:]]}"}"
case "${debug_wait_normalized}" in
  ""|0|false|no|off) ;;
  *) wait_args=(--wait-for-client) ;;
esac

echo "Starting bare runtime for user_id=${USER_ID} under debugpy."
echo "VS Code attach: ${DEBUG_HOST}:${DEBUG_PORT}"
echo "control-panel certificate bootstrap: ${CONTROL_PANEL_ADDR}"
echo "runtime-channel: ${RUNTIME_CHANNEL_ADDR}"
echo "runtime-id: ${RUNTIME_ID}"
echo "local-control: ${RUNTIME_AGENT_CONTROL_URL}"
echo "state-file: ${RUNTIME_BARE_STATE_FILE}"
echo "config: ${CONFIG_PATH}"
if [[ "${#wait_args[@]}" -gt 0 ]]; then
  echo "Worker will wait for VS Code attach before executing a session."
fi

cd "${STRATEGY_DIR}"

if [[ "${RUNTIME_CHANNEL_TLS_ENABLED}" == "1" || "${RUNTIME_CHANNEL_TLS_ENABLED}" == "true" ]]; then
  RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE="${RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE:-${BOOTSTRAP_DIR}/runtime-client.pem}"
  RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE="${RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE:-${BOOTSTRAP_DIR}/runtime-client.key}"
  BUNDLE_ROOT_CERT_FILE="${BOOTSTRAP_DIR}/control-panel-ca.pem"
  "${WORKER_PYTHON}" -I - <<PY
from pathlib import Path
from strategy_service import bare_bootstrap

output_dir = Path("${BOOTSTRAP_DIR}")
runtime_id = "${RUNTIME_ID}"
paths = bare_bootstrap.load_existing_runtime_mtls_bundle(output_dir, runtime_id=runtime_id)
action = "using existing"
if paths is None:
    paths = bare_bootstrap.bootstrap_bare_runtime_certificate(
        address="${CONTROL_PANEL_ADDR}",
        user_id=int("${USER_ID}"),
        runtime_id=runtime_id,
        name="${RUNTIME_NAME}",
        root_cert_file="${BOOTSTRAP_ROOT_CERT_FILE}",
        server_name="${RUNTIME_CHANNEL_TLS_SERVER_NAME}",
        tls_enabled=False,
        output_dir=output_dir,
    )
    action = "bootstrapped"
print(f"{action} bare runtime mTLS: {paths.client_cert_file}")
PY
  RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE="${BUNDLE_ROOT_CERT_FILE}"
fi

mkdir -p "$(dirname "${RUNTIME_BARE_STATE_FILE}")"
cat > "${RUNTIME_BARE_STATE_FILE}" <<EOF
export USER_ID="${USER_ID}"
export RUNTIME_RUNTIME_ID="${RUNTIME_ID}"
export RUNTIME_NAME="${RUNTIME_NAME}"
export RUNTIME_AGENT_CONTROL_ADDR="${RUNTIME_AGENT_CONTROL_ADDR}"
export RUNTIME_AGENT_CONTROL_URL="${RUNTIME_AGENT_CONTROL_URL}"
export DEBUG_HOST="${DEBUG_HOST}"
export DEBUG_PORT="${DEBUG_PORT}"
EOF
chmod 600 "${RUNTIME_BARE_STATE_FILE}"

RUNTIME_AGENT_START_SCRIPT="${RUNTIME_AGENT_START_SCRIPT:-${STRATEGY_DIR}/scripts/start-runtime-agent.sh}"

agent_env=(
  "PATH=${PATH}"
  "HOME=${HOME}"
  "USER=${USER:-}"
  "TMPDIR=${TMPDIR:-/tmp}"
  "HUSHINE_WORKER_PYTHON=${WORKER_PYTHON}"
  "HUSHINE_RUNTIME_PROFILE_NAME=${HUSHINE_RUNTIME_PROFILE_NAME}"
  "HUSHINE_RUNTIME_PROFILE_VERSION=${HUSHINE_RUNTIME_PROFILE_VERSION}"
  "HUSHINE_RUNTIME_CONTRACT_SHA256=${HUSHINE_RUNTIME_CONTRACT_SHA256}"
  "HUSHINE_RUNTIME_HOSTED_PYTHON=${HUSHINE_RUNTIME_HOSTED_PYTHON}"
  "HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS=${HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS}"
  "HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT=${HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT}"
  "HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT=${HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT}"
  "HUSHINE_RUNTIME_IMAGE_BUILD_ID=${HUSHINE_RUNTIME_IMAGE_BUILD_ID}"
  "RUNTIME_CHANNEL_GRPC_ADDR=${RUNTIME_CHANNEL_ADDR}"
  "RUNTIME_CHANNEL_TLS_ENABLED=${RUNTIME_CHANNEL_TLS_ENABLED}"
  "RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE=${RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE}"
  "RUNTIME_CHANNEL_TLS_SERVER_NAME=${RUNTIME_CHANNEL_TLS_SERVER_NAME}"
  "RUNTIME_RUNTIME_ID=${RUNTIME_ID}"
  "RUNTIME_NAME=${RUNTIME_NAME}"
  "RUNTIME_AGENT_CONTROL_ADDR=${RUNTIME_AGENT_CONTROL_ADDR}"
  "DEBUG_PORT=${DEBUG_PORT}"
  "DEBUG_WAIT=${DEBUG_WAIT}"
)
if [[ -n "${RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE:-}" ]]; then
  agent_env+=("RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE=${RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE}")
fi
if [[ -n "${RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE:-}" ]]; then
  agent_env+=("RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE=${RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE}")
fi
for key in RUNTIME_AGENT_BIN RUNTIME_AGENT_BIN_DIR RUNTIME_AGENT_DIST_DIR RUNTIME_AGENT_ALLOW_GO_RUN HUSHINE_WORKER_PYTHON_ARGS; do
  if [[ -n "${!key:-}" ]]; then
    agent_env+=("${key}=${!key}")
  fi
done

exec env -i "${agent_env[@]}" "${RUNTIME_AGENT_START_SCRIPT}" -- \
  --config "${CONFIG_PATH}" \
  --runtime-channel-addr "${RUNTIME_CHANNEL_ADDR}" \
  --user-id "${USER_ID}"
