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
    --core-service-addr 192.168.88.6:50051 \
    --control-panel-addr 192.168.88.6:50054 \
    --runtime-channel-addr 192.168.88.6:50055

Defaults:
  USER_ID=6
  PLATFORM_HOST=127.0.0.1
  core-service      PLATFORM_HOST:50051
  control-panel     PLATFORM_HOST:50054
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
    --core-service-addr|--core-addr)
      CORE_SERVICE_ADDR="${2:?missing value for $1}"
      shift 2
      ;;
    --order-service-addr|--order-addr)
      ORDER_SERVICE_ADDR="${2:?missing value for $1}"
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

CORE_SERVICE_ADDR="${CORE_SERVICE_ADDR:-${CORE_SERVICE_GRPC_ADDR:-${PLATFORM_HOST}:50051}}"
ORDER_SERVICE_ADDR="${ORDER_SERVICE_ADDR:-${ORDER_SERVICE_GRPC_ADDR:-${CORE_SERVICE_ADDR}}}"
CONTROL_PANEL_ADDR="${CONTROL_PANEL_ADDR:-${CONTROL_PANEL_SERVICE_GRPC_ADDR:-${PLATFORM_HOST}:50054}}"
MARKET_DATA_CONTROL_PANEL_ADDR="${MARKET_DATA_CONTROL_PANEL_ADDR:-${MARKET_DATA_CONTROL_PANEL_GRPC_ADDR:-${CONTROL_PANEL_ADDR}}}"
RUNTIME_CHANNEL_ADDR="${RUNTIME_CHANNEL_ADDR:-${RUNTIME_CHANNEL_GRPC_ADDR:-${PLATFORM_HOST}:50055}}"

if [[ -z "${CONFIG_PATH:-}" ]]; then
  CONFIG_PATH="./config.yaml"
fi
if [[ ! -f "${STRATEGY_DIR}/${CONFIG_PATH#./}" && ! -f "${CONFIG_PATH}" ]]; then
  echo "config file not found: ${CONFIG_PATH}" >&2
  echo "Set CONFIG_PATH or run from a checkout that contains config.yaml." >&2
  exit 1
fi

random_suffix() {
  python3 -c 'import sys, uuid; print(uuid.uuid4().hex[:int(sys.argv[1])])' "$1"
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
  RUNTIME_AGENT_CONTROL_PORT="$(python3 -c 'import sys; print(5700 + (int(sys.argv[1]) % 1000))' "${USER_ID}")"
  RUNTIME_AGENT_CONTROL_ADDR="127.0.0.1:${RUNTIME_AGENT_CONTROL_PORT}"
fi
RUNTIME_AGENT_CONTROL_URL="http://${RUNTIME_AGENT_CONTROL_ADDR}"

export CORE_SERVICE_GRPC_ADDR="${CORE_SERVICE_ADDR}"
export ORDER_SERVICE_GRPC_ADDR="${ORDER_SERVICE_ADDR}"
export CONTROL_PANEL_SERVICE_GRPC_ADDR="${CONTROL_PANEL_ADDR}"
export MARKET_DATA_CONTROL_PANEL_GRPC_ADDR="${MARKET_DATA_CONTROL_PANEL_ADDR}"
export RUNTIME_CHANNEL_GRPC_ADDR="${RUNTIME_CHANNEL_ADDR}"
export RUNTIME_CHANNEL_TLS_ENABLED="${RUNTIME_CHANNEL_TLS_ENABLED:-true}"
export RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE="${RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE:-${REPO_ROOT}/hushine-deploy/certs/runtime-channel-server.pem}"
export RUNTIME_CHANNEL_TLS_SERVER_NAME="${RUNTIME_CHANNEL_TLS_SERVER_NAME:-runtime-channel.local}"
BOOTSTRAP_ROOT_CERT_FILE="${RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE}"
export RUNTIME_BARE_BOOTSTRAP_DIR="${BOOTSTRAP_DIR}"
export RUNTIME_BARE_STATE_FILE="${RUNTIME_BARE_STATE_FILE}"
export RUNTIME_RUNTIME_ID="${RUNTIME_ID}"
export RUNTIME_NAME="${RUNTIME_NAME}"
export RUNTIME_AGENT_CONTROL_ADDR="${RUNTIME_AGENT_CONTROL_ADDR}"
export HUSHINE_RUNTIME_SOURCE="bare"
export HUSHINE_RUNTIME_NAME="${RUNTIME_NAME}"
export PYTHONPATH="${PYTHONPATH:-.:./strategy-library}"
if [[ -z "${LOG_TRACING_ENDPOINT:-}" && "${PLATFORM_HOST}" != "127.0.0.1" && "${PLATFORM_HOST}" != "localhost" ]]; then
  export LOG_TRACING_ENDPOINT="http://${PLATFORM_HOST}:4318"
fi
if [[ -n "${NO_PROXY:-}" ]]; then
  export NO_PROXY="${NO_PROXY},${PLATFORM_HOST}"
else
  export NO_PROXY="127.0.0.1,localhost,::1,192.168.88.10,${PLATFORM_HOST}"
fi
export no_proxy="${no_proxy:-${NO_PROXY}}"

wait_args=()
if [[ "${DEBUG_WAIT}" != "0" && "${DEBUG_WAIT}" != "false" ]]; then
  wait_args=(--wait-for-client)
fi

echo "Starting bare runtime for user_id=${USER_ID} under debugpy."
echo "VS Code attach: ${DEBUG_HOST}:${DEBUG_PORT}"
echo "core-service: ${CORE_SERVICE_ADDR}"
echo "control-panel: ${CONTROL_PANEL_ADDR}"
echo "runtime-channel: ${RUNTIME_CHANNEL_ADDR}"
echo "runtime-id: ${RUNTIME_ID}"
echo "local-control: ${RUNTIME_AGENT_CONTROL_URL}"
echo "state-file: ${RUNTIME_BARE_STATE_FILE}"
echo "config: ${CONFIG_PATH}"
echo "tracing endpoint: ${LOG_TRACING_ENDPOINT:-config default}"
if [[ "${#wait_args[@]}" -gt 0 ]]; then
  echo "Worker will wait for VS Code attach before executing a session."
fi

cd "${STRATEGY_DIR}"

if [[ "${RUNTIME_CHANNEL_TLS_ENABLED}" == "1" || "${RUNTIME_CHANNEL_TLS_ENABLED}" == "true" ]]; then
  export RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE="${RUNTIME_CHANNEL_TLS_CLIENT_CERT_FILE:-${BOOTSTRAP_DIR}/runtime-client.pem}"
  export RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE="${RUNTIME_CHANNEL_TLS_CLIENT_KEY_FILE:-${BOOTSTRAP_DIR}/runtime-client.key}"
  BUNDLE_ROOT_CERT_FILE="${BOOTSTRAP_DIR}/control-panel-ca.pem"
  bootstrap_python=()
  if [[ -x "${STRATEGY_DIR}/.venv/bin/python" ]]; then
    bootstrap_python=("${STRATEGY_DIR}/.venv/bin/python")
  elif command -v uv >/dev/null 2>&1; then
    bootstrap_python=(uv run python)
  elif [[ -x "${HOME}/.local/bin/uv" ]]; then
    bootstrap_python=("${HOME}/.local/bin/uv" run python)
  else
    bootstrap_python=(python3)
  fi
  "${bootstrap_python[@]}" - <<PY
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
  export RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE="${BUNDLE_ROOT_CERT_FILE}"
fi

mkdir -p "$(dirname "${RUNTIME_BARE_STATE_FILE}")"
cat > "${RUNTIME_BARE_STATE_FILE}" <<EOF
export USER_ID="${USER_ID}"
export PLATFORM_HOST="${PLATFORM_HOST}"
export CORE_SERVICE_GRPC_ADDR="${CORE_SERVICE_ADDR}"
export CONTROL_PANEL_SERVICE_GRPC_ADDR="${CONTROL_PANEL_ADDR}"
export RUNTIME_CHANNEL_GRPC_ADDR="${RUNTIME_CHANNEL_ADDR}"
export RUNTIME_RUNTIME_ID="${RUNTIME_ID}"
export RUNTIME_NAME="${RUNTIME_NAME}"
export RUNTIME_AGENT_CONTROL_ADDR="${RUNTIME_AGENT_CONTROL_ADDR}"
export RUNTIME_AGENT_CONTROL_URL="${RUNTIME_AGENT_CONTROL_URL}"
export DEBUG_HOST="${DEBUG_HOST}"
export DEBUG_PORT="${DEBUG_PORT}"
EOF
chmod 600 "${RUNTIME_BARE_STATE_FILE}"

RUNTIME_AGENT_START_SCRIPT="${RUNTIME_AGENT_START_SCRIPT:-${STRATEGY_DIR}/scripts/start-runtime-agent.sh}"

exec "${RUNTIME_AGENT_START_SCRIPT}" -- \
  --config "${CONFIG_PATH}" \
  --runtime-channel-addr "${RUNTIME_CHANNEL_ADDR}" \
  --control-panel-addr "${CONTROL_PANEL_ADDR}" \
  --user-id "${USER_ID}"
