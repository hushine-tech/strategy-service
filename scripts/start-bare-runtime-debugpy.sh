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

CONFIG_PATH="${CONFIG_PATH:-./config.local.yaml}"
BOOTSTRAP_DIR="${RUNTIME_BARE_BOOTSTRAP_DIR:-/tmp/hushine-bare-debugpy-user-${USER_ID}}"

export CORE_SERVICE_GRPC_ADDR="${CORE_SERVICE_ADDR}"
export ORDER_SERVICE_GRPC_ADDR="${ORDER_SERVICE_ADDR}"
export CONTROL_PANEL_SERVICE_GRPC_ADDR="${CONTROL_PANEL_ADDR}"
export MARKET_DATA_CONTROL_PANEL_GRPC_ADDR="${MARKET_DATA_CONTROL_PANEL_ADDR}"
export RUNTIME_CHANNEL_GRPC_ADDR="${RUNTIME_CHANNEL_ADDR}"
export RUNTIME_CHANNEL_TLS_ENABLED="${RUNTIME_CHANNEL_TLS_ENABLED:-true}"
export RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE="${RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE:-${REPO_ROOT}/hushine-deploy/certs/runtime-channel-server.pem}"
export RUNTIME_CHANNEL_TLS_SERVER_NAME="${RUNTIME_CHANNEL_TLS_SERVER_NAME:-runtime-channel.local}"
export RUNTIME_BARE_BOOTSTRAP_DIR="${BOOTSTRAP_DIR}"
export PYTHONPATH="${PYTHONPATH:-.:./strategy-library}"
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
if [[ "${#wait_args[@]}" -gt 0 ]]; then
  echo "Waiting for VS Code attach before registering runtime."
fi

cd "${STRATEGY_DIR}"
exec uv run --with debugpy python -Xfrozen_modules=off -m debugpy \
  --listen "${DEBUG_HOST}:${DEBUG_PORT}" \
  "${wait_args[@]}" \
  -m hushine_runtime_cli start \
  --config "${CONFIG_PATH}" \
  --runtime-channel-addr "${RUNTIME_CHANNEL_ADDR}" \
  --control-panel-addr "${CONTROL_PANEL_ADDR}" \
  --user-id "${USER_ID}"
