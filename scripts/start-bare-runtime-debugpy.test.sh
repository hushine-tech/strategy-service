#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_SCRIPT="${REPO_ROOT}/strategy-service/scripts/start-bare-runtime-debugpy.sh"
RESTART_SCRIPT="${REPO_ROOT}/strategy-service/scripts/restart-bare-worker-session.sh"
ATTACH_EXAMPLE="${REPO_ROOT}/strategy-service/scripts/vscode-bare-runtime-attach.launch.json"
README="${REPO_ROOT}/strategy-service/README.md"
OUTER_SCRIPT="${REPO_ROOT}/scripts/start-bare-runtime-debugpy.sh"

if [[ ! -f "${RUNTIME_SCRIPT}" ]]; then
  echo "missing runtime-owned bare debugpy launcher: ${RUNTIME_SCRIPT}" >&2
  exit 1
fi

if [[ ! -f "${RESTART_SCRIPT}" ]]; then
  echo "missing runtime-owned bare worker restart launcher: ${RESTART_SCRIPT}" >&2
  exit 1
fi

if [[ -e "${OUTER_SCRIPT}" ]]; then
  echo "bare debugpy launcher must live under strategy-service/scripts, not outer scripts: ${OUTER_SCRIPT}" >&2
  exit 1
fi

if [[ ! -f "${ATTACH_EXAMPLE}" ]]; then
  echo "missing VS Code bare runtime attach example: ${ATTACH_EXAMPLE}" >&2
  exit 1
fi

bash -n "${RUNTIME_SCRIPT}"
bash -n "${RESTART_SCRIPT}"

required_literals=(
  'STRATEGY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"'
  'REPO_ROOT="$(cd "${STRATEGY_DIR}/.." && pwd)"'
  '--platform-host|--host)'
  '--control-panel-addr|--control-addr)'
  '--runtime-channel-addr|--runtime-addr)'
  'RUNTIME_CHANNEL_GRPC_ADDR=${RUNTIME_CHANNEL_ADDR}'
  'exec env -i'
  'RUNTIME_RUNTIME_ID=${RUNTIME_ID}'
  'RUNTIME_AGENT_CONTROL_ADDR=${RUNTIME_AGENT_CONTROL_ADDR}'
  'RUNTIME_BARE_STATE_FILE="${RUNTIME_BARE_STATE_FILE}"'
  'RUNTIME_AGENT_CONTROL_URL="http://${RUNTIME_AGENT_CONTROL_ADDR}"'
  'previous_runtime_id="$(read_state_value RUNTIME_RUNTIME_ID)"'
  'RUNTIME_ID="${RUNTIME_RUNTIME_ID:-${previous_runtime_id:-bare-${USER_ID}-$(random_suffix 8)}}"'
  'paths = bare_bootstrap.load_existing_runtime_mtls_bundle(output_dir, runtime_id=runtime_id)'
  'address="${CONTROL_PANEL_ADDR}"'
  'cat > "${RUNTIME_BARE_STATE_FILE}" <<EOF'
  'CONFIG_PATH="./config.yaml"'
  'config file not found: ${CONFIG_PATH}'
  'cd "${STRATEGY_DIR}"'
  'RUNTIME_AGENT_START_SCRIPT="${RUNTIME_AGENT_START_SCRIPT:-${STRATEGY_DIR}/scripts/start-runtime-agent.sh}"'
  '--user-id "${USER_ID}"'
)

for literal in "${required_literals[@]}"; do
  if ! grep -Fq -- "${literal}" "${RUNTIME_SCRIPT}"; then
    echo "missing bare debugpy launcher literal: ${literal}" >&2
    exit 1
  fi
done

forbidden_literals=(
  '--core-service-addr|--core-addr)'
  '--order-service-addr|--order-addr)'
  'export CORE_SERVICE_GRPC_ADDR='
  'export ORDER_SERVICE_GRPC_ADDR='
  'export CONTROL_PANEL_SERVICE_GRPC_ADDR='
  'export MARKET_DATA_CONTROL_PANEL_GRPC_ADDR='
  'CORE_SERVICE_ADDR='
  'ORDER_SERVICE_ADDR='
  'MARKET_DATA_CONTROL_PANEL_ADDR='
  'LOG_TRACING_ENDPOINT='
  'export NO_PROXY='
  'export no_proxy='
  'export PLATFORM_HOST='
  '--control-panel-addr "${CONTROL_PANEL_ADDR}"'
  'export CORE_SERVICE_GRPC_ADDR="${CORE_SERVICE_ADDR}"'
  'export CONTROL_PANEL_SERVICE_GRPC_ADDR="${CONTROL_PANEL_ADDR}"'
)
for literal in "${forbidden_literals[@]}"; do
  if grep -Fq -- "${literal}" "${RUNTIME_SCRIPT}"; then
    echo "forbidden bare launcher literal remains: ${literal}" >&2
    exit 1
  fi
done

if grep -Fq 'CONFIG_PATH="./config.local.yaml"' "${RUNTIME_SCRIPT}"; then
  echo "bare debugpy launcher must not default to legacy config.local.yaml; pass CONFIG_PATH explicitly when needed" >&2
  exit 1
fi

readme_literals=(
  'DEBUG_WAIT=0 scripts/start-bare-runtime-debugpy.sh 6 192.168.88.6'
  'scripts/restart-bare-worker-session.sh'
  'There is no `attach.json`.'
  'scripts/vscode-bare-runtime-attach.launch.json'
)

for literal in "${readme_literals[@]}"; do
  if ! grep -Fq -- "${literal}" "${README}"; then
    echo "missing README bare debugpy literal: ${literal}" >&2
    exit 1
  fi
done

tmp_dir="$(mktemp -d)"
trap 'rm -rf "${tmp_dir}"' EXIT
fake_start="${tmp_dir}/start-runtime-agent"
env_out="${tmp_dir}/agent.env"
{
  printf '%s\n' '#!/usr/bin/env bash'
  printf 'env | sort > %q\n' "${env_out}"
} > "${fake_start}"
chmod +x "${fake_start}"

for debug_wait in 0 1 off; do
  launcher_out="${tmp_dir}/launcher-${debug_wait}.out"
  env -i \
    PATH="${PATH}" \
    HOME="${HOME}" \
    USER="${USER:-hushine-test}" \
    TMPDIR="${TMPDIR:-/tmp}" \
    DATABASE_PASSWORD=parent-db-secret \
    KAFKA_BROKERS=parent-kafka-secret \
    CORE_SERVICE_GRPC_ADDR=parent-core-secret \
    ORDER_SERVICE_GRPC_ADDR=parent-order-secret \
    CONTROL_PANEL_SERVICE_GRPC_ADDR=parent-control-secret \
    MARKET_DATA_CONTROL_PANEL_GRPC_ADDR=parent-market-secret \
    QUANT_HANDLER_JWT_SECRET=parent-jwt-secret \
    LOG_TRACING_ENDPOINT=http://parent-tracing:4318 \
    http_proxy=http://parent-proxy \
    no_proxy=parent.internal \
    RUNTIME_CHANNEL_TLS_ENABLED=false \
    DEBUG_WAIT="${debug_wait}" \
    RUNTIME_BARE_BOOTSTRAP_DIR="${tmp_dir}/bootstrap" \
    RUNTIME_BARE_STATE_FILE="${tmp_dir}/runtime.env" \
    RUNTIME_AGENT_START_SCRIPT="${fake_start}" \
    CONFIG_PATH="${REPO_ROOT}/strategy-service/config.yaml" \
    bash "${RUNTIME_SCRIPT}" --user-id 6 --platform-host 127.0.0.1 > "${launcher_out}"

  if [[ "${debug_wait}" == "1" ]]; then
    if ! grep -Fq 'Worker will wait for VS Code attach before executing a session.' "${launcher_out}"; then
      echo "missing debugger wait message for DEBUG_WAIT=${debug_wait}" >&2
      exit 1
    fi
  elif grep -Fq 'Worker will wait for VS Code attach before executing a session.' "${launcher_out}"; then
    echo "unexpected debugger wait message for DEBUG_WAIT=${debug_wait}" >&2
    exit 1
  fi

  while IFS='=' read -r key _; do
    case "${key}" in
      DEBUG_PORT|DEBUG_WAIT|HOME|OLDPWD|PATH|PWD|PYTHONPATH|RUNTIME_AGENT_CONTROL_ADDR|RUNTIME_CHANNEL_GRPC_ADDR|RUNTIME_CHANNEL_TLS_ENABLED|RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE|RUNTIME_CHANNEL_TLS_SERVER_NAME|RUNTIME_NAME|RUNTIME_RUNTIME_ID|SHLVL|TMPDIR|USER|_) ;;
      *)
        echo "unexpected key reached runtime-agent: ${key}" >&2
        exit 1
        ;;
    esac
  done < "${env_out}"

  for required in \
    'RUNTIME_CHANNEL_GRPC_ADDR=127.0.0.1:50055' \
    'RUNTIME_CHANNEL_TLS_ENABLED=false' \
    'RUNTIME_NAME=' \
    'RUNTIME_RUNTIME_ID=' \
    'RUNTIME_AGENT_CONTROL_ADDR=' \
    'DEBUG_PORT=5678' \
    "DEBUG_WAIT=${debug_wait}"
  do
    if [[ "${required}" == *= ]]; then
      if ! grep -q "^${required}" "${env_out}"; then
        echo "missing required runtime-agent env prefix: ${required}" >&2
        exit 1
      fi
    else
      if ! grep -Fxq "${required}" "${env_out}"; then
        echo "missing required runtime-agent env: ${required}" >&2
        exit 1
      fi
    fi
  done
done

state_keys="$(sed -n 's/^export \([^=]*\)=.*/\1/p' "${tmp_dir}/runtime.env" | LC_ALL=C sort)"
expected_state_keys="$(printf '%s\n' \
  DEBUG_HOST DEBUG_PORT RUNTIME_AGENT_CONTROL_ADDR RUNTIME_AGENT_CONTROL_URL \
  RUNTIME_NAME RUNTIME_RUNTIME_ID USER_ID | LC_ALL=C sort)"
if [[ "${state_keys}" != "${expected_state_keys}" ]]; then
  echo "bare runtime state keys = ${state_keys}" >&2
  exit 1
fi
