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
  'WORKER_PYTHON="${HUSHINE_WORKER_PYTHON:-}"'
  '"${WORKER_PYTHON}" -I -'
  'metadata.distribution(distribution)'
  'HUSHINE_WORKER_PYTHON=${WORKER_PYTHON}'
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
  'PYTHONPATH=${STRATEGY_DIR}:${REPO_ROOT}/strategy-library'
  'uv run python'
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
      DEBUG_PORT|DEBUG_WAIT|HOME|HUSHINE_RUNTIME_CONTRACT_SHA256|HUSHINE_RUNTIME_HOSTED_PYTHON|HUSHINE_RUNTIME_IMAGE_BUILD_ID|HUSHINE_RUNTIME_PROFILE_NAME|HUSHINE_RUNTIME_PROFILE_VERSION|HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS|HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT|HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT|HUSHINE_WORKER_PYTHON|OLDPWD|PATH|PWD|RUNTIME_AGENT_CONTROL_ADDR|RUNTIME_CHANNEL_GRPC_ADDR|RUNTIME_CHANNEL_TLS_ENABLED|RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE|RUNTIME_CHANNEL_TLS_SERVER_NAME|RUNTIME_NAME|RUNTIME_RUNTIME_ID|SHLVL|TMPDIR|USER|_) ;;
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
    "DEBUG_WAIT=${debug_wait}" \
    'HUSHINE_RUNTIME_PROFILE_NAME=platform-python-3.13' \
    'HUSHINE_RUNTIME_PROFILE_VERSION=1.0.0' \
    'HUSHINE_RUNTIME_CONTRACT_SHA256=8457b3c35618558fc8bfc74d4135b7eb52e00c33a8c9a49d202830f3fd5b62c5' \
    'HUSHINE_RUNTIME_HOSTED_PYTHON=3.13' \
    'HUSHINE_RUNTIME_PUBLIC_IMPORT_ROOTS=dateutil,google,grpc,numpy,pandas,pydantic,requests,yaml' \
    'HUSHINE_RUNTIME_STRATEGY_SERVICE_COMMIT=local-dev' \
    'HUSHINE_RUNTIME_STRATEGY_LIBRARY_COMMIT=local-dev' \
    'HUSHINE_RUNTIME_IMAGE_BUILD_ID=local-dev' \
    'HUSHINE_WORKER_PYTHON='
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

missing_start_marker="${tmp_dir}/missing-started"
missing_start="${tmp_dir}/missing-start-runtime-agent"
{
  printf '%s\n' '#!/usr/bin/env bash'
  printf 'touch %q\n' "${missing_start_marker}"
} > "${missing_start}"
chmod +x "${missing_start}"
if env -i \
  PATH="${PATH}" \
  HOME="${HOME}" \
  USER="${USER:-hushine-test}" \
  TMPDIR="${TMPDIR:-/tmp}" \
  RUNTIME_CHANNEL_TLS_ENABLED=false \
  HUSHINE_WORKER_PYTHON="${tmp_dir}/missing-venv/bin/python" \
  RUNTIME_AGENT_START_SCRIPT="${missing_start}" \
  CONFIG_PATH="${REPO_ROOT}/strategy-service/config.yaml" \
  bash "${RUNTIME_SCRIPT}" --user-id 6 --platform-host 127.0.0.1 > "${tmp_dir}/missing.out" 2>&1
then
  echo "bare launcher accepted a missing guarded worker venv" >&2
  exit 1
fi
if [[ -e "${missing_start_marker}" ]]; then
  echo "runtime-agent was launched before guarded venv validation" >&2
  exit 1
fi

unguarded_python="${tmp_dir}/python-outside-venv-layout"
{
  printf '%s\n' '#!/usr/bin/env bash'
  printf 'exec %q "$@"\n' "${REPO_ROOT}/strategy-service/.venv/bin/python"
} > "${unguarded_python}"
chmod +x "${unguarded_python}"
unguarded_start_marker="${tmp_dir}/unguarded-started"
unguarded_start="${tmp_dir}/unguarded-start-runtime-agent"
{
  printf '%s\n' '#!/usr/bin/env bash'
  printf 'touch %q\n' "${unguarded_start_marker}"
} > "${unguarded_start}"
chmod +x "${unguarded_start}"
if env -i \
  PATH="${PATH}" \
  HOME="${HOME}" \
  USER="${USER:-hushine-test}" \
  TMPDIR="${TMPDIR:-/tmp}" \
  RUNTIME_CHANNEL_TLS_ENABLED=false \
  HUSHINE_WORKER_PYTHON="${unguarded_python}" \
  RUNTIME_AGENT_START_SCRIPT="${unguarded_start}" \
  CONFIG_PATH="${REPO_ROOT}/strategy-service/config.yaml" \
  bash "${RUNTIME_SCRIPT}" --user-id 6 --platform-host 127.0.0.1 > "${tmp_dir}/unguarded.out" 2>&1
then
  echo "bare launcher accepted Python outside the guarded venv layout" >&2
  exit 1
fi
if [[ -e "${unguarded_start_marker}" ]]; then
  echo "runtime-agent was launched with Python outside the guarded venv layout" >&2
  exit 1
fi

state_keys="$(sed -n 's/^export \([^=]*\)=.*/\1/p' "${tmp_dir}/runtime.env" | LC_ALL=C sort)"
expected_state_keys="$(printf '%s\n' \
  DEBUG_HOST DEBUG_PORT RUNTIME_AGENT_CONTROL_ADDR RUNTIME_AGENT_CONTROL_URL \
  RUNTIME_NAME RUNTIME_RUNTIME_ID USER_ID | LC_ALL=C sort)"
if [[ "${state_keys}" != "${expected_state_keys}" ]]; then
  echo "bare runtime state keys = ${state_keys}" >&2
  exit 1
fi
