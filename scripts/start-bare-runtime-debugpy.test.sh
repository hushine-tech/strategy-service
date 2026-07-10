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
  '--core-service-addr|--core-addr)'
  '--control-panel-addr|--control-addr)'
  '--runtime-channel-addr|--runtime-addr)'
  'CORE_SERVICE_GRPC_ADDR="${CORE_SERVICE_ADDR}"'
  'CONTROL_PANEL_SERVICE_GRPC_ADDR="${CONTROL_PANEL_ADDR}"'
  'RUNTIME_CHANNEL_GRPC_ADDR="${RUNTIME_CHANNEL_ADDR}"'
  'RUNTIME_RUNTIME_ID="${RUNTIME_ID}"'
  'RUNTIME_AGENT_CONTROL_ADDR="${RUNTIME_AGENT_CONTROL_ADDR}"'
  'RUNTIME_BARE_STATE_FILE="${RUNTIME_BARE_STATE_FILE}"'
  'RUNTIME_AGENT_CONTROL_URL="http://${RUNTIME_AGENT_CONTROL_ADDR}"'
  'previous_runtime_id="$(read_state_value RUNTIME_RUNTIME_ID)"'
  'RUNTIME_ID="${RUNTIME_RUNTIME_ID:-${previous_runtime_id:-bare-${USER_ID}-$(random_suffix 8)}}"'
  'paths = bare_bootstrap.load_existing_runtime_mtls_bundle(output_dir, runtime_id=runtime_id)'
  'cat > "${RUNTIME_BARE_STATE_FILE}" <<EOF'
  'CONFIG_PATH="./config.yaml"'
  'config file not found: ${CONFIG_PATH}'
  'LOG_TRACING_ENDPOINT="http://${PLATFORM_HOST}:4318"'
  'cd "${STRATEGY_DIR}"'
  'RUNTIME_AGENT_START_SCRIPT="${RUNTIME_AGENT_START_SCRIPT:-${STRATEGY_DIR}/scripts/start-runtime-agent.sh}"'
  'exec "${RUNTIME_AGENT_START_SCRIPT}" --'
  '--user-id "${USER_ID}"'
)

for literal in "${required_literals[@]}"; do
  if ! grep -Fq -- "${literal}" "${RUNTIME_SCRIPT}"; then
    echo "missing bare debugpy launcher literal: ${literal}" >&2
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
