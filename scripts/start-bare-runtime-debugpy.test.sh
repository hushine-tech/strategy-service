#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
RUNTIME_SCRIPT="${REPO_ROOT}/strategy-service/scripts/start-bare-runtime-debugpy.sh"
OUTER_SCRIPT="${REPO_ROOT}/scripts/start-bare-runtime-debugpy.sh"

if [[ ! -f "${RUNTIME_SCRIPT}" ]]; then
  echo "missing runtime-owned bare debugpy launcher: ${RUNTIME_SCRIPT}" >&2
  exit 1
fi

if [[ -e "${OUTER_SCRIPT}" ]]; then
  echo "bare debugpy launcher must live under strategy-service/scripts, not outer scripts: ${OUTER_SCRIPT}" >&2
  exit 1
fi

bash -n "${RUNTIME_SCRIPT}"

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
  'cd "${STRATEGY_DIR}"'
  'hushine_runtime_cli start'
  '--user-id "${USER_ID}"'
)

for literal in "${required_literals[@]}"; do
  if ! grep -Fq -- "${literal}" "${RUNTIME_SCRIPT}"; then
    echo "missing bare debugpy launcher literal: ${literal}" >&2
    exit 1
  fi
done
