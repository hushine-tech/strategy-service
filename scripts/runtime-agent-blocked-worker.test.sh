#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

command -v go >/dev/null 2>&1 || {
  echo "go is required" >&2
  exit 1
}
test -x ".venv/bin/python" || {
  echo "strategy-service .venv/bin/python is required; run 'uv sync --frozen --extra dev' first" >&2
  exit 1
}

export HUSHINE_BLOCKED_WORKER_SECONDS="${HUSHINE_BLOCKED_WORKER_SECONDS:-660}"
export HUSHINE_BLOCKED_WORKER_OBSERVE_SECONDS="${HUSHINE_BLOCKED_WORKER_OBSERVE_SECONDS:-600}"

timeout_seconds="$(
  awk -v block="${HUSHINE_BLOCKED_WORKER_SECONDS}" \
    'BEGIN { printf "%d", block + 90 }'
)"
go test -tags=integration ./internal/runtimeagent \
  -run TestBlockedWorkerKeepsRuntimeHeartbeatAndCanBeReplaced \
  -count=1 \
  -timeout "${timeout_seconds}s" \
  -v
