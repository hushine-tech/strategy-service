#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PRINT_COMMAND=0

usage() {
  cat <<'EOF'
Usage:
  scripts/start-runtime-agent.sh [--print-command] -- [runtime-agent args...]

Starts an already-built runtime-agent for the current OS/architecture.
This launcher does not build binaries. Release binaries live under:
  dist/runtime-agent/<os>-<arch>/runtime-agent[.exe]

For local source development, run `make build` once, or set
RUNTIME_AGENT_ALLOW_GO_RUN=1 to explicitly fall back to `go run`.
EOF
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --print-command)
      PRINT_COMMAND=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    *)
      break
      ;;
  esac
done

normalize_os() {
  local value="$1"
  case "${value}" in
    Darwin|darwin|mac|macos) echo "darwin" ;;
    Linux|linux) echo "linux" ;;
    MINGW*|MSYS*|CYGWIN*|Windows_NT|windows) echo "windows" ;;
    *)
      echo "unsupported OS: ${value}" >&2
      return 1
      ;;
  esac
}

normalize_arch() {
  local value="$1"
  case "${value}" in
    x86_64|amd64|AMD64) echo "amd64" ;;
    arm64|aarch64|ARM64) echo "arm64" ;;
    *)
      echo "unsupported architecture: ${value}" >&2
      return 1
      ;;
  esac
}

raw_os="${RUNTIME_AGENT_OS:-$(uname -s)}"
raw_arch="${RUNTIME_AGENT_ARCH:-$(uname -m)}"
runtime_os="$(normalize_os "${raw_os}")"
runtime_arch="$(normalize_arch "${raw_arch}")"

binary_name="runtime-agent"
if [[ "${runtime_os}" == "windows" ]]; then
  binary_name="runtime-agent.exe"
fi

dist_dir="${RUNTIME_AGENT_DIST_DIR:-${SERVICE_DIR}/dist/runtime-agent}"
bin_dir="${RUNTIME_AGENT_BIN_DIR:-${SERVICE_DIR}/bin}"
candidates=()
if [[ -n "${RUNTIME_AGENT_BIN:-}" ]]; then
  candidates+=("${RUNTIME_AGENT_BIN}")
fi
candidates+=(
  "${bin_dir}/${binary_name}"
  "${dist_dir}/${runtime_os}-${runtime_arch}/${binary_name}"
)

cmd=()
for candidate in "${candidates[@]}"; do
  if [[ -x "${candidate}" ]]; then
    cmd=("${candidate}")
    break
  fi
done

if [[ "${#cmd[@]}" -eq 0 ]]; then
  allow_go_run="${RUNTIME_AGENT_ALLOW_GO_RUN:-0}"
  if [[ "${allow_go_run}" == "1" || "${allow_go_run}" == "true" || "${allow_go_run}" == "yes" ]]; then
    cmd=(go run ./cmd/runtime-agent)
  else
    echo "runtime-agent binary not found for ${runtime_os}-${runtime_arch}" >&2
    echo "searched:" >&2
    for candidate in "${candidates[@]}"; do
      echo "  ${candidate}" >&2
    done
    echo "run 'make build' for local development, or package release binaries with scripts/build-runtime-agent-release.sh" >&2
    exit 1
  fi
fi

cd "${SERVICE_DIR}"
if [[ "${PRINT_COMMAND}" == "1" ]]; then
  printf '%q ' "${cmd[@]}" "$@"
  printf '\n'
  exit 0
fi
exec "${cmd[@]}" "$@"
