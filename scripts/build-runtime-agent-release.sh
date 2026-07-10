#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVICE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DIST_DIR="${RUNTIME_AGENT_DIST_DIR:-${SERVICE_DIR}/dist/runtime-agent}"
VERSION="dev"
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage:
  scripts/build-runtime-agent-release.sh [--version VERSION] [--dry-run]

Builds release runtime-agent binaries for:
  darwin-amd64, darwin-arm64
  linux-amd64, linux-arm64
  windows-amd64, windows-arm64

The script is for release packaging only. Normal start scripts do not build.
EOF
}

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:?missing value for --version}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

platforms=(
  "darwin amd64"
  "darwin arm64"
  "linux amd64"
  "linux arm64"
  "windows amd64"
  "windows arm64"
)

echo "runtime-agent release version: ${VERSION}"
echo "output: ${DIST_DIR}"

for platform in "${platforms[@]}"; do
  read -r goos goarch <<<"${platform}"
  suffix=""
  if [[ "${goos}" == "windows" ]]; then
    suffix=".exe"
  fi
  out="${DIST_DIR}/${goos}-${goarch}/runtime-agent${suffix}"
  cmd=(
    env
    CGO_ENABLED=0
    GOOS="${goos}"
    GOARCH="${goarch}"
    go build
    -trimpath
    -ldflags "-s -w"
    -o "${out}"
    ./cmd/runtime-agent
  )
  if [[ "${DRY_RUN}" == "1" ]]; then
    printf '%q ' "${cmd[@]}"
    printf '\n'
    continue
  fi
  mkdir -p "$(dirname "${out}")"
  (
    cd "${SERVICE_DIR}"
    "${cmd[@]}"
  )
  echo "built ${out}"
done
