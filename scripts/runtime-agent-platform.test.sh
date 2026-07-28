#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
SERVICE_DIR="${REPO_ROOT}/strategy-service"
BUILD_SCRIPT="${SERVICE_DIR}/scripts/build-runtime-agent-release.sh"
START_SCRIPT="${SERVICE_DIR}/scripts/start-runtime-agent.sh"
START_PS1="${SERVICE_DIR}/scripts/start-runtime-agent.ps1"
BARE_SCRIPT="${SERVICE_DIR}/scripts/start-bare-runtime-debugpy.sh"

for script in "${BUILD_SCRIPT}" "${START_SCRIPT}" "${BARE_SCRIPT}"; do
  if [[ ! -f "${script}" ]]; then
    echo "missing script: ${script}" >&2
    exit 1
  fi
  bash -n "${script}"
done

if [[ ! -f "${START_PS1}" ]]; then
  echo "missing Windows runtime-agent launcher: ${START_PS1}" >&2
  exit 1
fi

release_output="$("${BUILD_SCRIPT}" --dry-run --version test)"
for expected in \
  "darwin-amd64/runtime-agent" \
  "darwin-arm64/runtime-agent" \
  "linux-amd64/runtime-agent" \
  "linux-arm64/runtime-agent" \
  "windows-amd64/runtime-agent.exe" \
  "windows-arm64/runtime-agent.exe"; do
  if ! grep -Fq "${expected}" <<<"${release_output}"; then
    echo "release dry-run is missing ${expected}" >&2
    exit 1
  fi
done

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT
tmp_build="${tmpdir}/build"
mkdir -p "${tmp_build}"
(
  cd "${SERVICE_DIR}"
  CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
    go build -o "${tmp_build}/runtime-agent.exe" ./cmd/runtime-agent
  CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
    go test -c -o "${tmp_build}/runtimeagent.test.exe" ./internal/runtimeagent
)
test -s "${tmp_build}/runtime-agent.exe"
test -s "${tmp_build}/runtimeagent.test.exe"

mkdir -p "${tmpdir}/dist/runtime-agent/linux-amd64" "${tmpdir}/bin"
printf '#!/usr/bin/env bash\n' > "${tmpdir}/dist/runtime-agent/linux-amd64/runtime-agent"
printf '#!/usr/bin/env bash\n' > "${tmpdir}/bin/runtime-agent"
chmod +x "${tmpdir}/dist/runtime-agent/linux-amd64/runtime-agent"
chmod +x "${tmpdir}/bin/runtime-agent"

cmd_output="$(
  RUNTIME_AGENT_OS=linux \
  RUNTIME_AGENT_ARCH=x86_64 \
  RUNTIME_AGENT_DIST_DIR="${tmpdir}/dist/runtime-agent" \
  RUNTIME_AGENT_BIN_DIR="${tmpdir}/bin" \
  "${START_SCRIPT}" --print-command -- --config config.yaml --runtime-channel-addr 127.0.0.1:50055
)"
if ! grep -Fq "${tmpdir}/bin/runtime-agent" <<<"${cmd_output}"; then
  echo "launcher did not prefer local bin/runtime-agent" >&2
  exit 1
fi
if ! grep -Fq -- "--runtime-channel-addr" <<<"${cmd_output}"; then
  echo "launcher did not preserve runtime-agent arguments" >&2
  exit 1
fi

rm -f "${tmpdir}/bin/runtime-agent"
dist_cmd_output="$(
  RUNTIME_AGENT_OS=linux \
  RUNTIME_AGENT_ARCH=x86_64 \
  RUNTIME_AGENT_DIST_DIR="${tmpdir}/dist/runtime-agent" \
  RUNTIME_AGENT_BIN_DIR="${tmpdir}/bin" \
  "${START_SCRIPT}" --print-command -- --config config.yaml
)"
if ! grep -Fq "${tmpdir}/dist/runtime-agent/linux-amd64/runtime-agent" <<<"${dist_cmd_output}"; then
  echo "launcher did not fall back to linux-amd64 release binary" >&2
  exit 1
fi

missing_output="$(
  RUNTIME_AGENT_OS=darwin \
  RUNTIME_AGENT_ARCH=arm64 \
  RUNTIME_AGENT_DIST_DIR="${tmpdir}/missing-dist" \
  RUNTIME_AGENT_BIN_DIR="${tmpdir}/missing-bin" \
  "${START_SCRIPT}" --print-command -- --help 2>&1 || true
)"
if ! grep -Fq "runtime-agent binary not found for darwin-arm64" <<<"${missing_output}"; then
  echo "launcher should fail clearly when no binary exists" >&2
  exit 1
fi

required_ps1_literals=(
  'RuntimeInformation'
  'windows-amd64'
  'windows-arm64'
  'runtime-agent.exe'
)
for literal in "${required_ps1_literals[@]}"; do
  if ! grep -Fq "${literal}" "${START_PS1}"; then
    echo "Windows launcher is missing literal: ${literal}" >&2
    exit 1
  fi
done

if grep -Fq 'go run ./cmd/runtime-agent' "${BARE_SCRIPT}"; then
  echo "bare debug launcher must not default to go run" >&2
  exit 1
fi
if ! grep -Fq 'scripts/start-runtime-agent.sh' "${BARE_SCRIPT}"; then
  echo "bare debug launcher must delegate to start-runtime-agent.sh" >&2
  exit 1
fi
