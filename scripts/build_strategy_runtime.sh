#!/usr/bin/env bash
# Build strategy-runtime container images.
#
# Build context = repo root, so the Dockerfile can COPY both
# strategy-service/ and strategy-library/.
#
# Usage:
#   ./scripts/build_strategy_runtime.sh           # tag dev
#   ./scripts/build_strategy_runtime.sh v0.1.0    # tag custom

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_ROOT="$(cd "${SERVICE_DIR}/.." && pwd)"

VERSION="${1:-dev}"
IMAGE_PREFIX="${IMAGE_PREFIX:-hushine/strategy-runtime}"

EXECUTOR_IMAGE="${IMAGE_PREFIX}:executor-${VERSION}"
DEBUGGER_IMAGE="${IMAGE_PREFIX}:debugger-${VERSION}"
LEGACY_IMAGE="${IMAGE_PREFIX}:${VERSION}"

echo "Building ${EXECUTOR_IMAGE} from ${REPO_ROOT}"
docker build \
    --target executor \
    -f "${SERVICE_DIR}/Dockerfile" \
    -t "${EXECUTOR_IMAGE}" \
    -t "${IMAGE_PREFIX}:executor" \
    -t "${LEGACY_IMAGE}" \
    "${REPO_ROOT}"

echo
echo "Building ${DEBUGGER_IMAGE} from ${REPO_ROOT}"
docker build \
    --target debugger \
    -f "${SERVICE_DIR}/Dockerfile" \
    -t "${DEBUGGER_IMAGE}" \
    -t "${IMAGE_PREFIX}:debugger" \
    "${REPO_ROOT}"

echo
echo "Built:"
echo "  ${EXECUTOR_IMAGE}"
echo "  ${DEBUGGER_IMAGE}"
echo "  ${LEGACY_IMAGE}  # compatibility tag for existing control-panel configs"
echo
echo "Quick-run examples:"
echo
echo "  # 1. Boot without registration (legacy direct-dial deployments):"
echo "  docker run --rm -p 50053:50053 ${EXECUTOR_IMAGE}"
echo
echo "  # 2. Hosted executor mode is launched by control-panel DockerProvisioner:"
echo "  #    outbound RuntimeChannel only, no published strategy gRPC host port."
echo
echo "  # 3. Boot as a user's self-hosted executor runtime (RuntimeChannel only):"
echo "  docker run --rm \\"
echo "    -v \$HOME/.hushine/runtime.cred:/etc/hushine/runtime.cred:ro \\"
echo "    -e RUNTIME_INGRESS_MODE=outbound \\"
echo "    -e RUNTIME_CREDENTIAL_PATH=/etc/hushine/runtime.cred \\"
echo "    -e CONTROL_PANEL_SERVICE_GRPC_ADDR=host.docker.internal:50054 \\"
echo "    ${EXECUTOR_IMAGE}"
echo
echo "  # 4. Boot as a user's self-hosted debugger runtime:"
echo "  mkdir -p \$HOME/hushine-debug-workspace"
echo "  docker run --rm -it \\"
echo "    -v \$HOME/.hushine/runtime.cred:/etc/hushine/runtime.cred:ro \\"
echo "    -v \$HOME/hushine-debug-workspace:/workspace \\"
echo "    -e RUNTIME_INGRESS_MODE=outbound \\"
echo "    -e RUNTIME_CREDENTIAL_PATH=/etc/hushine/runtime.cred \\"
echo "    -e CONTROL_PANEL_SERVICE_GRPC_ADDR=host.docker.internal:50054 \\"
echo "    ${DEBUGGER_IMAGE}"
echo "  # Optional for VSCode debugpy attach only: -p 127.0.0.1:5678:5678"
echo "  # PyCharm Debug Server does not need published container ports."
echo
echo "  In outbound mode the process ignores account/order/Kafka/database"
echo "  config and uses only RuntimeChannel platform proxy calls."
echo "  After loading a debugger dataset from the Account page, enter the"
echo "  debugger container and run: hushine-debug replay"
