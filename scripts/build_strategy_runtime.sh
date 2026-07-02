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
DEFAULT_IMAGE="${IMAGE_PREFIX}:${VERSION}"

echo "Building ${EXECUTOR_IMAGE} from ${REPO_ROOT}"
docker build \
    --target executor \
    -f "${SERVICE_DIR}/Dockerfile" \
    -t "${EXECUTOR_IMAGE}" \
    -t "${IMAGE_PREFIX}:executor" \
    -t "${IMAGE_PREFIX}:dev" \
    -t "${DEFAULT_IMAGE}" \
    "${REPO_ROOT}"

echo
echo "Built:"
echo "  ${EXECUTOR_IMAGE}"
echo "  ${DEFAULT_IMAGE}  # default tag for existing control-panel configs"
echo
echo "Quick-run examples:"
echo
echo "  # 1. Hosted executor mode is launched by control-panel DockerProvisioner:"
echo "  #    RuntimeChannel only, no published strategy gRPC host port."
echo
echo "  # 2. Boot as a user's self-hosted executor runtime:"
echo "  docker run --rm \\"
echo "    -v \$HOME/.hushine/runtime.cred:/etc/hushine/runtime.cred:ro \\"
echo "    -e RUNTIME_CREDENTIAL_PATH=/etc/hushine/runtime.cred \\"
echo "    -e RUNTIME_CHANNEL_GRPC_ADDR=host.docker.internal:50055 \\"
echo "    ${EXECUTOR_IMAGE}"
echo
echo "  RuntimeChannel startup ignores account/order/Kafka/database"
echo "  config and uses only RuntimeChannel platform proxy calls."
echo
echo "  # 3. Local bare debug on a machine with uv installed:"
echo "  uv run hushine-runtime start --config config.yaml --runtime-channel-addr 127.0.0.1:50055 --user-id 123"
