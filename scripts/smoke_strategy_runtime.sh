#!/usr/bin/env bash
# RuntimeChannel strategy-runtime container smoke.
#
# This is the *container-only* smoke that does not require external services.
# It verifies:
#   1. The image imports the Python config loader.
#   2. RuntimeChannel proto stubs are importable.
#   3. The Go runtime-agent entrypoint exposes help.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

TAG="${1:-dev}"
IMAGE="hushine/strategy-runtime:${TAG}"

if ! docker image inspect "${IMAGE}" >/dev/null 2>&1; then
    echo "image ${IMAGE} not found; building first…"
    "${SCRIPT_DIR}/build_strategy_runtime.sh" "${TAG}"
fi

echo
echo "=== smoke 1: Python package imports ==="
# Run a one-shot script inside the image; --rm cleans up.
docker run --rm --entrypoint python "${IMAGE}" -c "
import sys
print('python:', sys.version.split()[0])
from strategy_service.gen import control_panel_service_pb2 as cp
from strategy_service.gen import runtime_worker_pb2 as worker
from strategy_service.session_worker_entry import main as worker_main
print('runtime proto:', cp.RuntimeHello.DESCRIPTOR.full_name)
print('worker proto:', worker.WorkerFrame.DESCRIPTOR.full_name)
print('worker entry:', worker_main.__name__)
print('OK')
"

echo
echo "=== smoke 2: runtime-agent help ==="
docker run --rm --entrypoint ./bin/runtime-agent "${IMAGE}" --help

echo
echo "All container smoke checks passed for ${IMAGE}."
