#!/usr/bin/env bash
# Phase D1 hosted strategy-runtime container smoke.
#
# This is the *container-only* smoke that does not require external services.
# It verifies:
#   1. The image we just built starts the Python entrypoint without crashing
#      on the import / config-load path (i.e. no missing module, no broken
#      symlink, no PYTHONPATH issue, no syntax error in our changes).
#   2. The runtime_client + control_panel_service stubs are importable.
#   3. The runtime config block parses cleanly with all defaults.
#
# Full mode=0 backtest end-to-end smoke (against a real core-service +
# TimescaleDB) is out of section 4 scope; that lands in D1 section 7
# verification.

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
echo "=== smoke 1: Python entrypoint module imports ==="
# Run a one-shot script inside the image; --rm cleans up.
docker run --rm --entrypoint python "${IMAGE}" -c "
import sys
print('python:', sys.version.split()[0])
from strategy_service.config import Config
cfg = Config.load('config.yaml')
cfg.apply_env_overrides()
print('config loaded:', cfg.server.grpc_addr)
print('runtime defaults: register=%s name=%s grpc_port=%d'
      % (cfg.runtime.register_with_control_panel,
         cfg.runtime.name,
         cfg.runtime.grpc_port))
from strategy_service.runtime_client import ControlPlaneClient, RuntimeIdentity
from strategy_service.gen import control_panel_service_pb2 as cp
print('proto:', cp.RegisterRuntimeRequest.DESCRIPTOR.full_name)
print('OK')
"

echo
echo "=== smoke 2: gRPC server entrypoint --help (compile-only) ==="
# We don't actually start the server here because _restore_running_sessions
# in StrategyServiceServicer.__init__ requires core-service to be
# reachable. That dependency exists in main HEAD (pre-existing) and the
# real cross-service smoke belongs to D1 section 7.
docker run --rm --entrypoint python "${IMAGE}" -c "
import argparse, run_grpc_server
print('run_grpc_server entrypoint loaded:', run_grpc_server.main.__name__)
"

echo
echo "All container smoke checks passed for ${IMAGE}."
echo "Full mode=0 backtest smoke needs core-service + TimescaleDB"
echo "and is exercised in D1 section 7 cross-service verification."
