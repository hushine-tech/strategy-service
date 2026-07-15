#!/usr/bin/env bash
# Generate Python + Go gRPC stubs from all proto files.
# Usage: ./generate_proto.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/strategy_service/gen"
GO_OUT_DIR="${SCRIPT_DIR}/gen/strategyv1"
PORTFOLIO_GO_OUT_DIR="${SCRIPT_DIR}/gen/portfoliov1"
RUNTIME_WORKER_GO_OUT_DIR="${SCRIPT_DIR}/gen/runtimeworkerv1"
CONTROL_PANEL_GO_OUT_DIR="${SCRIPT_DIR}/gen/controlpanelv1"

mkdir -p "$OUT_DIR" "$GO_OUT_DIR" "$PORTFOLIO_GO_OUT_DIR" "$RUNTIME_WORKER_GO_OUT_DIR" "$CONTROL_PANEL_GO_OUT_DIR"

if [ -n "${HOME:-}" ]; then
  PATH="${HOME}/go/bin:${PATH}"
  export PATH
fi

PYTHON_BIN="${PYTHON:-}"
if [ -z "$PYTHON_BIN" ]; then
  if ! PYTHON_BIN="$(command -v python3)"; then
    echo "python3 is required to generate Python protobuf stubs" >&2
    exit 1
  fi
fi
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  echo "Python interpreter is unavailable: $PYTHON_BIN" >&2
  exit 1
fi
if ! "$PYTHON_BIN" -c 'import grpc_tools.protoc' >/dev/null 2>&1; then
  echo "grpc_tools.protoc is unavailable from: $PYTHON_BIN" >&2
  exit 1
fi

for required_tool in protoc protoc-gen-go protoc-gen-go-grpc; do
  if ! command -v "$required_tool" >/dev/null 2>&1; then
    echo "$required_tool is required to generate Go protobuf stubs" >&2
    exit 1
  fi
done
PROTOC_BIN="$(command -v protoc)"

sed_in_place() {
  if sed --version >/dev/null 2>&1; then
    sed -i "$@"
  else
    sed -i '' "$@"
  fi
}

# --- core-service proto (Python stubs only) ---
ACCT_PROTO_SRC="${SCRIPT_DIR}/../core-service/proto"

"$PYTHON_BIN" -m grpc_tools.protoc \
  -I "$ACCT_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$ACCT_PROTO_SRC/portfolio_service.proto"

sed_in_place 's/^import portfolio_service_pb2/from . import portfolio_service_pb2/' "$OUT_DIR/portfolio_service_pb2_grpc.py"

"$PROTOC_BIN" \
  -I "$ACCT_PROTO_SRC" \
  --go_out="$PORTFOLIO_GO_OUT_DIR" --go_opt=paths=source_relative \
  --go_opt=Mportfolio_service.proto=github.com/hushine-tech/strategy-service/gen/portfoliov1 \
  --go-grpc_out="$PORTFOLIO_GO_OUT_DIR" --go-grpc_opt=paths=source_relative \
  --go-grpc_opt=Mportfolio_service.proto=github.com/hushine-tech/strategy-service/gen/portfoliov1 \
  "$ACCT_PROTO_SRC/portfolio_service.proto"

# --- order.v1 proto (Python stubs only) ---
ORDER_PROTO_SRC="${SCRIPT_DIR}/../core-service/proto"

"$PYTHON_BIN" -m grpc_tools.protoc \
  -I "$ORDER_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$ORDER_PROTO_SRC/order_service.proto"

sed_in_place 's/^import order_service_pb2/from . import order_service_pb2/' "$OUT_DIR/order_service_pb2_grpc.py"

# --- control-panel-service proto (Python stubs only) ---
# Phase D1: strategy-runtime registers itself + heartbeats with control-plane.
CP_PROTO_SRC="${SCRIPT_DIR}/../control-panel-service/proto"
STRAT_PROTO_SRC="${SCRIPT_DIR}/proto"

"$PYTHON_BIN" -m grpc_tools.protoc \
  -I "$CP_PROTO_SRC" \
  -I "$STRAT_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$CP_PROTO_SRC/control_panel_service.proto"

sed_in_place 's/^import control_panel_service_pb2/from . import control_panel_service_pb2/' "$OUT_DIR/control_panel_service_pb2_grpc.py"
sed_in_place 's/^import strategy_service_pb2/from . import strategy_service_pb2/' "$OUT_DIR/control_panel_service_pb2.py"
sed_in_place 's/^import strategy_service_pb2/from . import strategy_service_pb2/' "$OUT_DIR/control_panel_service_pb2_grpc.py"

# Go stubs for the runtime-agent. Keep these local to strategy-service so the
# agent does not depend on the control-panel-service module at runtime.
"$PROTOC_BIN" \
  -I "$CP_PROTO_SRC" \
  -I "$STRAT_PROTO_SRC" \
  --go_out="$CONTROL_PANEL_GO_OUT_DIR" --go_opt=paths=source_relative \
  --go_opt=Mcontrol_panel_service.proto=github.com/hushine-tech/strategy-service/gen/controlpanelv1 \
  --go-grpc_out="$CONTROL_PANEL_GO_OUT_DIR" --go-grpc_opt=paths=source_relative \
  --go-grpc_opt=Mcontrol_panel_service.proto=github.com/hushine-tech/strategy-service/gen/controlpanelv1 \
  "$CP_PROTO_SRC/control_panel_service.proto"

# Phase D2: market-data control-plane RPCs moved out of core-service into
# control-panel-service (package controlpanel.marketdata.v1). Strategy-service
# calls a subset (lease lifecycle + stream status) via a separate client.
"$PYTHON_BIN" -m grpc_tools.protoc \
  -I "$CP_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$CP_PROTO_SRC/marketdata_service.proto"

sed_in_place 's/^import marketdata_service_pb2/from . import marketdata_service_pb2/' "$OUT_DIR/marketdata_service_pb2_grpc.py"

# --- strategy-service proto (Python stubs + Go stubs) ---
# Python stubs
"$PYTHON_BIN" -m grpc_tools.protoc \
  -I "$STRAT_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$STRAT_PROTO_SRC/strategy_service.proto"

sed_in_place 's/^import strategy_service_pb2/from . import strategy_service_pb2/' "$OUT_DIR/strategy_service_pb2_grpc.py"

# Go stubs (for handler to import)
"$PROTOC_BIN" \
  -I "$STRAT_PROTO_SRC" \
  --go_out="$GO_OUT_DIR" --go_opt=paths=source_relative \
  --go-grpc_out="$GO_OUT_DIR" --go-grpc_opt=paths=source_relative \
  "$STRAT_PROTO_SRC/strategy_service.proto"

# --- runtime worker local IPC proto (Python + Go stubs) ---
"$PYTHON_BIN" -m grpc_tools.protoc \
  -I "$STRAT_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$STRAT_PROTO_SRC/runtime_worker.proto"

sed_in_place 's/^import runtime_worker_pb2/from . import runtime_worker_pb2/' "$OUT_DIR/runtime_worker_pb2_grpc.py"
sed_in_place 's/^import strategy_service_pb2/from . import strategy_service_pb2/' "$OUT_DIR/runtime_worker_pb2.py"

"$PROTOC_BIN" \
  -I "$STRAT_PROTO_SRC" \
  --go_out="$RUNTIME_WORKER_GO_OUT_DIR" --go_opt=paths=source_relative \
  --go-grpc_out="$RUNTIME_WORKER_GO_OUT_DIR" --go-grpc_opt=paths=source_relative \
  "$STRAT_PROTO_SRC/runtime_worker.proto"

echo "Generated stubs:"
echo "  Python → $OUT_DIR"
echo "  Go     → $GO_OUT_DIR"
echo "  Portfolio → $PORTFOLIO_GO_OUT_DIR"
echo "  Worker → $RUNTIME_WORKER_GO_OUT_DIR"
echo "  RuntimeChannel → $CONTROL_PANEL_GO_OUT_DIR"
