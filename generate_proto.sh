#!/usr/bin/env bash
# Generate Python + Go gRPC stubs from all proto files.
# Usage: ./generate_proto.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT_DIR="${SCRIPT_DIR}/strategy_service/gen"
GO_OUT_DIR="${SCRIPT_DIR}/gen/strategyv1"

mkdir -p "$OUT_DIR" "$GO_OUT_DIR"

# --- core-service proto (Python stubs only) ---
ACCT_PROTO_SRC="${SCRIPT_DIR}/../core-service/proto"

/opt/anaconda3/bin/python3 -m grpc_tools.protoc \
  -I "$ACCT_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$ACCT_PROTO_SRC/account_service.proto"

sed -i '' 's/^import account_service_pb2/from . import account_service_pb2/' "$OUT_DIR/account_service_pb2_grpc.py"

# --- order.v1 proto (Python stubs only) ---
ORDER_PROTO_SRC="${SCRIPT_DIR}/../core-service/proto"

/opt/anaconda3/bin/python3 -m grpc_tools.protoc \
  -I "$ORDER_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$ORDER_PROTO_SRC/order_service.proto"

sed -i '' 's/^import order_service_pb2/from . import order_service_pb2/' "$OUT_DIR/order_service_pb2_grpc.py"

# --- control-panel-service proto (Python stubs only) ---
# Phase D1: strategy-runtime registers itself + heartbeats with control-plane.
CP_PROTO_SRC="${SCRIPT_DIR}/../control-panel-service/proto"
STRAT_PROTO_SRC="${SCRIPT_DIR}/proto"

/opt/anaconda3/bin/python3 -m grpc_tools.protoc \
  -I "$CP_PROTO_SRC" \
  -I "$STRAT_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$CP_PROTO_SRC/control_panel_service.proto"

sed -i '' 's/^import control_panel_service_pb2/from . import control_panel_service_pb2/' "$OUT_DIR/control_panel_service_pb2_grpc.py"
sed -i '' 's/^import strategy_service_pb2/from . import strategy_service_pb2/' "$OUT_DIR/control_panel_service_pb2.py"
sed -i '' 's/^import strategy_service_pb2/from . import strategy_service_pb2/' "$OUT_DIR/control_panel_service_pb2_grpc.py"

# Phase D2: market-data control-plane RPCs moved out of core-service into
# control-panel-service (package controlpanel.marketdata.v1). Strategy-service
# calls a subset (lease lifecycle + stream status) via a separate client.
/opt/anaconda3/bin/python3 -m grpc_tools.protoc \
  -I "$CP_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$CP_PROTO_SRC/marketdata_service.proto"

sed -i '' 's/^import marketdata_service_pb2/from . import marketdata_service_pb2/' "$OUT_DIR/marketdata_service_pb2_grpc.py"

# --- strategy-service proto (Python stubs + Go stubs) ---
# Python stubs
/opt/anaconda3/bin/python3 -m grpc_tools.protoc \
  -I "$STRAT_PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$STRAT_PROTO_SRC/strategy_service.proto"

sed -i '' 's/^import strategy_service_pb2/from . import strategy_service_pb2/' "$OUT_DIR/strategy_service_pb2_grpc.py"

# Go stubs (for handler to import)
PATH="$HOME/go/bin:$PATH" protoc \
  -I "$STRAT_PROTO_SRC" \
  --go_out="$GO_OUT_DIR" --go_opt=paths=source_relative \
  --go-grpc_out="$GO_OUT_DIR" --go-grpc_opt=paths=source_relative \
  "$STRAT_PROTO_SRC/strategy_service.proto"

echo "Generated stubs:"
echo "  Python → $OUT_DIR"
echo "  Go     → $GO_OUT_DIR"
