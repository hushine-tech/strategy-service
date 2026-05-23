#!/usr/bin/env bash
# Generate Python gRPC stubs from order.v1 proto.
# Usage: ./generate_order_proto.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROTO_SRC="${SCRIPT_DIR}/../account-service/proto"
OUT_DIR="${SCRIPT_DIR}/strategy_service/gen"

mkdir -p "$OUT_DIR"

python -m grpc_tools.protoc \
  -I "$PROTO_SRC" \
  --python_out="$OUT_DIR" \
  --grpc_python_out="$OUT_DIR" \
  "$PROTO_SRC/order_service.proto"

# Fix absolute import to relative import so the gen/ package works as a subpackage
sed -i '' 's/^import order_service_pb2/from . import order_service_pb2/' "$OUT_DIR/order_service_pb2_grpc.py"

echo "Generated order.v1 Python stubs in $OUT_DIR"
