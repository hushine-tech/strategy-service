# Strategy Service

Python strategy runtime service for Hushine.

`strategy-service` is now the platform executor runtime only. It runs hosted and
self-hosted executor sessions through the normal gRPC / RuntimeChannel path.
Local strategy debugging has moved to the standalone `strategy-debugger-cli`
repository so users can debug strategy code offline without running a
platform-connected debugger container.

## Development

```bash
pip install -r requirements.txt
./generate_proto.sh
PYTHONPATH=.:../strategy-library python run_grpc_server.py -config config.yaml
```

Run tests:

```bash
PYTHONPATH=.:../strategy-library pytest -q
```

## Executor Runtime Image

The runtime Dockerfile builds one executor image:

```bash
./scripts/build_strategy_runtime.sh
```

The script tags:

- `hushine/strategy-runtime:executor-<version>`
- `hushine/strategy-runtime:executor`
- `hushine/strategy-runtime:dev`
- `hushine/strategy-runtime:<version>` for existing control-panel configs

Self-hosted executor example:

```bash
docker run --rm \
  -v $HOME/.hushine/runtime.cred:/etc/hushine/runtime.cred:ro \
  -e RUNTIME_INGRESS_MODE=outbound \
  -e RUNTIME_CREDENTIAL_PATH=/etc/hushine/runtime.cred \
  -e CONTROL_PANEL_SERVICE_GRPC_ADDR=host.docker.internal:50054 \
  hushine/strategy-runtime:executor-dev
```

In outbound mode the process ignores account/order/Kafka/database endpoints from
the local config and talks to the platform through RuntimeChannel proxy calls.

## Local Strategy Debugging

Use `strategy-debugger-cli` for local debugging:

```bash
hushine-debug init --dir hushine-debug-workspace
hushine-debug import debug-package.zip --dir hushine-debug-workspace
cd hushine-debug-workspace
cp strategy.py.template strategy.py
hushine-debug replay
```

The debug package is generated from Account Detail -> Local Debug in the
frontend. It contains historical futures bars, wallet config, and a strategy
template. VSCode/PyCharm attach to the local CLI process instead of a
strategy-service runtime container.
