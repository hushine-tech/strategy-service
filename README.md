# Strategy Service

Python strategy runtime service for Hushine.

`strategy-service` is now the platform executor runtime only. It runs hosted and
self-hosted executor sessions through RuntimeChannel. The old standalone gRPC
runtime mode has been removed.

## Development

```bash
uv sync
./generate_proto.sh
uv run hushine-runtime start --config config.yaml
```

Run tests:

```bash
uv run --extra dev pytest -q
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
  -e RUNTIME_CREDENTIAL_PATH=/etc/hushine/runtime.cred \
  -e CONTROL_PANEL_SERVICE_GRPC_ADDR=host.docker.internal:50055 \
  hushine/strategy-runtime:executor-dev
```

The process ignores account/order/Kafka/database endpoints from the local config
and talks to the platform through RuntimeChannel proxy calls.

## Local Strategy Debugging

When control-panel is deployed with debug bare runtime enabled, a local process
can connect without a runtime credential by naming the debug user explicitly:

```bash
uv run hushine-runtime start --config config.yaml --user-id 123
```

The existing debug replay helper remains available through uv:

```bash
uv run hushine-debug replay --debugpy --wait
```
