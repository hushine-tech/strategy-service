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
  -e RUNTIME_CHANNEL_GRPC_ADDR=host.docker.internal:50055 \
  hushine/strategy-runtime:executor-dev
```

The process ignores account/order/Kafka/database endpoints from the local config
and talks to the platform through RuntimeChannel proxy calls.

## RuntimeChannel Data Path

- Backtests call `marketdata.FetchBacktestPage` through the platform proxy.
  control-panel-service reads `{exchange}_{year}` market-data tables and returns
  fixed pages of `8192` bars; the runtime streams those pages into the strategy
  and does not hold a full multi-year dataset in memory.
- Demo/live sessions receive authorized market-data frames through
  RuntimeChannel and consume them from local queues. Order updates have their
  own queue and are handled before the next market-data callback, so
  `on_order_update` and `on_market_data` remain serialized.
- If demo/live market data becomes stale while a strategy is blocked or under a
  breakpoint, stale bars are dropped by lag time and the runtime reports
  `DATA_BACKPRESSURE`. Repeated drops mark the session failed via status patch.

## Local Strategy Debugging

When control-panel is deployed with debug bare runtime enabled, a local process
can connect without a runtime credential by naming the debug user explicitly:

```bash
RUNTIME_CHANNEL_TLS_ENABLED=true \
RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE=../hushine-deploy/certs/runtime-channel-server.pem \
RUNTIME_CHANNEL_TLS_SERVER_NAME=runtime-channel.local \
uv run hushine-runtime start --config config.local.yaml \
  --control-panel-addr 127.0.0.1:50054 \
  --runtime-channel-addr 127.0.0.1:50055 \
  --user-id 123
```

`--control-panel-addr` is used only for the debug-gated certificate bootstrap.
After that, runtime traffic uses `--runtime-channel-addr` with the issued mTLS
client certificate.

The existing debug replay helper remains available through uv:

```bash
uv run hushine-debug replay --debugpy --wait
```
