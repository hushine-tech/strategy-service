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

The process ignores portfolio/order/Kafka/database endpoints from the local config
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

For VS Code/debugpy attach mode, use the runtime-owned shortcut script:

```bash
scripts/start-bare-runtime-debugpy.sh 123
```

To connect a local bare runtime to another platform machine, pass the platform
host. The script derives core-service, control-panel, and RuntimeChannel ports:

```bash
scripts/start-bare-runtime-debugpy.sh 123 192.168.88.6
```

Equivalent explicit form:

```bash
scripts/start-bare-runtime-debugpy.sh \
  --user-id 123 \
  --platform-host 192.168.88.6
```

When ports are non-standard, pass full addresses:

```bash
scripts/start-bare-runtime-debugpy.sh \
  --user-id 123 \
  --core-service-addr 192.168.88.6:50051 \
  --control-panel-addr 192.168.88.6:50054 \
  --runtime-channel-addr 192.168.88.6:50055
```

`core-service` is exported for config compatibility. Runtime traffic still goes
through RuntimeChannel; direct core-service calls are ignored after startup.

Optional environment overrides include `DEBUG_HOST`, `DEBUG_PORT`,
`DEBUG_WAIT`, `PLATFORM_HOST`, `CORE_SERVICE_ADDR`, `CONTROL_PANEL_ADDR`,
`RUNTIME_CHANNEL_ADDR`, and `CONFIG_PATH`.

The existing debug replay helper remains available through uv:

```bash
uv run hushine-debug replay --debugpy --wait
```

When a bare debug run materializes strategy code locally, edits live under
`.hushine-runtime/strategies`. Upload them back to the remote portfolio database
with:

```bash
uv run python scripts/upload_debug_strategies.py --user-id 123
```

Preview without writing:

```bash
uv run python scripts/upload_debug_strategies.py --user-id 123 --dry-run
```

Use `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, and `PGPASSWORD` or the matching
`--db-*` flags when the target database is not the local default.
