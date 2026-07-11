# Strategy Service

Python strategy runtime service for Hushine.

`strategy-service` is now the platform executor runtime only. It runs hosted and
self-hosted executor sessions through RuntimeChannel. The old standalone gRPC
runtime mode has been removed.

## Development

```bash
uv sync
./generate_proto.sh
make build
scripts/start-runtime-agent.sh -- --config config.yaml
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

`config.yaml` is intentionally runtime-only. Do not put portfolio/order,
Kafka, database, market-data policy, or notification endpoints in this
repository's default config; executor runtimes talk to the platform through
RuntimeChannel proxy calls.

## Runtime Agent Binaries

Daily development builds only the current platform binary:

```bash
make build
```

Release packaging is the only flow that cross-compiles all supported platforms:

```bash
scripts/build-runtime-agent-release.sh --version v0.1.0
```

That writes:

- `dist/runtime-agent/darwin-amd64/runtime-agent`
- `dist/runtime-agent/darwin-arm64/runtime-agent`
- `dist/runtime-agent/linux-amd64/runtime-agent`
- `dist/runtime-agent/linux-arm64/runtime-agent`
- `dist/runtime-agent/windows-amd64/runtime-agent.exe`
- `dist/runtime-agent/windows-arm64/runtime-agent.exe`

The launchers auto-detect the current platform and start an existing binary;
they do not build:

```bash
scripts/start-runtime-agent.sh -- --config config.yaml
```

Windows:

```powershell
.\scripts\start-runtime-agent.ps1 -- --config config.yaml
```

For a source checkout with no release binary, run `make build` once. To
explicitly allow source-mode fallback, set `RUNTIME_AGENT_ALLOW_GO_RUN=1`.

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
can connect without a runtime credential by naming the debug user explicitly.
For the current bare-runtime debugpy path, start from the `strategy-service`
directory:

```bash
make build
DEBUG_WAIT=0 scripts/start-bare-runtime-debugpy.sh 6 192.168.88.6
```

The first positional argument is `user_id`; the second positional argument is
the target platform host. With the command above, the script derives:

- control-panel certificate bootstrap: `192.168.88.6:50054`
- RuntimeChannel: `192.168.88.6:50055`

`DEBUG_WAIT=0` starts the runtime immediately. Omit it, or set `DEBUG_WAIT=1`,
when you want the Python session worker to pause for VS Code before executing a
session.

When the local strategy code has changed and the currently selected bare
session should be restarted, keep the runtime-agent running and execute:

```bash
scripts/restart-bare-worker-session.sh
```

The command reads the state file written by
`scripts/start-bare-runtime-debugpy.sh`, stops the old local Python worker,
marks the old session recoverable through RuntimeChannel, clears local worker
buffers, and starts a fresh worker against the same runtime. To target a known
session explicitly:

```bash
scripts/restart-bare-worker-session.sh <session_id>
```

Equivalent explicit form:

```bash
scripts/start-bare-runtime-debugpy.sh \
  --user-id 6 \
  --platform-host 192.168.88.6
```

When ports are non-standard, pass full addresses:

```bash
scripts/start-bare-runtime-debugpy.sh \
  --user-id 6 \
  --control-panel-addr 192.168.88.6:50054 \
  --runtime-channel-addr 192.168.88.6:50055
```

`--control-panel-addr` is a launcher-only certificate-bootstrap input. After
bootstrap, the launcher does not place it in runtime-agent arguments,
environment, or restart state.

There is no `attach.json`. VS Code uses `.vscode/launch.json`; `attach` is the
debug configuration's `request` mode. Use the tracked example as the base for
your local `.vscode/launch.json`:

```bash
scripts/vscode-bare-runtime-attach.launch.json
```

The default attach endpoint is `127.0.0.1:5678`, matching the script output:
`VS Code attach: 127.0.0.1:5678`. If you start the runtime with
`DEBUG_PORT=5679`, update the launch configuration port to `5679` as well.

The raw Go agent form is available for non-debugpy diagnostics after a bare
runtime certificate has already been bootstrapped:

```bash
RUNTIME_CHANNEL_TLS_ENABLED=true \
RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE=../hushine-deploy/certs/runtime-channel-server.pem \
RUNTIME_CHANNEL_TLS_SERVER_NAME=runtime-channel.local \
scripts/start-runtime-agent.sh -- --config config.local.yaml \
  --runtime-channel-addr 127.0.0.1:50055 \
  --user-id 123
```

Runtime traffic goes through RuntimeChannel. The runtime-agent and Python worker
never receive core-service, order-service, Kafka, database, or tracing
endpoints from this launcher.

Optional environment overrides include `DEBUG_HOST`, `DEBUG_PORT`,
`DEBUG_WAIT`, `PLATFORM_HOST`, `CONTROL_PANEL_ADDR`, `RUNTIME_CHANNEL_ADDR`, and
`CONFIG_PATH`.

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
