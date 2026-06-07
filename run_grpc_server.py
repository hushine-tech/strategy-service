#!/usr/bin/env python3
"""strategy-service gRPC 入口。

默认读取 config.yaml，支持 env var 覆盖（SERVER_GRPC_ADDR、DATABASE_HOST 等，
以及兼容旧名 GRPC_ADDR、TIMESCALE_HOST、KAFKA_BROKERS 等）。

用法:
    python run_grpc_server.py [-config path/to/config.yaml]
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
from concurrent import futures

import grpc

from strategy_service.config import Config
from strategy_service.gen import strategy_service_pb2_grpc as pb2_grpc
from strategy_service.grpc_server import StrategyServiceServicer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("strategy-service")


def _parse_runtime_ingress_mode(raw: str | None) -> tuple[bool, bool, str]:
    mode = (raw or "inbound").strip().lower()
    if mode not in ("inbound", "outbound", "both"):
        raise SystemExit(
            f"invalid RUNTIME_INGRESS_MODE={raw!r}; expected inbound, outbound, or both"
        )
    return mode in ("inbound", "both"), mode in ("outbound", "both"), mode


def _apply_runtime_dependency_boundary(
    cfg: Config,
    *,
    open_inbound: bool,
    open_outbound: bool,
    ingress_mode: str,
) -> str:
    """Return platform access mode and strip hosted-only deps in outbound mode.

    Self-hosted RuntimeChannel mode must not require or use direct internal
    core-service, order API, Kafka, or database endpoints. Until
    approved proxy clients exist, strategy execution fails closed in
    ``proxy_only`` mode instead of silently dialing internal services.
    """

    if not open_outbound or open_inbound:
        return "direct"

    ignored: list[str] = []
    if cfg.dependencies.account_service_grpc:
        ignored.append("dependencies.account_service_grpc")
        cfg.dependencies.account_service_grpc = ""
    if cfg.dependencies.order_service_grpc:
        ignored.append("dependencies.order_service_grpc")
        cfg.dependencies.order_service_grpc = ""
    if cfg.dependencies.market_data_control_panel_grpc:
        ignored.append("dependencies.market_data_control_panel_grpc")
        cfg.dependencies.market_data_control_panel_grpc = ""
    if cfg.kafka.brokers:
        ignored.append("kafka.brokers")
        cfg.kafka.brokers = ""
    if cfg.log.kafka_enabled or cfg.log.kafka_brokers:
        ignored.append("log.kafka")
        cfg.log.kafka_enabled = False
        cfg.log.kafka_brokers = []
    if cfg.database.host or cfg.database.database:
        ignored.append("database")
        cfg.database.host = ""
        cfg.database.database = ""

    if ignored:
        logger.warning(
            "RUNTIME_INGRESS_MODE=%s uses proxy-only platform access; "
            "ignoring unsupported direct dependencies: %s",
            ingress_mode,
            ", ".join(sorted(set(ignored))),
        )
    return "proxy_only"


def _init_log_with_kafka(cfg: Config) -> None:
    """Initialize strategy-library logging with Kafka enabled (if configured)."""
    try:
        from utils.log import init_log_with_kafka  # type: ignore
    except Exception:
        logger.debug("utils.log.init_log_with_kafka unavailable; skipping kafka log init")
        return
    if not cfg.log.kafka_enabled or not cfg.log.kafka_brokers:
        return
    try:
        init_log_with_kafka(
            cfg.log.output_dir,
            brokers=cfg.log.kafka_brokers,
            topic_prefix=cfg.log.topic_prefix,
        )
        logger.info("log initialized with Kafka brokers=%s", cfg.log.kafka_brokers)
    except Exception as e:
        logger.warning("init_log_with_kafka failed: %s (continuing with local log only)", e)


def _init_tracer(cfg: Config):
    """Initialize OpenTelemetry tracing from `cfg.log.tracing`.

    Optional: when `opentelemetry` is not installed, logs a debug line and
    returns — service still boots, trace_id fields in logs stay empty.
    Returns a shutdown callable so buffered spans are flushed on exit.
    """
    t = cfg.log.tracing
    if not t.enabled:
        logger.info("tracing disabled (log.tracing.enabled=false); running with noop tracer")
        return lambda: None
    try:
        from utils.log.tracer import init_tracer  # type: ignore
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "utils.log.tracer.init_tracer unavailable (%s); tracing skipped", e
        )
        return lambda: None
    try:
        shutdown = init_tracer(
            service_name=t.service_name or "strategy-service",
            endpoint=t.endpoint or "",
        )
        logger.info(
            "tracing enabled: service=%s endpoint=%s",
            t.service_name, t.endpoint,
        )
        return shutdown
    except Exception as e:  # noqa: BLE001
        logger.warning("init_tracer failed: %s (continuing with noop tracer)", e)
        return lambda: None


def _maybe_register_runtime(cfg: Config):
    """Phase D1 hosted strategy-runtime self-registration helper.

    Returns a registered `ControlPlaneClient` (without the heartbeat loop
    started yet — the caller starts it AFTER the gRPC server is listening
    so heartbeat-driven health flips happen against a server that can
    actually serve traffic).

    Returns None when registration is disabled / misconfigured. Failures
    do NOT block service startup — the runtime keeps listening on its
    inbound gRPC port and the control plane will surface its absence.
    """
    if not cfg.runtime.register_with_control_panel:
        return None
    cp_addr = cfg.dependencies.control_panel_service_grpc
    if not cp_addr:
        logger.warning(
            "runtime.register_with_control_panel=true but "
            "dependencies.control_panel_service_grpc is empty; skipping registration"
        )
        return None
    if cfg.runtime.bind_user_id <= 0:
        logger.warning(
            "runtime.register_with_control_panel=true but runtime.bind_user_id<=0; "
            "hosted runtime requires bind_user_id"
        )
        return None
    if not cfg.runtime.endpoint_host:
        logger.warning(
            "runtime.register_with_control_panel=true but runtime.endpoint_host is empty; "
            "control panel needs an advertised host:port for handler dial"
        )
        return None
    try:
        from strategy_service.runtime_client import ControlPlaneClient
    except Exception as e:  # noqa: BLE001
        logger.warning("runtime_client import failed: %s; skipping registration", e)
        return None
    client = ControlPlaneClient(cp_addr)
    try:
        identity = client.register(
            bind_user_id=cfg.runtime.bind_user_id,
            name=cfg.runtime.name,
            endpoint_host=cfg.runtime.endpoint_host,
            grpc_port=cfg.runtime.grpc_port,
            capabilities=cfg.runtime.capabilities,
            resource_profile=cfg.runtime.resource_profile,
            version=cfg.runtime.version,
            runtime_id=cfg.runtime.runtime_id,
            debug_port=cfg.runtime.debug_port,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "runtime registration failed (%s); continuing without control-plane integration",
            e,
        )
        try:
            client.stop()
        except Exception:  # noqa: BLE001
            pass
        return None
    logger.info(
        "  control-panel  → %s (runtime_id=%s user_id=%d name=%s)",
        cp_addr, identity.runtime_id, identity.user_id, identity.name,
    )
    return client


def _build_caller_token_interceptor(runtime_client, cfg: Config):
    """Phase D1 section 6.5 — build the inbound caller_token validator.

    Returns the interceptor instance, or None when:
      * runtime_client is None (registration disabled / failed) — without
        a runtime_id we can't validate against the right binding.
      * The interceptor module is unavailable for any reason.

    Defaults to enforce=True (reject invalid tokens). Operators flip
    to log-only via env `RUNTIME_CALLER_TOKEN_ENFORCE=0` during rollout
    while quant-handler is still allowed to call a standalone runtime directly.
    """
    if runtime_client is None or runtime_client.identity is None:
        return None
    try:
        from strategy_service.caller_token_interceptor import CallerTokenInterceptor
    except Exception as e:  # noqa: BLE001
        logger.warning("CallerTokenInterceptor import failed: %s; skipping", e)
        return None
    enforce = True
    raw = os.environ.get("RUNTIME_CALLER_TOKEN_ENFORCE")
    if raw is not None and raw.lower() in ("0", "false", "no", "off"):
        enforce = False
        logger.warning(
            "RUNTIME_CALLER_TOKEN_ENFORCE=%s → caller_token interceptor in LOG-ONLY mode",
            raw,
        )
    try:
        return CallerTokenInterceptor(
            runtime_id=runtime_client.identity.runtime_id,
            control_panel_stub=runtime_client.stub,
            enforce=enforce,
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("CallerTokenInterceptor construction failed: %s; skipping", e)
        return None


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-config", "--config", default="config.yaml", help="path to config.yaml")
    args = parser.parse_args()

    cfg = Config.load(args.config)
    cfg.apply_env_overrides()
    open_inbound, open_outbound, ingress_mode = _parse_runtime_ingress_mode(cfg.runtime.ingress_mode)
    platform_access_mode = _apply_runtime_dependency_boundary(
        cfg,
        open_inbound=open_inbound,
        open_outbound=open_outbound,
        ingress_mode=ingress_mode,
    )

    _init_log_with_kafka(cfg)
    tracer_shutdown = _init_tracer(cfg)

    # Phase D1 hosted strategy-runtime self-registration must happen
    # BEFORE we build the gRPC server because the caller_token
    # interceptor needs the registered runtime_id at server construction
    # time (gRPC interceptors are immutable post-creation). Heartbeat
    # is started AFTER server.start() so the runtime is observable as
    # healthy only when it can actually serve traffic.
    runtime_client = _maybe_register_runtime(cfg) if open_inbound else None
    caller_token_interceptor = (
        _build_caller_token_interceptor(runtime_client, cfg)
        if open_inbound
        else None
    )

    # Wire the Elemental gRPC server-access interceptor so every inbound
    # unary RPC produces a `grpc_access` log entry (method, client_ip,
    # latency, status_code, request/response snapshot), matching the
    # coverage Go services get via golang-lib/middleware/grpc.
    try:
        from utils.log import ServerAccessInterceptor  # type: ignore
        _server_interceptors = [ServerAccessInterceptor()]
    except Exception as _e:  # noqa: BLE001
        logger.warning("ServerAccessInterceptor unavailable: %s", _e)
        _server_interceptors = []
    if caller_token_interceptor is not None:
        # Place caller_token check BEFORE access logging so denied calls
        # don't pollute the access log with would-be successes.
        _server_interceptors.insert(0, caller_token_interceptor)
        logger.info("caller_token interceptor installed (runtime_id=%s)", runtime_client.identity.runtime_id)
    server = None
    runtime_channel_client = None
    runtime_agent = None
    servicer = None
    bound_user_id = (
        runtime_client.identity.user_id
        if runtime_client is not None and runtime_client.identity is not None
        else 0
    )
    notification_client = None
    if cfg.notification.enabled and runtime_client is not None and runtime_client.identity is not None:
        try:
            from strategy_service.notification import ControlPanelNotificationClient

            notification_client = ControlPanelNotificationClient(
                runtime_client.stub,
                user_id=runtime_client.identity.user_id,
                runtime_id=runtime_client.identity.runtime_id,
                timeout_seconds=cfg.notification.timeout_seconds,
            )
            logger.info("strategy self.notify enabled through control-panel-service")
        except Exception as exc:  # noqa: BLE001
            logger.warning("strategy self.notify setup failed: %s", exc)
    restore_running_sessions = open_inbound and not open_outbound
    if not restore_running_sessions:
        logger.info(
            "startup session recovery disabled for RUNTIME_INGRESS_MODE=%s",
            ingress_mode,
        )
    if open_inbound or open_outbound:
        runtime_id = (
            runtime_client.identity.runtime_id
            if runtime_client is not None and runtime_client.identity is not None
            else cfg.runtime.runtime_id
        )
        runtime_name = (
            runtime_client.identity.name
            if runtime_client is not None and runtime_client.identity is not None
            else cfg.runtime.name
        )
        runtime_source = "hosted" if open_inbound else "self_hosted"
        servicer = StrategyServiceServicer(
            account_service_addr=cfg.dependencies.account_service_grpc,
            order_service_addr=cfg.dependencies.order_service_grpc,
            timescale_config=cfg.timescale_dict(),
            kafka_brokers=cfg.kafka.brokers,
            market_data_policy={
                "preflight_enabled": cfg.market_data.preflight_enabled,
                "lease_management_enabled": cfg.market_data.lease_management_enabled,
                "lease_heartbeat_seconds": cfg.market_data.lease_heartbeat_seconds,
                "lease_ttl_seconds": cfg.market_data.lease_ttl_seconds,
                "freshness_grace_seconds": cfg.market_data.freshness_grace_seconds,
            },
            bound_user_id=bound_user_id,
            runtime_id=runtime_id,
            runtime_source=runtime_source,
            runtime_name=runtime_name,
            platform_access_mode=platform_access_mode,
            market_data_control_panel_addr=cfg.dependencies.market_data_control_panel_grpc,
            restore_running_sessions=restore_running_sessions,
            notification_client=notification_client,
        )
        if bound_user_id > 0:
            logger.info("strategy RPCs pinned to user_id=%d", bound_user_id)
    if open_inbound:
        server = grpc.server(
            futures.ThreadPoolExecutor(max_workers=10),
            interceptors=_server_interceptors,
        )
        pb2_grpc.add_StrategyServiceServicer_to_server(servicer, server)
        server.add_insecure_port(cfg.server.grpc_addr)
        server.start()
        logger.info("strategy-service gRPC listening on %s", cfg.server.grpc_addr)
    else:
        logger.info("strategy-service inbound gRPC disabled (RUNTIME_INGRESS_MODE=%s)", ingress_mode)

    if open_outbound:
        cp_addr = cfg.dependencies.control_panel_service_grpc
        if not cp_addr:
            logger.error(
                "RUNTIME_INGRESS_MODE=%s requires dependencies.control_panel_service_grpc",
                ingress_mode,
            )
            raise SystemExit(1)
        try:
            from strategy_service.runtime_channel import (
                RuntimeChannelClient,
                RuntimeChannelStrategyDispatcher,
                RuntimeHelloArgs,
                load_runtime_credential,
            )
            from strategy_service.runtime_agent import RuntimeAgent
            from strategy_service.platform_proxy import (
                RuntimeChannelPlatformProxy,
                install_runtime_channel_log_handler,
            )

            credential = load_runtime_credential(cfg.runtime.credential_path or None)
            if servicer is None:
                raise RuntimeError("strategy servicer is not available for RuntimeChannel dispatch")
            runtime_channel_client = RuntimeChannelClient(
                cp_addr,
                credential,
                RuntimeHelloArgs(
                    key_id=credential.key_id,
                    private_key_pem=credential.private_key_pem,
                    runtime_id=cfg.runtime.runtime_id,
                    name=cfg.runtime.name,
                    endpoint_host=cfg.runtime.endpoint_host if open_inbound else "",
                    grpc_port=cfg.runtime.grpc_port if open_inbound else 0,
                    debug_port=cfg.runtime.debug_port if open_inbound else 0,
                    capabilities=tuple(cfg.runtime.capabilities),
                    resource_profile=cfg.runtime.resource_profile,
                    version=cfg.runtime.version,
                ),
                heartbeat_seconds=cfg.runtime.heartbeat_interval_seconds,
                request_handler=RuntimeChannelStrategyDispatcher(servicer),
            )
            runtime_agent = RuntimeAgent(
                runtime_channel_client,
                runtime_id=cfg.runtime.runtime_id,
            )
            runtime_channel_client.set_data_handler(runtime_agent.handle_data_frame)
            runtime_channel_client.set_command_handler(runtime_agent.handle_runtime_command)
            servicer.set_runtime_data_source(runtime_agent)
            platform_proxy = RuntimeChannelPlatformProxy(runtime_channel_client)
            servicer.set_platform_proxy(platform_proxy)
            if cfg.notification.enabled:
                servicer.set_notification_client(platform_proxy.notification_client())
        except Exception as e:  # noqa: BLE001
            logger.error("runtime credential / RuntimeChannel setup failed: %s", e)
            raise SystemExit(1) from e
        runtime_agent.start_channel()
        if platform_access_mode == "proxy_only":
            install_runtime_channel_log_handler(platform_proxy)
        logger.info(
            "RuntimeChannel outbound client started → %s (credential=%s)",
            cp_addr,
            credential.key_id,
        )
    logger.info("  core-service → %s", cfg.dependencies.account_service_grpc)
    logger.info("  order API       → %s", cfg.dependencies.order_service_grpc)
    logger.info("  timescale       → %s:%s/%s", cfg.database.host, cfg.database.port, cfg.database.database)
    logger.info("  kafka           → %s", cfg.kafka.brokers)
    logger.info(
        "  market-data     → preflight=%s leases=%s heartbeat=%ss ttl=%ss freshness_grace=%ss",
        cfg.market_data.preflight_enabled,
        cfg.market_data.lease_management_enabled,
        cfg.market_data.lease_heartbeat_seconds,
        cfg.market_data.lease_ttl_seconds,
        cfg.market_data.freshness_grace_seconds,
    )

    # Start the heartbeat loop now that the gRPC server is listening —
    # control-panel will see status=active only when the runtime can
    # actually serve traffic, not when it's still booting.
    if runtime_client is not None:
        runtime_client.start_heartbeat(
            interval_seconds=cfg.runtime.heartbeat_interval_seconds,
        )

    shutdown_event = threading.Event()

    def _shutdown(signum, frame):
        logger.info("shutting down (signal %s)", signum)
        shutdown_event.set()
        if runtime_channel_client is not None:
            try:
                if runtime_agent is not None:
                    runtime_agent.stop()
                else:
                    runtime_channel_client.stop()
            except Exception as e:  # noqa: BLE001
                logger.warning("runtime channel client stop failed: %s", e)
        if runtime_client is not None:
            try:
                runtime_client.stop()
            except Exception as e:  # noqa: BLE001
                logger.warning("runtime client stop failed: %s", e)
        if server is not None:
            server.stop(grace=5)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        if server is not None:
            server.wait_for_termination()
        else:
            while not shutdown_event.wait(1.0):
                if runtime_channel_client is not None and not runtime_channel_client.is_alive():
                    logger.error("RuntimeChannel stopped; shutting down outbound runtime")
                    shutdown_event.set()
                    break
    finally:
        if runtime_channel_client is not None:
            try:
                if runtime_agent is not None:
                    runtime_agent.stop()
                else:
                    runtime_channel_client.stop()
            except Exception as e:  # noqa: BLE001
                logger.debug("runtime channel client final stop: %s", e)
        if runtime_client is not None:
            try:
                runtime_client.stop()
            except Exception as e:  # noqa: BLE001
                logger.debug("runtime client final stop: %s", e)
        try:
            tracer_shutdown()
        except Exception as e:  # noqa: BLE001
            logger.warning("tracer shutdown failed: %s", e)


if __name__ == "__main__":
    main()
