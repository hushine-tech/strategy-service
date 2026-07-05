from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import signal
import threading
import uuid
from typing import Callable

import grpc

from strategy_service.config import Config
from strategy_service.grpc_server import PLATFORM_ACCESS_PROXY_ONLY, StrategyServiceServicer
from strategy_service.platform_proxy import RuntimeChannelPlatformProxy, install_runtime_channel_log_handler
from strategy_service.runtime_agent import RuntimeAgent
from strategy_service.runtime_channel import (
    RuntimeChannelClient,
    RuntimeChannelStrategyDispatcher,
    RuntimeHelloArgs,
    load_runtime_credential,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("hushine-runtime")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hushine-runtime")
    sub = parser.add_subparsers(dest="command", required=True)
    start = sub.add_parser("start")
    start.add_argument("-config", "--config", default="config.yaml", help="path to config.yaml")
    start.add_argument(
        "--runtime-channel-addr",
        default="",
        help="control-panel RuntimeChannel gRPC address, for example host.example:50055",
    )
    start.add_argument(
        "--control-panel-addr",
        default="",
        help="control-panel gRPC address used only for bare runtime certificate bootstrap",
    )
    start.add_argument(
        "--user-id",
        "--user_id",
        dest="user_id",
        type=int,
        default=0,
        help="debug bare runtime user id; control-panel must enable debug bare runtime",
    )
    args = parser.parse_args(argv)

    if args.command == "start":
        return start_runtime(
            config_path=args.config,
            bare_user_id=int(args.user_id or 0),
            runtime_channel_addr=str(args.runtime_channel_addr or ""),
            control_panel_addr=str(args.control_panel_addr or ""),
        )
    raise AssertionError(f"unknown command {args.command}")


def start_runtime(
    *,
    config_path: str,
    bare_user_id: int = 0,
    runtime_channel_addr: str = "",
    control_panel_addr: str = "",
) -> int:
    cfg = Config.load(config_path)
    cfg.apply_env_overrides()
    bare_bootstrap_addr = control_panel_addr.strip() or cfg.dependencies.control_panel_service_grpc
    _force_runtime_channel_boundary(cfg)
    _init_log_with_kafka(cfg)
    tracer_shutdown = _init_tracer(cfg)

    cp_addr = runtime_channel_addr.strip() or cfg.dependencies.runtime_channel_grpc
    if not cp_addr:
        logger.error("dependencies.runtime_channel_grpc or --runtime-channel-addr is required for RuntimeChannel startup")
        return 1

    source = "bare" if bare_user_id > 0 else (cfg.runtime.source or "self_hosted")
    runtime_id = cfg.runtime.runtime_id
    runtime_name = cfg.runtime.name
    credential = None
    key_id = ""
    private_key_pem = ""

    try:
        _materialize_tls_bundle(cfg)
        if bare_user_id > 0:
            runtime_id = _bare_runtime_id(runtime_id, bare_user_id)
            runtime_name = _bare_runtime_name(runtime_name, bare_user_id)
            _bootstrap_bare_runtime_mtls_if_needed(
                cfg,
                cp_addr=bare_bootstrap_addr or cp_addr,
                bare_user_id=bare_user_id,
                runtime_id=runtime_id,
                runtime_name=runtime_name,
            )
        else:
            credential = load_runtime_credential(cfg.runtime.credential_path or None)
            key_id = credential.key_id
            private_key_pem = credential.private_key_pem
            if cfg.runtime.source == "":
                source = ""

        servicer = StrategyServiceServicer(
            portfolio_service_addr="",
            order_service_addr="",
            timescale_config=cfg.timescale_dict(),
            kafka_brokers="",
            market_data_policy={
                "preflight_enabled": cfg.market_data.preflight_enabled,
                "lease_management_enabled": cfg.market_data.lease_management_enabled,
                "lease_heartbeat_seconds": cfg.market_data.lease_heartbeat_seconds,
                "lease_ttl_seconds": cfg.market_data.lease_ttl_seconds,
                "freshness_grace_seconds": cfg.market_data.freshness_grace_seconds,
            },
            bound_user_id=bare_user_id,
            runtime_id=runtime_id,
            runtime_source=source or "self_hosted",
            runtime_name=runtime_name,
            platform_access_mode=PLATFORM_ACCESS_PROXY_ONLY,
            market_data_control_panel_addr="",
            restore_running_sessions=False,
        )
        runtime_channel_client = RuntimeChannelClient(
            cp_addr,
            credential,
            RuntimeHelloArgs(
                key_id=key_id,
                private_key_pem=private_key_pem,
                source=source,
                user_id=bare_user_id,
                runtime_id=runtime_id,
                name=runtime_name,
                capabilities=tuple(cfg.runtime.capabilities),
                resource_profile=cfg.runtime.resource_profile,
                version=cfg.runtime.version,
            ),
            heartbeat_seconds=cfg.runtime.heartbeat_interval_seconds,
            request_handler=RuntimeChannelStrategyDispatcher(servicer),
            grpc_channel_factory=_runtime_channel_factory(cfg),
        )
        runtime_agent = RuntimeAgent(runtime_channel_client, runtime_id=runtime_id)
        runtime_channel_client.set_data_handler(runtime_agent.handle_data_frame)
        runtime_channel_client.set_command_handler(runtime_agent.handle_runtime_command)
        servicer.set_runtime_data_source(runtime_agent)
        platform_proxy = RuntimeChannelPlatformProxy(runtime_channel_client)
        servicer.set_platform_proxy(platform_proxy)
        if cfg.notification.enabled:
            servicer.set_notification_client(platform_proxy.notification_client())
        install_runtime_channel_log_handler(platform_proxy)
        runtime_agent.start_channel()
    except Exception as exc:  # noqa: BLE001
        logger.error("RuntimeChannel startup failed: %s", exc)
        try:
            tracer_shutdown()
        except Exception:  # noqa: BLE001
            pass
        return 1

    logger.info(
        "RuntimeChannel runtime started → %s (source=%s runtime_id=%s name=%s tls=%s)",
        cp_addr,
        source or "credential-inferred",
        runtime_id or "<control-panel-generated>",
        runtime_name or "<control-panel-generated>",
        cfg.runtime_channel_tls.enabled,
    )

    shutdown_event = threading.Event()

    def _shutdown(signum, _frame):
        logger.info("shutting down (signal %s)", signum)
        shutdown_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    exit_code = 0
    try:
        while not shutdown_event.wait(1.0):
            if not runtime_channel_client.is_alive():
                logger.error("RuntimeChannel stopped; shutting down runtime")
                exit_code = 1
                break
    finally:
        runtime_agent.stop()
        try:
            tracer_shutdown()
        except Exception as exc:  # noqa: BLE001
            logger.warning("tracer shutdown failed: %s", exc)
    return exit_code


def _force_runtime_channel_boundary(cfg: Config) -> None:
    ignored: list[str] = []
    if cfg.dependencies.portfolio_service_grpc:
        ignored.append("dependencies.portfolio_service_grpc")
        cfg.dependencies.portfolio_service_grpc = ""
    if cfg.dependencies.order_service_grpc:
        ignored.append("dependencies.order_service_grpc")
        cfg.dependencies.order_service_grpc = ""
    if cfg.dependencies.control_panel_service_grpc:
        ignored.append("dependencies.control_panel_service_grpc")
        cfg.dependencies.control_panel_service_grpc = ""
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
        logger.info("RuntimeChannel startup ignores internal platform dependencies: %s", ", ".join(sorted(set(ignored))))


def _runtime_channel_factory(cfg: Config) -> Callable[[str], grpc.Channel]:
    if not cfg.runtime_channel_tls.enabled:
        return grpc.insecure_channel

    root_certificates = None
    if cfg.runtime_channel_tls.root_cert_file:
        with open(cfg.runtime_channel_tls.root_cert_file, "rb") as f:
            root_certificates = f.read()
    private_key = None
    certificate_chain = None
    if cfg.runtime_channel_tls.client_key_file:
        with open(cfg.runtime_channel_tls.client_key_file, "rb") as f:
            private_key = f.read()
    if cfg.runtime_channel_tls.client_cert_file:
        with open(cfg.runtime_channel_tls.client_cert_file, "rb") as f:
            certificate_chain = f.read()
    credentials = grpc.ssl_channel_credentials(
        root_certificates=root_certificates,
        private_key=private_key,
        certificate_chain=certificate_chain,
    )
    options: list[tuple[str, str]] = []
    if cfg.runtime_channel_tls.server_name:
        options.append(("grpc.ssl_target_name_override", cfg.runtime_channel_tls.server_name))
        options.append(("grpc.default_authority", cfg.runtime_channel_tls.server_name))
    return lambda address: grpc.secure_channel(address, credentials, options=options)


def _materialize_tls_bundle(cfg: Config) -> None:
    raw = cfg.runtime_channel_tls.bundle_json or _tls_bundle_json_from_credential(cfg.runtime.credential_path or None)
    if not raw:
        return
    body = json.loads(raw)
    cfg.runtime_channel_tls.enabled = True
    if not cfg.runtime_channel_tls.server_name:
        cfg.runtime_channel_tls.server_name = str(body.get("server_name") or "runtime-channel.local")
    target_dir = pathlib.Path(os.environ.get("RUNTIME_CHANNEL_TLS_BUNDLE_DIR", "/etc/hushine"))
    target_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "root_cert_file": ("control-panel-ca.pem", body.get("server_ca_pem", "")),
        "client_cert_file": ("runtime-client.pem", body.get("client_cert_pem", "")),
        "client_key_file": ("runtime-client.key", body.get("client_key_pem", "")),
    }
    for attr, (name, content) in files.items():
        if not content:
            raise RuntimeError(f"runtime TLS bundle missing {attr}")
        path = target_dir / name
        path.write_text(content, encoding="utf-8")
        if attr == "client_key_file":
            path.chmod(0o600)
        setattr(cfg.runtime_channel_tls, attr, str(path))


def _tls_bundle_json_from_credential(path: str | None) -> str:
    resolved = path or os.environ.get("RUNTIME_CREDENTIAL_PATH")
    if not resolved:
        return ""
    try:
        with open(resolved, "r", encoding="utf-8") as f:
            body = json.load(f)
    except (OSError, json.JSONDecodeError):
        return ""
    required = ("client_cert_pem", "client_key_pem", "server_ca_pem")
    if not isinstance(body, dict) or not all(body.get(k) for k in required):
        return ""
    return json.dumps(
        {
            "client_cert_pem": body["client_cert_pem"],
            "client_key_pem": body["client_key_pem"],
            "server_ca_pem": body["server_ca_pem"],
            "server_name": body.get("server_name") or "runtime-channel.local",
        }
    )


def _bootstrap_bare_runtime_mtls_if_needed(
    cfg: Config,
    *,
    cp_addr: str,
    bare_user_id: int,
    runtime_id: str,
    runtime_name: str,
    cache_dir: pathlib.Path | None = None,
) -> None:
    if bare_user_id <= 0:
        return
    if not cfg.runtime_channel_tls.enabled:
        return
    if cfg.runtime_channel_tls.client_cert_file:
        return

    from strategy_service import bare_bootstrap

    target_dir = cache_dir or pathlib.Path(os.environ.get("RUNTIME_BARE_BOOTSTRAP_DIR", ".hushine-runtime/bare"))
    paths = bare_bootstrap.load_existing_runtime_mtls_bundle(target_dir, runtime_id=runtime_id)
    if paths is None:
        paths = bare_bootstrap.bootstrap_bare_runtime_certificate(
            address=cp_addr,
            user_id=bare_user_id,
            runtime_id=runtime_id,
            name=runtime_name,
            root_cert_file=cfg.runtime_channel_tls.root_cert_file,
            server_name=cfg.runtime_channel_tls.server_name,
            tls_enabled=_env_bool("RUNTIME_BARE_BOOTSTRAP_TLS_ENABLED", default=False),
            output_dir=target_dir,
        )
    else:
        logger.info("reusing cached bare runtime mTLS bundle: %s", target_dir)
    cfg.runtime_channel_tls.root_cert_file = str(paths.root_cert_file)
    cfg.runtime_channel_tls.client_cert_file = str(paths.client_cert_file)
    cfg.runtime_channel_tls.client_key_file = str(paths.client_key_file)


def _bare_runtime_id(configured: str, user_id: int) -> str:
    configured = str(configured or "").strip()
    if configured:
        return configured
    return f"bare-{int(user_id)}-{uuid.uuid4().hex[:8]}"


def _bare_runtime_name(configured: str, user_id: int) -> str:
    configured = str(configured or "").strip()
    if configured:
        return configured
    suffix = uuid.uuid4().hex[:6]
    return f"bare-debug-{int(user_id)}-{suffix}"


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.lower() in ("1", "true", "yes", "on")


def _init_log_with_kafka(cfg: Config) -> None:
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
    except Exception as exc:  # noqa: BLE001
        logger.warning("init_log_with_kafka failed: %s (continuing with local log only)", exc)


def _init_tracer(cfg: Config):
    t = cfg.log.tracing
    if not t.enabled:
        logger.info("tracing disabled (log.tracing.enabled=false); running with noop tracer")
        return lambda: None
    try:
        from utils.log.tracer import init_tracer  # type: ignore
    except Exception as exc:  # noqa: BLE001
        logger.warning("utils.log.tracer.init_tracer unavailable (%s); tracing skipped", exc)
        return lambda: None
    try:
        shutdown = init_tracer(
            service_name=t.service_name or "strategy-service",
            endpoint=t.endpoint or "",
        )
        logger.info("tracing enabled: service=%s endpoint=%s", t.service_name, t.endpoint)
        return shutdown
    except Exception as exc:  # noqa: BLE001
        logger.warning("init_tracer failed: %s (continuing with noop tracer)", exc)
        return lambda: None


if __name__ == "__main__":
    raise SystemExit(main())
