"""Config loader for strategy-service.

Loads YAML config and applies env var overrides. Legacy env vars are honored as aliases.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import yaml


@dataclass
class ServerConfig:
    grpc_addr: str = "0.0.0.0:50053"


@dataclass
class DatabaseConfig:
    host: str = "192.168.88.10"
    port: int = 5432
    database: str = "binance_{year}"
    user: str = "postgres"
    password: str = "postgres"


@dataclass
class DependenciesConfig:
    account_service_grpc: str = "localhost:50051"
    order_service_grpc: str = "127.0.0.1:50051"
    control_panel_service_grpc: str = ""
    # Phase D2: market-data control-plane RPCs moved out of core-service.
    # When unset, falls back to control_panel_service_grpc (same address —
    # both services live on the same gRPC port :50054 in D2).
    market_data_control_panel_grpc: str = ""


@dataclass
class RuntimeConfig:
    """RuntimeChannel startup metadata.

    The runtime has a single platform boundary now: it connects outward to
    control-panel-service RuntimeChannel. Hosted/self-hosted runtimes use a
    signed credential; local debug can use source=bare when control-panel is
    explicitly running with debug bare runtime enabled.
    """

    credential_path: str = ""  # empty => /etc/hushine/runtime.cred
    source: str = ""  # empty lets control-panel infer hosted/self_hosted from credential
    runtime_id: str = ""  # empty → control panel generates one
    name: str = ""
    capabilities: list[str] = field(default_factory=lambda: ["strategy", "spot", "futures"])
    resource_profile: str = "small"
    version: str = "0.1.0"
    heartbeat_interval_seconds: int = 10


@dataclass
class RuntimeChannelTLSConfig:
    enabled: bool = False
    root_cert_file: str = ""
    server_name: str = ""


@dataclass
class KafkaConfig:
    brokers: str = "192.168.88.10:19092"


@dataclass
class MarketDataPolicyConfig:
    preflight_enabled: bool = True
    lease_management_enabled: bool = True
    lease_heartbeat_seconds: int = 30
    lease_ttl_seconds: int = 90
    freshness_grace_seconds: int = 30


@dataclass
class NotificationConfig:
    enabled: bool = True
    timeout_seconds: float = 2.0


@dataclass
class TracingConfig:
    enabled: bool = False
    endpoint: str = ""
    service_name: str = ""


@dataclass
class LogConfig:
    output_dir: str = "./logs"
    kafka_enabled: bool = True
    kafka_brokers: list[str] = field(default_factory=lambda: ["192.168.88.10:19092"])
    topic_prefix: str = "app-logs"
    tracing: TracingConfig = field(default_factory=TracingConfig)


@dataclass
class Config:
    server: ServerConfig = field(default_factory=ServerConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    dependencies: DependenciesConfig = field(default_factory=DependenciesConfig)
    runtime: RuntimeConfig = field(default_factory=RuntimeConfig)
    runtime_channel_tls: RuntimeChannelTLSConfig = field(default_factory=RuntimeChannelTLSConfig)
    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    market_data: MarketDataPolicyConfig = field(default_factory=MarketDataPolicyConfig)
    notification: NotificationConfig = field(default_factory=NotificationConfig)
    log: LogConfig = field(default_factory=LogConfig)

    @classmethod
    def load(cls, path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        cfg = cls()
        if isinstance(data.get("server"), dict):
            s = data["server"]
            cfg.server.grpc_addr = s.get("grpc_addr", cfg.server.grpc_addr)
        if isinstance(data.get("database"), dict):
            d = data["database"]
            cfg.database.host = d.get("host", cfg.database.host)
            cfg.database.port = int(d.get("port", cfg.database.port))
            cfg.database.database = d.get("database", cfg.database.database)
            cfg.database.user = d.get("user", cfg.database.user)
            cfg.database.password = d.get("password", cfg.database.password)
        if isinstance(data.get("dependencies"), dict):
            dep = data["dependencies"]
            cfg.dependencies.account_service_grpc = dep.get("account_service_grpc", cfg.dependencies.account_service_grpc)
            cfg.dependencies.order_service_grpc = dep.get("order_service_grpc", cfg.dependencies.order_service_grpc)
            cfg.dependencies.control_panel_service_grpc = dep.get(
                "control_panel_service_grpc", cfg.dependencies.control_panel_service_grpc
            )
            cfg.dependencies.market_data_control_panel_grpc = dep.get(
                "market_data_control_panel_grpc",
                cfg.dependencies.market_data_control_panel_grpc,
            )
        if isinstance(data.get("runtime"), dict):
            r = data["runtime"]
            cfg.runtime.credential_path = r.get("credential_path", cfg.runtime.credential_path)
            cfg.runtime.source = r.get("source", cfg.runtime.source)
            cfg.runtime.runtime_id = r.get("runtime_id", cfg.runtime.runtime_id)
            cfg.runtime.name = r.get("name", cfg.runtime.name)
            caps = r.get("capabilities")
            if isinstance(caps, list):
                cfg.runtime.capabilities = [str(c) for c in caps]
            cfg.runtime.resource_profile = r.get("resource_profile", cfg.runtime.resource_profile)
            cfg.runtime.version = r.get("version", cfg.runtime.version)
            cfg.runtime.heartbeat_interval_seconds = int(
                r.get("heartbeat_interval_seconds", cfg.runtime.heartbeat_interval_seconds)
            )
        if isinstance(data.get("runtime_channel_tls"), dict):
            rtls = data["runtime_channel_tls"]
            cfg.runtime_channel_tls.enabled = bool(rtls.get("enabled", cfg.runtime_channel_tls.enabled))
            cfg.runtime_channel_tls.root_cert_file = rtls.get(
                "root_cert_file",
                cfg.runtime_channel_tls.root_cert_file,
            )
            cfg.runtime_channel_tls.server_name = rtls.get(
                "server_name",
                cfg.runtime_channel_tls.server_name,
            )
        if isinstance(data.get("kafka"), dict):
            k = data["kafka"]
            brokers = k.get("brokers")
            if isinstance(brokers, list):
                cfg.kafka.brokers = ",".join(str(b) for b in brokers)
            elif isinstance(brokers, str):
                cfg.kafka.brokers = brokers
        if isinstance(data.get("market_data"), dict):
            md = data["market_data"]
            cfg.market_data.preflight_enabled = bool(
                md.get("preflight_enabled", cfg.market_data.preflight_enabled)
            )
            cfg.market_data.lease_management_enabled = bool(
                md.get("lease_management_enabled", cfg.market_data.lease_management_enabled)
            )
            cfg.market_data.lease_heartbeat_seconds = int(
                md.get("lease_heartbeat_seconds", cfg.market_data.lease_heartbeat_seconds)
            )
            cfg.market_data.lease_ttl_seconds = int(
                md.get("lease_ttl_seconds", cfg.market_data.lease_ttl_seconds)
            )
            cfg.market_data.freshness_grace_seconds = int(
                md.get("freshness_grace_seconds", cfg.market_data.freshness_grace_seconds)
            )
        if isinstance(data.get("notification"), dict):
            nt = data["notification"]
            cfg.notification.enabled = bool(nt.get("enabled", cfg.notification.enabled))
            cfg.notification.timeout_seconds = float(
                nt.get("timeout_seconds", cfg.notification.timeout_seconds)
            )
        if isinstance(data.get("log"), dict):
            lg = data["log"]
            cfg.log.output_dir = lg.get("output_dir", cfg.log.output_dir)
            k = lg.get("kafka", {}) or {}
            cfg.log.kafka_enabled = bool(k.get("enabled", cfg.log.kafka_enabled))
            lb = k.get("brokers")
            if isinstance(lb, list):
                cfg.log.kafka_brokers = [str(b) for b in lb]
            cfg.log.topic_prefix = k.get("topic_prefix", cfg.log.topic_prefix)
            t = lg.get("tracing", {}) or {}
            cfg.log.tracing.enabled = bool(t.get("enabled", cfg.log.tracing.enabled))
            cfg.log.tracing.endpoint = t.get("endpoint", cfg.log.tracing.endpoint)
            cfg.log.tracing.service_name = t.get("service_name", cfg.log.tracing.service_name)
        return cfg

    def apply_env_overrides(self) -> None:
        # Database
        v = os.environ.get("DATABASE_HOST") or os.environ.get("TIMESCALE_HOST")
        if v:
            self.database.host = v
        v = os.environ.get("DATABASE_PORT") or os.environ.get("TIMESCALE_PORT")
        if v:
            self.database.port = int(v)
        v = os.environ.get("DATABASE_DATABASE") or os.environ.get("TIMESCALE_DB")
        if v:
            self.database.database = v
        v = os.environ.get("DATABASE_USER") or os.environ.get("TIMESCALE_USER")
        if v:
            self.database.user = v
        v = os.environ.get("DATABASE_PASSWORD") or os.environ.get("TIMESCALE_PASSWORD")
        if v:
            self.database.password = v
        # Dependencies
        v = (
            os.environ.get("DEPENDENCIES_CORE_SERVICE_GRPC")
            or os.environ.get("CORE_SERVICE_GRPC_ADDR")
        )
        if v:
            self.dependencies.account_service_grpc = v
        v = os.environ.get("DEPENDENCIES_ORDER_SERVICE_GRPC") or os.environ.get("ORDER_SERVICE_GRPC_ADDR")
        if v:
            self.dependencies.order_service_grpc = v
        v = os.environ.get("DEPENDENCIES_CONTROL_PANEL_SERVICE_GRPC") or os.environ.get(
            "CONTROL_PANEL_SERVICE_GRPC_ADDR"
        )
        if v:
            self.dependencies.control_panel_service_grpc = v
        v = os.environ.get("DEPENDENCIES_MARKET_DATA_CONTROL_PANEL_GRPC") or os.environ.get(
            "MARKET_DATA_CONTROL_PANEL_GRPC_ADDR"
        )
        if v:
            self.dependencies.market_data_control_panel_grpc = v
        # D2: if no explicit market-data address is set anywhere, fall back to
        # the runtime control-plane address (both services live on the same
        # gRPC port :50054 in D2). When a future deployment wants to point
        # them at separate processes, set the explicit override.
        if (
            not self.dependencies.market_data_control_panel_grpc
            and self.dependencies.control_panel_service_grpc
        ):
            self.dependencies.market_data_control_panel_grpc = (
                self.dependencies.control_panel_service_grpc
            )
        # RuntimeChannel metadata
        v = os.environ.get("RUNTIME_CREDENTIAL_PATH")
        if v:
            self.runtime.credential_path = v
        v = os.environ.get("RUNTIME_SOURCE")
        if v:
            self.runtime.source = v
        v = os.environ.get("RUNTIME_RUNTIME_ID")
        if v:
            self.runtime.runtime_id = v
        v = os.environ.get("RUNTIME_NAME")
        if v:
            self.runtime.name = v
        v = os.environ.get("RUNTIME_RESOURCE_PROFILE")
        if v:
            self.runtime.resource_profile = v
        v = os.environ.get("RUNTIME_VERSION")
        if v:
            self.runtime.version = v
        v = os.environ.get("RUNTIME_HEARTBEAT_INTERVAL_SECONDS")
        if v:
            self.runtime.heartbeat_interval_seconds = int(v)
        v = os.environ.get("RUNTIME_CHANNEL_TLS_ENABLED")
        if v:
            self.runtime_channel_tls.enabled = v.lower() in ("1", "true", "yes", "on")
        v = os.environ.get("RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE")
        if v:
            self.runtime_channel_tls.root_cert_file = v
        v = os.environ.get("RUNTIME_CHANNEL_TLS_SERVER_NAME")
        if v:
            self.runtime_channel_tls.server_name = v
        # Kafka
        v = os.environ.get("KAFKA_BROKERS")
        if v:
            self.kafka.brokers = v
        v = os.environ.get("MARKET_DATA_LEASE_HEARTBEAT_SECONDS")
        if v:
            self.market_data.lease_heartbeat_seconds = int(v)
        v = os.environ.get("MARKET_DATA_PREFLIGHT_ENABLED")
        if v:
            self.market_data.preflight_enabled = v.lower() in ("1", "true", "yes", "on")
        v = os.environ.get("MARKET_DATA_LEASE_MANAGEMENT_ENABLED")
        if v:
            self.market_data.lease_management_enabled = v.lower() in ("1", "true", "yes", "on")
        v = os.environ.get("MARKET_DATA_LEASE_TTL_SECONDS")
        if v:
            self.market_data.lease_ttl_seconds = int(v)
        v = os.environ.get("MARKET_DATA_FRESHNESS_GRACE_SECONDS")
        if v:
            self.market_data.freshness_grace_seconds = int(v)
        v = os.environ.get("NOTIFICATION_ENABLED")
        if v:
            self.notification.enabled = v.lower() in ("1", "true", "yes", "on")
        v = os.environ.get("NOTIFICATION_TIMEOUT_SECONDS")
        if v:
            self.notification.timeout_seconds = float(v)
        # Tracing
        v = os.environ.get("LOG_TRACING_ENABLED")
        if v:
            self.log.tracing.enabled = v.lower() in ("1", "true", "yes", "on")
        v = os.environ.get("LOG_TRACING_ENDPOINT")
        if v:
            self.log.tracing.endpoint = v
        v = os.environ.get("LOG_TRACING_SERVICE_NAME")
        if v:
            self.log.tracing.service_name = v

    def timescale_dict(self) -> dict[str, Any]:
        return {
            "host": self.database.host,
            "port": self.database.port,
            "database": self.database.database,
            "user": self.database.user,
            "password": self.database.password,
        }
