from __future__ import annotations

from strategy_service.cli.hushine_runtime import _bare_runtime_id, _bare_runtime_name, _force_runtime_channel_boundary
from strategy_service.config import Config


def test_runtime_channel_startup_ignores_internal_platform_dependencies():
    cfg = Config()
    cfg.dependencies.account_service_grpc = "127.0.0.1:50051"
    cfg.dependencies.order_service_grpc = "127.0.0.1:50051"
    cfg.dependencies.control_panel_service_grpc = "127.0.0.1:50055"
    cfg.dependencies.market_data_control_panel_grpc = "127.0.0.1:50055"
    cfg.kafka.brokers = "192.168.88.10:19092"
    cfg.log.kafka_enabled = True
    cfg.log.kafka_brokers = ["192.168.88.10:19092"]
    cfg.database.host = "192.168.88.10"
    cfg.database.database = "binance_{year}"

    _force_runtime_channel_boundary(cfg)

    assert cfg.dependencies.account_service_grpc == ""
    assert cfg.dependencies.order_service_grpc == ""
    assert cfg.dependencies.control_panel_service_grpc == "127.0.0.1:50055"
    assert cfg.dependencies.market_data_control_panel_grpc == ""
    assert cfg.kafka.brokers == ""
    assert cfg.log.kafka_enabled is False
    assert cfg.log.kafka_brokers == []
    assert cfg.database.host == ""
    assert cfg.database.database == ""


def test_runtime_channel_tls_env_overrides(monkeypatch):
    cfg = Config()
    monkeypatch.setenv("RUNTIME_CHANNEL_TLS_ENABLED", "true")
    monkeypatch.setenv("RUNTIME_CHANNEL_TLS_ROOT_CERT_FILE", "/tmp/ca.pem")
    monkeypatch.setenv("RUNTIME_CHANNEL_TLS_SERVER_NAME", "runtime-channel.local")

    cfg.apply_env_overrides()

    assert cfg.runtime_channel_tls.enabled is True
    assert cfg.runtime_channel_tls.root_cert_file == "/tmp/ca.pem"
    assert cfg.runtime_channel_tls.server_name == "runtime-channel.local"


def test_bare_runtime_id_and_name_can_be_configured():
    assert _bare_runtime_id("desk-runtime", 42) == "desk-runtime"
    assert _bare_runtime_name("desk-debug", 42) == "desk-debug"


def test_bare_runtime_id_and_name_are_generated_when_empty():
    runtime_id = _bare_runtime_id("", 42)
    runtime_name = _bare_runtime_name("", 42)

    assert runtime_id.startswith("bare-42-")
    assert runtime_name.startswith("bare-debug-42-")
