from pathlib import Path

from strategy_service.config import Config


def test_runtime_config_defaults_do_not_embed_platform_infrastructure():
    cfg = Config()

    assert cfg.database.host == ""
    assert cfg.database.database == ""
    assert cfg.database.user == ""
    assert cfg.database.password == ""
    assert cfg.dependencies.portfolio_service_grpc == ""
    assert cfg.dependencies.order_service_grpc == ""
    assert cfg.dependencies.control_panel_service_grpc == ""
    assert cfg.kafka.brokers == ""
    assert cfg.log.kafka_enabled is False
    assert cfg.log.kafka_brokers == []


def test_default_config_file_is_runtime_only():
    config_path = Path(__file__).resolve().parents[1] / "config.yaml"

    cfg = Config.load(str(config_path))

    assert cfg.dependencies.runtime_channel_grpc == "127.0.0.1:50055"
    assert cfg.database.host == ""
    assert cfg.database.database == ""
    assert cfg.dependencies.portfolio_service_grpc == ""
    assert cfg.dependencies.order_service_grpc == ""
    assert cfg.dependencies.control_panel_service_grpc == ""
    assert cfg.kafka.brokers == ""
    assert cfg.log.kafka_enabled is False
    assert cfg.log.kafka_brokers == []
    assert cfg.log.tracing.enabled is False
    assert cfg.log.tracing.endpoint == ""


def test_apply_env_overrides_uses_core_service_grpc_addr(monkeypatch):
    monkeypatch.setenv("CORE_SERVICE_GRPC_ADDR", "core.internal:50051")

    cfg = Config()
    cfg.apply_env_overrides()

    assert cfg.dependencies.portfolio_service_grpc == "core.internal:50051"
