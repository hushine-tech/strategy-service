from __future__ import annotations

import pytest

from run_grpc_server import _apply_runtime_dependency_boundary, _parse_runtime_ingress_mode
from strategy_service.config import Config


def test_runtime_ingress_mode_default_is_inbound():
    assert _parse_runtime_ingress_mode(None) == (True, False, "inbound")


def test_runtime_ingress_mode_outbound_disables_inbound_port():
    assert _parse_runtime_ingress_mode("outbound") == (False, True, "outbound")


def test_runtime_ingress_mode_both_is_dev_dual_path():
    assert _parse_runtime_ingress_mode("both") == (True, True, "both")


def test_runtime_ingress_mode_invalid_fails_closed():
    with pytest.raises(SystemExit):
        _parse_runtime_ingress_mode("sideways")


def test_default_order_api_address_shares_account_grpc_port():
    cfg = Config()

    assert cfg.dependencies.order_service_grpc == "127.0.0.1:50051"


def test_outbound_only_runtime_ignores_internal_dependencies():
    cfg = Config()
    cfg.dependencies.account_service_grpc = "127.0.0.1:50051"
    cfg.dependencies.order_service_grpc = "127.0.0.1:50051"
    cfg.dependencies.control_panel_service_grpc = "127.0.0.1:50054"
    cfg.dependencies.market_data_control_panel_grpc = "127.0.0.1:50054"
    cfg.kafka.brokers = "192.168.88.10:19092"
    cfg.log.kafka_enabled = True
    cfg.log.kafka_brokers = ["192.168.88.10:19092"]
    cfg.database.host = "192.168.88.10"
    cfg.database.database = "binance_{year}"

    mode = _apply_runtime_dependency_boundary(
        cfg,
        open_inbound=False,
        open_outbound=True,
        ingress_mode="outbound",
    )

    assert mode == "proxy_only"
    assert cfg.dependencies.account_service_grpc == ""
    assert cfg.dependencies.order_service_grpc == ""
    assert cfg.dependencies.control_panel_service_grpc == "127.0.0.1:50054"
    assert cfg.dependencies.market_data_control_panel_grpc == ""
    assert cfg.kafka.brokers == ""
    assert cfg.log.kafka_enabled is False
    assert cfg.log.kafka_brokers == []
    assert cfg.database.host == ""
    assert cfg.database.database == ""
