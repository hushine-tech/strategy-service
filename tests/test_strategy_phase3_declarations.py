from __future__ import annotations

import pytest

import hushine_strategy.types as sdk_types
import strategy_service.types as service_types
from strategy_service.inputs import (
    StrategyDeclarationError,
    extract_declarations,
    parse_declared_inputs,
    parse_order_targets,
)
from strategy_service.types import (
    Exchange,
    Market,
    MarketData,
    OrderDecision,
    OrderFill,
    OrderSide,
    OrderType,
    OrderUpdateEvent,
    OrderUpdateFill,
    PositionSide,
)


def test_phase3_strategy_types_are_reexported_from_public_sdk():
    symbols = (
        "Exchange",
        "Market",
        "MarketData",
        "OrderDecision",
        "OrderFill",
        "OrderSide",
        "OrderType",
        "OrderUpdateEvent",
        "OrderUpdateFill",
        "PositionSide",
    )

    for symbol in symbols:
        assert getattr(service_types, symbol) is getattr(sdk_types, symbol)


def test_phase3_strategy_types_construct_explicit_route_order_decision():
    decision = OrderDecision(
        exchange=Exchange.BINANCE,
        market=Market.PERPETUAL_FUTURES,
        symbol="ETHUSDT",
        side=OrderSide.BUY,
        qty="0.01",
        order_type=OrderType.MARKET,
        position_side=PositionSide.LONG,
    )

    assert decision.exchange == "binance"
    assert decision.market == "perpetual_futures"
    assert decision.qty == "0.01"
    assert isinstance(decision.qty, str)
    assert MarketData is sdk_types.MarketData
    assert OrderFill is sdk_types.OrderFill
    assert OrderUpdateEvent is sdk_types.OrderUpdateEvent
    assert OrderUpdateFill is sdk_types.OrderUpdateFill


def test_strategy_declaration_error_is_dedicated_exception_type():
    assert issubclass(StrategyDeclarationError, ValueError)
    assert StrategyDeclarationError is not ValueError


def test_extract_declarations_requires_order_targets():
    class StrategyWithoutOrderTargets:
        INPUTS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
                "interval": "1m",
            },
        ]

    with pytest.raises(StrategyDeclarationError, match="ORDER_TARGETS"):
        extract_declarations(StrategyWithoutOrderTargets())


def test_parse_declared_inputs_rejects_legacy_futures_alias_with_service_error():
    with pytest.raises(StrategyDeclarationError, match="unsupported market: futures"):
        parse_declared_inputs([
            {
                "exchange": "binance",
                "market": "futures",
                "symbol": "ETHUSDT",
                "interval": "1m",
            },
        ])


def test_parse_order_targets_requires_declaration_with_service_error():
    with pytest.raises(StrategyDeclarationError, match="ORDER_TARGETS"):
        parse_order_targets(None)


def test_required_routes_is_union_of_inputs_and_order_targets():
    class Strategy:
        INPUTS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
                "interval": "1m",
            },
            {
                "exchange": "okx",
                "market": "spot",
                "symbol": "BTCUSDT",
                "interval": "5m",
            },
        ]
        ORDER_TARGETS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
            },
            {
                "exchange": "okx",
                "market": "delivery_futures",
                "symbol": "ETHUSD",
            },
        ]

    declarations = extract_declarations(Strategy())

    assert declarations.required_routes == {
        ("binance", "perpetual_futures"),
        ("okx", "spot"),
        ("okx", "delivery_futures"),
    }


def test_order_target_keys_returns_exchange_market_symbol():
    class Strategy:
        INPUTS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
                "interval": "1m",
            },
        ]
        ORDER_TARGETS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
            },
            {
                "exchange": "okx",
                "market": "spot",
                "symbol": "btcusdt",
            },
        ]

    declarations = extract_declarations(Strategy())

    assert declarations.order_target_keys == {
        ("binance", "perpetual_futures", "ETHUSDT"),
        ("okx", "spot", "BTCUSDT"),
    }


def test_risk_controls_optional_defaults_empty():
    class Strategy:
        INPUTS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
                "interval": "1m",
            },
        ]
        ORDER_TARGETS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
            },
        ]

    declarations = extract_declarations(Strategy())

    assert declarations.risk_controls.max_loss_close_pct is None


def test_risk_controls_parse_max_loss_ratio():
    class Strategy:
        INPUTS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
                "interval": "1m",
            },
        ]
        ORDER_TARGETS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
            },
        ]
        RISK_CONTROLS = {"max_loss_close_pct": "0.2"}

    declarations = extract_declarations(Strategy())

    assert declarations.risk_controls.max_loss_close_pct == pytest.approx(0.2)


@pytest.mark.parametrize("value", [0, -0.1, 1.1, "abc"])
def test_risk_controls_reject_invalid_max_loss(value):
    class Strategy:
        INPUTS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
                "interval": "1m",
            },
        ]
        ORDER_TARGETS = [
            {
                "exchange": "binance",
                "market": "perpetual_futures",
                "symbol": "ETHUSDT",
            },
        ]
        RISK_CONTROLS = {"max_loss_close_pct": value}

    with pytest.raises(StrategyDeclarationError, match="max_loss_close_pct"):
        extract_declarations(Strategy())
