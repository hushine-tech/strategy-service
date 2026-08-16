"""Behavior tests for the BTC/ETH/ZEC cross-margin momentum template."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from strategy_service.inputs import InputView, parse_declared_inputs
from strategy_service.strategy_validator import validate_strategy_code
from strategy_service.types import (
    Exchange,
    Market,
    MarketData,
    OrderDecision,
    OrderSide,
    OrderType,
    PositionSide,
)


TEMPLATE_PATH = Path("strategy_templates/btc_eth_zec_cross_momentum.py")
STEPS = {"BTCUSDT": 0.0001, "ETHUSDT": 0.001, "ZECUSDT": 0.001}


def strategy_class():
    assert TEMPLATE_PATH.exists(), "three-symbol strategy template has not been implemented"
    from strategy_templates.btc_eth_zec_cross_momentum import MyStrategy

    return MyStrategy


class IndicatorRecorder:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.markers: list[tuple[str, str, float | None, str, str, str]] = []

    def set(self, key: str, value: object) -> None:
        self.values[key] = value

    def mark(
        self,
        key: str,
        text: str = "",
        price: float | None = None,
        color: str = "",
        *,
        position: str = "",
        shape: str = "",
    ) -> None:
        self.markers.append((key, text, price, color, position, shape))


class NotifyRecorder:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def warn(self, message: str, *, title: str = "") -> bool:
        self.calls.append((title, message))
        return True


class RouteWallet:
    def __init__(
        self,
        balance: float = 1000.0,
        margin_mode: str = "cross",
        position_mode: str = "one_way",
        leverage: float = 10.0,
    ) -> None:
        self.balance = balance
        self.futures = SimpleNamespace(
            margin_mode=margin_mode,
            position_mode=position_mode,
            risk_metadata={
                symbol: SimpleNamespace(
                    configured_leverage=leverage,
                    step_size=step,
                )
                for symbol, step in STEPS.items()
            },
        )

    def get_wallet_balance(self) -> float:
        return self.balance


class PortfolioWallet:
    def __init__(self, route_wallet: RouteWallet) -> None:
        self.route_wallet = route_wallet

    def get(self, exchange: str, market: str) -> RouteWallet:
        assert exchange == Exchange.BINANCE
        assert market == Market.PERPETUAL_FUTURES
        return self.route_wallet


def tick(symbol: str, price: float) -> MarketData:
    return MarketData(
        stream_id=f"futures-{symbol.lower()}-1m",
        exchange=Exchange.BINANCE,
        market=Market.PERPETUAL_FUTURES,
        kind="kline",
        symbol=symbol,
        interval="1m",
        price=price,
        timestamp=0,
    )


def feed(strategy, view: InputView, wallet: PortfolioWallet, symbol: str, price: float):
    assert view.update(tick(symbol, price)) is True
    return strategy.on_market_data(view, wallet)


def configured_strategy():
    strategy = strategy_class()()
    strategy.indicators = IndicatorRecorder()
    strategy.notify = NotifyRecorder()
    return strategy


def assert_market_order(
    decision: OrderDecision | None,
    *,
    symbol: str,
    side: str,
    qty: str,
) -> None:
    assert isinstance(decision, OrderDecision)
    assert decision.exchange == Exchange.BINANCE
    assert decision.market == Market.PERPETUAL_FUTURES
    assert decision.symbol == symbol
    assert decision.side == side
    assert decision.qty == qty
    assert decision.order_type == OrderType.MARKET
    assert decision.position_side == PositionSide.BOTH


def test_template_is_validator_accepted_and_declares_three_exact_streams_and_targets():
    assert TEMPLATE_PATH.exists(), "three-symbol strategy template has not been implemented"
    result = validate_strategy_code(TEMPLATE_PATH.read_text(encoding="utf-8"))
    assert result.ok is True, result.issues
    strategy_type = strategy_class()

    assert [item["symbol"] for item in strategy_type.INPUTS] == [
        "ZECUSDT",
        "ETHUSDT",
        "BTCUSDT",
    ]
    assert [item["stream_id"] for item in strategy_type.INPUTS] == [
        "futures-zecusdt-1m",
        "futures-ethusdt-1m",
        "futures-btcusdt-1m",
    ]
    assert [item["symbol"] for item in strategy_type.ORDER_TARGETS] == [
        "ZECUSDT",
        "ETHUSDT",
        "BTCUSDT",
    ]
    assert set(strategy_type.INDICATORS) == {
        "reference_price",
        "change_bps",
        "trade_signal",
    }


def test_interleaved_symbols_keep_independent_references_and_emit_correct_orders():
    strategy_type = strategy_class()
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(strategy_type.INPUTS))
    wallet = PortfolioWallet(RouteWallet())

    assert feed(strategy, view, wallet, "BTCUSDT", 100000.0) is None
    assert feed(strategy, view, wallet, "ETHUSDT", 3000.0) is None
    assert feed(strategy, view, wallet, "ZECUSDT", 50.0) is None

    btc = feed(strategy, view, wallet, "BTCUSDT", 100100.0)
    eth = feed(strategy, view, wallet, "ETHUSDT", 2997.0)
    assert feed(strategy, view, wallet, "ZECUSDT", 50.04) is None
    zec = feed(strategy, view, wallet, "ZECUSDT", 50.05)

    assert_market_order(btc, symbol="BTCUSDT", side=OrderSide.BUY, qty="0.0009")
    assert_market_order(eth, symbol="ETHUSDT", side=OrderSide.SELL, qty="0.033")
    assert_market_order(zec, symbol="ZECUSDT", side=OrderSide.BUY, qty="1.998")
    assert strategy._reference_prices == {
        "BTCUSDT": 100100.0,
        "ETHUSDT": 2997.0,
        "ZECUSDT": 50.05,
    }
    assert [marker[1] for marker in strategy.indicators.markers] == [
        "BUY",
        "SELL",
        "BUY",
    ]


def test_wallet_balance_one_percent_is_margin_budget_at_ten_x():
    strategy_type = strategy_class()
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(strategy_type.INPUTS))
    wallet = PortfolioWallet(RouteWallet(balance=2000.0))

    assert feed(strategy, view, wallet, "ETHUSDT", 3000.0) is None
    order = feed(strategy, view, wallet, "ETHUSDT", 3003.0)

    assert_market_order(order, symbol="ETHUSDT", side=OrderSide.BUY, qty="0.066")
    assert float(order.qty) * 3003.0 == pytest.approx(198.198)


def test_large_jump_emits_one_order_and_resets_only_that_symbol():
    strategy_type = strategy_class()
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(strategy_type.INPUTS))
    wallet = PortfolioWallet(RouteWallet())

    assert feed(strategy, view, wallet, "ZECUSDT", 50.0) is None
    first = feed(strategy, view, wallet, "ZECUSDT", 50.2)
    second = feed(strategy, view, wallet, "ZECUSDT", 50.24)

    assert_market_order(first, symbol="ZECUSDT", side=OrderSide.BUY, qty="1.992")
    assert second is None
    assert strategy._reference_prices == {"ZECUSDT": 50.2}
    assert len(strategy.indicators.markers) == 1


@pytest.mark.parametrize(
    ("margin_mode", "position_mode", "leverage", "warning"),
    [
        ("isolated", "one_way", 10.0, "cross"),
        ("cross", "hedge", 10.0, "one_way"),
        ("cross", "one_way", 20.0, "10x"),
    ],
)
def test_invalid_account_contract_warns_skips_and_does_not_advance_reference(
    margin_mode: str,
    position_mode: str,
    leverage: float,
    warning: str,
):
    strategy_type = strategy_class()
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(strategy_type.INPUTS))
    route = RouteWallet(
        margin_mode=margin_mode,
        position_mode=position_mode,
        leverage=leverage,
    )
    wallet = PortfolioWallet(route)

    assert feed(strategy, view, wallet, "ZECUSDT", 50.0) is None
    assert feed(strategy, view, wallet, "ZECUSDT", 50.05) is None
    assert strategy._reference_prices["ZECUSDT"] == 50.0
    assert warning in strategy.notify.calls[-1][1]


def test_indicator_values_and_marker_describe_the_triggered_callback():
    strategy_type = strategy_class()
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(strategy_type.INPUTS))
    wallet = PortfolioWallet(RouteWallet())

    assert feed(strategy, view, wallet, "BTCUSDT", 100000.0) is None
    assert strategy.indicators.values == {
        "reference_price": 100000.0,
        "change_bps": 0.0,
    }
    order = feed(strategy, view, wallet, "BTCUSDT", 100100.0)

    assert order is not None
    assert strategy.indicators.values["reference_price"] == 100100.0
    assert strategy.indicators.values["change_bps"] == pytest.approx(10.0)
    assert strategy.indicators.markers[-1] == (
        "trade_signal",
        "BUY",
        100100.0,
        "#16a34a",
        "belowBar",
        "arrowUp",
    )


def test_missing_symbol_metadata_warns_and_preserves_reference():
    strategy_type = strategy_class()
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(strategy_type.INPUTS))
    route = RouteWallet()
    del route.futures.risk_metadata["ETHUSDT"]
    wallet = PortfolioWallet(route)

    assert feed(strategy, view, wallet, "ETHUSDT", 3000.0) is None
    assert feed(strategy, view, wallet, "ETHUSDT", 3003.0) is None
    assert strategy._reference_prices["ETHUSDT"] == 3000.0
    assert "risk metadata missing for ETHUSDT" in strategy.notify.calls[-1][1]


@pytest.mark.parametrize("price", [0.0, -1.0, float("nan"), float("inf")])
def test_invalid_prices_do_not_initialize_reference(price: float):
    strategy_type = strategy_class()
    strategy = configured_strategy()
    view = InputView(parse_declared_inputs(strategy_type.INPUTS))
    wallet = PortfolioWallet(RouteWallet())

    assert feed(strategy, view, wallet, "BTCUSDT", price) is None
    assert strategy._reference_prices == {}
