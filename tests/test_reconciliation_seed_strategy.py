"""Tests for scripts/seed_reconciliation_test_strategy.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from strategy_service.inputs import InputView, parse_declared_inputs
from strategy_service.types import (
    Exchange,
    Market,
    MarketData,
    OrderDecision,
    OrderSide,
    OrderType,
    PositionSide,
)


def _load_seed_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "seed_reconciliation_test_strategy.py"
    spec = importlib.util.spec_from_file_location("seed_reconciliation_test_strategy", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_strategy_class():
    module = _load_seed_module()
    namespace: dict[str, object] = {}
    exec(module.RECONCILIATION_TEST_CODE, namespace)
    return namespace["MyStrategy"]


class _StubRouteWallet:
    def __init__(self, wallet_balance: float) -> None:
        self.wallet_balance = wallet_balance

    def get_wallet_balance(self) -> float:
        return self.wallet_balance


class _StubWallet:
    def __init__(self, wallet_balance: float) -> None:
        self.route_wallet = _StubRouteWallet(wallet_balance)

    def get(self, exchange: str, market: str):
        assert exchange == Exchange.BINANCE
        assert market == Market.PERPETUAL_FUTURES
        return self.route_wallet


class _IndicatorRecorder:
    def __init__(self) -> None:
        self.values: dict[str, object] = {}
        self.markers: list[tuple[str, str, float | None]] = []

    def set(self, key: str, value: object) -> None:
        self.values[key] = value

    def mark(self, key: str, text: str = "", price: float | None = None, color: str = "") -> None:
        del color
        self.markers.append((key, text, price))


def _md(price: float, symbol: str = "ETHUSDT", interval: str = "1m") -> MarketData:
    return MarketData(
        exchange=Exchange.BINANCE,
        market=Market.PERPETUAL_FUTURES,
        symbol=symbol,
        interval=interval,
        price=price,
        timestamp=0,
    )


def _make_view(strategy_cls) -> InputView:
    return InputView(parse_declared_inputs(strategy_cls.INPUTS))


def _feed(strategy, view: InputView, price: float, wallet: _StubWallet) -> OrderDecision | None:
    if not view.update(_md(price)):
        return None
    return strategy.on_market_data(view, wallet)


def test_declares_ethusdt_one_minute_futures_input_and_order_target():
    strategy_cls = _load_strategy_class()

    assert strategy_cls.INPUTS == [
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ETHUSDT",
            "interval": "1m",
        }
    ]
    assert strategy_cls.ORDER_TARGETS == [
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ETHUSDT",
        }
    ]
    assert strategy_cls.INDICATORS["bb_upper"]["type"] == "line"
    assert strategy_cls.INDICATORS["bb_upper"]["pane"] == "price"
    assert strategy_cls.INDICATORS["alpha_score"]["pane"] == "strategy"
    assert strategy_cls.INDICATORS["entry_signal"]["type"] == "marker"


def test_point_one_percent_rise_buys_one_percent_wallet_balance():
    strategy_cls = _load_strategy_class()
    strategy = strategy_cls()
    view = _make_view(strategy_cls)
    wallet = _StubWallet(wallet_balance=5000.0)

    assert _feed(strategy, view, 1000.0, wallet) is None
    decision = _feed(strategy, view, 1001.0, wallet)

    assert isinstance(decision, OrderDecision)
    assert decision.exchange == Exchange.BINANCE
    assert decision.market == Market.PERPETUAL_FUTURES
    assert decision.symbol == "ETHUSDT"
    assert decision.side == OrderSide.BUY
    assert decision.qty == "0.049"
    assert decision.order_type == OrderType.MARKET
    assert decision.position_side == PositionSide.BOTH


def test_point_one_percent_drop_sells_one_percent_wallet_balance():
    strategy_cls = _load_strategy_class()
    strategy = strategy_cls()
    view = _make_view(strategy_cls)
    wallet = _StubWallet(wallet_balance=5000.0)

    assert _feed(strategy, view, 1000.0, wallet) is None
    decision = _feed(strategy, view, 999.0, wallet)

    assert isinstance(decision, OrderDecision)
    assert decision.side == OrderSide.SELL
    assert decision.qty == "0.05"


def test_order_is_skipped_when_one_percent_wallet_balance_is_below_min_notional():
    strategy_cls = _load_strategy_class()
    strategy = strategy_cls()
    view = _make_view(strategy_cls)
    wallet = _StubWallet(wallet_balance=1000.0)

    assert _feed(strategy, view, 1000.0, wallet) is None
    assert _feed(strategy, view, 1001.0, wallet) is None


def test_seed_strategy_writes_custom_indicators_when_runtime_injects_writer():
    strategy_cls = _load_strategy_class()
    strategy = strategy_cls()
    strategy.indicators = _IndicatorRecorder()
    view = _make_view(strategy_cls)
    wallet = _StubWallet(wallet_balance=5000.0)

    assert _feed(strategy, view, 1000.0, wallet) is None
    decision = _feed(strategy, view, 1001.0, wallet)

    assert isinstance(decision, OrderDecision)
    assert {"bb_upper", "bb_middle", "bb_lower", "alpha_score"} <= set(strategy.indicators.values)
    assert strategy.indicators.values["bb_middle"] is not None
    assert strategy.indicators.markers == [("entry_signal", "BUY", 1001.0)]
