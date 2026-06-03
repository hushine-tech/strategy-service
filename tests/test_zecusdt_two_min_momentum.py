"""Unit tests for strategy_templates/zecusdt_two_min_momentum.py."""

from __future__ import annotations

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
from strategy_templates.zecusdt_two_min_momentum import MyStrategy


class _StubPosition:
    def __init__(self, qty: float = 0.0) -> None:
        self.position_qty = qty
        self.net_qty = qty


class _StubFutures:
    def __init__(self, qty: float = 0.0) -> None:
        self.positions = {("ZECUSDT", 0): _StubPosition(qty)}

    def set_qty(self, qty: float) -> None:
        self.positions[("ZECUSDT", 0)] = _StubPosition(qty)


class _StubRouteWallet:
    def __init__(self, qty: float = 0.0) -> None:
        self.futures = _StubFutures(qty)


class _StubWallet:
    def __init__(self, qty: float = 0.0) -> None:
        self.route_wallet = _StubRouteWallet(qty)

    def set_qty(self, qty: float) -> None:
        self.route_wallet.futures.set_qty(qty)

    def get(self, exchange: str, market: str):
        assert exchange == Exchange.BINANCE
        assert market == Market.PERPETUAL_FUTURES
        return self.route_wallet


def _md(price: float, symbol: str = "ZECUSDT", interval: str = "1m") -> MarketData:
    return MarketData(
        exchange=Exchange.BINANCE,
        market=Market.PERPETUAL_FUTURES,
        symbol=symbol,
        interval=interval,
        price=price,
        timestamp=0,
    )


def _make_view() -> InputView:
    return InputView(parse_declared_inputs(MyStrategy.INPUTS))


def _feed(strat: MyStrategy, view: InputView, price: float, wallet: _StubWallet) -> OrderDecision | None:
    md = _md(price)
    if not view.update(md):
        return None
    return strat.on_market_data(view, wallet)


def _assert_order(decision: OrderDecision | None, side: str) -> None:
    assert isinstance(decision, OrderDecision)
    assert decision.exchange == Exchange.BINANCE
    assert decision.market == Market.PERPETUAL_FUTURES
    assert decision.symbol == "ZECUSDT"
    assert decision.side == side
    assert decision.qty == "0.1"
    assert decision.order_type == OrderType.MARKET
    assert decision.position_side == PositionSide.BOTH


def test_declares_zecusdt_one_minute_futures_input_and_order_target():
    assert MyStrategy.INPUTS == [
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ZECUSDT",
            "interval": "1m",
        }
    ]
    assert MyStrategy.ORDER_TARGETS == [
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ZECUSDT",
        }
    ]


def test_two_consecutive_one_minute_rises_buy_from_flat():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.0)

    assert _feed(strat, view, 100.0, wallet) is None
    assert _feed(strat, view, 100.6, wallet) is None
    _assert_order(_feed(strat, view, 101.21, wallet), OrderSide.BUY)


def test_rise_sequence_must_be_consecutive():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.0)

    assert _feed(strat, view, 100.0, wallet) is None
    assert _feed(strat, view, 100.6, wallet) is None
    assert _feed(strat, view, 100.8, wallet) is None
    assert _feed(strat, view, 101.41, wallet) is None
    _assert_order(_feed(strat, view, 102.02, wallet), OrderSide.BUY)


def test_two_consecutive_one_minute_drops_sell_from_flat():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.0)

    assert _feed(strat, view, 100.0, wallet) is None
    assert _feed(strat, view, 99.4, wallet) is None
    _assert_order(_feed(strat, view, 98.8, wallet), OrderSide.SELL)


def test_down_signal_closes_long_then_opens_short_on_next_bearish_tick():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.1)

    assert _feed(strat, view, 100.0, wallet) is None
    assert _feed(strat, view, 99.4, wallet) is None
    _assert_order(_feed(strat, view, 98.8, wallet), OrderSide.SELL)

    wallet.set_qty(0.0)
    _assert_order(_feed(strat, view, 98.2, wallet), OrderSide.SELL)


def test_up_signal_closes_short_then_opens_long_on_next_bullish_tick():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=-0.1)

    assert _feed(strat, view, 100.0, wallet) is None
    assert _feed(strat, view, 100.6, wallet) is None
    _assert_order(_feed(strat, view, 101.21, wallet), OrderSide.BUY)

    wallet.set_qty(0.0)
    _assert_order(_feed(strat, view, 101.82, wallet), OrderSide.BUY)


def test_non_zecusdt_input_is_ignored_by_declared_view():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.0)

    assert view.update(_md(100.0, symbol="BTCUSDT")) is False
    assert strat.on_market_data(view, wallet) is None
