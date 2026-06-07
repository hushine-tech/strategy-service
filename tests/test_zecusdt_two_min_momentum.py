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


def _assert_order(decision: OrderDecision | None, side: str, qty: str = "0.1") -> None:
    assert isinstance(decision, OrderDecision)
    assert decision.exchange == Exchange.BINANCE
    assert decision.market == Market.PERPETUAL_FUTURES
    assert decision.symbol == "ZECUSDT"
    assert decision.side == side
    assert decision.qty == qty
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


def test_point_one_percent_one_minute_rise_buys_from_flat():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.0)

    assert _feed(strat, view, 100.0, wallet) is None
    _assert_order(_feed(strat, view, 100.1, wallet), OrderSide.BUY)


def test_sub_point_one_percent_move_does_not_trade():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.0)

    assert _feed(strat, view, 100.0, wallet) is None
    assert _feed(strat, view, 100.09, wallet) is None


def test_point_one_percent_one_minute_drop_sells_from_flat():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.0)

    assert _feed(strat, view, 100.0, wallet) is None
    _assert_order(_feed(strat, view, 99.9, wallet), OrderSide.SELL)


def test_each_point_one_percent_wave_resets_reference_and_can_sell_again():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.0)

    assert _feed(strat, view, 100.0, wallet) is None
    _assert_order(_feed(strat, view, 99.9, wallet), OrderSide.SELL)
    assert _feed(strat, view, 99.81, wallet) is None
    _assert_order(_feed(strat, view, 99.8001, wallet), OrderSide.SELL)


def test_same_direction_signal_places_another_order_for_reconciliation_samples():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.2)

    assert _feed(strat, view, 100.0, wallet) is None
    _assert_order(_feed(strat, view, 100.1, wallet), OrderSide.BUY)
    _assert_order(_feed(strat, view, 100.2001, wallet), OrderSide.BUY)


def test_non_zecusdt_input_is_ignored_by_declared_view():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(qty=0.0)

    assert view.update(_md(100.0, symbol="BTCUSDT")) is False
    assert strat.on_market_data(view, wallet) is None
