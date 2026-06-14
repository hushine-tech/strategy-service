"""Unit tests for strategy_templates/instant_once_order.py."""

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
from strategy_templates.instant_once_order import MyStrategy


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


def _feed(strat: MyStrategy, view: InputView, price: float) -> OrderDecision | None:
    md = _md(price)
    if not view.update(md):
        return None
    return strat.on_market_data(view, wallet=None)


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


def test_first_valid_kline_places_one_limit_order_then_never_again():
    strat = MyStrategy()
    view = _make_view()

    decision = _feed(strat, view, 2000.0)

    assert isinstance(decision, OrderDecision)
    assert decision.exchange == Exchange.BINANCE
    assert decision.market == Market.PERPETUAL_FUTURES
    assert decision.symbol == "ZECUSDT"
    assert decision.side == OrderSide.BUY
    assert decision.qty == "0.02"
    assert decision.order_type == OrderType.LIMIT
    assert decision.price == "2000"
    assert decision.time_in_force == "GTC"
    assert decision.position_side == PositionSide.BOTH
    assert decision.post_only is False
    assert decision.reduce_only is False

    assert _feed(strat, view, 1999.0) is None
    assert _feed(strat, view, 2100.0) is None


def test_invalid_price_does_not_consume_the_single_order():
    strat = MyStrategy()
    view = _make_view()

    assert _feed(strat, view, 0.0) is None
    assert isinstance(_feed(strat, view, 2000.0), OrderDecision)
    assert _feed(strat, view, 2001.0) is None


def test_non_zecusdt_input_is_ignored_by_declared_view():
    strat = MyStrategy()
    view = _make_view()

    assert view.update(_md(2000.0, symbol="BTCUSDT")) is False
    assert strat.on_market_data(view, wallet=None) is None
