"""Unit tests for strategy_templates/eth_pyramid_futures.py."""

from __future__ import annotations

from strategy_templates.eth_pyramid_futures import MyStrategy
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


class _StubFutures:
    def __init__(self, margin_balance: float = 10_000.0) -> None:
        self._margin_balance = margin_balance

    def get_margin_balance(self) -> float:
        return float(self._margin_balance)


class _StubWallet:
    def __init__(self, margin_balance: float = 10_000.0) -> None:
        self._futures = _StubFutures(margin_balance)

    def get(self, exchange: str, market: str):
        assert exchange == Exchange.BINANCE
        assert market == Market.PERPETUAL_FUTURES
        return self._futures


def _md(
    price: float,
    symbol: str = "ETHUSDT",
    market: str = Market.PERPETUAL_FUTURES,
    interval: str = "1m",
) -> MarketData:
    return MarketData(
        exchange=Exchange.BINANCE,
        symbol=symbol,
        price=price,
        timestamp=0,
        market=market,
        interval=interval,
    )


def _make_view() -> InputView:
    return InputView(parse_declared_inputs(MyStrategy.INPUTS))


def _feed(strat: MyStrategy, view: InputView, md: MarketData, wallet) -> OrderDecision | None:
    if not view.update(md):
        return None
    return strat.on_market_data(view, wallet)


def test_first_tick_only_sets_reference_price():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet()

    assert _feed(strat, view, _md(3000.0), wallet) is None


def test_rise_over_0_1pct_triggers_long_1pct_of_margin_balance():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(margin_balance=10_000.0)

    _feed(strat, view, _md(3000.0), wallet)
    decision = _feed(strat, view, _md(3003.1), wallet)  # > +0.1%

    assert isinstance(decision, OrderDecision)
    assert decision.exchange == Exchange.BINANCE
    assert decision.side == OrderSide.BUY
    assert decision.market == Market.PERPETUAL_FUTURES
    assert decision.symbol == "ETHUSDT"
    assert decision.qty == str(round(int((100.0 / 3003.1) / 0.001) * 0.001, 3))
    assert decision.order_type == OrderType.MARKET
    assert decision.position_side == PositionSide.BOTH


def test_drop_over_0_1pct_triggers_short_1pct_of_margin_balance():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(margin_balance=10_000.0)

    _feed(strat, view, _md(3000.0), wallet)
    decision = _feed(strat, view, _md(2996.9), wallet)  # < -0.1%

    assert isinstance(decision, OrderDecision)
    assert decision.exchange == Exchange.BINANCE
    assert decision.side == OrderSide.SELL
    assert decision.market == Market.PERPETUAL_FUTURES
    assert decision.symbol == "ETHUSDT"
    assert decision.qty == str(round(int((100.0 / 2996.9) / 0.001) * 0.001, 3))
    assert decision.order_type == OrderType.MARKET
    assert decision.position_side == PositionSide.BOTH


def test_reference_price_resets_after_each_trigger():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(margin_balance=10_000.0)

    _feed(strat, view, _md(3000.0), wallet)
    d1 = _feed(strat, view, _md(3003.1), wallet)
    d2 = _feed(strat, view, _md(3004.0), wallet)  # 相对 3003.1 不到 0.1%
    d3 = _feed(strat, view, _md(3006.2), wallet)  # 相对 3003.1 超过 0.1%

    assert d1 is not None and d1.side == OrderSide.BUY
    assert d2 is None
    assert d3 is not None and d3.side == OrderSide.BUY


def test_low_margin_balance_blocks_orders_but_resets_reference():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(margin_balance=5.0)

    _feed(strat, view, _md(3000.0), wallet)
    assert _feed(strat, view, _md(3003.1), wallet) is None
    # ref 已重置到 3003.1, 再次小波动不应继续按旧 anchor 触发
    assert _feed(strat, view, _md(3004.0), wallet) is None


def test_non_declared_input_is_dropped():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet()

    assert _feed(strat, view, _md(3000.0, symbol="BTCUSDT"), wallet) is None
    assert _feed(strat, view, _md(3000.0, market="spot"), wallet) is None


def test_zero_or_negative_price_noop():
    strat = MyStrategy()
    view = _make_view()
    wallet = _StubWallet()

    assert _feed(strat, view, _md(0.0), wallet) is None
    assert _feed(strat, view, _md(-1.0), wallet) is None
