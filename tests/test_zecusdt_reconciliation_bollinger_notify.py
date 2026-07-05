"""Tests for strategy_templates/zecusdt_reconciliation_bollinger_notify.py."""

from __future__ import annotations

from types import SimpleNamespace

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
from strategy_templates.zecusdt_reconciliation_bollinger_notify import MyStrategy


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
        self.markers: list[tuple[str, str, float | None, str]] = []

    def set(self, key: str, value: object) -> None:
        self.values[key] = value

    def mark(self, key: str, text: str = "", price: float | None = None, color: str = "") -> None:
        self.markers.append((key, text, price, color))


class _NotifyRecorder:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def info(self, message: str, *, title: str = "") -> bool:
        self.calls.append({"severity": "info", "title": title, "message": message})
        return True

    def warn(self, message: str, *, title: str = "") -> bool:
        self.calls.append({"severity": "warn", "title": title, "message": message})
        return True


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
    if not view.update(_md(price)):
        return None
    return strat.on_market_data(view, wallet)


def test_declares_zecusdt_one_minute_futures_inputs_targets_and_indicators():
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
    assert MyStrategy.INDICATORS["bb_upper"]["type"] == "line"
    assert MyStrategy.INDICATORS["bb_upper"]["pane"] == "price"
    assert MyStrategy.INDICATORS["bb_middle"]["pane"] == "price"
    assert MyStrategy.INDICATORS["bb_lower"]["pane"] == "price"
    assert MyStrategy.INDICATORS["bb_width_bps"]["pane"] == "strategy"
    assert "price_change_bps" not in MyStrategy.INDICATORS
    assert MyStrategy.INDICATORS["price_change_histogram_bps"]["type"] == "histogram"
    assert MyStrategy.INDICATORS["price_change_histogram_bps"]["pane"] == "strategy"
    assert MyStrategy.INDICATORS["price_change_histogram_bps"]["config"]["negative_color"] == "rgba(220, 38, 38, 0.65)"
    assert MyStrategy.INDICATORS["trade_signal"]["type"] == "marker"


def test_point_one_percent_rise_buys_one_percent_wallet_balance_and_notifies():
    strategy = MyStrategy()
    strategy.indicators = _IndicatorRecorder()
    strategy.notify = _NotifyRecorder()
    view = _make_view()
    wallet = _StubWallet(wallet_balance=5000.0)

    assert _feed(strategy, view, 100.0, wallet) is None
    decision = _feed(strategy, view, 100.1, wallet)

    assert isinstance(decision, OrderDecision)
    assert decision.exchange == Exchange.BINANCE
    assert decision.market == Market.PERPETUAL_FUTURES
    assert decision.symbol == "ZECUSDT"
    assert decision.side == OrderSide.BUY
    assert decision.qty == "0.499"
    assert decision.order_type == OrderType.MARKET
    assert decision.position_side == PositionSide.BOTH
    assert {
        "bb_upper",
        "bb_middle",
        "bb_lower",
        "price_change_histogram_bps",
        "bb_width_bps",
    } <= set(strategy.indicators.values)
    assert "price_change_bps" not in strategy.indicators.values
    assert abs(float(strategy.indicators.values["price_change_histogram_bps"]) - 10.0) < 1e-9
    assert strategy.indicators.markers == [("trade_signal", "BUY", 100.1, "#16a34a")]
    assert strategy.notify.calls
    assert strategy.notify.calls[-1]["severity"] == "warn"
    assert "BUY ZECUSDT" in strategy.notify.calls[-1]["message"]
    assert "0.1000%" in strategy.notify.calls[-1]["message"]


def test_point_one_percent_drop_sells_one_percent_wallet_balance_and_notifies():
    strategy = MyStrategy()
    strategy.indicators = _IndicatorRecorder()
    strategy.notify = _NotifyRecorder()
    view = _make_view()
    wallet = _StubWallet(wallet_balance=5000.0)

    assert _feed(strategy, view, 100.0, wallet) is None
    decision = _feed(strategy, view, 99.9, wallet)

    assert isinstance(decision, OrderDecision)
    assert decision.side == OrderSide.SELL
    assert decision.qty == "0.5"
    assert strategy.indicators.markers == [("trade_signal", "SELL", 99.9, "#dc2626")]
    assert strategy.notify.calls[-1]["severity"] == "warn"
    assert "SELL ZECUSDT" in strategy.notify.calls[-1]["message"]
    assert "-0.1000%" in strategy.notify.calls[-1]["message"]


def test_order_is_skipped_when_one_percent_wallet_balance_is_below_min_notional():
    strategy = MyStrategy()
    strategy.notify = _NotifyRecorder()
    view = _make_view()
    wallet = _StubWallet(wallet_balance=400.0)

    assert _feed(strategy, view, 100.0, wallet) is None
    assert _feed(strategy, view, 100.1, wallet) is None
    assert strategy.notify.calls[-1]["severity"] == "info"
    assert "below min notional" in strategy.notify.calls[-1]["message"]


def test_non_zecusdt_input_is_ignored_by_declared_view():
    strategy = MyStrategy()
    view = _make_view()
    wallet = _StubWallet(wallet_balance=5000.0)

    assert view.update(_md(100.0, symbol="BTCUSDT")) is False
    assert strategy.on_market_data(view, wallet) is None


def test_on_order_response_only_sends_notification():
    strategy = MyStrategy()
    strategy.indicators = _IndicatorRecorder()
    strategy.notify = _NotifyRecorder()

    result = strategy.on_order_response(
        SimpleNamespace(
            attempt_status="ACCEPTED",
            order_id="order-1",
            status="FILLED",
            side=OrderSide.BUY,
            qty="0.499",
        )
    )

    assert result is None
    assert strategy.indicators.values == {}
    assert strategy.indicators.markers == []
    assert strategy.notify.calls == [
        {
            "severity": "info",
            "title": "ZEC Order Response",
            "message": (
                "order response | attempt_status=ACCEPTED | order_id=order-1 | "
                "status=FILLED | side=BUY | qty=0.499 | error=-"
            ),
        }
    ]


def test_on_order_update_only_sends_notification():
    strategy = MyStrategy()
    strategy.indicators = _IndicatorRecorder()
    strategy.notify = _NotifyRecorder()

    result = strategy.on_order_update(
        SimpleNamespace(
            event_type="fill",
            order_status="FILLED",
            order_id="order-1",
            symbol="ZECUSDT",
            side=OrderSide.BUY,
            executed_qty="0.499",
            avg_price="100.1",
        ),
        object(),
    )

    assert result is None
    assert strategy.indicators.values == {}
    assert strategy.indicators.markers == []
    assert strategy.notify.calls == [
        {
            "severity": "info",
            "title": "ZEC Order Update",
            "message": (
                "order update | event_type=fill | order_status=FILLED | order_id=order-1 | "
                "symbol=ZECUSDT | side=BUY | executed_qty=0.499 | avg_price=100.1"
            ),
        }
    ]
