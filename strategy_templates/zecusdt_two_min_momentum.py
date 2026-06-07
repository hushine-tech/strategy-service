"""ZECUSDT 1m 高频对账动量策略.

规则:
- 价格相对上一次参考价上涨达到 0.1% 时买入 0.1 ZECUSDT
- 价格相对上一次参考价下跌达到 0.1% 时卖出 0.1 ZECUSDT
- 每次触发后把参考价重置到当前价, 让后续每波动 0.1% 都继续产生订单

这不是收益策略, 只用于让 order / fill / wallet / reconciliation 更频繁地产生样本。
"""

from __future__ import annotations

from strategy_service.types import (
    Exchange,
    Market,
    OrderDecision,
    OrderSide,
    OrderType,
    PositionSide,
)


class MyStrategy:
    INPUTS = [
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ZECUSDT",
            "interval": "1m",
        },
    ]
    ORDER_TARGETS = [
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ZECUSDT",
        },
    ]

    _SYMBOL = "ZECUSDT"
    _INTERVAL = "1m"
    _TRIGGER_PCT = 0.001
    _QTY = "0.1"
    _EPS = 1e-12

    def __init__(self) -> None:
        self._ref_price: float | None = None

    def _build_decision(self, side: str) -> OrderDecision:
        return OrderDecision(
            exchange=Exchange.BINANCE,
            market=Market.PERPETUAL_FUTURES,
            symbol=self._SYMBOL,
            side=side,
            qty=self._QTY,
            order_type=OrderType.MARKET,
            position_side=PositionSide.BOTH,
        )

    def _update_signal(self, price: float) -> int:
        if self._ref_price is None:
            self._ref_price = price
            return 0

        previous = self._ref_price
        if previous <= 0:
            self._ref_price = price
            return 0

        change = (price - previous) / previous
        if change >= self._TRIGGER_PCT - self._EPS:
            self._ref_price = price
            return 1
        if change <= -self._TRIGGER_PCT + self._EPS:
            self._ref_price = price
            return -1
        return 0

    def on_market_data(self, data, wallet) -> OrderDecision | None:
        tick = (
            data.exchange[Exchange.BINANCE]
            .market[Market.PERPETUAL_FUTURES]
            .symbol[self._SYMBOL]
            .interval[self._INTERVAL]
        )
        if tick is None:
            return None

        price = float(tick.price)
        if price <= 0:
            return None

        signal = self._update_signal(price)
        if signal == 0:
            return None

        if signal > 0:
            return self._build_decision(OrderSide.BUY)
        return self._build_decision(OrderSide.SELL)
