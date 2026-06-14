"""立即下单一次的 mock 测试策略.

用途:
- 任意有效 ZECUSDT 1m K 线到达后, 立刻发起一笔订单
- 之后无论再收到什么 K 线, 都不再发单
- 配合 Binance mock 的 scene=1..9 测试订单状态、partial fill、WS 回调和恢复逻辑

默认使用 LIMIT GTC, 因为它最适合测试 partial fill 后继续挂单的场景。
要测试 FOK / IOC / GTX / MARKET, 只改下面的配置常量。
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
    _SIDE = OrderSide.BUY
    _QTY = "0.02"
    _ORDER_TYPE = OrderType.LIMIT
    _TIME_IN_FORCE = "GTC"
    _POSITION_SIDE = PositionSide.BOTH
    _POST_ONLY = False
    _REDUCE_ONLY = False

    def __init__(self) -> None:
        self._sent = False

    def on_market_data(self, data, wallet) -> OrderDecision | None:
        if self._sent:
            return None

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

        self._sent = True
        order_price = None
        time_in_force = None
        if self._ORDER_TYPE == OrderType.LIMIT:
            order_price = self._format_price(price)
            time_in_force = self._TIME_IN_FORCE

        return OrderDecision(
            exchange=Exchange.BINANCE,
            market=Market.PERPETUAL_FUTURES,
            symbol=self._SYMBOL,
            side=self._SIDE,
            qty=self._QTY,
            order_type=self._ORDER_TYPE,
            price=order_price,
            position_side=self._POSITION_SIDE,
            time_in_force=time_in_force,
            post_only=self._POST_ONLY,
            reduce_only=self._REDUCE_ONLY,
        )

    def _format_price(self, price: float) -> str:
        text = f"{price:.8f}".rstrip("0").rstrip(".")
        if text == "":
            return "0"
        return text
