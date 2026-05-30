"""ETHUSDT futures 高频对账策略.

目标
-----
给 mode=2 testnet 对账持续制造成交:

- 价格相对参考价上涨 `+0.1%` → 做多 `margin_balance` 的 `1%`
- 价格相对参考价下跌 `-0.1%` → 做空 `margin_balance` 的 `1%`
- 每次触发后把参考价重置到当前价, 让后续继续按 `0.1%` 步进频繁触发

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
            "symbol": "ETHUSDT",
            "interval": "1m",
        },
    ]
    ORDER_TARGETS = [
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ETHUSDT",
        },
    ]

    _TRIGGER_PCT = 0.001          # 0.1%
    _SIZE_PCT = 0.01              # margin balance 的 1%
    _MIN_MARGIN_BALANCE = 10.0    # 太小就不下单
    _QTY_DECIMALS = 3             # ETHUSDT stepSize = 0.001

    def __init__(self) -> None:
        self._ref_price: float | None = None

    def _get_margin_balance(self, wallet) -> float:
        try:
            futures = wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES)
        except Exception:
            return 0.0
        getter = getattr(futures, "get_margin_balance", None)
        if callable(getter):
            return float(getter())
        fallback = getattr(futures, "get_wallet_balance", None)
        if callable(fallback):
            return float(fallback())
        return 0.0

    def _round_qty(self, qty: float) -> float:
        step = 10 ** (-self._QTY_DECIMALS)
        floored = int(qty / step) * step
        return round(floored, self._QTY_DECIMALS)

    def on_market_data(self, data, wallet) -> OrderDecision | None:
        tick = (
            data.exchange[Exchange.BINANCE]
            .market[Market.PERPETUAL_FUTURES]
            .symbol["ETHUSDT"]
            .interval["1m"]
        )
        if tick is None:
            return None

        price = float(tick.price)
        if price <= 0:
            return None

        if self._ref_price is None:
            self._ref_price = price
            return None

        change = (price - self._ref_price) / self._ref_price
        if abs(change) < self._TRIGGER_PCT:
            return None

        margin_balance = self._get_margin_balance(wallet)
        if margin_balance < self._MIN_MARGIN_BALANCE:
            self._ref_price = price
            return None

        qty = self._round_qty((margin_balance * self._SIZE_PCT) / price)
        self._ref_price = price
        if qty <= 0:
            return None

        if change > 0:
            return OrderDecision(
                exchange=Exchange.BINANCE,
                market=Market.PERPETUAL_FUTURES,
                symbol="ETHUSDT",
                side=OrderSide.BUY,
                qty=str(qty),
                order_type=OrderType.MARKET,
                position_side=PositionSide.BOTH,
            )

        return OrderDecision(
            exchange=Exchange.BINANCE,
            market=Market.PERPETUAL_FUTURES,
            symbol="ETHUSDT",
            side=OrderSide.SELL,
            qty=str(qty),
            order_type=OrderType.MARKET,
            position_side=PositionSide.BOTH,
        )
