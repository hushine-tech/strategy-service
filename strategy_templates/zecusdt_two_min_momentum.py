"""ZECUSDT 1m 连续动量策略.

规则:
- 连续两根 1m close-to-close 涨幅达到 0.5% 时买入 0.1 ZECUSDT
- 连续两根 1m close-to-close 跌幅达到 0.5% 时卖出 0.1 ZECUSDT
- 反向信号先用一笔订单平当前仓位, 下一根仍满足条件时再开反向仓位
"""

from __future__ import annotations

from typing import Any

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
    _TRIGGER_PCT = 0.005
    _CONSECUTIVE_BARS = 2
    _QTY = "0.1"
    _EPS = 1e-12

    def __init__(self) -> None:
        self._last_price: float | None = None
        self._rise_count = 0
        self._drop_count = 0
        self._fallback_position_direction = 0

    def _get_position_qty(self, wallet: Any) -> float | None:
        try:
            route_wallet = wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES)
        except Exception:
            return None

        futures = getattr(route_wallet, "futures", None)
        positions = getattr(futures, "positions", None)
        if not isinstance(positions, dict):
            return None

        matched = False
        qty = 0.0
        for key, position in positions.items():
            symbol = ""
            if isinstance(key, tuple) and key:
                symbol = str(key[0]).strip().upper()
            else:
                symbol = str(getattr(position, "symbol", "") or "").strip().upper()
            if symbol != self._SYMBOL:
                continue

            raw_qty = getattr(position, "position_qty", None)
            if raw_qty is None:
                raw_qty = getattr(position, "net_qty", 0.0)
            qty += float(raw_qty or 0.0)
            matched = True

        return qty if matched else 0.0

    def _position_direction(self, wallet: Any) -> int:
        qty = self._get_position_qty(wallet)
        if qty is None:
            return self._fallback_position_direction
        if qty > self._EPS:
            return 1
        if qty < -self._EPS:
            return -1
        return 0

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

    def _remember_decision(self, side: str, current_direction: int) -> None:
        if side == OrderSide.BUY:
            self._fallback_position_direction = 0 if current_direction < 0 else 1
            return
        if side == OrderSide.SELL:
            self._fallback_position_direction = 0 if current_direction > 0 else -1

    def _update_signal(self, price: float) -> int:
        if self._last_price is None:
            self._last_price = price
            return 0

        previous = self._last_price
        self._last_price = price
        if previous <= 0:
            self._rise_count = 0
            self._drop_count = 0
            return 0

        change = (price - previous) / previous
        if change >= self._TRIGGER_PCT:
            self._rise_count += 1
            self._drop_count = 0
        elif change <= -self._TRIGGER_PCT:
            self._drop_count += 1
            self._rise_count = 0
        else:
            self._rise_count = 0
            self._drop_count = 0

        if self._rise_count >= self._CONSECUTIVE_BARS:
            return 1
        if self._drop_count >= self._CONSECUTIVE_BARS:
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
            self._rise_count = 0
            self._drop_count = 0
            return None

        signal = self._update_signal(price)
        if signal == 0:
            return None

        current_direction = self._position_direction(wallet)
        if signal > 0:
            if current_direction > 0:
                return None
            side = OrderSide.BUY
        else:
            if current_direction < 0:
                return None
            side = OrderSide.SELL

        self._remember_decision(side, current_direction)
        return self._build_decision(side)
