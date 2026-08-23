"""BTC/ETH/ZEC cross-margin momentum strategy for multi-stream testing.

Each symbol keeps an independent reference price. A move of at least 0.1%
from that reference emits one market order and resets only that symbol's
reference. Order margin is 1% of Futures wallet balance and notional sizing
uses each symbol's confirmed leverage. This template is intended for
functional testing, not production trading.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_DOWN
import math

from strategy_service.types import (
    Exchange,
    Market,
    OrderDecision,
    OrderSide,
    OrderType,
    PositionSide,
)


class MyStrategy:
    SYMBOLS = ("ZECUSDT", "ETHUSDT", "BTCUSDT")
    INPUTS = [
        {
            "stream_id": "futures-zecusdt-1m",
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "kind": "kline",
            "symbol": "ZECUSDT",
            "interval": "1m",
        },
        {
            "stream_id": "futures-ethusdt-1m",
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "kind": "kline",
            "symbol": "ETHUSDT",
            "interval": "1m",
        },
        {
            "stream_id": "futures-btcusdt-1m",
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "kind": "kline",
            "symbol": "BTCUSDT",
            "interval": "1m",
        },
    ]
    ORDER_TARGETS = [
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ZECUSDT",
        },
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ETHUSDT",
        },
        {
            "exchange": Exchange.BINANCE,
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "BTCUSDT",
        },
    ]
    INDICATORS = {
        "reference_price": {
            "name": "Reference Price",
            "type": "line",
            "pane": "price",
            "color": "#2563eb",
            "unit": "USDT",
        },
        "change_bps": {
            "name": "Change From Reference",
            "type": "line",
            "pane": "strategy",
            "color": "#7c3aed",
            "unit": "bps",
        },
        "trade_signal": {
            "name": "Trade Signal",
            "type": "marker",
            "pane": "price",
            "color": "#0f766e",
        },
    }

    TRIGGER_PCT = 0.001
    MARGIN_FRACTION = Decimal("0.01")
    LEVERAGE = 10
    EPSILON = 1e-12

    def __init__(self) -> None:
        self._reference_prices: dict[str, float] = {}
        self._active_issues: set[str] = set()

    def _warn(self, message: str) -> None:
        notify = getattr(self, "notify", None)
        callback = getattr(notify, "warn", None)
        if callable(callback):
            callback(message, title="Multi-Symbol Futures Test")

    def _warn_once(self, issue: str, message: str) -> None:
        if issue in self._active_issues:
            return
        self._active_issues.add(issue)
        self._warn(message)

    def _recover(self, issue: str) -> None:
        self._active_issues.discard(issue)

    def _set_indicators(self, reference_price: float, change_bps: float) -> None:
        indicators = getattr(self, "indicators", None)
        if indicators is None:
            return
        indicators.set("reference_price", reference_price)
        indicators.set("change_bps", change_bps)

    @staticmethod
    def _decimal(value: object) -> Decimal:
        parsed = Decimal(str(value))
        if not parsed.is_finite():
            raise InvalidOperation
        return parsed

    def _declared_leverage(self, symbol: str) -> Decimal:
        for target in self.ORDER_TARGETS:
            if str(target.get("symbol", "")).upper() == symbol:
                return self._decimal(target.get("leverage", self.LEVERAGE))
        raise ValueError(f"order target missing for {symbol}")

    def _build_order(
        self,
        wallet,
        symbol: str,
        price: float,
        side: str,
    ) -> OrderDecision | None:
        try:
            route_wallet = wallet.get(
                Exchange.BINANCE,
                Market.PERPETUAL_FUTURES,
            )
            futures = getattr(route_wallet, "futures", route_wallet)
            account_margin_issue = "account-margin-mode"
            if str(getattr(futures, "margin_mode", "")) != "cross":
                self._warn_once(
                    account_margin_issue,
                    "account must use cross margin mode",
                )
                return None
            self._recover(account_margin_issue)

            position_mode_issue = "account-position-mode"
            if str(getattr(futures, "position_mode", "")).lower() != "one_way":
                self._warn_once(
                    position_mode_issue,
                    "account must use one_way position mode",
                )
                return None
            self._recover(position_mode_issue)

            metadata = getattr(futures, "risk_metadata", {}).get(symbol)
            metadata_issue = f"risk-metadata:{symbol}"
            if metadata is None:
                self._warn_once(
                    metadata_issue,
                    f"risk metadata missing for {symbol}",
                )
                return None
            self._recover(metadata_issue)

            configured_margin_mode = str(
                getattr(metadata, "configured_margin_mode", "")
            )
            symbol_margin_issue = f"symbol-margin-mode:{symbol}"
            if configured_margin_mode != "cross":
                self._warn_once(
                    symbol_margin_issue,
                    f"{symbol} must be configured in cross margin mode",
                )
                return None
            self._recover(symbol_margin_issue)

            expected_leverage = self._declared_leverage(symbol)
            leverage = self._decimal(metadata.configured_leverage)
            leverage_issue = f"symbol-leverage:{symbol}"
            if leverage != expected_leverage:
                expected_text = format(expected_leverage.normalize(), "f")
                self._warn_once(
                    leverage_issue,
                    f"{symbol} must be configured at {expected_text}x leverage",
                )
                return None
            self._recover(leverage_issue)

            step = self._decimal(metadata.step_size)
            balance = self._decimal(route_wallet.get_wallet_balance())
            price_decimal = self._decimal(price)
            sizing_issue = f"sizing-inputs:{symbol}"
            if step <= 0 or balance <= 0 or price_decimal <= 0:
                self._warn_once(
                    sizing_issue,
                    f"invalid sizing inputs for {symbol}",
                )
                return None
            self._recover(sizing_issue)

            notional = balance * self.MARGIN_FRACTION * leverage
            qty = (
                (notional / price_decimal / step).to_integral_value(
                    rounding=ROUND_DOWN,
                )
                * step
            )
            quantity_issue = f"rounded-quantity:{symbol}"
            if qty <= 0:
                self._warn_once(
                    quantity_issue,
                    f"rounded quantity is zero for {symbol}",
                )
                return None
            self._recover(quantity_issue)
        except (
            AttributeError,
            InvalidOperation,
            OverflowError,
            TypeError,
            ValueError,
        ) as exc:
            self._warn_once(
                f"sizing-error:{symbol}:{type(exc).__name__}",
                f"cannot size {symbol} order: {type(exc).__name__}",
            )
            return None

        return OrderDecision(
            exchange=Exchange.BINANCE,
            market=Market.PERPETUAL_FUTURES,
            symbol=symbol,
            side=side,
            qty=format(qty.normalize(), "f"),
            order_type=OrderType.MARKET,
            position_side=PositionSide.BOTH,
        )

    def on_market_data(self, data, wallet) -> OrderDecision | None:
        tick = getattr(data, "trigger", None)
        if tick is None:
            return None
        symbol = str(getattr(tick, "symbol", "")).upper()
        if symbol not in self.SYMBOLS:
            return None
        try:
            price = float(tick.price)
        except (OverflowError, TypeError, ValueError):
            return None
        if not math.isfinite(price) or price <= 0:
            return None

        reference = self._reference_prices.get(symbol)
        if reference is None:
            self._reference_prices[symbol] = price
            self._set_indicators(price, 0.0)
            return None

        change = (price - reference) / reference
        change_bps = change * 10_000.0
        self._set_indicators(reference, change_bps)
        if change >= self.TRIGGER_PCT - self.EPSILON:
            side = OrderSide.BUY
        elif change <= -self.TRIGGER_PCT + self.EPSILON:
            side = OrderSide.SELL
        else:
            return None

        decision = self._build_order(wallet, symbol, price, side)
        if decision is None:
            return None

        self._reference_prices[symbol] = price
        self._set_indicators(price, change_bps)
        indicators = getattr(self, "indicators", None)
        if indicators is not None:
            color = "#16a34a" if side == OrderSide.BUY else "#dc2626"
            position = "belowBar" if side == OrderSide.BUY else "aboveBar"
            shape = "arrowUp" if side == OrderSide.BUY else "arrowDown"
            indicators.mark(
                "trade_signal",
                text=str(side),
                price=price,
                color=color,
                position=position,
                shape=shape,
            )
        return decision
