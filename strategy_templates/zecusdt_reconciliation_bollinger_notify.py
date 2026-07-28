"""ZECUSDT 1m 对账采样策略: 追涨杀跌 + 自定义通知 + 布林带指标.

规则:
- 价格相对上一次参考价上涨达到 0.1% 时 BUY, 下跌达到 0.1% 时 SELL
- 每次订单名义值约等于 futures wallet_balance 的 1%
- 每次触发后重置参考价, 让后续每波动 0.1% 都继续产生订单样本
- 写入自定义 Bollinger Bands 指标、涨跌柱状图和 trade marker, 方便在策略图表中观察

这不是收益策略, 只用于让 order / fill / wallet / reconciliation 更频繁地产生样本。
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_DOWN

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
    INDICATORS = {
        "bb_upper": {
            "name": "BB Upper",
            "type": "line",
            "pane": "price",
            "color": "#2563eb",
            "unit": "USDT",
        },
        "bb_middle": {
            "name": "BB Middle",
            "type": "line",
            "pane": "price",
            "color": "#64748b",
            "unit": "USDT",
        },
        "bb_lower": {
            "name": "BB Lower",
            "type": "line",
            "pane": "price",
            "color": "#2563eb",
            "unit": "USDT",
        },
        "bb_width_bps": {
            "name": "BB Width",
            "type": "line",
            "pane": "strategy",
            "color": "#7c3aed",
            "unit": "bps",
        },
        "price_change_histogram_bps": {
            "name": "Price Change Histogram",
            "type": "histogram",
            "pane": "strategy",
            "color": "#16a34a",
            "unit": "bps",
            "config": {
                "positive_color": "rgba(22, 163, 74, 0.65)",
                "negative_color": "rgba(220, 38, 38, 0.65)",
            },
        },
        "trade_signal": {
            "name": "Trade Signal",
            "type": "marker",
            "pane": "price",
        },
    }

    _SYMBOL = "ZECUSDT"
    _INTERVAL = "1m"
    _TRIGGER_PCT = 0.001
    _SIZE_PCT = 0.01
    _MIN_NOTIONAL_USDT = 5.0
    _QTY_STEP = Decimal("0.001")
    _BOLLINGER_WINDOW = 20
    _BOLLINGER_K = 2.0
    _EPS = 1e-12

    def __init__(self) -> None:
        self._ref_price: float | None = None
        self._prices: list[float] = []
        self._startup_notified = False

    def _get_tick(self, data):
        return (
            data.exchange[Exchange.BINANCE]
            .market[Market.PERPETUAL_FUTURES]
            .symbol[self._SYMBOL]
            .interval[self._INTERVAL]
        )

    def _record_indicators(self, price: float, change: float) -> None:
        self._prices.append(float(price))
        if len(self._prices) > self._BOLLINGER_WINDOW:
            self._prices = self._prices[-self._BOLLINGER_WINDOW :]

        indicators = getattr(self, "indicators", None)
        if indicators is None:
            return

        if len(self._prices) >= 2:
            middle = sum(self._prices) / len(self._prices)
            variance = sum((item - middle) ** 2 for item in self._prices) / len(self._prices)
            band = self._BOLLINGER_K * (variance ** 0.5)
            upper = middle + band
            lower = middle - band
            width_bps = ((upper - lower) / middle) * 10000.0 if middle > 0 else None
            indicators.set("bb_upper", upper)
            indicators.set("bb_middle", middle)
            indicators.set("bb_lower", lower)
            indicators.set("bb_width_bps", width_bps)
        else:
            indicators.set("bb_upper", None)
            indicators.set("bb_middle", price)
            indicators.set("bb_lower", None)
            indicators.set("bb_width_bps", None)
        change_bps = float(change) * 10000.0
        indicators.set("price_change_histogram_bps", change_bps)

    def _mark_signal(self, side: str, price: float) -> None:
        indicators = getattr(self, "indicators", None)
        if indicators is None:
            return
        color = "#16a34a" if side == OrderSide.BUY else "#dc2626"
        indicators.mark("trade_signal", text=side, price=float(price), color=color)

    def _notify(self, severity: str, message: str, *, title: str = "ZEC Reconciliation") -> None:
        notifier = getattr(self, "notify", None)
        if notifier is None:
            return
        method = getattr(notifier, severity, None)
        if not callable(method):
            return
        try:
            method(message, title=title)
        except Exception:
            return

    def _field(self, obj, name: str, default: str = "-") -> str:
        try:
            value = getattr(obj, name, default)
        except Exception:
            return default
        if value is None or value == "":
            return default
        return str(value)

    def _order_response_severity(self, order_resp) -> str:
        status = self._field(order_resp, "attempt_status", "").upper()
        if status in {"", "ACCEPTED", "RECOVERED"}:
            return "info"
        return "warn"

    def _order_update_severity(self, event) -> str:
        status = self._field(event, "order_status", "").upper()
        if status in {"REJECTED", "FAILED", "CANCELED", "CANCELLED", "EXPIRED"}:
            return "warn"
        return "info"

    def _notify_startup(self, price: float) -> None:
        if self._startup_notified:
            return
        self._startup_notified = True
        self._notify(
            "info",
            f"{self._SYMBOL} reconciliation sampler initialized at price={price:.4f}; "
            f"trigger=0.1%, size=1% wallet_balance.",
        )

    def _get_wallet_balance(self, wallet) -> float:
        try:
            futures_wallet = wallet.get(Exchange.BINANCE, Market.PERPETUAL_FUTURES)
        except Exception:
            return 0.0

        for attr in ("get_wallet_balance", "get_margin_balance", "get_available_balance"):
            getter = getattr(futures_wallet, attr, None)
            if callable(getter):
                try:
                    value = float(getter())
                except Exception:
                    continue
                if value > 0:
                    return value

        raw = getattr(futures_wallet, "wallet_balance", 0.0)
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    def _round_qty(self, qty: float) -> float:
        try:
            parsed = Decimal(str(qty))
        except (InvalidOperation, ValueError):
            return 0.0
        if not parsed.is_finite() or parsed <= 0:
            return 0.0
        floored = parsed.quantize(self._QTY_STEP, rounding=ROUND_DOWN)
        return float(floored)

    def _build_decision(self, side: str, qty: float) -> OrderDecision:
        return OrderDecision(
            exchange=Exchange.BINANCE,
            market=Market.PERPETUAL_FUTURES,
            symbol=self._SYMBOL,
            side=side,
            qty=str(qty),
            order_type=OrderType.MARKET,
            position_side=PositionSide.BOTH,
        )

    def _signal_message(self, side: str, price: float, change: float, qty: float, notional: float) -> str:
        return (
            f"{side} {self._SYMBOL} | price={price:.4f} | change={change * 100.0:.4f}% | "
            f"qty={qty} | notional={notional:.4f} USDT | ref reset"
        )

    def on_market_data(self, data, wallet) -> OrderDecision | None:
        tick = self._get_tick(data)
        if tick is None:
            return None

        try:
            price = float(tick.price)
        except (TypeError, ValueError):
            return None
        if price <= 0:
            return None

        if self._ref_price is None:
            self._ref_price = price
            self._record_indicators(price, 0.0)
            self._notify_startup(price)
            return None

        previous = self._ref_price
        if previous <= 0:
            self._ref_price = price
            self._record_indicators(price, 0.0)
            return None

        change = (price - previous) / previous
        self._record_indicators(price, change)
        if abs(change) < self._TRIGGER_PCT - self._EPS:
            return None

        wallet_balance = self._get_wallet_balance(wallet)
        notional_usdt = wallet_balance * self._SIZE_PCT
        self._ref_price = price
        if notional_usdt < self._MIN_NOTIONAL_USDT:
            self._notify(
                "info",
                f"{self._SYMBOL} signal skipped: 1% wallet notional {notional_usdt:.4f} USDT "
                f"is below min notional {self._MIN_NOTIONAL_USDT:.4f} USDT.",
            )
            return None

        qty = self._round_qty(notional_usdt / price)
        if qty <= 0:
            self._notify(
                "info",
                f"{self._SYMBOL} signal skipped: rounded qty is zero at price={price:.4f}.",
            )
            return None

        side = OrderSide.BUY if change > 0 else OrderSide.SELL
        self._mark_signal(side, price)
        self._notify("warn", self._signal_message(side, price, change, qty, notional_usdt))
        return self._build_decision(side, qty)

    def on_order_response(self, order_resp) -> None:
        self._notify(
            self._order_response_severity(order_resp),
            "order response | "
            f"attempt_status={self._field(order_resp, 'attempt_status')} | "
            f"order_id={self._field(order_resp, 'order_id')} | "
            f"status={self._field(order_resp, 'status')} | "
            f"side={self._field(order_resp, 'side')} | "
            f"qty={self._field(order_resp, 'qty')} | "
            f"error={self._field(order_resp, 'error_message')}",
            title="ZEC Order Response",
        )

    def on_order_update(self, event, wallet) -> None:
        self._notify(
            self._order_update_severity(event),
            "order update | "
            f"event_type={self._field(event, 'event_type')} | "
            f"order_status={self._field(event, 'order_status')} | "
            f"order_id={self._field(event, 'order_id')} | "
            f"symbol={self._field(event, 'symbol')} | "
            f"side={self._field(event, 'side')} | "
            f"executed_qty={self._field(event, 'executed_qty')} | "
            f"avg_price={self._field(event, 'avg_price')}",
            title="ZEC Order Update",
        )
