"""Demo RSI strategy: BUY when RSI < 30 (oversold), flat otherwise."""

from strategy_service.types import MarketData, OrderDecision


class MyStrategy:
    """
    RSI-based strategy.

    Maintains a rolling window of close prices.
    Computes RSI(14) and submits BUY when RSI < 30.
    """

    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT"}]

    def __init__(self, rsi_period: int = 14, oversold: float = 30.0, qty: float = 0.01):
        self.rsi_period = rsi_period
        self.oversold = oversold
        self.qty = qty
        self._closes: list[float] = []
        self._ordered: bool = False

    def _rsi(self) -> float | None:
        if len(self._closes) < self.rsi_period + 1:
            return None
        gains = []
        losses = []
        for i in range(1, len(self._closes)):
            delta = self._closes[i] - self._closes[i - 1]
            if delta > 0:
                gains.append(delta)
                losses.append(0.0)
            else:
                gains.append(0.0)
                losses.append(-delta)
        avg_gain = sum(gains[-self.rsi_period:]) / self.rsi_period
        avg_loss = sum(losses[-self.rsi_period:]) / self.rsi_period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - 100.0 / (1.0 + rs)

    def on_market_data(self, data: MarketData, wallet) -> OrderDecision | None:
        close = float(data.klines["close"])
        self._closes.append(close)
        if len(self._closes) > self.rsi_period * 3:
            self._closes.pop(0)

        rsi_val = self._rsi()
        if rsi_val is None:
            return None

        if rsi_val < self.oversold and not self._ordered:
            self._ordered = True
            return OrderDecision(
                exchange="binance",
                market="perpetual_futures",
                symbol=data.symbol,
                side="BUY",
                qty=str(self.qty),
                order_type="MARKET",
            )
        return None

    def on_order_response(self, order_resp) -> None:
        print(
            f"  [Order Filled] {order_resp.side} {order_resp.qty} @ {order_resp.fill_price}",
            flush=True,
        )
