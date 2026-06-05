"""全链路测试策略：配合 TESTUSDT 测试数据（scripts/seed_test_data.py）使用。

逻辑：
  - 无持仓 + 价格 < 120 → BUY 0.1（开多）
  - 有持仓 + 价格 > 180 → SELL 0.1（平多）
  - 其他情况 → 不操作

注意：这里是“阈值判断”，不是“穿越判断”。
因为第一根 bar 的价格就是 100（已经 < 120），所以策略会在首根 bar 立即开多。

在当前 TESTUSDT 价格波形（100→200→50→200→80→150）上，预期产生 5 次交易：
  1. bar   0: 100.00  → BUY
  2. bar  32: >180    → SELL
  3. bar  61: <120    → BUY
  4. bar 115: >180    → SELL
  5. bar 147: <120    → BUY
"""

from strategy_service.types import OrderDecision


class MyStrategy:

    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT"}]

    def __init__(self):
        self._has_position = False

    def on_market_data(self, data, wallet):
        price = float(data.klines["close"])

        if not self._has_position and price < 120:
            self._has_position = True
            return OrderDecision(
                exchange="binance",
                market="perpetual_futures",
                symbol=data.symbol,
                side="BUY",
                qty="0.1",
                order_type="MARKET",
            )

        if self._has_position and price > 180:
            self._has_position = False
            return OrderDecision(
                exchange="binance",
                market="perpetual_futures",
                symbol=data.symbol,
                side="SELL",
                qty="0.1",
                order_type="MARKET",
            )

        return None
