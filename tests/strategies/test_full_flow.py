"""全链路测试策略：配合 TESTUSDT 测试数据（scripts/seed_test_data.py）使用。

逻辑：
  - 无持仓 + 价格 < 120 → LONG 0.1（买入）
  - 有持仓 + 价格 > 180 → SHORT 0.1（平仓）
  - 其他情况 → 不操作

注意：这里是“阈值判断”，不是“穿越判断”。
因为第一根 bar 的价格就是 100（已经 < 120），所以策略会在首根 bar 立即开多。

在当前 TESTUSDT 价格波形（100→200→50→200→80→150）上，预期产生 5 次交易：
  1. bar   0: 100.00  → LONG
  2. bar  32: >180    → SHORT
  3. bar  61: <120    → LONG
  4. bar 115: >180    → SHORT
  5. bar 147: <120    → LONG
"""

from strategy_service.types import OrderDecision


class MyStrategy:

    INPUTS = [{"exchange": "binance", "market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]

    def __init__(self):
        self._has_position = False

    def on_market_data(self, data, wallet):
        price = float(data.klines["close"])

        if not self._has_position and price < 120:
            self._has_position = True
            return OrderDecision(symbol=data.symbol, side="LONG", qty=0.1)

        if self._has_position and price > 180:
            self._has_position = False
            return OrderDecision(symbol=data.symbol, side="SHORT", qty=0.1)

        return None
