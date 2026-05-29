"""Strategy that tries to emit an order for a symbol it did NOT declare.

Pre-C3 order-universe guard (see ``BaseStrategy.running_strategy``) must
reject this at runtime with a ValueError, preventing a strategy from passing
preflight for ETHUSDT and then quietly placing BTCUSDT orders.
"""

from strategy_service.types import MarketData, OrderDecision


class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]

    def on_market_data(self, data: MarketData, wallet):
        # Declared: (futures, TESTUSDT). Rogue symbol → must be rejected.
        return OrderDecision(symbol="ETHUSDT", side="LONG", qty=0.01, price=None)
