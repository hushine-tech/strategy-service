"""User strategy that never emits an order."""

from strategy_service.types import MarketData


class MyStrategy:
    INPUTS = [{"market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]

    def on_market_data(self, data: MarketData, wallet):
        return None

