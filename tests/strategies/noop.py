"""User strategy that never emits an order."""

from strategy_service.types import MarketData


class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT", "interval": "1m"}]
    ORDER_TARGETS = []

    def on_market_data(self, data: MarketData, wallet):
        return None
