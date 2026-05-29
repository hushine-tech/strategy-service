from strategy_service.types import MarketData, OrderDecision


class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]

    def on_market_data(self, data: MarketData, wallet):
        return OrderDecision(symbol=data.symbol, side="INVALID", qty=0.1, price=None)
