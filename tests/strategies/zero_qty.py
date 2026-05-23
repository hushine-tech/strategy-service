from strategy_service.types import MarketData, OrderDecision


class MyStrategy:
    INPUTS = [{"market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]

    def on_market_data(self, data: MarketData, wallet):
        return OrderDecision(symbol=data.symbol, side="LONG", qty=0.0, price=None)
