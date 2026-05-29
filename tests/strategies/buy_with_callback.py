from strategy_service.types import MarketData, OrderDecision


class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]

    def __init__(self) -> None:
        self.last_resp = None

    def on_market_data(self, data: MarketData, wallet):
        return OrderDecision(symbol=data.symbol, side="LONG", qty=0.05, price=None)

    def on_order_response(self, order_resp) -> None:
        self.last_resp = order_resp
