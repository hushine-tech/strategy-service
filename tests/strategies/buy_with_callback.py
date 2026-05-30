from strategy_service.types import MarketData, OrderDecision


class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT"}]

    def __init__(self) -> None:
        self.last_resp = None

    def on_market_data(self, data: MarketData, wallet):
        return OrderDecision(
            exchange="binance",
            market="perpetual_futures",
            symbol=data.symbol,
            side="BUY",
            qty="0.05",
            order_type="MARKET",
            price=None,
        )

    def on_order_response(self, order_resp) -> None:
        self.last_resp = order_resp
