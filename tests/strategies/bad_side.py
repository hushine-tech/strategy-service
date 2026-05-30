from strategy_service.types import MarketData, OrderDecision


class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "TESTUSDT"}]

    def on_market_data(self, data: MarketData, wallet):
        return OrderDecision(
            exchange="binance",
            market="perpetual_futures",
            symbol=data.symbol,
            side="INVALID",
            qty="0.1",
            order_type="MARKET",
            price=None,
        )
