class MyStrategy:
    INPUTS = [
        {
            "exchange": "binance",
            "market": "perpetual_futures",
            "symbol": "BTCUSDT",
            "interval": "1m",
        }
    ]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        return None
