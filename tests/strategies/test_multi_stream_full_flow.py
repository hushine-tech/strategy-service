"""E2E strategy covering multiple symbols and multiple intervals."""


class MyStrategy:

    INPUTS = [
        {
            "exchange": "binance",
            "market": "perpetual_futures",
            "symbol": "TESTUSDT",
            "interval": "1m",
        },
        {
            "exchange": "binance",
            "market": "perpetual_futures",
            "symbol": "TESTUSDT",
            "interval": "5m",
        },
        {
            "exchange": "binance",
            "market": "perpetual_futures",
            "symbol": "ALTUSDT",
            "interval": "1m",
        },
    ]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        return None
