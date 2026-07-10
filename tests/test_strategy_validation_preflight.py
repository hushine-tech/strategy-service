"""Saved strategy validation shared by PreviewRunStrategy and RunStrategy."""

from strategy_service.grpc_server import _strategy_validation_error


VALID_STRATEGY = """
class MyStrategy:
    INPUTS = [{
        "exchange": "binance",
        "market": "perpetual_futures",
        "symbol": "BTCUSDT",
        "interval": "1m",
    }]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        return None
"""


def test_saved_strategy_validation_accepts_current_contract():
    assert _strategy_validation_error(VALID_STRATEGY) == ""


def test_saved_strategy_validation_returns_machine_readable_issues():
    error = _strategy_validation_error("import talib\n" + VALID_STRATEGY)

    assert error.startswith("strategy code validation failed: ")
    assert '"code":"unsupported_dependency"' in error
    assert '"module":"talib"' in error


def test_file_path_strategy_defers_to_existing_loader_contract():
    assert _strategy_validation_error(None) == ""
