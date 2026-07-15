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
    error = _strategy_validation_error("import talib.child\n" + VALID_STRATEGY)

    assert error.startswith("strategy code validation failed: ")
    assert '"code":"UNSUPPORTED_STRATEGY_DEPENDENCY"' in error
    assert '"module":"talib.child"' in error
    assert '"symbol":""' in error


def test_saved_strategy_validation_serializes_dynamic_safety_symbol():
    error = _strategy_validation_error('exec("import kafka")\n' + VALID_STRATEGY)
    assert '"code":"forbidden_call"' in error
    assert '"symbol":"exec"' in error


def test_saved_strategy_validation_serializes_platform_safety_symbol():
    error = _strategy_validation_error(
        "from hushine_strategy import LocalNotifier\n" + VALID_STRATEGY
    )
    assert '"code":"forbidden_import"' in error
    assert '"module":"hushine_strategy"' in error
    assert '"symbol":"LocalNotifier"' in error


def test_saved_strategy_validation_serializes_each_same_line_platform_symbol():
    error = _strategy_validation_error(
        "from hushine_strategy import LocalNotifier, runtime_dependencies\n"
        + VALID_STRATEGY
    )
    assert error.count('"code":"forbidden_import"') == 2
    assert '"symbol":"LocalNotifier"' in error
    assert '"symbol":"runtime_dependencies"' in error


def test_file_path_strategy_defers_to_existing_loader_contract():
    assert _strategy_validation_error(None) == ""
