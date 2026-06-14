from strategy_service.strategy_validator import validate_strategy_code


def test_validator_allows_stdlib_and_profile_modules():
    result = validate_strategy_code(
        """
import math
import pandas as pd
from typing import Any

class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        return None
"""
    )

    assert result.ok is True
    assert result.issues == []


def test_validator_allows_public_platform_strategy_types():
    result = validate_strategy_code(
        """
from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT"}]

    def on_market_data(self, data, wallet):
        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="BTCUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.MARKET)
"""
    )

    assert result.ok is True
    assert result.issues == []


def test_validator_accepts_public_hushine_strategy_import():
    result = validate_strategy_code(
        """
from hushine_strategy import Exchange, Market, OrderDecision, OrderSide, OrderType

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT"}]

    def on_market_data(self, data, wallet):
        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="BTCUSDT", side=OrderSide.BUY, qty="0.01", order_type=OrderType.MARKET)
"""
    )

    assert result.ok is True
    assert result.issues == []


def test_validator_rejects_internal_platform_modules():
    result = validate_strategy_code(
        """
from strategy_service.wallet import BinanceWalletRuntime

class MyStrategy:
    def on_market_data(self, data, wallet):
        return None
"""
    )

    assert result.ok is False
    assert result.issues[0].code == "unsupported_dependency"
    assert result.issues[0].module == "strategy_service"


def test_validator_rejects_unknown_dependency():
    result = validate_strategy_code(
        """
import talib

class MyStrategy:
    def on_market_data(self, data, wallet):
        return None
"""
    )

    assert result.ok is False
    assert result.issues[0].code == "unsupported_dependency"
    assert result.issues[0].module == "talib"


def test_validator_rejects_debugger_dependency_for_saved_strategy():
    result = validate_strategy_code(
        """
import debugpy

class MyStrategy:
    def on_market_data(self, data, wallet):
        return None
"""
    )

    assert result.ok is False
    assert result.issues[0].code == "debugger_dependency_not_allowed"
    assert result.issues[0].module == "debugpy"


def test_validator_reports_syntax_error_without_executing_code():
    result = validate_strategy_code("class MyStrategy(:\n    pass\n")

    assert result.ok is False
    assert result.issues[0].code == "syntax_error"


def test_validator_rejects_missing_phase3_declarations():
    result = validate_strategy_code(
        """
class MyStrategy:
    def on_market_data(self, data, wallet):
        return None
"""
    )

    assert result.ok is False
    codes = {issue.code for issue in result.issues}
    assert "missing_inputs" in codes
    assert "missing_order_targets" in codes


def test_validator_rejects_legacy_market_alias():
    result = validate_strategy_code(
        """
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        return None
"""
    )

    assert result.ok is False
    assert any(issue.code == "invalid_inputs" and "unsupported market" in issue.message for issue in result.issues)


def test_validator_accepts_risk_controls_declaration():
    result = validate_strategy_code(
        """
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT"}]
    RISK_CONTROLS = {"max_loss_close_pct": 0.2}

    def on_market_data(self, data, wallet):
        return None
"""
    )

    assert result.ok is True
    assert result.issues == []


def test_validator_rejects_invalid_risk_controls_declaration():
    result = validate_strategy_code(
        """
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT"}]
    RISK_CONTROLS = {"max_loss_close_pct": 1.2}

    def on_market_data(self, data, wallet):
        return None
"""
    )

    assert result.ok is False
    assert any(issue.code == "invalid_risk_controls" for issue in result.issues)


def test_validator_rejects_legacy_order_decision_shape():
    result = validate_strategy_code(
        """
from hushine_strategy import Exchange, Market, OrderDecision

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT"}]

    def on_market_data(self, data, wallet):
        return OrderDecision(symbol="BTCUSDT", side="BUY", qty=0.01, market=Market.PERPETUAL_FUTURES)
"""
    )

    assert result.ok is False
    codes = {issue.code for issue in result.issues}
    assert "invalid_order_decision" in codes
