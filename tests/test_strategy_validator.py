from strategy_service.strategy_validator import validate_strategy_code


def test_validator_allows_stdlib_and_profile_modules():
    result = validate_strategy_code(
        """
import math
import pandas as pd
from typing import Any

class MyStrategy:
    def on_market_data(self, data, wallet):
        return None
"""
    )

    assert result.ok is True
    assert result.issues == []


def test_validator_allows_public_platform_strategy_types():
    result = validate_strategy_code(
        """
from strategy_service.types import OrderDecision

class MyStrategy:
    def on_market_data(self, data, wallet):
        return OrderDecision(symbol=data.symbol, side="LONG", qty=0.1)
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
