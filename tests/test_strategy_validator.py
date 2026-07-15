import subprocess

import pytest

from hushine_strategy.runtime_dependencies import load_runtime_dependency_profile
from strategy_service.strategy_validator import (
    StrategyValidationIssue,
    validate_strategy_code,
)


VALID_STRATEGY = """
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        return None
"""


DYNAMIC_LOADING_CASES = [
    ('import importlib; importlib.import_module("kafka")', "forbidden_import"),
    ('from importlib import import_module as load; load("psycopg2")', "forbidden_import"),
    ('loader = __import__; loader("cryptography")', "forbidden_call"),
    ('(loader := __import__)("kafka")', "forbidden_call"),
    ('exec("import kafka")', "forbidden_call"),
    ('getattr(__builtins__, "__import__")("kafka")', "forbidden_builtin_access"),
    ('vars(__builtins__)["__import__"]("kafka")', "forbidden_builtin_access"),
    ('globals()["__builtins__"]["__import__"]("kafka")', "forbidden_builtin_access"),
]


PLATFORM_IMPORT_BYPASSES = [
    (
        "from hushine_strategy import runtime_dependencies as rd\n"
        'rd.importlib.import_module("kafka")'
    ),
    (
        "from hushine_strategy.runtime_dependencies import subprocess\n"
        'subprocess.run(["true"])'
    ),
    (
        "from hushine_strategy.notifier import Path\n"
        'Path("/tmp/escape").write_text("x")'
    ),
]

BUILTINS_IMPORT_ALIAS_BYPASSES = [
    (
        "from requests import __builtins__ as b\n"
        'b["__import__"]("kafka")'
    ),
    (
        "from requests import __dict__ as d\n"
        'loader = d.get("__builtins__").get("__import__")\n'
        'loader("kafka")'
    ),
]


def _validate_prefix(prefix: str):
    return validate_strategy_code(f"{prefix}\n{VALID_STRATEGY}")


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
    assert result.allowed_third_party_modules == list(
        load_runtime_dependency_profile().public_import_roots
    )


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
    result = _validate_prefix(
        "from strategy_service.wallet import BinanceWalletRuntime"
    )

    assert result.ok is False
    assert [(issue.code, issue.module, issue.symbol) for issue in result.issues] == [
        ("forbidden_import", "strategy_service.wallet", "BinanceWalletRuntime")
    ]


def test_validator_rejects_unknown_dependency():
    result = _validate_prefix("import talib.child")

    assert result.ok is False
    assert result.issues[0].code == "UNSUPPORTED_STRATEGY_DEPENDENCY"
    assert result.issues[0].module == "talib.child"
    assert result.issues[0].symbol == ""


def test_validator_rejects_debugger_dependency_for_saved_strategy():
    result = _validate_prefix("import debugpy")

    assert result.ok is False
    assert result.issues[0].code == "UNSUPPORTED_STRATEGY_DEPENDENCY"
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


@pytest.mark.parametrize(
    "module",
    ["os.path", "collections.abc", "requests.packages.urllib3"],
)
def test_validator_statically_allows_stdlib_and_runtime_aliases(module):
    result = _validate_prefix(f"import {module}")
    assert result.ok is True
    assert result.issues == []


@pytest.mark.parametrize(
    ("source", "module"),
    [
        ("from . import x", "."),
        ("from .hushine_strategy import X", ".hushine_strategy"),
        ("from ..pandas import X", "..pandas"),
    ],
)
def test_validator_rejects_relative_imports_with_leading_dots(source, module):
    result = _validate_prefix(source)
    assert [(issue.code, issue.module) for issue in result.issues] == [
        ("forbidden_import", module)
    ]


@pytest.mark.parametrize(
    "source",
    [
        "import hushine_strategy",
        "import hushine_strategy.types as sdk",
        "import strategy_service.types",
        "from hushine_strategy import LocalNotifier",
        "from hushine_strategy.types import *",
        "from strategy_service import StrategyEngine",
        "from strategy_service.types import BaseModel",
        "from strategy_service.types.child import Exchange",
    ],
)
def test_validator_rejects_platform_module_handles_and_non_surface_symbols(source):
    result = _validate_prefix(source)
    assert result.issues
    assert {issue.code for issue in result.issues} == {"forbidden_import"}
    assert all(issue.code != "UNSUPPORTED_STRATEGY_DEPENDENCY" for issue in result.issues)


@pytest.mark.parametrize("source", PLATFORM_IMPORT_BYPASSES)
def test_validator_closes_platform_module_handle_bypasses_without_dependency_issue(source):
    result = _validate_prefix(source)
    assert result.issues
    assert {issue.code for issue in result.issues} == {"forbidden_import"}
    assert all(issue.code != "UNSUPPORTED_STRATEGY_DEPENDENCY" for issue in result.issues)


@pytest.mark.parametrize(("source", "expected_code"), DYNAMIC_LOADING_CASES)
def test_validator_dynamic_loading_uses_shared_safety_codes(source, expected_code):
    result = _validate_prefix(source)
    assert result.issues
    assert expected_code in {issue.code for issue in result.issues}
    assert all(issue.code != "UNSUPPORTED_STRATEGY_DEPENDENCY" for issue in result.issues)


def test_validator_imported_forbidden_call_alias_suppresses_dependency_issue():
    result = _validate_prefix("from kafka import eval as load\nload('payload')")
    assert result.issues
    assert {issue.code for issue in result.issues} == {"forbidden_call"}


@pytest.mark.parametrize("source", BUILTINS_IMPORT_ALIAS_BYPASSES)
def test_validator_imported_builtins_containers_cannot_bypass_safety(source):
    result = _validate_prefix(source)
    codes = {issue.code for issue in result.issues}
    assert "forbidden_builtin_access" in codes
    assert "forbidden_call" in codes
    assert "UNSUPPORTED_STRATEGY_DEPENDENCY" not in codes


def test_validator_preserves_same_line_platform_symbols():
    result = _validate_prefix(
        "from hushine_strategy import LocalNotifier, runtime_dependencies"
    )
    assert [
        issue.symbol
        for issue in result.issues
        if issue.code == "forbidden_import"
    ] == ["LocalNotifier", "runtime_dependencies"]


def test_validator_preserves_dynamic_and_platform_symbols():
    dynamic = _validate_prefix('exec("import kafka")')
    assert any(
        issue.code == "forbidden_call" and issue.symbol == "exec"
        for issue in dynamic.issues
    )
    platform = _validate_prefix("from hushine_strategy import LocalNotifier")
    assert any(
        issue.code == "forbidden_import" and issue.symbol == "LocalNotifier"
        for issue in platform.issues
    )


def test_strategy_validation_issue_symbol_defaults_to_empty_string():
    issue = StrategyValidationIssue(code="code", message="message")
    assert issue.symbol == ""


def test_validator_allows_normal_getattr_template_usage():
    result = validate_strategy_code(
        """
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        return getattr(data, "indicators", None)
"""
    )
    assert result.ok is True
    assert result.issues == []


def test_validator_sees_static_import_inside_callback():
    result = validate_strategy_code(
        """
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        import numpy
        return numpy.array([1])
"""
    )
    assert result.ok is True
    assert result.issues == []


@pytest.mark.parametrize(
    "source",
    PLATFORM_IMPORT_BYPASSES
    + BUILTINS_IMPORT_ALIAS_BYPASSES
    + [source for source, _ in DYNAMIC_LOADING_CASES],
)
def test_validator_performs_no_child_probe_for_safety_rejection(monkeypatch, source):
    calls = []

    def child_probe(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("static validation must not spawn a child")

    monkeypatch.setattr(subprocess, "Popen", child_probe)
    result = _validate_prefix(source)
    assert result.issues
    assert calls == []
