"""Tests for the strategy-input-universe contract.

Covers:
  1. Declaration parsing/validation (`parse_declared_inputs`)
  2. InputView accessor semantics
  3. Router binding driven ONLY by declared INPUTS (pre_C3 §2.2 + spec)
  4. Multi-symbol / multi-market / multi-interval support
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from strategy_service import MarketData, StrategyService
from strategy_service.inputs import (
    InputView,
    StrategyDeclarationError,
    StrategyInput,
    parse_declared_inputs,
)
from tests.helpers.wallet_fixtures import make_backtest_wallet


# ── parse_declared_inputs ───────────────────────────────────────────────────

def test_parse_dict_entries_normalizes_case_and_whitespace():
    result = parse_declared_inputs([
        {"market": "FUTURES", "symbol": "ethusdt", "interval": " 1m "},
    ])
    assert result == [StrategyInput("futures", "ETHUSDT", "1m")]


def test_parse_tuple_entries():
    result = parse_declared_inputs([("spot", "BTCUSDT", "5m")])
    assert result == [StrategyInput("spot", "BTCUSDT", "5m")]


def test_parse_colon_string_entries():
    result = parse_declared_inputs(["futures:ADAUSDT:15m"])
    assert result == [StrategyInput("futures", "ADAUSDT", "15m")]


def test_parse_dedupes_equal_entries():
    result = parse_declared_inputs([
        {"market": "futures", "symbol": "BTCUSDT", "interval": "1m"},
        {"market": "futures", "symbol": "btcusdt", "interval": "1m"},
    ])
    assert result == [StrategyInput("futures", "BTCUSDT", "1m")]


def test_parse_rejects_missing_declaration():
    with pytest.raises(StrategyDeclarationError, match="no INPUTS declaration"):
        parse_declared_inputs(None)


def test_parse_rejects_empty_list():
    with pytest.raises(StrategyDeclarationError, match="empty universe"):
        parse_declared_inputs([])


def test_parse_rejects_unknown_market():
    with pytest.raises(StrategyDeclarationError, match="unsupported market"):
        parse_declared_inputs([{"market": "perp", "symbol": "BTCUSDT", "interval": "1m"}])


def test_parse_rejects_empty_symbol():
    with pytest.raises(StrategyDeclarationError):
        parse_declared_inputs([{"market": "futures", "symbol": "   ", "interval": "1m"}])


def test_parse_rejects_empty_interval():
    with pytest.raises(StrategyDeclarationError):
        parse_declared_inputs([{"market": "futures", "symbol": "BTCUSDT", "interval": ""}])


def test_parse_rejects_bare_scalar():
    # A bare dict at the top level is almost always a typo.
    with pytest.raises(StrategyDeclarationError):
        parse_declared_inputs({"market": "futures", "symbol": "BTCUSDT", "interval": "1m"})


# ── InputView ───────────────────────────────────────────────────────────────

def _md(symbol: str, market: str, interval: str, price: float = 100.0) -> MarketData:
    return MarketData(
        symbol=symbol, price=price, timestamp=datetime.now(timezone.utc),
        market=market, interval=interval,
    )


def test_view_returns_none_for_declared_key_before_first_update():
    view = InputView([StrategyInput("futures", "ETHUSDT", "1m")])
    assert view.market["futures"].symbol["ETHUSDT"].interval["1m"] is None


def test_view_returns_latest_md_after_update():
    view = InputView([StrategyInput("futures", "ETHUSDT", "1m")])
    md = _md("ETHUSDT", "futures", "1m", price=3000.0)
    assert view.update(md) is True
    got = view.market["futures"].symbol["ETHUSDT"].interval["1m"]
    assert got is md


def test_view_update_returns_false_for_undeclared_key():
    view = InputView([StrategyInput("futures", "ETHUSDT", "1m")])
    assert view.update(_md("BTCUSDT", "futures", "1m")) is False
    # Nothing cached for undeclared key.
    with pytest.raises(KeyError):
        _ = view.market["futures"].symbol["BTCUSDT"]


def test_view_raises_key_error_on_undeclared_market():
    view = InputView([StrategyInput("futures", "ETHUSDT", "1m")])
    with pytest.raises(KeyError, match="market"):
        _ = view.market["spot"]


def test_view_raises_key_error_on_undeclared_symbol():
    view = InputView([StrategyInput("futures", "ETHUSDT", "1m")])
    with pytest.raises(KeyError, match="symbol"):
        _ = view.market["futures"].symbol["BTCUSDT"]


def test_view_raises_key_error_on_undeclared_interval():
    view = InputView([StrategyInput("futures", "ETHUSDT", "1m")])
    with pytest.raises(KeyError, match="interval"):
        _ = view.market["futures"].symbol["ETHUSDT"].interval["5m"]


def test_view_trigger_is_most_recently_updated_md():
    view = InputView([
        StrategyInput("futures", "BTCUSDT", "1m"),
        StrategyInput("futures", "ETHUSDT", "1m"),
    ])
    view.update(_md("BTCUSDT", "futures", "1m", price=50000.0))
    view.update(_md("ETHUSDT", "futures", "1m", price=3000.0))
    assert view.trigger is not None
    assert view.trigger.symbol == "ETHUSDT"
    # Proxies delegate to trigger.
    assert view.price == 3000.0
    assert view.symbol == "ETHUSDT"


def test_view_keys_expose_declared_structure():
    view = InputView([
        StrategyInput("futures", "BTCUSDT", "1m"),
        StrategyInput("futures", "BTCUSDT", "5m"),
        StrategyInput("spot", "ETHUSDT", "1m"),
    ])
    assert set(view.market.keys()) == {"futures", "spot"}
    assert set(view.market["futures"].symbol.keys()) == {"BTCUSDT"}
    assert set(view.market["futures"].symbol["BTCUSDT"].interval.keys()) == {"1m", "5m"}
    assert set(view.market["spot"].symbol.keys()) == {"ETHUSDT"}


# ── Router: declared-only binding ──────────────────────────────────────────

def _empty_wallet():
    """A backtest wallet with no futures positions and no spot assets.
    Pre_C3 §2.2: strategy creation MUST still succeed on an empty wallet."""
    return make_backtest_wallet(margin_mode="isolated")


def test_router_binds_only_to_declared_inputs_even_on_empty_wallet():
    wallet = _empty_wallet()
    svc = StrategyService()
    code = """
class MyStrategy:
    INPUTS = [{"market": "futures", "symbol": "ETHUSDT", "interval": "1m"}]
    def on_market_data(self, data, wallet):
        return None
"""
    svc.create_strategy("u1", "<db:router_test>", wallet, strategy_code=code)

    assert ("futures", "ETHUSDT", "1m") in svc.strategy_router
    # Undeclared keys MUST NOT appear.
    assert ("futures", "BTCUSDT", "1m") not in svc.strategy_router
    assert ("spot", "ETHUSDT", "1m") not in svc.strategy_router


def test_router_drops_undeclared_ticks_silently():
    wallet = _empty_wallet()
    svc = StrategyService()
    code = """
class MyStrategy:
    INPUTS = [{"market": "futures", "symbol": "ETHUSDT", "interval": "1m"}]
    def on_market_data(self, data, wallet):
        return None
"""
    svc.create_strategy("u1", "<db:drop_test>", wallet, strategy_code=code)

    # Declared → routes (returns True).
    assert svc.running_strategy(_md("ETHUSDT", "futures", "1m")) is True
    # Undeclared market/symbol/interval → all dropped.
    assert svc.running_strategy(_md("ETHUSDT", "spot", "1m")) is False
    assert svc.running_strategy(_md("BTCUSDT", "futures", "1m")) is False
    assert svc.running_strategy(_md("ETHUSDT", "futures", "5m")) is False


def test_declared_input_routes_without_wallet_position():
    """Spec scenario: declared input is routable without a wallet slot."""
    wallet = _empty_wallet()
    svc = StrategyService()
    code = """
class MyStrategy:
    INPUTS = [{"market": "futures", "symbol": "ETHUSDT", "interval": "1m"}]
    def __init__(self):
        self.ticks_seen = 0
    def on_market_data(self, data, wallet):
        self.ticks_seen += 1
        return None
"""
    strat = svc.create_strategy("u1", "<db:empty_wallet_routes>", wallet, strategy_code=code)
    svc.running_strategy(_md("ETHUSDT", "futures", "1m"))
    assert strat._strategy_instance.ticks_seen == 1


# ── Multi-input coverage (task 3.1) ────────────────────────────────────────

def test_multi_symbol_same_market_both_route():
    wallet = _empty_wallet()
    svc = StrategyService()
    code = """
class MyStrategy:
    INPUTS = [
        {"market": "futures", "symbol": "BTCUSDT", "interval": "1m"},
        {"market": "futures", "symbol": "ETHUSDT", "interval": "1m"},
    ]
    def __init__(self):
        self.seen = []
    def on_market_data(self, data, wallet):
        self.seen.append((data.trigger.market, data.trigger.symbol, data.trigger.interval))
        return None
"""
    strat = svc.create_strategy("u1", "<db:multi_symbol>", wallet, strategy_code=code)
    svc.running_strategy(_md("BTCUSDT", "futures", "1m"))
    svc.running_strategy(_md("ETHUSDT", "futures", "1m"))
    assert strat._strategy_instance.seen == [
        ("futures", "BTCUSDT", "1m"),
        ("futures", "ETHUSDT", "1m"),
    ]


def test_mixed_spot_and_futures_both_route():
    wallet = _empty_wallet()
    svc = StrategyService()
    code = """
class MyStrategy:
    INPUTS = [
        {"market": "futures", "symbol": "BTCUSDT", "interval": "1m"},
        {"market": "spot",    "symbol": "BTCUSDT", "interval": "1m"},
    ]
    def __init__(self):
        self.markets = []
    def on_market_data(self, data, wallet):
        self.markets.append(data.trigger.market)
        return None
"""
    strat = svc.create_strategy("u1", "<db:mixed>", wallet, strategy_code=code)
    svc.running_strategy(_md("BTCUSDT", "futures", "1m"))
    svc.running_strategy(_md("BTCUSDT", "spot", "1m"))
    assert strat._strategy_instance.markets == ["futures", "spot"]


def test_multiple_intervals_same_symbol_route_independently():
    wallet = _empty_wallet()
    svc = StrategyService()
    code = """
class MyStrategy:
    INPUTS = [
        {"market": "futures", "symbol": "BTCUSDT", "interval": "1m"},
        {"market": "futures", "symbol": "BTCUSDT", "interval": "5m"},
    ]
    def __init__(self):
        self.intervals = []
    def on_market_data(self, data, wallet):
        self.intervals.append(data.trigger.interval)
        return None
"""
    strat = svc.create_strategy("u1", "<db:multi_interval>", wallet, strategy_code=code)
    svc.running_strategy(_md("BTCUSDT", "futures", "1m"))
    svc.running_strategy(_md("BTCUSDT", "futures", "5m"))
    assert strat._strategy_instance.intervals == ["1m", "5m"]
    # Router has both interval-scoped keys (not collapsed).
    assert ("futures", "BTCUSDT", "1m") in svc.strategy_router
    assert ("futures", "BTCUSDT", "5m") in svc.strategy_router


def test_multi_interval_view_indexes_each_interval_separately():
    """Strategy can read two intervals independently through the bound view."""
    wallet = _empty_wallet()
    svc = StrategyService()
    code = """
class MyStrategy:
    INPUTS = [
        {"market": "futures", "symbol": "BTCUSDT", "interval": "1m"},
        {"market": "futures", "symbol": "BTCUSDT", "interval": "5m"},
    ]
    def __init__(self):
        self.snapshot = None
    def on_market_data(self, data, wallet):
        self.snapshot = {
            "1m": data.market["futures"].symbol["BTCUSDT"].interval["1m"],
            "5m": data.market["futures"].symbol["BTCUSDT"].interval["5m"],
        }
        return None
"""
    strat = svc.create_strategy("u1", "<db:multi_interval_view>", wallet, strategy_code=code)
    svc.running_strategy(_md("BTCUSDT", "futures", "1m", price=50_000.0))
    svc.running_strategy(_md("BTCUSDT", "futures", "5m", price=50_100.0))
    snap = strat._strategy_instance.snapshot
    assert snap is not None
    assert snap["1m"].price == 50_000.0
    assert snap["5m"].price == 50_100.0


# ── Fail-fast on invalid declaration (spec) ─────────────────────────────────

def test_create_strategy_without_inputs_fails_fast():
    wallet = _empty_wallet()
    svc = StrategyService()
    code = "class MyStrategy:\n    def on_market_data(self, data, wallet):\n        return None\n"
    with pytest.raises(StrategyDeclarationError, match="no INPUTS declaration"):
        svc.create_strategy("u1", "<db:no_inputs>", wallet, strategy_code=code)


def test_create_strategy_with_empty_inputs_fails_fast():
    wallet = _empty_wallet()
    svc = StrategyService()
    code = "class MyStrategy:\n    INPUTS = []\n    def on_market_data(self, data, wallet):\n        return None\n"
    with pytest.raises(StrategyDeclarationError, match="empty universe"):
        svc.create_strategy("u1", "<db:empty_inputs>", wallet, strategy_code=code)


def test_create_strategy_with_invalid_market_fails_fast():
    wallet = _empty_wallet()
    svc = StrategyService()
    code = (
        "class MyStrategy:\n"
        '    INPUTS = [{"market": "perp", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )
    with pytest.raises(StrategyDeclarationError, match="unsupported market"):
        svc.create_strategy("u1", "<db:bad_market>", wallet, strategy_code=code)


# ── Order-side universe guard (concern 2) ──────────────────────────────────

def test_order_guard_rejects_undeclared_symbol():
    """Strategy declaring (futures, TESTUSDT, 1m) cannot place a BTCUSDT order."""
    wallet = make_backtest_wallet(
        margin_mode="isolated",
        position_mode="one_way",
        futures_positions=[
            {
                "symbol": "BTCUSDT",  # wallet has BTCUSDT slot, strategy doesn't declare it
                "position_qty": 0.0, "entry_price": 0.0, "mark_price": 0.0,
                "leverage": 10, "initial_balance": 5_000, "fee_rate": 0.0004,
                "margin_mode": "isolated",
            },
        ],
    )
    svc = StrategyService()
    code = (
        "from strategy_service.types import OrderDecision\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        return OrderDecision(symbol='BTCUSDT', side='LONG', qty=0.1, market='futures')\n"
    )
    svc.create_strategy("u1", "<db:rogue_symbol>", wallet, strategy_code=code)
    # Tick in the declared universe so the strategy gets invoked.
    with pytest.raises(ValueError, match="outside declared universe"):
        svc.running_strategy(_md("TESTUSDT", "futures", "1m"))
    # Wallet BTCUSDT position MUST remain untouched by the rogue order.
    assert wallet.futures.positions[("BTCUSDT", 0)].net_qty == 0.0


def test_order_guard_rejects_undeclared_market():
    """Strategy declaring futures-only cannot place a spot order."""
    wallet = make_backtest_wallet(
        margin_mode="isolated",
        spot_assets=[{"symbol": "TESTUSDT", "qty": 100.0, "price": 1.0}],
        spot_free=1_000.0,
    )
    svc = StrategyService()
    code = (
        "from strategy_service.types import OrderDecision\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        return OrderDecision(symbol='TESTUSDT', side='BUY', qty=0.5, market='spot')\n"
    )
    svc.create_strategy("u1", "<db:rogue_market>", wallet, strategy_code=code)
    with pytest.raises(ValueError, match="outside declared universe"):
        svc.running_strategy(_md("TESTUSDT", "futures", "1m"))


def test_order_guard_allows_declared_orders():
    """Same strategy emitting declared orders must proceed normally."""
    wallet = make_backtest_wallet(
        margin_mode="isolated",
        position_mode="one_way",
        futures_positions=[
            {
                "symbol": "TESTUSDT",
                "position_qty": 0.0, "entry_price": 0.0, "mark_price": 0.0,
                "leverage": 10, "initial_balance": 5_000, "fee_rate": 0.0004,
                "margin_mode": "isolated",
            },
        ],
    )
    svc = StrategyService()
    code = (
        "from strategy_service.types import OrderDecision\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        return OrderDecision(symbol='TESTUSDT', side='LONG', qty=0.1, market='futures')\n"
    )
    svc.create_strategy("u1", "<db:declared_ok>", wallet, strategy_code=code)
    svc.running_strategy(_md("TESTUSDT", "futures", "1m", price=100.0))
    # Order landed — position opened.
    assert wallet.futures.positions[("TESTUSDT", 0)].net_qty == pytest.approx(0.1)


def test_order_guard_rejects_signal_market_override_outside_universe():
    """signal.market=spot when the strategy only declared futures must be rejected,
    even if a tick in the declared universe arrives."""
    wallet = make_backtest_wallet(margin_mode="isolated")
    svc = StrategyService()
    code = (
        "from strategy_service.types import OrderDecision\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"market": "futures", "symbol": "TESTUSDT", "interval": "1m"}]\n'
        "    def on_market_data(self, data, wallet):\n"
        "        # Inherits tick market (futures) if signal.market is None — legal.\n"
        "        # Override to 'spot' → outside declared universe.\n"
        "        return OrderDecision(symbol='TESTUSDT', side='BUY', qty=0.1, market='spot')\n"
    )
    svc.create_strategy("u1", "<db:override>", wallet, strategy_code=code)
    with pytest.raises(ValueError, match="outside declared universe"):
        svc.running_strategy(_md("TESTUSDT", "futures", "1m"))
