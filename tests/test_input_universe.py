"""Tests for the Phase 3 strategy declaration contract."""

from __future__ import annotations

from datetime import datetime, timezone
import sys

import pytest

from strategy_service import StrategyEngine
from strategy_service.inputs import (
    InputView,
    StrategyDeclarationError,
    StrategyInput,
    parse_declared_inputs,
)
from strategy_service.types import (
    Exchange,
    Market,
    MarketData,
    OrderSide,
    OrderType,
    PositionSide,
)
from strategy_service.strategy_imports import (
    gate_strategy_source,
    prepare_strategy,
    resolve_strategy_source,
)
from strategy_service.wallet.portfolio import PortfolioWalletRuntime
from strategy_service.wallet.canonical import SpotSymbolMetadata
from tests.helpers.order_client import FilledOrderClient
from tests.helpers.wallet_fixtures import make_backtest_wallet


def _create_strategy(
    engine,
    user_id,
    strategy_path,
    wallet,
    *args,
    strategy_code=None,
    **kwargs,
):
    gate = gate_strategy_source(
        resolve_strategy_source(strategy_path, strategy_code),
        python_invocation_path=sys.executable,
    )
    assert gate.ok, gate.issues
    assert gate.gated_source is not None
    return engine.create_strategy(
        user_id,
        prepare_strategy(gate.gated_source),
        wallet,
        *args,
        **kwargs,
    )


def _portfolio_wallet(default_wallet, *routes: tuple[str, str]) -> PortfolioWalletRuntime:
    route_set = set(routes) or {
        (Exchange.BINANCE, Market.PERPETUAL_FUTURES),
        (Exchange.BINANCE, Market.SPOT),
    }
    wallets = {
        (exchange, market, idx): default_wallet
        for idx, (exchange, market) in enumerate(sorted(route_set), start=1001)
    }
    return PortfolioWalletRuntime(1, route_set, wallets)


def _empty_wallet() -> PortfolioWalletRuntime:
    wallet = make_backtest_wallet(margin_mode="isolated")
    wallet.spot.register_metadata(
        SpotSymbolMetadata(
            venue_id=1002,
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            status="TRADING",
            base_asset="BTC",
            quote_asset="USDT",
            base_asset_precision=8,
            quote_asset_precision=8,
            spot_trading_allowed=True,
            permission_sets=(("SPOT",),),
            order_types=("LIMIT", "MARKET"),
        )
    )
    return _portfolio_wallet(wallet)


def _md(
    symbol: str,
    market: str,
    interval: str,
    price: float = 100.0,
    *,
    exchange: str = Exchange.BINANCE,
) -> MarketData:
    return MarketData(
        exchange=exchange,
        symbol=symbol,
        price=price,
        timestamp=datetime.now(timezone.utc),
        market=market,
        interval=interval,
    )


def _perp_input(symbol: str = "ETHUSDT", interval: str = "1m") -> StrategyInput:
    return StrategyInput(Exchange.BINANCE, Market.PERPETUAL_FUTURES, symbol, interval)


def test_parse_dict_entries_normalizes_case_and_whitespace():
    result = parse_declared_inputs([
        {
            "exchange": "Binance",
            "market": "PERPETUAL_FUTURES",
            "symbol": "ethusdt",
            "interval": " 1m ",
        },
    ])
    assert result == [_perp_input("ETHUSDT", "1m")]


def test_parse_rejects_tuple_entries():
    with pytest.raises(StrategyDeclarationError, match="each INPUTS item"):
        parse_declared_inputs([("spot", "BTCUSDT", "5m")])


def test_parse_rejects_colon_string_entries():
    with pytest.raises(StrategyDeclarationError, match="each INPUTS item"):
        parse_declared_inputs(["perpetual_futures:ADAUSDT:15m"])


def test_parse_keeps_normalized_entries():
    result = parse_declared_inputs([
        {
            "exchange": "binance",
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "BTCUSDT",
            "interval": "1m",
        },
        {
            "exchange": "binance",
            "market": Market.PERPETUAL_FUTURES,
            "symbol": "ethusdt",
            "interval": "5m",
        },
    ])
    assert result == [
        _perp_input("BTCUSDT", "1m"),
        _perp_input("ETHUSDT", "5m"),
    ]


def test_parse_rejects_missing_declaration():
    with pytest.raises(StrategyDeclarationError, match="at least one stream"):
        parse_declared_inputs(None)


def test_parse_rejects_empty_list():
    with pytest.raises(StrategyDeclarationError, match="at least one stream"):
        parse_declared_inputs([])


def test_parse_rejects_legacy_futures_alias():
    with pytest.raises(StrategyDeclarationError, match="unsupported market: futures"):
        parse_declared_inputs([
            {"exchange": "binance", "market": "futures", "symbol": "BTCUSDT", "interval": "1m"}
        ])


def test_parse_rejects_unknown_market():
    with pytest.raises(StrategyDeclarationError, match="unsupported market"):
        parse_declared_inputs([
            {"exchange": "binance", "market": "margin", "symbol": "BTCUSDT", "interval": "1m"}
        ])


def test_parse_rejects_empty_symbol():
    with pytest.raises(StrategyDeclarationError):
        parse_declared_inputs([
            {"exchange": "binance", "market": Market.PERPETUAL_FUTURES, "symbol": "   ", "interval": "1m"}
        ])


def test_parse_rejects_empty_interval():
    with pytest.raises(StrategyDeclarationError):
        parse_declared_inputs([
            {"exchange": "binance", "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": ""}
        ])


def test_parse_rejects_bare_scalar():
    with pytest.raises(StrategyDeclarationError):
        parse_declared_inputs(
            {
                "exchange": "binance",
                "market": Market.PERPETUAL_FUTURES,
                "symbol": "BTCUSDT",
                "interval": "1m",
            }
        )


def test_view_returns_none_for_declared_key_before_first_update():
    view = InputView([_perp_input()])
    assert (
        view.exchange[Exchange.BINANCE]
        .market[Market.PERPETUAL_FUTURES]
        .symbol["ETHUSDT"]
        .interval["1m"]
        is None
    )


def test_view_returns_latest_md_after_update():
    view = InputView([_perp_input()])
    md = _md("ETHUSDT", Market.PERPETUAL_FUTURES, "1m", price=3000.0)
    assert view.update(md) is True
    got = (
        view.exchange[Exchange.BINANCE]
        .market[Market.PERPETUAL_FUTURES]
        .symbol["ETHUSDT"]
        .interval["1m"]
    )
    assert got is md


def test_view_supports_exchange_market_shorthand_matching_strategy_library():
    view = InputView([_perp_input()])
    md = _md("ETHUSDT", Market.PERPETUAL_FUTURES, "1m", price=3000.0)
    assert view.update(md) is True

    got = (
        view.exchange[Exchange.BINANCE][Market.PERPETUAL_FUTURES]
        .symbol["ETHUSDT"]
        .interval["1m"]
    )

    assert got is md


def test_view_update_returns_false_for_undeclared_key():
    view = InputView([_perp_input()])
    assert view.update(_md("BTCUSDT", Market.PERPETUAL_FUTURES, "1m")) is False
    with pytest.raises(KeyError):
        _ = view.exchange[Exchange.BINANCE].market[Market.PERPETUAL_FUTURES].symbol["BTCUSDT"]


def test_view_raises_key_error_on_undeclared_exchange():
    view = InputView([_perp_input()])
    with pytest.raises(KeyError, match="exchange"):
        _ = view.exchange[Exchange.OKX]


def test_view_raises_key_error_on_undeclared_market():
    view = InputView([_perp_input()])
    with pytest.raises(KeyError, match="market"):
        _ = view.exchange[Exchange.BINANCE].market[Market.SPOT]


def test_view_raises_key_error_on_undeclared_symbol():
    view = InputView([_perp_input()])
    with pytest.raises(KeyError, match="symbol"):
        _ = view.exchange[Exchange.BINANCE].market[Market.PERPETUAL_FUTURES].symbol["BTCUSDT"]


def test_view_raises_key_error_on_undeclared_interval():
    view = InputView([_perp_input()])
    with pytest.raises(KeyError, match="interval"):
        _ = (
            view.exchange[Exchange.BINANCE]
            .market[Market.PERPETUAL_FUTURES]
            .symbol["ETHUSDT"]
            .interval["5m"]
        )


def test_view_trigger_is_most_recently_updated_md():
    view = InputView([
        _perp_input("BTCUSDT", "1m"),
        _perp_input("ETHUSDT", "1m"),
    ])
    view.update(_md("BTCUSDT", Market.PERPETUAL_FUTURES, "1m", price=50_000.0))
    view.update(_md("ETHUSDT", Market.PERPETUAL_FUTURES, "1m", price=3000.0))
    assert view.trigger is not None
    assert view.trigger.symbol == "ETHUSDT"
    assert view.price == 3000.0
    assert view.symbol == "ETHUSDT"


def test_view_keys_expose_declared_structure():
    view = InputView([
        _perp_input("BTCUSDT", "1m"),
        _perp_input("BTCUSDT", "5m"),
        StrategyInput(Exchange.BINANCE, Market.SPOT, "ETHUSDT", "1m"),
    ])
    assert set(view.exchange.keys()) == {Exchange.BINANCE}
    binance = view.exchange[Exchange.BINANCE]
    assert set(binance.market.keys()) == {Market.PERPETUAL_FUTURES, Market.SPOT}
    assert set(binance.market[Market.PERPETUAL_FUTURES].symbol.keys()) == {"BTCUSDT"}
    assert set(binance.market[Market.PERPETUAL_FUTURES].symbol["BTCUSDT"].interval.keys()) == {"1m", "5m"}
    assert set(binance.market[Market.SPOT].symbol.keys()) == {"ETHUSDT"}


def test_router_binds_only_to_declared_inputs_even_on_empty_wallet():
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    def on_market_data(self, data, wallet):
        return None
"""
    _create_strategy(svc, "u1", "<db:router_test>", _empty_wallet(), strategy_code=code)

    assert (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "ETHUSDT", "1m") in svc.strategy_router
    assert (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "BTCUSDT", "1m") not in svc.strategy_router
    assert (Exchange.BINANCE, Market.SPOT, "ETHUSDT", "1m") not in svc.strategy_router


def test_router_drops_undeclared_ticks_silently():
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    def on_market_data(self, data, wallet):
        return None
"""
    _create_strategy(svc, "u1", "<db:drop_test>", _empty_wallet(), strategy_code=code)

    assert svc.running_strategy(_md("ETHUSDT", Market.PERPETUAL_FUTURES, "1m")) is True
    assert svc.running_strategy(_md("ETHUSDT", Market.SPOT, "1m")) is False
    assert svc.running_strategy(_md("BTCUSDT", Market.PERPETUAL_FUTURES, "1m")) is False
    assert svc.running_strategy(_md("ETHUSDT", Market.PERPETUAL_FUTURES, "5m")) is False


def test_declared_input_routes_without_wallet_position():
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    def __init__(self):
        self.ticks_seen = 0
    def on_market_data(self, data, wallet):
        self.ticks_seen += 1
        return None
"""
    strat = _create_strategy(svc, "u1", "<db:empty_wallet_routes>", _empty_wallet(), strategy_code=code)
    svc.running_strategy(_md("ETHUSDT", Market.PERPETUAL_FUTURES, "1m"))
    assert strat._strategy_instance.ticks_seen == 1


def test_multi_symbol_same_market_both_route():
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market

class MyStrategy:
    INPUTS = [
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": "1m"},
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"},
    ]
    ORDER_TARGETS = []
    def __init__(self):
        self.seen = []
    def on_market_data(self, data, wallet):
        self.seen.append((data.trigger.market, data.trigger.symbol, data.trigger.interval))
        return None
"""
    strat = _create_strategy(svc, "u1", "<db:multi_symbol>", _empty_wallet(), strategy_code=code)
    svc.running_strategy(_md("BTCUSDT", Market.PERPETUAL_FUTURES, "1m"))
    svc.running_strategy(_md("ETHUSDT", Market.PERPETUAL_FUTURES, "1m"))
    assert strat._strategy_instance.seen == [
        (Market.PERPETUAL_FUTURES, "BTCUSDT", "1m"),
        (Market.PERPETUAL_FUTURES, "ETHUSDT", "1m"),
    ]


def test_mixed_spot_and_perpetual_futures_both_route():
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market

class MyStrategy:
    INPUTS = [
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": "1m"},
        {"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "BTCUSDT", "interval": "1m"},
    ]
    ORDER_TARGETS = []
    def __init__(self):
        self.markets = []
    def on_market_data(self, data, wallet):
        self.markets.append(data.trigger.market)
        return None
"""
    strat = _create_strategy(svc, "u1", "<db:mixed>", _empty_wallet(), strategy_code=code)
    svc.running_strategy(_md("BTCUSDT", Market.PERPETUAL_FUTURES, "1m"))
    svc.running_strategy(_md("BTCUSDT", Market.SPOT, "1m"))
    assert strat._strategy_instance.markets == [Market.PERPETUAL_FUTURES, Market.SPOT]


def test_multiple_intervals_same_symbol_route_independently():
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market

class MyStrategy:
    INPUTS = [
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": "1m"},
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": "5m"},
    ]
    ORDER_TARGETS = []
    def __init__(self):
        self.intervals = []
    def on_market_data(self, data, wallet):
        self.intervals.append(data.trigger.interval)
        return None
"""
    strat = _create_strategy(svc, "u1", "<db:multi_interval>", _empty_wallet(), strategy_code=code)
    svc.running_strategy(_md("BTCUSDT", Market.PERPETUAL_FUTURES, "1m"))
    svc.running_strategy(_md("BTCUSDT", Market.PERPETUAL_FUTURES, "5m"))
    assert strat._strategy_instance.intervals == ["1m", "5m"]
    assert (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "BTCUSDT", "1m") in svc.strategy_router
    assert (Exchange.BINANCE, Market.PERPETUAL_FUTURES, "BTCUSDT", "5m") in svc.strategy_router


def test_multi_interval_view_indexes_each_interval_separately():
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market

class MyStrategy:
    INPUTS = [
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": "1m"},
        {"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "BTCUSDT", "interval": "5m"},
    ]
    ORDER_TARGETS = []
    def __init__(self):
        self.snapshot = None
    def on_market_data(self, data, wallet):
        perp = data.exchange[Exchange.BINANCE].market[Market.PERPETUAL_FUTURES].symbol["BTCUSDT"].interval
        self.snapshot = {"1m": perp["1m"], "5m": perp["5m"]}
        return None
"""
    strat = _create_strategy(svc, "u1", "<db:multi_interval_view>", _empty_wallet(), strategy_code=code)
    svc.running_strategy(_md("BTCUSDT", Market.PERPETUAL_FUTURES, "1m", price=50_000.0))
    svc.running_strategy(_md("BTCUSDT", Market.PERPETUAL_FUTURES, "5m", price=50_100.0))
    snap = strat._strategy_instance.snapshot
    assert snap is not None
    assert snap["1m"].price == 50_000.0
    assert snap["5m"].price == 50_100.0


def test_create_strategy_without_inputs_fails_fast():
    code = "class MyStrategy:\n    ORDER_TARGETS = []\n    def on_market_data(self, data, wallet):\n        return None\n"
    gate = gate_strategy_source(
        resolve_strategy_source("<db:no_inputs>", code),
        python_invocation_path=sys.executable,
    )
    assert not gate.ok
    assert [issue.code for issue in gate.issues] == ["missing_inputs"]


def test_create_strategy_with_empty_inputs_fails_fast():
    code = "class MyStrategy:\n    INPUTS = []\n    ORDER_TARGETS = []\n    def on_market_data(self, data, wallet):\n        return None\n"
    gate = gate_strategy_source(
        resolve_strategy_source("<db:empty_inputs>", code),
        python_invocation_path=sys.executable,
    )
    assert not gate.ok
    assert [issue.code for issue in gate.issues] == ["invalid_inputs"]
    assert "at least one stream" in gate.issues[0].message


def test_create_strategy_with_invalid_market_fails_fast():
    code = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "margin", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )
    gate = gate_strategy_source(
        resolve_strategy_source("<db:bad_market>", code),
        python_invocation_path=sys.executable,
    )
    assert not gate.ok
    assert [issue.code for issue in gate.issues] == ["invalid_inputs"]
    assert "unsupported market" in gate.issues[0].message


def test_order_guard_rejects_undeclared_symbol():
    raw_wallet = make_backtest_wallet(
        margin_mode="isolated",
        position_mode="one_way",
        futures_positions=[
            {
                "symbol": "BTCUSDT",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "leverage": 10,
                "initial_balance": 5_000,
                "fee_rate": 0.0004,
                "margin_mode": "isolated",
            },
        ],
    )
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType, PositionSide

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "TESTUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "TESTUSDT"}]
    def on_market_data(self, data, wallet):
        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="BTCUSDT", side=OrderSide.BUY, qty="0.1", order_type=OrderType.MARKET, position_side=PositionSide.BOTH)
"""
    _create_strategy(svc, "u1", "<db:rogue_symbol>", _portfolio_wallet(raw_wallet), strategy_code=code)
    with pytest.raises(ValueError, match="outside ORDER_TARGETS"):
        svc.running_strategy(_md("TESTUSDT", Market.PERPETUAL_FUTURES, "1m"))
    assert raw_wallet.futures.positions[("BTCUSDT", 0)].net_qty == 0.0


def test_order_guard_rejects_undeclared_market():
    raw_wallet = make_backtest_wallet(
        margin_mode="isolated",
        spot_assets=[
            {"asset": "USDT", "free_decimal": "1000", "locked_decimal": "0"},
            {"asset": "TEST", "free_decimal": "100", "locked_decimal": "0", "price_decimal": "1"},
        ],
    )
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "TESTUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "TESTUSDT"}]
    def on_market_data(self, data, wallet):
        return OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol="TESTUSDT", side=OrderSide.BUY, qty="0.5", order_type=OrderType.MARKET)
"""
    _create_strategy(svc, "u1", "<db:rogue_market>", _portfolio_wallet(raw_wallet), strategy_code=code)
    with pytest.raises(ValueError, match="outside ORDER_TARGETS"):
        svc.running_strategy(_md("TESTUSDT", Market.PERPETUAL_FUTURES, "1m"))


def test_order_guard_allows_declared_orders():
    raw_wallet = make_backtest_wallet(
        margin_mode="isolated",
        position_mode="one_way",
        futures_positions=[
            {
                "symbol": "TESTUSDT",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "leverage": 10,
                "initial_balance": 5_000,
                "fee_rate": 0.0004,
                "margin_mode": "isolated",
            },
        ],
    )
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType, PositionSide

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "TESTUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "TESTUSDT"}]
    def on_market_data(self, data, wallet):
        return OrderDecision(exchange=Exchange.BINANCE, market=Market.PERPETUAL_FUTURES, symbol="TESTUSDT", side=OrderSide.BUY, qty="0.1", order_type=OrderType.MARKET, position_side=PositionSide.BOTH)
"""
    _create_strategy(svc,
        "u1",
        "<db:declared_ok>",
        _portfolio_wallet(raw_wallet),
        order_client=FilledOrderClient(),
        strategy_code=code,
    )
    svc.running_strategy(_md("TESTUSDT", Market.PERPETUAL_FUTURES, "1m", price=100.0))
    assert raw_wallet.futures.positions[("TESTUSDT", 0)].net_qty == pytest.approx(0.1)


def test_order_guard_rejects_signal_market_override_outside_targets():
    svc = StrategyEngine()
    code = """
from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "TESTUSDT", "interval": "1m"}]
    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "TESTUSDT"}]
    def on_market_data(self, data, wallet):
        return OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol="TESTUSDT", side=OrderSide.BUY, qty="0.1", order_type=OrderType.MARKET)
"""
    _create_strategy(svc, "u1", "<db:override>", _empty_wallet(), strategy_code=code)
    with pytest.raises(ValueError, match="outside ORDER_TARGETS"):
        svc.running_strategy(_md("TESTUSDT", Market.PERPETUAL_FUTURES, "1m"))
