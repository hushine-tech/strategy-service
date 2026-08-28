"""C2a cutover smoke tests: environment=0 backtest routed through BinanceWalletRuntime.

These tests drive the registry-based path (canonical state →
build_wallet_from_portfolio) used by RunStrategy in production.
"""

from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest
from hushine_strategy.replay import ReplayConfig, run_replay
from hushine_strategy.wallet import FuturesWallet as OfflineFuturesWallet

from strategy_service import MarketData, StrategyEngine
from strategy_service.gen import portfolio_service_pb2
from strategy_service.position_side import position_side_from_label
from strategy_service.strategy_imports import (
    gate_strategy_source,
    prepare_strategy,
    resolve_strategy_source,
)
from strategy_service.wallet import BinanceWalletRuntime
from strategy_service.wallet.portfolio_adapter import build_portfolio_wallet_from_snapshot
from strategy_service.wallet_adapter import proto_to_portfolio_spec
from strategy_service.wallet_factory import build_wallet_from_portfolio
from tests.helpers.order_client import FilledOrderClient


def _canonical_mode0(
    *,
    margin_mode: str = "cross",
    position_mode: str = "one_way",
    wallet_balance: float = 10_000.0,
    available_balance: float = 10_000.0,
    initial_balance: float = 10_000.0,
    positions: list | None = None,
):
    """Build a canonical PortfolioWalletState proto representing a environment=0 portfolio."""
    futures = portfolio_service_pb2.FuturesWallet(
        margin_mode=margin_mode,
        position_mode=position_mode,
        initial_balance=initial_balance,
        wallet_balance=wallet_balance,
        available_balance=available_balance,
        total_margin_balance=wallet_balance,
        margin_balance=wallet_balance,
        total_unrealized_pnl=0.0,
        unrealized_pnl=0.0,
    )
    if positions:
        for p in positions:
            fp = futures.positions.add()
            fp.symbol = p["symbol"]
            fp.position_side = position_side_from_label(p.get("position_side", "BOTH"))
            fp.qty = float(p.get("position_qty", 0.0))
            fp.entry_price = float(p.get("entry_price", 0.0))
            fp.mark_price = float(p.get("mark_price", 0.0))
            fp.leverage = float(p.get("leverage", 10.0))
            fp.margin_mode = p.get("margin_mode", margin_mode)
    return portfolio_service_pb2.PortfolioWalletState(
        environment=0,
        total_value=wallet_balance,
        spot_estimated_value=0.0,
        futures_position_equity=wallet_balance,
        futures=futures,
        spot=portfolio_service_pb2.SpotWallet(),
    )


# ── Routing correctness ─────────────────────────────────────────────────

def test_mode0_cross_routes_to_binance_runtime():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_canonical_mode0(margin_mode="cross")))
    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.environment_code == 0


def test_mode0_isolated_routes_to_binance_runtime():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_canonical_mode0(margin_mode="isolated")))
    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.environment_code == 0
    assert wallet.futures.margin_mode == "isolated"


def test_mode0_hedge_routes_to_binance_runtime():
    wallet = build_wallet_from_portfolio(
        proto_to_portfolio_spec(_canonical_mode0(position_mode="hedge"))
    )
    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.environment_code == 0
    assert wallet.futures.position_mode == "hedge"


# ── Bootstrap values flow correctly into parity runtime ─────────────────

def test_mode0_cross_wallet_balance_bootstrap_preserved():
    """cross mode: wallet_balance from canonical state surfaces unchanged."""
    wallet = build_wallet_from_portfolio(
        proto_to_portfolio_spec(_canonical_mode0(margin_mode="cross", wallet_balance=15_000.0))
    )
    assert wallet.get_wallet_balance() == pytest.approx(15_000.0)


def test_mode0_available_balance_uses_parity_formula():
    """C2a behavior change: available_balance is COMPUTED from canonical
    inputs per parity formula, not passed through blindly. With no open
    positions and no open orders, collapses to wallet_balance (cross) or
    margin_balance (isolated). This locks in the expected post-cutover value.
    """
    wallet = build_wallet_from_portfolio(
        proto_to_portfolio_spec(
            _canonical_mode0(
                margin_mode="cross",
                wallet_balance=10_000.0,
                available_balance=8_500.0,  # exchange-reported; no longer authoritative
            )
        )
    )
    # cross, no positions/orders → available = max(0, wallet_balance + 0 - 0 - 0)
    assert wallet.get_available_balance() == pytest.approx(10_000.0)


# ── Position hydration works end-to-end ─────────────────────────────────

def test_mode0_position_hydration_through_canonical_ingress():
    wallet = build_wallet_from_portfolio(
        proto_to_portfolio_spec(
            _canonical_mode0(
                positions=[{
                    "symbol": "BTCUSDT",
                    "position_qty": 0.1,
                    "entry_price": 45000.0,
                    "mark_price": 45500.0,
                    "leverage": 20.0,
                }],
            )
        )
    )
    assert wallet.environment_code == 0
    # Keyed by (symbol, direction_key) — for one-way, direction_key == 0.
    pos = wallet.futures.positions[("BTCUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.1)
    assert pos.entry_price == pytest.approx(45000.0)
    assert pos.mark_price == pytest.approx(45500.0)
    # Parity runtime computes UPnL locally: qty * (mark - entry) = 0.1 * 500 = 50.0
    assert pos.get_unrealized_pnl() == pytest.approx(50.0)


def test_mode0_multi_symbol_state_bootstrap():
    wallet = build_wallet_from_portfolio(
        proto_to_portfolio_spec(
            _canonical_mode0(
                margin_mode="cross",
                wallet_balance=20_000.0,
                positions=[
                    {
                        "symbol": "BTCUSDT",
                        "position_qty": 0.1,
                        "entry_price": 45_000.0,
                        "mark_price": 45_500.0,
                        "leverage": 20.0,
                    },
                    {
                        "symbol": "ETHUSDT",
                        "position_qty": -2.0,
                        "position_side": "BOTH",
                        "entry_price": 3_000.0,
                        "mark_price": 2_950.0,
                        "leverage": 10.0,
                    },
                ],
            )
        )
    )

    btc = wallet.futures.positions[("BTCUSDT", 0)]
    eth = wallet.futures.positions[("ETHUSDT", 0)]
    assert btc.get_unrealized_pnl() == pytest.approx(50.0)
    assert eth.get_unrealized_pnl() == pytest.approx(100.0)
    assert wallet.futures.get_unrealized_pnl() == pytest.approx(150.0)
    assert wallet.futures.get_margin_balance() == pytest.approx(20_150.0)


# ── Backtest runtime on_market_data / on_order lifecycle works ──────────

def test_mode0_parity_runtime_accepts_market_data_tick():
    """Parity runtime's on_market_data must work when fed a backtest tick
    without exchange-provided oracle values. This is the core of what C2a
    enables."""
    wallet = build_wallet_from_portfolio(
        proto_to_portfolio_spec(
            _canonical_mode0(
                positions=[{
                    "symbol": "BTCUSDT",
                    "position_qty": 0.1,
                    "entry_price": 45000.0,
                    "mark_price": 45000.0,
                    "leverage": 20.0,
                }],
            )
        )
    )
    wallet.on_market_data("BTCUSDT", "futures", 46000.0)
    pos = wallet.futures.positions[("BTCUSDT", 0)]
    assert pos.mark_price == pytest.approx(46000.0)
    # UPnL updated after mark: 0.1 * (46000 - 45000) = 100.0
    assert pos.get_unrealized_pnl() == pytest.approx(100.0)


def test_mode0_registry_binding_is_binance_runtime():
    from strategy_service.wallet_factory import RUNTIME_REGISTRY, _populate_runtime_registry

    _populate_runtime_registry()
    assert RUNTIME_REGISTRY[("local", "backtest")] is BinanceWalletRuntime


def _route_aware_leverage_strategy_source(
    *,
    strategy_leverage: int | None,
    target_leverage: int | None,
) -> str:
    template = (
        Path(__file__).resolve().parents[1].parent
        / "strategy-library"
        / "tests"
        / "fixtures"
        / "route_aware_leverage_strategy.py.template"
    ).read_text(encoding="utf-8")
    return template.replace(
        "__STRATEGY_LEVERAGE_DECLARATION__",
        "" if strategy_leverage is None else f"    LEVERAGE = {strategy_leverage}",
    ).replace(
        "__TARGET_LEVERAGE_DECLARATION__",
        "" if target_leverage is None else f'            "leverage": {target_leverage},',
    )


@pytest.mark.parametrize(
    ("strategy_leverage", "target_leverage", "expected_leverage", "expected_source"),
    [
        (None, None, 1, "platform_default"),
        (5, None, 5, "strategy_default"),
        (5, 10, 10, "order_target"),
    ],
)
def test_mode0_engine_and_offline_replay_execute_identical_route_aware_strategy(
    monkeypatch,
    strategy_leverage,
    target_leverage,
    expected_leverage,
    expected_source,
):
    def fail_network(*_args, **_kwargs):
        raise AssertionError("simulated leverage must not access Binance")

    monkeypatch.setattr(socket, "create_connection", fail_network)
    source = _route_aware_leverage_strategy_source(
        strategy_leverage=strategy_leverage,
        target_leverage=target_leverage,
    )
    gate = gate_strategy_source(
        resolve_strategy_source("route-aware-leverage.py", source),
        python_invocation_path=sys.executable,
    )
    assert gate.ok, gate.issues
    assert gate.gated_source is not None
    prepared = prepare_strategy(gate.gated_source)

    full_wallet = _canonical_mode0(
        wallet_balance=1000.0,
        available_balance=1000.0,
        initial_balance=1000.0,
    )
    venue = portfolio_service_pb2.VenueSnapshot(
        venue_id=11,
        exchange=1,
        market=2,
        total_value=1000.0,
        wallet_balance=1000.0,
        available_balance=1000.0,
    )
    venue.wallet.CopyFrom(full_wallet)
    snapshot = portfolio_service_pb2.PortfolioSnapshot(
        portfolio_id=7,
        user_id=3,
        total_value=1000.0,
        wallet_balance=1000.0,
        available_balance=1000.0,
        venues=[venue],
    )
    hosted_wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "perpetual_futures")},
        simulated_order_targets=prepared.declarations.order_targets,
    )
    hosted_orders = FilledOrderClient()
    hosted_engine = StrategyEngine()
    hosted_engine.create_strategy(
        "parity",
        prepared,
        hosted_wallet,
        order_client=hosted_orders,
        portfolio_id=7,
    )
    assert hosted_engine.running_strategy(
        MarketData(
            symbol="BTCUSDT",
            price=100.0,
            timestamp=1,
            exchange="binance",
            market="perpetual_futures",
            interval="1m",
        )
    )

    offline_futures = OfflineFuturesWallet(initial_balance=1000.0)
    offline_result = run_replay(
        ReplayConfig(
            strategy_code=source,
            ticks=[
                MarketData(
                    symbol="BTCUSDT",
                    price=100.0,
                    timestamp=1,
                    exchange="binance",
                    market="perpetual_futures",
                    interval="1m",
                )
            ],
            wallet=offline_futures,
        )
    )
    offline_route_wallet = offline_result.wallet.get(
        "binance",
        "perpetual_futures",
    )
    hosted_route_wallet = hosted_wallet.get("binance", "perpetual_futures")
    hosted_metadata = hosted_route_wallet.futures.risk_metadata["BTCUSDT"]
    offline_metadata = offline_route_wallet.futures.risk_metadata["BTCUSDT"]
    assert (
        hosted_metadata.configured_leverage,
        hosted_metadata.leverage_source,
    ) == (
        offline_metadata.configured_leverage,
        offline_metadata.leverage_source,
    ) == (expected_leverage, expected_source)
    assert len(hosted_orders.calls) == 1
    hosted_qty = float(hosted_orders.calls[0]["decision"].qty)
    offline_qty = offline_route_wallet.position_qty("BTCUSDT")
    assert hosted_qty == pytest.approx(offline_qty)
    assert hosted_qty == pytest.approx(expected_leverage * 0.1)
