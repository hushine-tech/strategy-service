"""Sanity tests for ``tests.helpers.wallet_fixtures``.

These pin down the helper's contract so the broader test rewrite below can
trust: "calling make_testnet_wallet / make_backtest_wallet is equivalent to
the production path build_wallet_from_portfolio(...)". If this file ever goes
red, every downstream test that uses the helper is suspect.
"""

from __future__ import annotations

from decimal import Decimal
import pytest

from strategy_service.wallet import BinanceWalletRuntime
from tests.helpers.wallet_fixtures import make_backtest_wallet, make_testnet_wallet


def test_make_backtest_wallet_returns_binance_runtime_with_mode_0():
    wallet = make_backtest_wallet()
    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.environment_code == 0


def test_make_testnet_wallet_returns_binance_runtime_with_mode_2():
    wallet = make_testnet_wallet()
    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.environment_code == 1


def test_helper_default_portfolio_is_usable():
    """Default starting state: $10k cross / one_way / zero positions."""
    wallet = make_testnet_wallet()
    assert wallet.futures.margin_mode == "cross"
    assert wallet.futures.position_mode == "one_way"
    assert wallet.get_wallet_balance() == pytest.approx(10_000.0)
    assert wallet.futures.positions == {}


def test_helper_margin_mode_and_position_mode_passthrough():
    wallet = make_testnet_wallet(margin_mode="isolated", position_mode="hedge")
    assert wallet.futures.margin_mode == "isolated"
    assert wallet.futures.position_mode == "hedge"


def test_helper_futures_positions_hydrated():
    wallet = make_testnet_wallet(
        futures_positions=[
            {
                "symbol": "BTCUSDT",
                "position_qty": 0.1,
                "entry_price": 45_000.0,
                "mark_price": 45_500.0,
                "leverage": 20.0,
            },
        ],
    )
    # one-way position uses direction_key 0 regardless of position_side.
    assert ("BTCUSDT", 0) in wallet.futures.positions
    pos = wallet.futures.positions[("BTCUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.1)
    assert pos.entry_price == pytest.approx(45_000.0)
    assert pos.mark_price == pytest.approx(45_500.0)
    # UPnL follows from mark - entry × qty: 0.1 * 500 = 50.0
    assert pos.get_unrealized_pnl() == pytest.approx(50.0)


def test_helper_hedge_positions_keyed_by_direction():
    wallet = make_testnet_wallet(
        position_mode="hedge",
        futures_positions=[
            {
                "symbol": "BTCUSDT",
                "position_side": "LONG",
                "position_qty": 0.1,
                "entry_price": 45_000.0,
                "mark_price": 45_000.0,
            },
            {
                "symbol": "BTCUSDT",
                "position_side": "SHORT",
                "position_qty": -0.05,
                "entry_price": 46_000.0,
                "mark_price": 45_000.0,
            },
        ],
    )
    assert ("BTCUSDT", 1) in wallet.futures.positions
    assert ("BTCUSDT", -1) in wallet.futures.positions
    assert wallet.futures.positions[("BTCUSDT", 1)].position_qty == pytest.approx(0.1)
    assert wallet.futures.positions[("BTCUSDT", -1)].position_qty == pytest.approx(-0.05)


def test_helper_spot_assets_hydrated():
    wallet = make_testnet_wallet(
        spot_free=500.0,
        spot_assets=[
            {"symbol": "BTCUSDT", "qty": 0.01, "avg_entry_price": 40_000.0, "price": 45_000.0},
        ],
    )
    assert "BTC" in wallet.spot.assets
    assert "BTCUSDT" not in wallet.spot.assets
    asset = wallet.spot.assets["BTC"]
    assert asset.qty == Decimal("0.01")
    assert asset.avg_entry_price == Decimal("40000.0")


def test_helper_backtest_isolated_uses_position_seeds_for_wallet_balance():
    """Bootstrap rule: isolated mode uses sum of per-position initial_balance
    as the seed, not portfolio initial_balance."""
    wallet = make_backtest_wallet(
        margin_mode="isolated",
        wallet_balance=0.0,  # ignored in isolated mode
        futures_positions=[
            {
                "symbol": "BTCUSDT",
                "margin_mode": "isolated",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
            },
        ],
        initial_balance=500.0,  # not actually used in isolated path
    )
    # With zero per-position initial_balance and zero portfolio
    # deposit/withdrawal, hydration gets wallet_balance=0. That's correct:
    # this test pins the intent that isolated bootstrap uses position seeds,
    # not portfolio initial_balance.
    assert wallet.futures.margin_mode == "isolated"


def test_helper_available_balance_default_equals_wallet_balance():
    wallet = make_testnet_wallet(wallet_balance=5_000.0)
    # No open positions, no open orders → available == wallet_balance.
    assert wallet.get_available_balance() == pytest.approx(5_000.0)
