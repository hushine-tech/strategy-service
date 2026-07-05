"""Shared wallet fixtures for strategy-service tests.

After Phase C2b legacy cleanup, all test code constructs wallets through the
same path production code uses: a proto ``PortfolioWalletState`` →
``proto_to_portfolio_spec`` → ``build_wallet_from_portfolio`` →
``BinanceWalletRuntime``. This module exposes two convenience constructors:

- ``make_backtest_wallet(...)``  environment=0 — routes through ``("local", "backtest")``
- ``make_demo_wallet(...)``      environment=1 — routes through ``("binance", "demo")``

Both return a ``BinanceWalletRuntime``. Tests SHOULD NOT construct wallet
objects by hand — the helper keeps test behavior aligned with the production
runtime and prevents "green in test, red in production" drift.
"""

from __future__ import annotations

from typing import Any, Iterable

from strategy_service.gen import portfolio_service_pb2
from strategy_service.wallet import BinanceWalletRuntime
from strategy_service.wallet_adapter import proto_to_portfolio_spec
from strategy_service.wallet_factory import build_wallet_from_portfolio


def _apply_futures_position(
    futures_proto: portfolio_service_pb2.FuturesWallet,
    *,
    symbol: str,
    position_qty: float = 0.0,
    entry_price: float = 0.0,
    mark_price: float = 0.0,
    leverage: float = 10.0,
    margin_mode: str | None = None,
    position_side: str = "BOTH",
    initial_balance: float = 0.0,
    initial_margin: float = 0.0,
    position_initial_margin: float = 0.0,
    isolated_wallet: float = 0.0,
    break_even_price: float = 0.0,
    fee_rate: float = 0.0,
    portfolio_margin_mode: str = "cross",
) -> None:
    """Append one position entry to a FuturesWallet proto.

    ``margin_mode`` defaults to the portfolio-level ``portfolio_margin_mode`` when
    the caller doesn't specify it; the canonical contract (strict ingress)
    requires every position to carry a non-empty ``margin_mode``.

    ``initial_balance`` is the per-position seed used by the isolated-margin
    bootstrap formula (see ``wallet_factory._bootstrap_futures_equity``).
    """
    effective_margin_mode = (margin_mode or portfolio_margin_mode).strip().lower()
    p = futures_proto.positions.add()
    p.symbol = symbol
    p.position_side = position_side
    p.position_qty = float(position_qty)
    p.qty = abs(float(position_qty))  # legacy alias preserved on wire
    p.entry_price = float(entry_price)
    p.mark_price = float(mark_price)
    p.leverage = float(leverage)
    p.margin_mode = effective_margin_mode
    p.margin_type = effective_margin_mode
    if initial_balance:
        p.initial_balance = float(initial_balance)
    if initial_margin:
        p.initial_margin = float(initial_margin)
    if position_initial_margin:
        p.position_initial_margin = float(position_initial_margin)
    if isolated_wallet:
        p.isolated_wallet = float(isolated_wallet)
    if break_even_price:
        p.break_even_price = float(break_even_price)
    if fee_rate:
        # Some proto versions expose fee_rate on FuturesPosition; others rely
        # on canonical defaults. Set defensively via setattr so we don't
        # raise if the field isn't present in the current proto build.
        if hasattr(p, "fee_rate"):
            p.fee_rate = float(fee_rate)


def _apply_spot_asset(
    spot_proto: portfolio_service_pb2.SpotWallet,
    *,
    symbol: str,
    qty: float = 0.0,
    locked: float = 0.0,
    avg_entry_price: float = 0.0,
    price: float | None = None,
) -> None:
    """Append one asset entry to a SpotWallet proto.

    ``SpotWallet.assets`` is a ``repeated SpotAsset`` (list), not a map;
    each call appends one entry via ``add()``.
    """
    a = spot_proto.assets.add()
    a.symbol = symbol
    a.qty = float(qty)
    a.locked = float(locked)
    a.avg_entry_price = float(avg_entry_price)
    if price is not None:
        a.price = float(price)


def _build_wallet_proto(
    *,
    environment: int,
    margin_mode: str,
    position_mode: str,
    wallet_balance: float,
    available_balance: float | None,
    initial_balance: float,
    deposit_sum: float,
    withdrawal_sum: float,
    futures_positions: Iterable[dict[str, Any]] | None,
    spot_assets: Iterable[dict[str, Any]] | None,
    spot_free: float,
    spot_locked: float,
) -> portfolio_service_pb2.PortfolioWalletState:
    available = wallet_balance if available_balance is None else available_balance
    futures = portfolio_service_pb2.FuturesWallet(
        margin_mode=margin_mode,
        position_mode=position_mode,
        initial_balance=initial_balance,
        deposit_sum=deposit_sum,
        withdrawal_sum=withdrawal_sum,
        wallet_balance=wallet_balance,
        available_balance=available,
        total_margin_balance=wallet_balance,
        margin_balance=wallet_balance,
        total_unrealized_pnl=0.0,
        unrealized_pnl=0.0,
    )
    for pos in futures_positions or []:
        # Forward the portfolio-level margin_mode so positions that don't
        # specify one adopt the portfolio default (strict canonical contract
        # requires FuturesPosition.margin_mode to be non-empty).
        _apply_futures_position(futures, portfolio_margin_mode=margin_mode, **pos)

    spot = portfolio_service_pb2.SpotWallet(free=spot_free, locked=spot_locked)
    for asset in spot_assets or []:
        _apply_spot_asset(spot, **asset)

    return portfolio_service_pb2.PortfolioWalletState(
        environment=environment,
        total_value=wallet_balance,
        spot_estimated_value=0.0,
        futures_position_equity=wallet_balance,
        futures=futures,
        spot=spot,
    )


def make_backtest_wallet(
    *,
    margin_mode: str = "cross",
    position_mode: str = "one_way",
    wallet_balance: float = 10_000.0,
    available_balance: float | None = None,
    initial_balance: float | None = None,
    deposit_sum: float = 0.0,
    withdrawal_sum: float = 0.0,
    futures_positions: Iterable[dict[str, Any]] | None = None,
    spot_assets: Iterable[dict[str, Any]] | None = None,
    spot_free: float = 0.0,
    spot_locked: float = 0.0,
) -> BinanceWalletRuntime:
    """Build a environment=0 backtest wallet runtime via the canonical path.

    Defaults produce a $10k cross-margin one-way portfolio with no open
    positions and no spot assets — the simplest viable starting state. All
    kwargs accept straightforward overrides.

    ``futures_positions`` is a list of dicts; each dict is forwarded to
    ``_apply_futures_position`` (see that function for the keys).
    ``spot_assets`` is a list of dicts forwarded to ``_apply_spot_asset``.
    """
    # For environment=0 the bootstrap formula reads `initial_balance` (cross) or the
    # per-position `initial_balance` (isolated) — we wire `wallet_balance` by
    # default into `initial_balance` so the hydrated runtime matches the
    # caller's intuition of "I asked for $10k; wallet_balance = $10k".
    seed = wallet_balance if initial_balance is None else initial_balance
    wallet_proto = _build_wallet_proto(
        environment=0,
        margin_mode=margin_mode,
        position_mode=position_mode,
        wallet_balance=wallet_balance,
        available_balance=available_balance,
        initial_balance=seed,
        deposit_sum=deposit_sum,
        withdrawal_sum=withdrawal_sum,
        futures_positions=futures_positions,
        spot_assets=spot_assets,
        spot_free=spot_free,
        spot_locked=spot_locked,
    )
    return build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))


def make_demo_wallet(
    *,
    margin_mode: str = "cross",
    position_mode: str = "one_way",
    wallet_balance: float = 10_000.0,
    available_balance: float | None = None,
    initial_balance: float | None = None,
    deposit_sum: float = 0.0,
    withdrawal_sum: float = 0.0,
    futures_positions: Iterable[dict[str, Any]] | None = None,
    spot_assets: Iterable[dict[str, Any]] | None = None,
    spot_free: float = 0.0,
    spot_locked: float = 0.0,
) -> BinanceWalletRuntime:
    """Build an environment=1 demo wallet runtime via the canonical path.

    Same defaults and kwargs as ``make_backtest_wallet`` — the only
    difference is ``environment=1`` so the registry resolves to ``("binance","demo")``.
    """
    seed = wallet_balance if initial_balance is None else initial_balance
    wallet_proto = _build_wallet_proto(
        environment=1,
        margin_mode=margin_mode,
        position_mode=position_mode,
        wallet_balance=wallet_balance,
        available_balance=available_balance,
        initial_balance=seed,
        deposit_sum=deposit_sum,
        withdrawal_sum=withdrawal_sum,
        futures_positions=futures_positions,
        spot_assets=spot_assets,
        spot_free=spot_free,
        spot_locked=spot_locked,
    )
    return build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))


make_testnet_wallet = make_demo_wallet
