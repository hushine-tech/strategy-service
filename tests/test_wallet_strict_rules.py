"""Strict-mode invariants on the canonical wallet runtime.

Phase C2b cleanup reduced this file from 3 tests to 1. The two dropped
tests exercised pre-canonical margin-mode validation branches that do not
exist on `BinanceWalletRuntime`, which is
built via the canonical proto path where `margin_mode` is normalized
during `from_canonical` rather than validated as an enum. See
`strategy-service/docs/legacy-test-coverage-audit.md` for the rationale;
the unsupported-margin-mode contract is covered separately by the
`unsupported-binance-margin-modes` spec (multi_assets / portfolio_margin
fail-closed tests live in `tests/test_wallet_runtime.py`).

The surviving invariant is "hedge mode + missing/invalid position_side on
on_order → ValueError", which does still apply to `BinanceWalletRuntime`
(see `_position_key_from_order` in `strategy_service/wallet/binance.py`).
"""

from __future__ import annotations

import pytest

from strategy_service.position_side import BOTH, LONG, SHORT
from tests.helpers.wallet_fixtures import make_testnet_wallet


def test_hedge_mode_on_order_requires_explicit_valid_position_side() -> None:
    """`BinanceWalletRuntime.on_order` in hedge mode MUST raise when the
    order's `position_side` is missing or not one of `LONG` / `SHORT`.

    This preserves the same fail-closed behavior at the canonical runtime
    boundary. The raise point is `_position_key_from_order` (binance.py:341):
        raise ValueError("hedge-mode parity orders require explicit position_side")
    """
    wallet = make_testnet_wallet(margin_mode="isolated", position_mode="hedge")

    # Case 1: empty position_side → raise
    class FillWithoutSide:
        status = "FILLED"
        side = "BUY"
        position_side = ""
        qty = 0.1
        fill_price = 50_000.0
        order_id = "hedge-no-side-1"

    with pytest.raises(ValueError, match="explicit position_side"):
        wallet.on_order("BTCUSDT", "futures", FillWithoutSide())

    # Case 2: invalid (non LONG/SHORT) position_side → raise
    class FillWithInvalidSide:
        status = "FILLED"
        side = "BUY"
        position_side = "INVALID"
        qty = 0.1
        fill_price = 50_000.0
        order_id = "hedge-bad-side-1"

    with pytest.raises(ValueError, match="explicit position_side"):
        wallet.on_order("BTCUSDT", "futures", FillWithInvalidSide())

    class FillWithProtoDefaultSide:
        status = "FILLED"
        side = "BUY"
        position_side = BOTH
        qty = 0.1
        fill_price = 50_000.0
        order_id = "hedge-default-side-1"

    with pytest.raises(ValueError, match="explicit position_side"):
        wallet.on_order("BTCUSDT", "futures", FillWithProtoDefaultSide())

    # Sanity: after the raises the runtime state is unchanged (no partial
    # position created). Hedge mode builds positions lazily on valid fills;
    # there should still be no positions because every attempted fill
    # raised before mutating state.
    assert wallet.futures.positions == {}


def test_one_way_on_order_rejects_explicit_hedge_side() -> None:
    wallet = make_testnet_wallet(margin_mode="cross", position_mode="one_way")

    class FillWithLongSide:
        status = "FILLED"
        side = "BUY"
        position_side = "LONG"
        qty = 0.1
        fill_price = 50_000.0
        order_id = "one-way-long-side-1"

    with pytest.raises(ValueError, match="one-way FuturesPosition must use BOTH"):
        wallet.on_order("BTCUSDT", "futures", FillWithLongSide())

    class FillWithProtoLong:
        status = "FILLED"
        side = "BUY"
        position_side = LONG
        qty = 0.1
        fill_price = 50_000.0
        order_id = "one-way-proto-long-side-1"

    with pytest.raises(ValueError, match="one-way FuturesPosition must use BOTH"):
        wallet.on_order("BTCUSDT", "futures", FillWithProtoLong())


def test_hedge_order_key_is_derived_from_shared_position_side() -> None:
    wallet = make_testnet_wallet(margin_mode="cross", position_mode="hedge")

    assert wallet.futures._position_key_from_order("BTCUSDT", "LONG", "BUY") == 1
    assert wallet.futures._position_key_from_order("BTCUSDT", SHORT, "SELL") == -1
