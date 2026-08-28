from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from strategy_service.wallet.order_types import OrderResponse
from strategy_service.wallet.spot import SpotWallet
from tests.helpers.wallet_fixtures import make_backtest_wallet
from tests.test_spot_end_to_end import spot_metadata


@pytest.mark.parametrize(
    ("case", "status", "fill_qty", "remaining_qty", "want_open"),
    [
        ("full", "FILLED", "0.01", "0", False),
        ("gtc-partial", "PARTIALLY_FILLED", "0.004", "0.006", True),
        ("ioc-partial-expired", "EXPIRED", "0.004", "0", False),
        ("fok-zero", "EXPIRED", "0", "0", False),
    ],
    ids=["full", "gtc-partial", "ioc-partial-expired", "fok-zero"],
)
def test_spot_canonical_order_outcome_wallet_delta_matrix(
    case: str,
    status: str,
    fill_qty: str,
    remaining_qty: str,
    want_open: bool,
):
    wallet = SpotWallet.from_assets({"USDT": ("1000", "0"), "BNB": ("1", "0")})
    executed = Decimal(fill_qty)
    quote = executed * Decimal("50000")
    fee = Decimal("0.001") if executed else Decimal()
    order_id = f"spot-{case}"
    def update_for(event_status: str, event_remaining: str) -> OrderResponse:
        return OrderResponse(
            symbol="BTCUSDT",
            side="BUY",
            qty=float(executed),
            fill_price=50_000.0 if executed else 0.0,
            status=event_status,
            fee=float(fee),
            order_id=order_id,
            venue_id=10,
            exchange="binance",
            market="spot",
            exchange_order_id=order_id,
            exchange_trade_id=f"trade-{case}" if executed else "",
            qty_decimal=fill_qty,
            quote_qty_decimal=format(quote, "f"),
            fill_price_decimal="50000" if executed else "0",
            fee_decimal=format(fee, "f"),
            fee_asset="BNB" if executed else "",
            orig_qty_decimal="0.01",
            executed_qty_decimal=fill_qty,
            remaining_qty_decimal=event_remaining,
            cumulative_quote_qty_decimal=format(quote, "f"),
            price_decimal="50000",
        )

    if case == "ioc-partial-expired":
        wallet.apply_order_update(update_for("PARTIALLY_FILLED", "0.006"), spot_metadata())
    terminal_or_current = update_for(status, remaining_qty)
    wallet.apply_order_update(terminal_or_current, spot_metadata())
    if case == "full":
        wallet.apply_order_update(terminal_or_current, spot_metadata())

    quote_asset = wallet.assets["USDT"]
    assert quote_asset.free + quote_asset.locked == Decimal("1000") - quote
    expected_locked = Decimal(remaining_qty) * Decimal("50000") if want_open else Decimal()
    assert quote_asset.locked == expected_locked
    assert wallet.assets["BNB"].free == Decimal("1") - fee
    if executed:
        assert wallet.assets["BTC"].free == executed
    else:
        assert "BTC" not in wallet.assets
    assert "BTCUSDT" not in wallet.assets
    key = (10, "binance", "spot", "BTCUSDT", order_id)
    assert (key in wallet.open_orders) is want_open


@pytest.mark.parametrize(
    ("case", "status", "fill_qty", "remaining_qty", "want_open"),
    [
        ("gtc-full", "FILLED", 1.0, 0.0, False),
        ("gtc-partial", "PARTIALLY_FILLED", 0.2, 0.8, True),
        ("ioc-partial", "EXPIRED", 0.2, 0.0, False),
        ("fok-full", "FILLED", 1.0, 0.0, False),
        ("fok-zero", "EXPIRED", 0.0, 0.0, False),
    ],
    ids=["gtc-full", "gtc-partial", "ioc-partial", "fok-full", "fok-zero"],
)
def test_futures_time_in_force_wallet_delta_matrix(
    case: str,
    status: str,
    fill_qty: float,
    remaining_qty: float,
    want_open: bool,
):
    wallet = make_backtest_wallet(wallet_balance=1000.0)
    order_id = f"futures-{case}"
    wallet.on_order(
        "BTCUSDT",
        "futures",
        SimpleNamespace(
            order_id=order_id,
            status=status,
            side="BUY",
            position_side="BOTH",
            qty=fill_qty,
            fill_price=100.0 if fill_qty else 0.0,
            fee=fill_qty * 0.04,
            orig_qty=1.0,
            executed_qty=fill_qty,
            remaining_qty=remaining_qty,
            price=100.0,
            reduce_only=False,
        ),
    )

    assert wallet.futures.wallet_balance == pytest.approx(
        1000.0 - fill_qty * 0.04
    )
    position = wallet.futures.positions.get(("BTCUSDT", 0))
    if fill_qty:
        assert position is not None
        assert position.position_qty == pytest.approx(fill_qty)
        assert position.entry_price == pytest.approx(100.0)
    else:
        assert position is None
    assert (order_id in wallet.futures.open_orders) is want_open
