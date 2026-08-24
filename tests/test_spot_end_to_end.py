from __future__ import annotations

from decimal import Decimal

import pytest

from strategy_service.wallet.canonical import SpotSymbolMetadata
from strategy_service.wallet.order_types import OrderResponse
from strategy_service.wallet.spot import SpotWallet
from strategy_service.portfolio_client import _serialize_spot_wallet


def spot_metadata(
    symbol: str = "BTCUSDT",
    *,
    venue_id: int = 10,
    base_asset: str = "BTC",
    quote_asset: str = "USDT",
) -> SpotSymbolMetadata:
    return SpotSymbolMetadata(
        venue_id=venue_id,
        exchange="binance",
        market="spot",
        symbol=symbol,
        status="TRADING",
        base_asset=base_asset,
        quote_asset=quote_asset,
        base_asset_precision=8,
        quote_asset_precision=8,
        spot_trading_allowed=True,
        permission_sets=(("SPOT",),),
        order_types=("LIMIT", "MARKET"),
    )


def fill_update(
    *,
    trade_id: str = "7",
    executed: str = "0.01",
    cumulative_quote: str = "500",
    fill_qty: str = "0.01",
    fill_quote: str = "500",
    fee: str = "0.001",
    fee_asset: str = "BNB",
    venue_id: int = 10,
    side: str = "BUY",
    status: str = "FILLED",
) -> OrderResponse:
    return OrderResponse(
        symbol="BTCUSDT",
        side=side,
        qty=float(fill_qty),
        fill_price=50_000.0,
        status=status,
        fee=float(fee),
        order_id="hushine-order-1",
        venue_id=venue_id,
        exchange="binance",
        market="spot",
        exchange_order_id="42",
        exchange_trade_id=trade_id,
        qty_decimal=fill_qty,
        quote_qty_decimal=fill_quote,
        fill_price_decimal="50000",
        fee_decimal=fee,
        fee_asset=fee_asset,
        orig_qty_decimal="0.02",
        executed_qty_decimal=executed,
        remaining_qty_decimal=str(Decimal("0.02") - Decimal(executed)),
        cumulative_quote_qty_decimal=cumulative_quote,
    )


def test_spot_wallet_uses_asset_codes_and_market_data_never_creates_symbol_asset():
    wallet = SpotWallet.from_assets({"USDT": ("1000.00", "0"), "BTC": ("0.10", "0")})
    metadata = spot_metadata()

    wallet.on_market_data("BTCUSDT", Decimal("50000"), metadata)

    assert set(wallet.assets) == {"USDT", "BTC"}
    assert "BTCUSDT" not in wallet.assets
    assert wallet.assets["BTC"].price == Decimal("50000")


def test_spot_market_data_does_not_invent_an_unowned_base_asset():
    wallet = SpotWallet.from_assets({"USDT": ("1000", "0")})

    wallet.on_market_data("BTCUSDT", Decimal("50000"), spot_metadata())

    assert set(wallet.assets) == {"USDT"}
    assert wallet.symbol_prices[(10, "binance", "spot", "BTCUSDT")] == Decimal("50000")


def test_spot_buy_applies_actual_fill_and_bnb_commission_once_across_replays():
    wallet = SpotWallet.from_assets({"USDT": ("1500", "0"), "BNB": ("1", "0")})
    metadata = spot_metadata()
    update = fill_update()

    wallet.apply_order_update(update, metadata)
    wallet.apply_order_update(update, metadata)  # WebSocket replay of POST FULL.
    wallet.apply_order_update(fill_update(), metadata)  # REST recovery replay.

    assert wallet.assets["BTC"].free == Decimal("0.01")
    assert wallet.assets["USDT"].free == Decimal("1000")
    assert wallet.assets["BNB"].free == Decimal("0.999")
    assert len(wallet.applied_trade_ids) == 1


def test_spot_duplicate_fill_can_advance_terminal_order_state_without_double_debit():
    wallet = SpotWallet.from_assets({"USDT": ("1500", "0"), "BNB": ("1", "0")})
    metadata = spot_metadata()
    order_key = (10, "binance", "spot", "BTCUSDT", "42")
    wallet.apply_order_update(
        OrderResponse(
            symbol="BTCUSDT",
            side="BUY",
            qty=0.0,
            fill_price=0.0,
            status="NEW",
            order_id="hushine-order-1",
            venue_id=10,
            exchange="binance",
            market="spot",
            exchange_order_id="42",
            qty_decimal="0",
            fill_price_decimal="0",
            fee_decimal="0",
            quote_qty_decimal="0",
            orig_qty_decimal="0.02",
            executed_qty_decimal="0",
            remaining_qty_decimal="0.02",
            price_decimal="50000",
            cumulative_quote_qty_decimal="0",
        ),
        metadata,
    )
    partial = fill_update(status="PARTIALLY_FILLED")

    assert wallet.apply_order_update(partial, metadata) is True
    assert wallet.apply_order_update(fill_update(status="CANCELED"), metadata) is False

    assert wallet.assets["BTC"].free == Decimal("0.01")
    assert wallet.assets["USDT"].free == Decimal("1000")
    assert wallet.assets["USDT"].locked == Decimal("0")
    assert wallet.assets["BNB"].free == Decimal("0.999")
    assert wallet.order_states[order_key].status == "CANCELED"
    assert order_key not in wallet.open_orders


def test_spot_distinct_trade_ids_on_one_order_apply_independently():
    wallet = SpotWallet.from_assets({"USDT": ("1500", "0"), "BNB": ("1", "0")})
    metadata = spot_metadata()

    wallet.apply_order_update(fill_update(), metadata)
    wallet.apply_order_update(
        fill_update(
            trade_id="8",
            executed="0.02",
            cumulative_quote="1000",
            fee="0.002",
        ),
        metadata,
    )

    assert wallet.assets["BTC"].free == Decimal("0.02")
    assert wallet.assets["USDT"].free == Decimal("500")
    assert wallet.assets["BNB"].free == Decimal("0.997")
    assert len(wallet.applied_trade_ids) == 2


def test_spot_trade_id_zero_is_a_valid_idempotency_identity():
    wallet = SpotWallet.from_assets({"USDT": ("1000", "0"), "BNB": ("1", "0")})
    update = fill_update(trade_id="0")

    assert wallet.apply_order_update(update, spot_metadata()) is True
    assert wallet.apply_order_update(update, spot_metadata()) is False
    assert wallet.assets["BTC"].free == Decimal("0.01")


def test_spot_nonzero_fill_without_trade_id_is_recovery_pending_and_not_applied():
    wallet = SpotWallet.from_assets({"USDT": ("1000", "0"), "BNB": ("1", "0")})

    applied = wallet.apply_order_update(fill_update(trade_id=""), spot_metadata())

    assert applied is False
    assert wallet.assets["USDT"].free == Decimal("1000")
    assert "BTC" not in wallet.assets
    assert len(wallet.recovery_pending_orders) == 1


def test_spot_late_new_cannot_regress_a_terminal_order_or_reopen_locks():
    wallet = SpotWallet.from_assets({"USDT": ("1000", "0"), "BNB": ("1", "0")})
    metadata = spot_metadata()
    wallet.apply_order_update(fill_update(), metadata)

    applied = wallet.apply_order_update(
        OrderResponse(
            symbol="BTCUSDT",
            side="BUY",
            qty=0.0,
            fill_price=0.0,
            status="NEW",
            order_id="hushine-order-1",
            venue_id=10,
            exchange="binance",
            market="spot",
            exchange_order_id="42",
            qty_decimal="0",
            fill_price_decimal="0",
            fee_decimal="0",
            quote_qty_decimal="0",
            orig_qty_decimal="0.02",
            executed_qty_decimal="0.01",
            remaining_qty_decimal="0.01",
            price_decimal="50000",
            cumulative_quote_qty_decimal="500",
        ),
        metadata,
    )

    order_key = (10, "binance", "spot", "BTCUSDT", "42")
    assert applied is False
    assert wallet.order_states[order_key].status == "FILLED"
    assert order_key not in wallet.open_orders
    assert wallet.assets["USDT"].locked == Decimal("0")


def test_spot_cumulative_advance_without_fill_delta_stays_recovery_pending():
    wallet = SpotWallet.from_assets({"USDT": ("1500", "0")})
    metadata = spot_metadata()
    order_key = (10, "binance", "spot", "BTCUSDT", "42")
    wallet.apply_order_update(
        OrderResponse(
            symbol="BTCUSDT",
            side="BUY",
            qty=0.0,
            fill_price=0.0,
            status="NEW",
            order_id="hushine-order-1",
            venue_id=10,
            exchange="binance",
            market="spot",
            exchange_order_id="42",
            qty_decimal="0",
            fill_price_decimal="0",
            fee_decimal="0",
            quote_qty_decimal="0",
            orig_qty_decimal="0.02",
            executed_qty_decimal="0",
            remaining_qty_decimal="0.02",
            price_decimal="50000",
            cumulative_quote_qty_decimal="0",
        ),
        metadata,
    )

    applied = wallet.apply_order_update(
        OrderResponse(
            symbol="BTCUSDT",
            side="BUY",
            qty=0.0,
            fill_price=0.0,
            status="PARTIALLY_FILLED",
            order_id="hushine-order-1",
            venue_id=10,
            exchange="binance",
            market="spot",
            exchange_order_id="42",
            qty_decimal="0",
            fill_price_decimal="0",
            fee_decimal="0",
            quote_qty_decimal="0",
            orig_qty_decimal="0.02",
            executed_qty_decimal="0.01",
            remaining_qty_decimal="0.01",
            price_decimal="50000",
            cumulative_quote_qty_decimal="500",
        ),
        metadata,
    )

    assert applied is False
    assert order_key in wallet.recovery_pending_orders
    assert wallet.order_states[order_key].status == "NEW"
    assert wallet.assets["USDT"].locked == Decimal("1000")


def test_spot_route_mismatch_fails_closed_before_wallet_mutation():
    wallet = SpotWallet.from_assets({"USDT": ("1000", "0"), "BNB": ("1", "0")})

    with pytest.raises(ValueError, match="route"):
        wallet.apply_order_update(fill_update(venue_id=11), spot_metadata(venue_id=10))

    assert wallet.assets["USDT"].free == Decimal("1000")
    assert "BTC" not in wallet.assets


def test_spot_wallet_serialization_emits_asset_codes_and_exact_balances():
    wallet = SpotWallet.from_assets(
        {"USDT": ("1000.00000000", "2.00000000"), "BTC": ("0.01000000", "0")}
    )

    payload = _serialize_spot_wallet(wallet)
    by_asset = {item.asset: item for item in payload.assets}

    assert set(by_asset) == {"USDT", "BTC"}
    assert by_asset["USDT"].free_decimal == "1000.00000000"
    assert by_asset["USDT"].locked_decimal == "2.00000000"
    assert by_asset["BTC"].free_decimal == "0.01000000"
    assert "symbol" not in payload.assets[0].DESCRIPTOR.fields_by_name
    assert "qty" not in payload.assets[0].DESCRIPTOR.fields_by_name


def test_spot_wallet_serialization_emits_scaled_zero_as_plain_decimal():
    wallet = SpotWallet.from_assets(
        {"USDT": ("0E-8", "0E-8"), "BTC": ("0.01000000", "0E-8")}
    )

    payload = _serialize_spot_wallet(wallet)
    by_asset = {item.asset: item for item in payload.assets}

    assert by_asset["USDT"].free_decimal == "0.00000000"
    assert by_asset["USDT"].locked_decimal == "0.00000000"
    assert by_asset["BTC"].locked_decimal == "0.00000000"


def test_spot_wallet_serialization_normalizes_signed_scaled_zero():
    wallet = SpotWallet.from_assets(
        {"USDT": ("-0E-8", "-0E-8"), "BTC": ("0.01000000", "-0E-8")}
    )

    payload = _serialize_spot_wallet(wallet)
    by_asset = {item.asset: item for item in payload.assets}

    assert by_asset["USDT"].free_decimal == "0.00000000"
    assert by_asset["USDT"].locked_decimal == "0.00000000"
    assert by_asset["BTC"].locked_decimal == "0.00000000"


def test_spot_wallet_serialization_rejects_negative_exact_balance():
    wallet = SpotWallet.from_assets({"USDT": ("1", "0")})
    wallet.assets["USDT"].free = Decimal("-0.01")

    with pytest.raises(ValueError, match="Spot balance must be non-negative"):
        _serialize_spot_wallet(wallet)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_spot_wallet_serialization_rejects_nonfinite_exact_balance(value: str):
    wallet = SpotWallet.from_assets({"USDT": ("1", "0")})
    wallet.assets["USDT"].free = Decimal(value)

    with pytest.raises(ValueError, match="Spot balance must be finite"):
        _serialize_spot_wallet(wallet)


@pytest.mark.parametrize(
    ("side", "fee_asset", "fee", "expected_btc", "expected_usdt"),
    [
        ("BUY", "BTC", "0.001", "0.009", "500"),
        ("BUY", "USDT", "1", "0.01", "499"),
        ("SELL", "BTC", "0.001", "0.989", "1500"),
        ("SELL", "USDT", "1", "0.99", "1499"),
    ],
)
def test_spot_commission_is_debited_from_the_actual_asset(
    side: str,
    fee_asset: str,
    fee: str,
    expected_btc: str,
    expected_usdt: str,
):
    initial_btc = "0" if side == "BUY" else "1"
    initial = {"USDT": ("1000", "0"), "BTC": (initial_btc, "0")}
    wallet = SpotWallet.from_assets(initial)

    wallet.apply_order_update(
        fill_update(side=side, fee_asset=fee_asset, fee=fee),
        spot_metadata(),
    )

    assert wallet.assets["BTC"].free == Decimal(expected_btc)
    assert wallet.assets["USDT"].free == Decimal(expected_usdt)
