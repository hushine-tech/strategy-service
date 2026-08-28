from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from strategy_service.funding_position_tracker import (
    FundingPositionLegFact,
    FundingPositionTracker,
)
from strategy_service.position_side import BOTH, LONG, SHORT


def _fill(**overrides):
    values = {
        "venue_id": 11,
        "symbol": "BTCUSDT",
        "side": "BUY",
        "position_side": "BOTH",
        "exchange_trade_id": "trade-1",
        "qty_decimal": "0.100000000000000001",
        "event_type": "fill",
        "occurred_at": 100,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _legs(tracker: FundingPositionTracker, venue_id: int, symbol: str):
    return [
        (leg.symbol, leg.position_side, leg.margin_mode, leg.signed_qty_decimal)
        for leg in tracker.legs_for(venue_id, symbol, funding_time=123)
    ]


def test_one_way_partial_and_terminal_fills_use_exact_decimals_once():
    tracker = FundingPositionTracker()
    tracker.on_lifecycle_fill(_fill(), position_mode="one_way", margin_mode="cross")
    tracker.on_lifecycle_fill(
        _fill(
            side="SELL",
            exchange_trade_id="trade-2",
            qty_decimal="0.040000000000000001",
            event_type="liquidation",
        ),
        position_mode="one_way",
        margin_mode="cross",
    )
    tracker.on_lifecycle_fill(
        _fill(exchange_trade_id="trade-2", side="SELL", qty_decimal="0.9"),
        position_mode="one_way",
        margin_mode="cross",
    )

    assert _legs(tracker, 11, "BTCUSDT") == [
        ("BTCUSDT", "BOTH", "cross", "0.060000000000000000")
    ]


def test_one_way_lifecycle_rejects_explicit_hedge_side():
    tracker = FundingPositionTracker()

    with pytest.raises(ValueError, match="one-way FuturesPosition must use BOTH"):
        tracker.on_lifecycle_fill(
            _fill(position_side="LONG"),
            position_mode="one_way",
            margin_mode="cross",
        )


def test_hedge_legs_and_venues_remain_independent_with_isolated_attribution():
    tracker = FundingPositionTracker()
    tracker.on_lifecycle_fill(
        _fill(position_side="LONG", qty_decimal="2", exchange_trade_id="long"),
        position_mode="hedge",
        margin_mode="isolated",
    )
    tracker.on_lifecycle_fill(
        _fill(
            symbol="ETHUSDT",
            side="SELL",
            position_side="SHORT",
            qty_decimal="3",
            exchange_trade_id="short",
        ),
        position_mode="hedge",
        margin_mode="cross",
    )
    tracker.on_lifecycle_fill(
        _fill(venue_id=12, symbol="ZECUSDT", qty_decimal="4", exchange_trade_id="zec"),
        position_mode="one_way",
        margin_mode="cross",
    )

    assert _legs(tracker, 11, "BTCUSDT") == [("BTCUSDT", "LONG", "isolated", "2")]
    assert _legs(tracker, 11, "ETHUSDT") == [("ETHUSDT", "SHORT", "cross", "-3")]
    assert _legs(tracker, 12, "ZECUSDT") == [("ZECUSDT", "BOTH", "cross", "4")]


def test_recovery_restores_exact_legs_before_later_fill():
    tracker = FundingPositionTracker()
    tracker.restore(
        11,
        [FundingPositionLegFact("BTCUSDT", "BOTH", "cross", "1.25")],
    )
    tracker.on_lifecycle_fill(
        _fill(side="SELL", qty_decimal="0.25", exchange_trade_id="later"),
        position_mode="one_way",
        margin_mode="cross",
    )

    assert _legs(tracker, 11, "BTCUSDT") == [("BTCUSDT", "BOTH", "cross", "1.00")]


def test_historical_legs_are_ordered_by_canonical_time_not_arrival_order():
    tracker = FundingPositionTracker()
    tracker.on_lifecycle_fill(
        _fill(side="SELL", qty_decimal="0.4", exchange_trade_id="later", occurred_at=200),
        position_mode="one_way",
        margin_mode="cross",
    )
    tracker.on_lifecycle_fill(
        _fill(side="BUY", qty_decimal="1", exchange_trade_id="earlier", occurred_at=100),
        position_mode="one_way",
        margin_mode="cross",
    )

    assert [
        (leg.symbol, leg.position_side, leg.margin_mode, leg.signed_qty_decimal)
        for leg in tracker.legs_for(11, "BTCUSDT")
    ] == [("BTCUSDT", "BOTH", "cross", "0.6")]
    assert [
        (leg.position_side, leg.signed_qty_decimal)
        for leg in tracker.legs_for(11, "BTCUSDT", funding_time=150)
    ] == [("BOTH", "1")]


def test_trade_identity_is_scoped_to_venue_and_symbol() -> None:
    tracker = FundingPositionTracker()
    tracker.on_lifecycle_fill(
        _fill(exchange_trade_id="same", venue_id=11, symbol="BTCUSDT"),
        position_mode="one_way",
        margin_mode="cross",
    )
    tracker.on_lifecycle_fill(
        _fill(exchange_trade_id="same", venue_id=12, symbol="BTCUSDT"),
        position_mode="one_way",
        margin_mode="cross",
    )
    tracker.on_lifecycle_fill(
        _fill(exchange_trade_id="same", venue_id=11, symbol="ETHUSDT"),
        position_mode="one_way",
        margin_mode="cross",
    )

    assert _legs(tracker, 11, "BTCUSDT") == [("BTCUSDT", "BOTH", "cross", "0.100000000000000001")]
    assert _legs(tracker, 12, "BTCUSDT") == [("BTCUSDT", "BOTH", "cross", "0.100000000000000001")]
    assert _legs(tracker, 11, "ETHUSDT") == [("ETHUSDT", "BOTH", "cross", "0.100000000000000001")]


def test_liquidation_reduce_only_fill_uses_exact_terminal_delta() -> None:
    tracker = FundingPositionTracker()
    tracker.on_lifecycle_fill(
        _fill(qty_decimal="1", exchange_trade_id="open", occurred_at=100),
        position_mode="one_way",
        margin_mode="cross",
    )
    tracker.on_lifecycle_fill(
        _fill(
            side="SELL",
            qty_decimal="0.25",
            exchange_trade_id="reduce",
            event_type="liquidation",
            reduce_only=True,
            occurred_at=200,
        ),
        position_mode="one_way",
        margin_mode="cross",
    )

    assert [
        (leg.symbol, leg.position_side, leg.margin_mode, leg.signed_qty_decimal)
        for leg in tracker.legs_for(11, "BTCUSDT")
    ] == [("BTCUSDT", "BOTH", "cross", "0.75")]


@pytest.mark.parametrize(
    ("event", "position_mode", "margin_mode", "message"),
    [
        (_fill(side="HOLD"), "one_way", "cross", "side"),
        (_fill(position_side=""), "hedge", "cross", "position_side"),
        (_fill(qty_decimal="", qty=0.1), "one_way", "cross", "qty_decimal"),
        (_fill(occurred_at=True), "one_way", "cross", "time"),
        (_fill(), "unknown", "cross", "position_mode"),
        (_fill(), "one_way", "", "margin_mode"),
        (_fill(event_type="adl"), "one_way", "cross", "event_type"),
    ],
)
def test_invalid_or_float_only_fill_identity_fails_closed(event, position_mode, margin_mode, message):
    tracker = FundingPositionTracker()

    with pytest.raises(ValueError, match=message):
        tracker.on_lifecycle_fill(event, position_mode=position_mode, margin_mode=margin_mode)

    assert _legs(tracker, 11, "BTCUSDT") == []


def test_decimal_authority_never_reconstructs_from_float_qty():
    tracker = FundingPositionTracker()
    exact = Decimal("0.100000000000000001")
    tracker.on_lifecycle_fill(
        _fill(qty=0.1, qty_decimal=str(exact)),
        position_mode="one_way",
        margin_mode="cross",
    )

    assert _legs(tracker, 11, "BTCUSDT")[-1][-1] == "0.100000000000000001"


def test_generated_enum_inputs_are_labeled_before_funding_details_are_serialized():
    tracker = FundingPositionTracker()
    tracker.on_lifecycle_fill(
        _fill(position_side=LONG, exchange_trade_id="long-enum"),
        position_mode="hedge",
        margin_mode="cross",
    )

    assert _legs(tracker, 11, "BTCUSDT") == [("BTCUSDT", "LONG", "cross", "0.100000000000000001")]
