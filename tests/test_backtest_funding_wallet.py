from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from strategy_service.gen import portfolio_service_pb2
from strategy_service.funding_position_tracker import FundingPositionLegFact
from strategy_service.wallet.portfolio_adapter import (
    apply_venue_wallet_snapshot,
    build_portfolio_wallet_from_snapshot,
)
from strategy_service.wallet.portfolio import PortfolioWalletRuntime
from tests.helpers.wallet_fixtures import make_backtest_wallet


def _portfolio(*, long_margin: str = "isolated", short_margin: str = "isolated"):
    wallet = make_backtest_wallet(
        margin_mode="cross",
        position_mode="hedge",
        wallet_balance=100.0,
        futures_positions=[
            {
                "symbol": "BTCUSDT",
                "position_side": "LONG",
                "position_qty": 1.0,
                "entry_price": 100.0,
                "mark_price": 100.0,
                "margin_mode": long_margin,
                "isolated_wallet": 5.0 if long_margin == "isolated" else 0.0,
            },
            {
                "symbol": "BTCUSDT",
                "position_side": "SHORT",
                "position_qty": -0.5,
                "entry_price": 100.0,
                "mark_price": 100.0,
                "margin_mode": short_margin,
                "isolated_wallet": 5.0 if short_margin == "isolated" else 0.0,
            },
        ],
    )
    wallet.futures.venue_id = 11
    return PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): wallet},
    ), wallet


def _entry(entry_id: int, legs: list[dict[str, str]]):
    total = sum((Decimal(leg["applied_amount_decimal"]) for leg in legs), start=Decimal())
    return SimpleNamespace(
        income_entry_id=entry_id,
        venue_id=11,
        income_type="funding_fee",
        symbol="BTCUSDT",
        asset="USDT",
        applied_amount_decimal=format(total, "f"),
        calculation_details_json=json.dumps(legs, separators=(",", ":")),
        status="calculated",
    )


def _leg(side: str, margin: str, amount: str) -> dict[str, str]:
    return {
        "symbol": "BTCUSDT",
        "position_side": side,
        "margin_mode": margin,
        "signed_qty_decimal": "1" if side == "LONG" else "-0.5",
        "funding_rate_decimal": "0.0001",
        "mark_price_decimal": "100",
        "calculated_amount_decimal": amount,
        "applied_amount_decimal": amount,
        "calculator_version": "binance-usdm-linear-v1",
    }


def test_atomic_income_applies_both_isolated_hedge_legs_then_advances_cursor_once():
    portfolio, wallet = _portfolio()
    long = wallet.futures.positions[("BTCUSDT", 1)]
    short = wallet.futures.positions[("BTCUSDT", -1)]

    portfolio.apply_funding_income_entry(
        "binance",
        "perpetual_futures",
        11,
        _entry(7, [_leg("LONG", "isolated", "-0.2"), _leg("SHORT", "isolated", "0.1")]),
    )

    assert long.isolated_wallet == pytest.approx(4.8)
    assert short.isolated_wallet == pytest.approx(5.1)
    assert long.carry_cost == pytest.approx(0.2)
    assert short.carry_cost == pytest.approx(-0.1)
    assert wallet.futures.wallet_balance == pytest.approx(100.0)
    assert wallet.futures.last_applied_income_entry_id == 7


def test_atomic_income_applies_mixed_cross_and_isolated_allocations():
    portfolio, wallet = _portfolio(long_margin="cross", short_margin="isolated")
    short = wallet.futures.positions[("BTCUSDT", -1)]

    portfolio.apply_funding_income_entry(
        "binance",
        "perpetual_futures",
        11,
        _entry(8, [_leg("LONG", "cross", "-0.2"), _leg("SHORT", "isolated", "0.1")]),
    )

    assert wallet.futures.wallet_balance == pytest.approx(99.8)
    assert short.isolated_wallet == pytest.approx(5.1)
    assert wallet.futures.last_applied_income_entry_id == 8


def test_atomic_income_duplicate_entry_is_a_whole_entry_noop():
    portfolio, wallet = _portfolio()
    entry = _entry(9, [_leg("LONG", "isolated", "-0.2"), _leg("SHORT", "isolated", "0.1")])

    portfolio.apply_funding_income_entry("binance", "perpetual_futures", 11, entry)
    first = (
        wallet.futures.positions[("BTCUSDT", 1)].isolated_wallet,
        wallet.futures.positions[("BTCUSDT", -1)].isolated_wallet,
    )
    stale_replay = SimpleNamespace(income_entry_id=9, venue_id=11)
    portfolio.apply_funding_income_entry(
        "binance", "perpetual_futures", 11, stale_replay
    )

    assert (
        wallet.futures.positions[("BTCUSDT", 1)].isolated_wallet,
        wallet.futures.positions[("BTCUSDT", -1)].isolated_wallet,
    ) == first
    assert wallet.futures.last_applied_income_entry_id == 9


def test_atomic_income_rolls_back_every_leg_and_does_not_advance_cursor(monkeypatch):
    portfolio, wallet = _portfolio(long_margin="cross", short_margin="isolated")
    short = wallet.futures.positions[("BTCUSDT", -1)]
    before = (wallet.futures.wallet_balance, short.isolated_wallet, short.carry_cost)

    monkeypatch.setattr(
        wallet.futures,
        "_refresh_portfolio_fields",
        lambda: (_ for _ in ()).throw(RuntimeError("refresh failed")),
    )
    with pytest.raises(RuntimeError, match="refresh failed"):
        portfolio.apply_funding_income_entry(
            "binance",
            "perpetual_futures",
            11,
            _entry(10, [_leg("LONG", "cross", "-0.2"), _leg("SHORT", "isolated", "0.1")]),
        )

    assert (wallet.futures.wallet_balance, short.isolated_wallet, short.carry_cost) == before
    assert wallet.futures.last_applied_income_entry_id == 0


def test_portfolio_snapshot_restores_exact_initial_funding_legs_by_venue():
    venue = portfolio_service_pb2.VenueSnapshot(
        venue_id=11,
        exchange=1,
        environment=0,
        market=2,
    )
    venue.wallet.CopyFrom(portfolio_service_pb2.PortfolioWalletState(
        environment=0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="isolated",
            position_mode="hedge",
            initial_balance=100.0,
            positions=[portfolio_service_pb2.FuturesPosition(
                symbol="BTCUSDT",
                position_side="LONG",
                position_qty=0.1,
                signed_qty_decimal="0.100000000000000001",
                venue_id=11,
                margin_mode="isolated",
            )],
        ),
    ))
    snapshot = portfolio_service_pb2.PortfolioSnapshot(
        portfolio_id=7,
        user_id=17,
        venues=[venue],
    )

    runtime = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "perpetual_futures")},
    )

    assert runtime.funding_position_tracker.legs_for(11, "BTCUSDT") == [
        FundingPositionLegFact(
            "BTCUSDT", "LONG", "isolated", "0.100000000000000001"
        )
    ]


def test_portfolio_snapshot_replacement_refreshes_exact_funding_legs_and_clears_old_state():
    def venue(quantity: str, *, include_zec: bool = False):
        item = portfolio_service_pb2.VenueSnapshot(
            venue_id=11, exchange=1, environment=0, market=2,
        )
        wallet = portfolio_service_pb2.PortfolioWalletState(
            environment=0,
            futures=portfolio_service_pb2.FuturesWallet(
                margin_mode="cross",
                position_mode="one_way",
                initial_balance=100.0,
                positions=[portfolio_service_pb2.FuturesPosition(
                    symbol="ETHUSDT",
                    position_side="BOTH",
                    position_qty=float(quantity),
                    signed_qty_decimal=quantity,
                    venue_id=11,
                    margin_mode="cross",
                )],
            ),
        )
        if include_zec:
            wallet.futures.positions.append(portfolio_service_pb2.FuturesPosition(
                symbol="ZECUSDT", position_side="BOTH", position_qty=1.0,
                signed_qty_decimal="1", venue_id=11, margin_mode="cross",
            ))
        item.wallet.CopyFrom(wallet)
        return item

    runtime = build_portfolio_wallet_from_snapshot(
        portfolio_service_pb2.PortfolioSnapshot(
            portfolio_id=7, user_id=17,
            venues=[venue("1.000000000000000001", include_zec=True)],
        ),
        allowed_routes={("binance", "perpetual_futures")},
    )
    runtime.funding_position_tracker.on_lifecycle_fill(
        SimpleNamespace(
            event_type="fill", venue_id=11, symbol="BTCUSDT",
            exchange_trade_id="pre-snapshot", occurred_at=100,
            side="BUY", qty_decimal="0.5", position_side="BOTH",
        ),
        position_mode="one_way",
        margin_mode="cross",
    )

    apply_venue_wallet_snapshot(runtime, venue("0.200000000000000001"), expected_environment=0)

    assert runtime.funding_position_tracker.legs_for(11, "ETHUSDT") == [
        FundingPositionLegFact("ETHUSDT", "BOTH", "cross", "0.200000000000000001")
    ]
    assert runtime.funding_position_tracker.legs_for(11, "ZECUSDT") == []
    assert runtime.funding_position_tracker.legs_for(11, "BTCUSDT") == []


def test_invalid_replacement_leaves_prior_exact_tracker_snapshot_unchanged():
    venue = portfolio_service_pb2.VenueSnapshot(
        venue_id=11, exchange=1, environment=0, market=2,
    )
    venue.wallet.CopyFrom(portfolio_service_pb2.PortfolioWalletState(
        environment=0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross", position_mode="one_way", initial_balance=100.0,
            positions=[portfolio_service_pb2.FuturesPosition(
                symbol="ETHUSDT", position_side="BOTH", position_qty=1.0,
                signed_qty_decimal="1.000000000000000001", venue_id=11,
                margin_mode="cross",
            )],
        ),
    ))
    runtime = build_portfolio_wallet_from_snapshot(
        portfolio_service_pb2.PortfolioSnapshot(
            portfolio_id=7, user_id=17, venues=[venue],
        ),
        allowed_routes={("binance", "perpetual_futures")},
    )
    invalid = portfolio_service_pb2.VenueSnapshot()
    invalid.CopyFrom(venue)
    invalid.wallet.futures.positions[0].signed_qty_decimal = ""

    with pytest.raises(ValueError, match="signed_qty_decimal"):
        apply_venue_wallet_snapshot(runtime, invalid, expected_environment=0)

    assert runtime.funding_position_tracker.legs_for(11, "ETHUSDT") == [
        FundingPositionLegFact("ETHUSDT", "BOTH", "cross", "1.000000000000000001")
    ]


def test_portfolio_snapshot_rejects_float_only_initial_futures_position():
    venue = portfolio_service_pb2.VenueSnapshot(
        venue_id=11, exchange=1, environment=0, market=2,
    )
    venue.wallet.CopyFrom(portfolio_service_pb2.PortfolioWalletState(
        environment=0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            initial_balance=100.0,
            positions=[portfolio_service_pb2.FuturesPosition(
                symbol="ZECUSDT",
                position_side="BOTH",
                position_qty=1.0,
                venue_id=11,
                margin_mode="cross",
            )],
        ),
    ))

    with pytest.raises(ValueError, match="signed_qty_decimal"):
        build_portfolio_wallet_from_snapshot(
            portfolio_service_pb2.PortfolioSnapshot(
                portfolio_id=7, user_id=17, venues=[venue],
            ),
            allowed_routes={("binance", "perpetual_futures")},
        )


@pytest.mark.parametrize("position_venue_id", [0, 22])
def test_portfolio_snapshot_rejects_missing_or_mismatched_position_venue_identity(
    position_venue_id,
):
    venue = portfolio_service_pb2.VenueSnapshot(
        venue_id=11, exchange=1, environment=0, market=2,
    )
    venue.wallet.CopyFrom(portfolio_service_pb2.PortfolioWalletState(
        environment=0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            initial_balance=100.0,
            positions=[portfolio_service_pb2.FuturesPosition(
                symbol="BTCUSDT",
                position_side="BOTH",
                position_qty=1.0,
                signed_qty_decimal="1",
                venue_id=position_venue_id,
                margin_mode="cross",
            )],
        ),
    ))

    with pytest.raises(ValueError, match="venue_id"):
        build_portfolio_wallet_from_snapshot(
            portfolio_service_pb2.PortfolioSnapshot(
                portfolio_id=7, user_id=17, venues=[venue],
            ),
            allowed_routes={("binance", "perpetual_futures")},
        )
