from __future__ import annotations

from types import SimpleNamespace

import pytest

from strategy_service.gen import account_service_pb2
from strategy_service.wallet.binance import BinanceWalletRuntime
from strategy_service.wallet.portfolio_adapter import build_portfolio_wallet_from_snapshot


EXCHANGE_BINANCE = 1
EXCHANGE_OKX = 2
MARKET_SPOT = 1
MARKET_PERPETUAL_FUTURES = 2
MARKET_DELIVERY_FUTURES = 3


def _balance(asset: str, wallet_balance: float, available_balance: float, locked: float = 0.0):
    return account_service_pb2.BalanceEntry(
        asset=asset,
        wallet_balance=wallet_balance,
        available_balance=available_balance,
        locked=locked,
        value_usdt=wallet_balance,
    )


def _position(
    symbol: str = "ETHUSDT",
    position_side: str = "BOTH",
    qty: float = 0.25,
    entry_price: float = 3000.0,
    mark_price: float = 3100.0,
    unrealized_pnl: float = 25.0,
    margin_balance: float = 1025.0,
    liquidation_price: float = 2200.0,
):
    return account_service_pb2.PositionEntry(
        symbol=symbol,
        position_side=position_side,
        qty=qty,
        entry_price=entry_price,
        mark_price=mark_price,
        unrealized_pnl=unrealized_pnl,
        margin_balance=margin_balance,
        liquidation_price=liquidation_price,
    )


def _venue(
    *,
    venue_id: int,
    exchange: int | str = EXCHANGE_BINANCE,
    market: int | str = MARKET_PERPETUAL_FUTURES,
    total_value: float = 1000.0,
    wallet_balance: float = 1000.0,
    available_balance: float = 900.0,
    balances=None,
    positions=None,
):
    if isinstance(exchange, str) or isinstance(market, str):
        return SimpleNamespace(
            venue_id=venue_id,
            exchange=exchange,
            market=market,
            total_value=total_value,
            wallet_balance=wallet_balance,
            available_balance=available_balance,
            balances=list(balances or []),
            positions=list(positions or []),
        )

    return account_service_pb2.VenueSnapshot(
        venue_id=venue_id,
        exchange=exchange,
        market=market,
        total_value=total_value,
        wallet_balance=wallet_balance,
        available_balance=available_balance,
        balances=list(balances or []),
        positions=list(positions or []),
    )


def _snapshot(*venues):
    return account_service_pb2.PortfolioSnapshot(
        account_id=7,
        user_id=3,
        total_value=1200.0,
        wallet_balance=1100.0,
        available_balance=900.0,
        venues=list(venues),
    )


def test_build_portfolio_wallet_from_spot_and_futures_venues():
    snapshot = _snapshot(
        _venue(
            venue_id=10,
            market=MARKET_SPOT,
            wallet_balance=210.0,
            available_balance=200.0,
            balances=[
                _balance("USDT", wallet_balance=100.0, available_balance=90.0, locked=10.0),
                _balance("BTC", wallet_balance=0.5, available_balance=0.4, locked=0.1),
            ],
        ),
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            wallet_balance=1000.0,
            available_balance=800.0,
            positions=[_position()],
        ),
    )

    wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "spot"), ("binance", "perpetual_futures")},
    )

    spot = wallet.get("binance", "spot")
    futures = wallet.get("binance", "perpetual_futures")
    assert isinstance(spot, BinanceWalletRuntime)
    assert isinstance(futures, BinanceWalletRuntime)
    assert spot is not futures
    assert spot.spot.free == pytest.approx(90.0)
    assert spot.spot.locked == pytest.approx(10.0)
    assert spot.spot.assets["BTC"].qty == pytest.approx(0.5)
    assert spot.spot.assets["BTC"].locked == pytest.approx(0.1)
    assert futures.get_wallet_balance() == pytest.approx(1000.0)
    assert futures.futures.oracle_available_balance == pytest.approx(800.0)


def test_futures_position_and_balance_fields_are_readable_after_mapping():
    snapshot = _snapshot(
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            wallet_balance=1000.0,
            available_balance=700.0,
            positions=[
                _position(
                    symbol="btcusdt",
                    qty=-0.2,
                    entry_price=45000.0,
                    mark_price=44000.0,
                    unrealized_pnl=200.0,
                    margin_balance=1200.0,
                    liquidation_price=52000.0,
                )
            ],
        )
    )

    wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "perpetual_futures")},
    ).get("binance", "perpetual_futures")

    pos = wallet.futures.positions[("BTCUSDT", 0)]
    assert pos.position_qty == pytest.approx(-0.2)
    assert pos.entry_price == pytest.approx(45000.0)
    assert pos.mark_price == pytest.approx(44000.0)
    assert pos.oracle_unrealized_pnl == pytest.approx(200.0)
    assert pos.oracle_liquidation_price == pytest.approx(52000.0)
    assert wallet.futures.oracle_available_balance == pytest.approx(700.0)
    assert wallet.futures.oracle_margin_balance == pytest.approx(1200.0)


@pytest.mark.parametrize(
    ("exchange", "market", "message"),
    [
        (EXCHANGE_OKX, MARKET_SPOT, "unsupported portfolio wallet exchange"),
        (EXCHANGE_BINANCE, MARKET_DELIVERY_FUTURES, "unsupported portfolio wallet market"),
        (99, MARKET_SPOT, "unsupported enum value"),
        (EXCHANGE_BINANCE, 99, "unsupported enum value"),
    ],
)
def test_unsupported_exchange_or_market_fails_closed(exchange, market, message):
    snapshot = _snapshot(_venue(venue_id=11, exchange=exchange, market=market))

    with pytest.raises(ValueError, match=message):
        build_portfolio_wallet_from_snapshot(snapshot, allowed_routes={("binance", "spot")})


def test_missing_or_invalid_venue_id_fails_closed():
    snapshot = _snapshot(_venue(venue_id=0, market=MARKET_SPOT))

    with pytest.raises(ValueError, match="missing VenueSnapshot.venue_id"):
        build_portfolio_wallet_from_snapshot(snapshot, allowed_routes={("binance", "spot")})


def test_declared_route_missing_from_snapshot_fails_closed_on_get():
    wallet = build_portfolio_wallet_from_snapshot(
        _snapshot(),
        allowed_routes={("binance", "spot")},
    )

    with pytest.raises(ValueError, match="missing wallet"):
        wallet.get("binance", "spot")


def test_duplicate_route_keeps_portfolio_runtime_ambiguous_fail_closed():
    snapshot = _snapshot(
        _venue(venue_id=10, market=MARKET_SPOT),
        _venue(venue_id=11, market=MARKET_SPOT),
    )

    wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "spot")},
    )

    with pytest.raises(ValueError, match="ambiguous wallet route.*10.*11"):
        wallet.get("binance", "spot")
