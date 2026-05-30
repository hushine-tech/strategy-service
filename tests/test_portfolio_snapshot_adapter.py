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


def _futures_wallet(
    *,
    wallet_balance: float = 1000.0,
    available_balance: float = 800.0,
    margin_balance: float = 1025.0,
    positions=None,
    risk_metadata=None,
):
    return account_service_pb2.AccountWalletState(
        mode=2,
        futures=account_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=wallet_balance,
            available_balance=available_balance,
            margin_balance=margin_balance,
            total_cross_wallet_balance=wallet_balance,
            total_cross_un_pnl=margin_balance - wallet_balance,
            positions=list(positions or []),
            risk_metadata=list(risk_metadata or []),
        ),
    )


def _spot_wallet(*, free: float = 90.0, locked: float = 10.0, assets=None):
    return account_service_pb2.AccountWalletState(
        mode=2,
        spot=account_service_pb2.SpotWallet(
            free=free,
            locked=locked,
            assets=list(assets or []),
        ),
    )


def _futures_position(
    symbol: str = "ETHUSDT",
    position_side: str = "BOTH",
    position_qty: float = 0.25,
    entry_price: float = 3000.0,
    mark_price: float = 3100.0,
    unrealized_pnl: float = 25.0,
    leverage: float = 20.0,
    liquidation_price: float = 2200.0,
):
    return account_service_pb2.FuturesPosition(
        symbol=symbol,
        position_side=position_side,
        position_qty=position_qty,
        qty=position_qty,
        entry_price=entry_price,
        mark_price=mark_price,
        unrealized_pnl=unrealized_pnl,
        leverage=leverage,
        margin_mode="cross",
        margin_type="cross",
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
    wallet=None,
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
            wallet=wallet,
        )

    venue = account_service_pb2.VenueSnapshot(
        venue_id=venue_id,
        exchange=exchange,
        market=market,
        total_value=total_value,
        wallet_balance=wallet_balance,
        available_balance=available_balance,
        balances=list(balances or []),
        positions=list(positions or []),
    )
    if wallet is not None:
        venue.wallet.CopyFrom(wallet)
    return venue


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
            wallet=_spot_wallet(
                free=90.0,
                locked=10.0,
                assets=[
                    account_service_pb2.SpotAsset(
                        symbol="BTC",
                        qty=0.5,
                        locked=0.1,
                    )
                ],
            ),
        ),
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            wallet_balance=1000.0,
            available_balance=800.0,
            positions=[_position()],
            wallet=_futures_wallet(
                wallet_balance=1000.0,
                available_balance=800.0,
                margin_balance=1025.0,
                positions=[_futures_position()],
            ),
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
            wallet=_futures_wallet(
                wallet_balance=1000.0,
                available_balance=700.0,
                margin_balance=1200.0,
                positions=[
                    _futures_position(
                        symbol="btcusdt",
                        position_qty=-0.2,
                        entry_price=45000.0,
                        mark_price=44000.0,
                        unrealized_pnl=200.0,
                        liquidation_price=52000.0,
                    )
                ],
            ),
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


def test_futures_venue_uses_full_canonical_wallet_instead_of_compact_position_defaults():
    full_wallet = account_service_pb2.AccountWalletState(
        mode=2,
        futures=account_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=1000.0,
            available_balance=950.0,
            margin_balance=1015.0,
            total_cross_wallet_balance=1000.0,
            total_cross_un_pnl=15.0,
            risk_metadata=[
                account_service_pb2.FuturesRiskMetadata(
                    symbol="ETHUSDT",
                    configured_leverage=20.0,
                    configured_margin_mode="cross",
                )
            ],
            positions=[
                account_service_pb2.FuturesPosition(
                    symbol="ETHUSDT",
                    position_side="BOTH",
                    position_qty=0.3,
                    qty=0.3,
                    entry_price=3000.0,
                    mark_price=3050.0,
                    unrealized_pnl=15.0,
                    leverage=20.0,
                    margin_mode="cross",
                    margin_type="cross",
                    initial_margin=45.75,
                    position_initial_margin=45.75,
                )
            ],
        ),
    )
    snapshot = _snapshot(
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            wallet_balance=1000.0,
            available_balance=400.0,
            positions=[_position(qty=0.3, mark_price=3050.0)],
            wallet=full_wallet,
        )
    )

    wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "perpetual_futures")},
    ).get("binance", "perpetual_futures")

    pos = wallet.futures.positions[("ETHUSDT", 0)]
    assert pos.leverage == pytest.approx(20.0)
    assert wallet.futures.oracle_available_balance == pytest.approx(950.0)
    assert wallet.get_available_balance() == pytest.approx(969.25)
    assert wallet.futures.risk_metadata["ETHUSDT"].configured_leverage == pytest.approx(20.0)


def test_futures_compact_position_without_full_wallet_fails_closed():
    snapshot = _snapshot(
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            positions=[_position()],
        )
    )

    with pytest.raises(ValueError, match="full canonical wallet"):
        build_portfolio_wallet_from_snapshot(
            snapshot,
            allowed_routes={("binance", "perpetual_futures")},
        )


def test_spot_empty_compact_without_full_wallet_fails_closed():
    snapshot = _snapshot(
        _venue(
            venue_id=10,
            market=MARKET_SPOT,
            total_value=0.0,
            wallet_balance=0.0,
            available_balance=0.0,
        )
    )

    with pytest.raises(ValueError, match="spot.*full canonical wallet"):
        build_portfolio_wallet_from_snapshot(
            snapshot,
            allowed_routes={("binance", "spot")},
        )


def test_futures_empty_compact_without_full_wallet_fails_closed():
    snapshot = _snapshot(
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            total_value=0.0,
            wallet_balance=0.0,
            available_balance=0.0,
        )
    )

    with pytest.raises(ValueError, match="futures.*full canonical wallet"):
        build_portfolio_wallet_from_snapshot(
            snapshot,
            allowed_routes={("binance", "perpetual_futures")},
        )


def test_spot_empty_full_wallet_with_compact_balances_fails_closed():
    snapshot = _snapshot(
        _venue(
            venue_id=10,
            market=MARKET_SPOT,
            balances=[
                _balance("USDT", wallet_balance=100.0, available_balance=90.0, locked=10.0),
                _balance("BTC", wallet_balance=0.5, available_balance=0.4, locked=0.1),
            ],
            wallet=account_service_pb2.AccountWalletState(mode=2),
        )
    )

    with pytest.raises(ValueError, match="spot.*full canonical wallet"):
        build_portfolio_wallet_from_snapshot(
            snapshot,
            allowed_routes={("binance", "spot")},
        )


def test_futures_empty_full_wallet_with_compact_positions_fails_closed():
    snapshot = _snapshot(
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            positions=[_position()],
            wallet=account_service_pb2.AccountWalletState(mode=2),
        )
    )

    with pytest.raises(ValueError, match="futures.*full canonical wallet"):
        build_portfolio_wallet_from_snapshot(
            snapshot,
            allowed_routes={("binance", "perpetual_futures")},
        )


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
        _venue(venue_id=10, market=MARKET_SPOT, wallet=_spot_wallet(free=1.0, locked=0.0)),
        _venue(venue_id=11, market=MARKET_SPOT, wallet=_spot_wallet(free=2.0, locked=0.0)),
    )

    wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "spot")},
    )

    with pytest.raises(ValueError, match="ambiguous wallet route.*10.*11"):
        wallet.get("binance", "spot")
