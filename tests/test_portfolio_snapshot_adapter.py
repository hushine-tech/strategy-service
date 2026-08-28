from __future__ import annotations

from decimal import Decimal
import socket
from types import SimpleNamespace

import pytest

from strategy_service.gen import portfolio_service_pb2
from strategy_service.position_side import BOTH, position_side_from_label
from strategy_service.inputs import parse_order_targets, resolve_order_target_leverages
from strategy_service.wallet.binance import BinanceWalletRuntime
from strategy_service.wallet import portfolio_adapter
from strategy_service.wallet.portfolio_adapter import (
    apply_venue_wallet_snapshot,
    attach_spot_risk_snapshots,
    build_portfolio_wallet_from_snapshot,
)
from strategy_service.wallet.spot import SpotFilterViolation


EXCHANGE_BINANCE = 1
EXCHANGE_OKX = 2
MARKET_SPOT = 1
MARKET_PERPETUAL_FUTURES = 2
MARKET_DELIVERY_FUTURES = 3


def _balance(asset: str, wallet_balance: float, available_balance: float, locked: float = 0.0):
    return portfolio_service_pb2.BalanceEntry(
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
    return portfolio_service_pb2.PositionEntry(
        symbol=symbol,
        position_side=position_side_from_label(position_side),
        qty=qty,
        entry_price=entry_price,
        mark_price=mark_price,
        unrealized_pnl=unrealized_pnl,
        margin_balance=margin_balance,
        liquidation_price=liquidation_price,
    )


def test_exact_funding_legs_reject_boolean_position_side_without_coercion():
    venue = SimpleNamespace(
        venue_id=11,
        wallet=SimpleNamespace(
            futures=SimpleNamespace(
                position_mode="one_way",
                positions=[
                    SimpleNamespace(
                        venue_id=11,
                        symbol="BTCUSDT",
                        position_side=True,
                        margin_mode="cross",
                        signed_qty_decimal="1",
                    )
                ],
            )
        ),
    )

    with pytest.raises(ValueError, match="invalid FuturesPositionSide"):
        portfolio_adapter._exact_funding_legs_from_venue(venue)


def _futures_wallet(
    *,
    environment: int = 1,
    wallet_balance: float = 1000.0,
    available_balance: float = 800.0,
    margin_balance: float = 1025.0,
    positions=None,
    risk_metadata=None,
):
    return portfolio_service_pb2.PortfolioWalletState(
        environment=environment,
        futures=portfolio_service_pb2.FuturesWallet(
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


def test_backtest_installs_resolved_target_leverage_without_exchange_side_effects(monkeypatch):
    def fail_network(*_args, **_kwargs):
        raise AssertionError("simulated Backtest leverage must not access Binance")

    monkeypatch.setattr(socket, "create_connection", fail_network)
    existing = portfolio_service_pb2.FuturesRiskMetadata(
        symbol="BTCUSDT",
        configured_leverage=20,
        configured_margin_mode="cross",
        step_size=0.001,
        brackets=[
            portfolio_service_pb2.FuturesRiskBracket(
                bracket=1,
                notional_cap=50_000,
                initial_leverage=20,
                maint_margin_ratio=0.004,
            )
        ],
    )
    snapshot = _snapshot(
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            wallet=_futures_wallet(
                environment=0,
                risk_metadata=[existing],
                positions=[_futures_position(symbol="BTCUSDT", leverage=20)],
            ),
        )
    )
    targets = resolve_order_target_leverages(
        parse_order_targets(
            [
                {"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT"},
                {
                    "exchange": "binance",
                    "market": "perpetual_futures",
                    "symbol": "ETHUSDT",
                    "leverage": 10,
                },
                {"exchange": "binance", "market": "perpetual_futures", "symbol": "ZECUSDT"},
                {"exchange": "binance", "market": "spot", "symbol": "BTCUSDT"},
            ]
        ),
        5,
    )

    routed = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "perpetual_futures")},
        simulated_order_targets=targets,
    )

    futures = routed.get("binance", "perpetual_futures").futures
    assert {
        symbol: (metadata.configured_leverage, metadata.leverage_source)
        for symbol, metadata in futures.risk_metadata.items()
    } == {
        "BTCUSDT": (5, "strategy_default"),
        "ETHUSDT": (10, "order_target"),
        "ZECUSDT": (5, "strategy_default"),
    }
    assert futures.risk_metadata["BTCUSDT"].configured_margin_mode == "cross"
    assert futures.risk_metadata["BTCUSDT"].step_size == pytest.approx(0.001)
    assert futures.risk_metadata["BTCUSDT"].brackets[0].notional_cap == pytest.approx(50_000)
    assert futures.positions[("BTCUSDT", 0)].leverage == pytest.approx(5)


def test_demo_wallet_ignores_simulated_target_leverage_overlay():
    snapshot = _snapshot(
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            wallet=_futures_wallet(
                environment=1,
                risk_metadata=[
                    portfolio_service_pb2.FuturesRiskMetadata(
                        symbol="BTCUSDT",
                        configured_leverage=20,
                        configured_margin_mode="cross",
                    )
                ],
            ),
        )
    )
    targets = resolve_order_target_leverages(
        parse_order_targets(
            [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT"}]
        ),
        5,
    )

    routed = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "perpetual_futures")},
        simulated_order_targets=targets,
    )

    metadata = routed.get("binance", "perpetual_futures").futures.risk_metadata["BTCUSDT"]
    assert metadata.configured_leverage == pytest.approx(20)
    assert not hasattr(metadata, "leverage_source")


def _spot_asset(
    asset: str,
    *,
    free_decimal: str,
    locked_decimal: str = "0",
    avg_entry_price_decimal: str = "",
    price_decimal: str | None = None,
):
    value = portfolio_service_pb2.SpotAsset(
        asset=asset,
        free_decimal=free_decimal,
        locked_decimal=locked_decimal,
        avg_entry_price_decimal=avg_entry_price_decimal,
    )
    if price_decimal is not None:
        value.price_decimal = price_decimal
    return value


def _spot_wallet(*, assets, environment: int = 1):
    return portfolio_service_pb2.PortfolioWalletState(
        environment=environment,
        spot=portfolio_service_pb2.SpotWallet(assets=list(assets)),
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
    venue_id: int = 11,
):
    return portfolio_service_pb2.FuturesPosition(
        venue_id=venue_id,
        symbol=symbol,
        position_side=position_side_from_label(position_side),
        qty=position_qty,
        signed_qty_decimal=format(Decimal(str(position_qty)), "f"),
        entry_price=entry_price,
        mark_price=mark_price,
        unrealized_pnl=unrealized_pnl,
        leverage=leverage,
        margin_mode="cross",
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
    spot_symbols=None,
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
            spot_symbols=list(spot_symbols or []),
        )

    venue = portfolio_service_pb2.VenueSnapshot(
        venue_id=venue_id,
        exchange=exchange,
        market=market,
        total_value=total_value,
        wallet_balance=wallet_balance,
        available_balance=available_balance,
        balances=list(balances or []),
        positions=list(positions or []),
        spot_symbols=list(spot_symbols or []),
    )
    if wallet is not None:
        venue.wallet.CopyFrom(wallet)
    return venue


def _snapshot(*venues):
    return portfolio_service_pb2.PortfolioSnapshot(
        portfolio_id=7,
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
                assets=[
                    _spot_asset("USDT", free_decimal="90", locked_decimal="10"),
                    _spot_asset("BTC", free_decimal="0.4", locked_decimal="0.1"),
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
    assert spot.spot.assets["USDT"].free == Decimal("90")
    assert spot.spot.assets["USDT"].locked == Decimal("10")
    assert spot.spot.assets["BTC"].total == Decimal("0.5")
    assert spot.spot.assets["BTC"].locked == Decimal("0.1")
    assert futures.get_wallet_balance() == pytest.approx(1000.0)
    assert futures.futures.oracle_available_balance == pytest.approx(800.0)


def test_spot_snapshot_uses_replay_price_when_backtest_risk_reference_is_empty():
    metadata = portfolio_service_pb2.SpotSymbolMetadata(
        symbol="BTCUSDT",
        status="TRADING",
        base_asset="BTC",
        quote_asset="USDT",
        base_asset_precision=8,
        quote_asset_precision=8,
        spot_trading_allowed=True,
        permission_sets=[portfolio_service_pb2.SpotSymbolPermissionSet(alternatives=["SPOT"])],
        order_types=["LIMIT", "MARKET"],
        filters=[
            portfolio_service_pb2.SpotSymbolFilter(
                filter_type="LOT_SIZE",
                min_qty="0.00001",
                max_qty="1000",
                step_size="0.00001",
            )
        ],
    )
    snapshot = _snapshot(
        _venue(
            venue_id=10,
            market=MARKET_SPOT,
            spot_symbols=[metadata],
            wallet=_spot_wallet(
                environment=0,
                assets=[
                    _spot_asset("USDT", free_decimal="1000.00000000"),
                    _spot_asset("BTC", free_decimal="0.00000000"),
                ],
            ),
        )
    )

    routed = build_portfolio_wallet_from_snapshot(snapshot, {("binance", "spot")})
    spot = routed.get("binance", "spot").spot
    assert set(spot.assets) == {"USDT", "BTC"}
    assert spot.assets["USDT"].free == Decimal("1000.00000000")
    spot.on_market_data("BTCUSDT", Decimal("50000"))
    assert "BTCUSDT" not in spot.assets
    assert spot.assets["BTC"].price == Decimal("50000")

    attach_spot_risk_snapshots(
        routed,
        [
            portfolio_service_pb2.SpotRiskFactSnapshot(
                snapshot_id="risk-1",
                venue_id=10,
                exchange=EXCHANGE_BINANCE,
                environment=0,
                market=MARKET_SPOT,
                symbol="BTCUSDT",
                metadata=metadata,
                reference_price_decimal="",
            )
        ],
    )
    assert spot.review_order(
        symbol="BTCUSDT",
        side="BUY",
        order_type="MARKET",
        qty_decimal="0.00020",
        price_decimal=None,
    ) == "risk-1"


def test_demo_spot_risk_never_replaces_binance_reference_with_kline_price():
    metadata = portfolio_service_pb2.SpotSymbolMetadata(
        symbol="BTCUSDT",
        status="TRADING",
        base_asset="BTC",
        quote_asset="USDT",
        base_asset_precision=8,
        quote_asset_precision=8,
        spot_trading_allowed=True,
        order_types=["MARKET"],
        filters=[
            portfolio_service_pb2.SpotSymbolFilter(
                filter_type="LOT_SIZE",
                min_qty="0.00001",
                max_qty="1000",
                step_size="0.00001",
            )
        ],
    )
    routed = build_portfolio_wallet_from_snapshot(
        _snapshot(
            _venue(
                venue_id=10,
                market=MARKET_SPOT,
                spot_symbols=[metadata],
                wallet=_spot_wallet(
                    assets=[
                        _spot_asset("USDT", free_decimal="1000"),
                        _spot_asset("BTC", free_decimal="0"),
                    ],
                ),
            )
        ),
        {("binance", "spot")},
    )
    spot = routed.get("binance", "spot").spot
    attach_spot_risk_snapshots(
        routed,
        [
            portfolio_service_pb2.SpotRiskFactSnapshot(
                snapshot_id="risk-demo",
                venue_id=10,
                exchange=EXCHANGE_BINANCE,
                environment=1,
                market=MARKET_SPOT,
                symbol="BTCUSDT",
                metadata=metadata,
            )
        ],
    )
    spot.on_market_data("BTCUSDT", Decimal("50000"))

    with pytest.raises(SpotFilterViolation, match="SPOT_REFERENCE_PRICE_UNAVAILABLE"):
        spot.review_order(
            symbol="BTCUSDT",
            side="BUY",
            order_type="MARKET",
            qty_decimal="0.00020",
            price_decimal=None,
        )


def test_demo_spot_risk_counts_exchange_open_orders_before_new_order():
    open_order_type = getattr(portfolio_service_pb2, "SpotOpenOrderFact", None)
    assert open_order_type is not None
    metadata = portfolio_service_pb2.SpotSymbolMetadata(
        symbol="BTCUSDT",
        status="TRADING",
        base_asset="BTC",
        quote_asset="USDT",
        base_asset_precision=8,
        quote_asset_precision=8,
        spot_trading_allowed=True,
        order_types=["MARKET"],
        filters=[
            portfolio_service_pb2.SpotSymbolFilter(
                filter_type="LOT_SIZE",
                min_qty="0.00001",
                max_qty="1000",
                step_size="0.00001",
            ),
            portfolio_service_pb2.SpotSymbolFilter(
                filter_type="MAX_NUM_ORDERS",
                max_num_orders=1,
            ),
        ],
    )
    routed = build_portfolio_wallet_from_snapshot(
        _snapshot(
            _venue(
                venue_id=10,
                market=MARKET_SPOT,
                spot_symbols=[metadata],
                wallet=_spot_wallet(
                    assets=[
                        _spot_asset("USDT", free_decimal="1000"),
                        _spot_asset("BTC", free_decimal="0"),
                    ],
                ),
            )
        ),
        {("binance", "spot")},
    )
    attach_spot_risk_snapshots(
        routed,
        [
            portfolio_service_pb2.SpotRiskFactSnapshot(
                snapshot_id="risk-demo-open-orders",
                venue_id=10,
                exchange=EXCHANGE_BINANCE,
                environment=1,
                market=MARKET_SPOT,
                symbol="BTCUSDT",
                metadata=metadata,
                reference_price_decimal="50000",
                open_orders=[
                    open_order_type(
                        symbol="BTCUSDT",
                        side="BUY",
                        orig_qty_decimal="0.01",
                        executed_qty_decimal="0",
                    )
                ],
            )
        ],
    )

    with pytest.raises(SpotFilterViolation, match="SPOT_MAX_NUM_ORDERS"):
        routed.get("binance", "spot").spot.review_order(
            symbol="BTCUSDT",
            side="BUY",
            order_type="MARKET",
            qty_decimal="0.00020",
            price_decimal=None,
        )


def test_unrequested_venue_snapshot_is_ignored_before_wallet_validation():
    snapshot = _snapshot(
        _venue(
            venue_id=10,
            market=MARKET_SPOT,
            total_value=0.0,
            wallet_balance=0.0,
            available_balance=0.0,
            wallet=portfolio_service_pb2.PortfolioWalletState(environment=1),
        ),
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
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
        allowed_routes={("binance", "perpetual_futures")},
    )

    assert wallet.get("binance", "perpetual_futures").get_wallet_balance() == pytest.approx(1000.0)
    with pytest.raises(ValueError, match="wallet route binance/spot is not declared"):
        wallet.get("binance", "spot")


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
    full_wallet = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=1000.0,
            available_balance=950.0,
            margin_balance=1015.0,
            total_cross_wallet_balance=1000.0,
            total_cross_un_pnl=15.0,
            risk_metadata=[
                portfolio_service_pb2.FuturesRiskMetadata(
                    symbol="ETHUSDT",
                    configured_leverage=20.0,
                    configured_margin_mode="cross",
                )
            ],
            positions=[
                portfolio_service_pb2.FuturesPosition(
                    venue_id=11,
                    symbol="ETHUSDT",
                    position_side=BOTH,
                    qty=0.3,
                    signed_qty_decimal="0.3",
                    entry_price=3000.0,
                    mark_price=3050.0,
                    unrealized_pnl=15.0,
                    leverage=20.0,
                    margin_mode="cross",
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


def test_futures_zero_wallet_with_route_metadata_is_accepted():
    snapshot = _snapshot(
        _venue(
            venue_id=11,
            market=MARKET_PERPETUAL_FUTURES,
            wallet=_futures_wallet(
                wallet_balance=0.0,
                available_balance=0.0,
                margin_balance=0.0,
                positions=[],
            ),
            total_value=0.0,
            wallet_balance=0.0,
            available_balance=0.0,
        )
    )

    wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "perpetual_futures")},
    ).get("binance", "perpetual_futures")

    assert wallet.get_wallet_balance() == 0.0
    assert wallet.get_available_balance() == 0.0


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


def test_present_empty_spot_wallet_hydrates_as_empty_canonical_state():
    routed = build_portfolio_wallet_from_snapshot(
        _snapshot(
            _venue(
                venue_id=10,
                market=MARKET_SPOT,
                wallet=_spot_wallet(assets=[]),
                total_value=0.0,
                wallet_balance=0.0,
                available_balance=0.0,
            )
        ),
        allowed_routes={("binance", "spot")},
    )

    assert routed.get("binance", "spot").spot.assets == {}


def test_present_empty_spot_wallet_replaces_existing_assets_during_sync():
    routed = build_portfolio_wallet_from_snapshot(
        _snapshot(
            _venue(
                venue_id=10,
                market=MARKET_SPOT,
                wallet=_spot_wallet(
                    assets=[_spot_asset("USDT", free_decimal="100")]
                ),
            )
        ),
        allowed_routes={("binance", "spot")},
    )

    empty_venue = _venue(
        venue_id=10,
        market=MARKET_SPOT,
        wallet=_spot_wallet(assets=[]),
        total_value=0.0,
        wallet_balance=0.0,
        available_balance=0.0,
    )
    empty_venue.environment = 1

    route = apply_venue_wallet_snapshot(
        routed,
        empty_venue,
        expected_environment=1,
    )

    assert route == ("binance", "spot", 10)
    assert routed.get("binance", "spot").spot.assets == {}


def test_stale_futures_snapshot_cannot_lower_income_cursor():
    current = _futures_wallet(wallet_balance=1000.0, available_balance=1000.0, margin_balance=1000.0)
    current.futures.last_applied_income_entry_id = 8
    routed = build_portfolio_wallet_from_snapshot(
        _snapshot(_venue(venue_id=11, market=MARKET_PERPETUAL_FUTURES, wallet=current)),
        allowed_routes={("binance", "perpetual_futures")},
    )
    stale = _venue(
        venue_id=11,
        market=MARKET_PERPETUAL_FUTURES,
        wallet=_futures_wallet(wallet_balance=999.0, available_balance=999.0, margin_balance=999.0),
    )
    stale.environment = 1
    stale.wallet.futures.last_applied_income_entry_id = 6

    apply_venue_wallet_snapshot(routed, stale, expected_environment=1)

    assert routed.get("binance", "perpetual_futures").futures.last_applied_income_entry_id == 8


def test_authoritative_futures_snapshot_preserves_local_terminal_order_checkpoint():
    current = _futures_wallet(wallet_balance=1000.0, available_balance=1000.0, margin_balance=1000.0)
    routed = build_portfolio_wallet_from_snapshot(
        _snapshot(_venue(venue_id=11, market=MARKET_PERPETUAL_FUTURES, wallet=current)),
        allowed_routes={("binance", "perpetual_futures")},
    )
    terminal = SimpleNamespace(
        order_id="terminal-across-authoritative-snapshot",
        status="FILLED",
        side="BUY",
        position_side="BOTH",
        qty=1.0,
        fill_price=100.0,
        fee=0.04,
        orig_qty=1.0,
        executed_qty=1.0,
        remaining_qty=0.0,
        price=100.0,
        reduce_only=False,
    )
    routed.get("binance", "perpetual_futures").on_order("BTCUSDT", "futures", terminal)
    replacement = _venue(
        venue_id=11,
        market=MARKET_PERPETUAL_FUTURES,
        wallet=_futures_wallet(wallet_balance=999.96, available_balance=999.96, margin_balance=999.96),
    )
    replacement.environment = 1

    apply_venue_wallet_snapshot(routed, replacement, expected_environment=1)
    refreshed = routed.get("binance", "perpetual_futures")
    refreshed.on_order("BTCUSDT", "futures", terminal)

    assert refreshed.futures.wallet_balance == pytest.approx(999.96)
    assert ("BTCUSDT", 0) not in refreshed.futures.positions
    assert [
        item.order_id
        for item in refreshed.to_canonical_state().futures.order_checkpoints
    ] == ["terminal-across-authoritative-snapshot"]


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


def test_spot_full_wallet_without_spot_message_fails_closed():
    snapshot = _snapshot(
        _venue(
            venue_id=10,
            market=MARKET_SPOT,
            balances=[
                _balance("USDT", wallet_balance=100.0, available_balance=90.0, locked=10.0),
                _balance("BTC", wallet_balance=0.5, available_balance=0.4, locked=0.1),
            ],
            wallet=portfolio_service_pb2.PortfolioWalletState(environment=1),
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
            wallet=portfolio_service_pb2.PortfolioWalletState(environment=1),
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
    allowed_routes = {
        (EXCHANGE_OKX, MARKET_SPOT): {("okx", "spot")},
        (EXCHANGE_BINANCE, MARKET_DELIVERY_FUTURES): {("binance", "delivery_futures")},
    }.get((exchange, market), {("binance", "spot")})

    with pytest.raises(ValueError, match=message):
        build_portfolio_wallet_from_snapshot(snapshot, allowed_routes=allowed_routes)


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
        _venue(
            venue_id=10,
            market=MARKET_SPOT,
            wallet=_spot_wallet(assets=[_spot_asset("USDT", free_decimal="1")]),
        ),
        _venue(
            venue_id=11,
            market=MARKET_SPOT,
            wallet=_spot_wallet(assets=[_spot_asset("USDT", free_decimal="2")]),
        ),
    )

    wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "spot")},
    )

    with pytest.raises(ValueError, match="ambiguous wallet route.*10.*11"):
        wallet.get("binance", "spot")
