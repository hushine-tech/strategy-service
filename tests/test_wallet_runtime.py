from __future__ import annotations

import math
from decimal import Decimal
from types import SimpleNamespace

import pytest

from strategy_service.portfolio_client import _serialize_future_wallet
from strategy_service.gen import portfolio_service_pb2
from strategy_service.inputs import parse_order_targets, resolve_order_target_leverages
from strategy_service.position_side import (
    BOTH,
    LONG,
    SHORT,
    position_direction_key,
    position_side_label,
)
from strategy_service.types import OrderResponse
from strategy_service.wallet import BinanceWalletRuntime, LedgerEvent
from strategy_service.wallet.canonical import (
    CanonicalFuturesPositionState,
    CanonicalFuturesState,
    CanonicalPortfolioState,
    SpotSymbolMetadata,
)
from strategy_service.wallet_adapter import proto_to_portfolio_spec
from strategy_service.wallet_factory import (
    RUNTIME_REGISTRY,
    _populate_runtime_registry,
    build_wallet_from_portfolio,
    install_simulated_target_leverages,
    resolve_target,
)
from tests.helpers.wallet_fixtures import make_testnet_wallet


def _break_even_from_carry(entry_price: float, position_qty: float, carry_cost: float) -> float:
    direction = 1.0 if position_qty > 0 else -1.0
    return float(entry_price) + direction * float(carry_cost) / abs(float(position_qty))


def _wallet_proto(*, environment: int) -> portfolio_service_pb2.PortfolioWalletState:
    return portfolio_service_pb2.PortfolioWalletState(
        environment=environment,
        total_value=1_010.0,
        spot_estimated_value=9.0,
        futures_position_equity=1_001.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=1_000.0,
            available_balance=998.9,
            total_unrealized_pnl=1.0,
            unrealized_pnl=1.0,
            total_margin_balance=1_001.0,
            margin_balance=1_001.0,
            total_position_initial_margin=1.1,
            total_open_order_initial_margin=0.0,
            total_cross_wallet_balance=1_000.0,
            total_cross_un_pnl=1.0,
            positions=[
                portfolio_service_pb2.FuturesPosition(
                    symbol="BTCUSDT",
                    initial_balance=100.0,
                    leverage=10.0,
                    fee_rate=0.0004,
                    mark_price=110.0,
                    qty=0.1,
                    entry_price=100.0,
                    unrealized_pnl=1.0,
                    position_side=BOTH,
                    margin_mode="cross",
                    notional=11.0,
                    initial_margin=1.1,
                    position_initial_margin=1.1,
                    open_order_initial_margin=0.0,
                    maint_margin=0.05,
                    liquidation_price=80.0,
                    break_even_price=100.04,
                ),
            ],
        ),
        spot=portfolio_service_pb2.SpotWallet(assets=[
            portfolio_service_pb2.SpotAsset(
                asset="USDT",
                free_decimal="9",
                locked_decimal="0",
            ),
        ]),
    )


def test_one_way_negative_position_serializes_as_canonical_both_leg() -> None:
    """One-way exposure sign must never change its shared position-side enum."""
    wallet = make_testnet_wallet(margin_mode="cross", position_mode="one_way")
    wallet.futures.positions[("BTCUSDT", 0)] = wallet.futures._ensure_position(
        "BTCUSDT", 0, BOTH
    )
    wallet.futures.positions[("BTCUSDT", 0)].position_qty = -0.25

    serialized = _serialize_future_wallet(wallet.futures)

    assert serialized.positions[0].position_side == BOTH
    assert position_side_label(serialized.positions[0].position_side) == "BOTH"


def test_shared_position_side_constants_match_core_enum_descriptor() -> None:
    enum = portfolio_service_pb2.FuturesPositionSide

    assert enum.DESCRIPTOR.full_name == "portfolio.v1.FuturesPositionSide"
    assert (BOTH, LONG, SHORT) == (
        enum.Value("FUTURES_POSITION_SIDE_BOTH"),
        enum.Value("FUTURES_POSITION_SIDE_LONG"),
        enum.Value("FUTURES_POSITION_SIDE_SHORT"),
    )


def test_position_key_rejects_non_generated_enum_values() -> None:
    with pytest.raises(ValueError, match="invalid FuturesPositionSide"):
        position_direction_key(position_mode="hedge", position_side=True)


def test_direct_canonical_hedge_both_leg_is_rejected_before_runtime_installation() -> None:
    portfolio = CanonicalPortfolioState(
        environment=0,
        futures=CanonicalFuturesState(
            position_mode="hedge",
            positions=[
                CanonicalFuturesPositionState(
                    symbol="BTCUSDT",
                    position_side=BOTH,
                    direction_key=0,
                    margin_mode="cross",
                )
            ],
        ),
    )

    with pytest.raises(ValueError, match="hedge FuturesPosition requires LONG or SHORT"):
        build_wallet_from_portfolio(portfolio)


def test_direct_canonical_position_key_must_match_shared_enum() -> None:
    portfolio = CanonicalPortfolioState(
        environment=0,
        futures=CanonicalFuturesState(
            position_mode="hedge",
            positions=[
                CanonicalFuturesPositionState(
                    symbol="BTCUSDT",
                    position_side=LONG,
                    direction_key=0,
                    margin_mode="cross",
                )
            ],
        ),
    )

    with pytest.raises(ValueError, match="direction_key"):
        build_wallet_from_portfolio(portfolio)


def test_direct_canonical_invalid_position_mode_is_rejected_without_positions() -> None:
    portfolio = CanonicalPortfolioState(
        environment=0,
        futures=CanonicalFuturesState(position_mode="unknown"),
    )

    with pytest.raises(ValueError, match="position_mode"):
        build_wallet_from_portfolio(portfolio)


def test_wallet_adapter_rejects_any_typed_boolean_position_side() -> None:
    position = SimpleNamespace(
        symbol="BTCUSDT",
        position_side=True,
        qty=0.1,
        margin_mode="cross",
        initial_balance=0.0,
        leverage=10.0,
        fee_rate=0.0004,
        mark_price=50_000.0,
        entry_price=50_000.0,
        unrealized_pnl=0.0,
        notional=5_000.0,
        initial_margin=500.0,
        position_initial_margin=500.0,
        open_order_initial_margin=0.0,
        maint_margin=0.0,
        isolated_wallet=0.0,
        liquidation_price=0.0,
        break_even_price=0.0,
    )
    futures = SimpleNamespace(
        position_mode="hedge",
        positions=[position],
        margin_mode="cross",
        multi_assets_mode=False,
        portfolio_margin=False,
        initial_balance=0.0,
        deposit_sum=0.0,
        withdrawal_sum=0.0,
        wallet_balance=1_000.0,
        available_balance=1_000.0,
        margin_balance=1_000.0,
        unrealized_pnl=0.0,
        total_margin_balance=1_000.0,
        total_unrealized_pnl=0.0,
        total_position_initial_margin=0.0,
        total_open_order_initial_margin=0.0,
        total_maint_margin=0.0,
        total_cross_wallet_balance=1_000.0,
        total_cross_un_pnl=0.0,
        last_applied_income_entry_id=0,
        risk_metadata=[],
    )
    wallet = SimpleNamespace(
        futures=futures,
        spot=None,
        environment=0,
        total_value=1_000.0,
        spot_estimated_value=0.0,
        futures_position_equity=1_000.0,
        metrics_authoritative=False,
        updated_at=None,
    )

    with pytest.raises(ValueError, match="invalid FuturesPositionSide"):
        proto_to_portfolio_spec(wallet)


def _isolated_wallet_proto_with_metadata() -> portfolio_service_pb2.PortfolioWalletState:
    return portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=9.0,
        spot_estimated_value=0.0,
        futures_position_equity=9.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="isolated",
            position_mode="one_way",
            wallet_balance=8.0,
            available_balance=6.9,
            total_unrealized_pnl=1.0,
            unrealized_pnl=1.0,
            total_margin_balance=9.0,
            margin_balance=9.0,
            total_position_initial_margin=1.1,
            total_open_order_initial_margin=0.0,
            total_maint_margin=0.6,
            positions=[
                portfolio_service_pb2.FuturesPosition(
                    symbol="BTCUSDT",
                    initial_balance=5.0,
                    leverage=10.0,
                    fee_rate=0.0004,
                    mark_price=110.0,
                    qty=0.1,
                    entry_price=100.0,
                    unrealized_pnl=1.0,
                    position_side=BOTH,
                    margin_mode="isolated",
                    notional=11.0,
                    initial_margin=1.1,
                    position_initial_margin=1.1,
                    open_order_initial_margin=0.0,
                    maint_margin=0.6,
                    isolated_wallet=5.0,
                    liquidation_price=50.0,
                    break_even_price=100.04,
                ),
            ],
            risk_metadata=[
                portfolio_service_pb2.FuturesRiskMetadata(
                    symbol="BTCUSDT",
                    configured_leverage=10.0,
                    configured_margin_mode="isolated",
                    price_precision=2,
                    quantity_precision=3,
                    tick_size=0.1,
                    step_size=0.001,
                    brackets=[
                        portfolio_service_pb2.FuturesRiskBracket(
                            bracket=1,
                            notional_floor=0.0,
                            notional_cap=12.0,
                            initial_leverage=10.0,
                            maint_margin_ratio=0.1,
                            cumulative=0.5,
                        ),
                        portfolio_service_pb2.FuturesRiskBracket(
                            bracket=2,
                            notional_floor=12.0,
                            notional_cap=1_000_000.0,
                            initial_leverage=5.0,
                            maint_margin_ratio=0.2,
                            cumulative=0.0,
                        ),
                    ],
                ),
            ],
        ),
        spot=portfolio_service_pb2.SpotWallet(),
    )


def test_proto_to_portfolio_spec_requires_canonical_fields():
    state = proto_to_portfolio_spec(_wallet_proto(environment=1))

    assert state.environment == 1
    assert state.total_value == pytest.approx(1010.0)
    assert state.futures_position_equity == pytest.approx(1001.0)
    assert state.futures.margin_balance == pytest.approx(1001.0)
    assert state.futures.unrealized_pnl == pytest.approx(1.0)
    assert len(state.futures.positions) == 1
    pos = state.futures.positions[0]
    assert pos.position_qty == pytest.approx(0.1)
    assert pos.margin_mode == "cross"
    assert pos.direction_key == 0


def test_build_wallet_from_portfolio_mode0_uses_binance_parity_runtime_after_c2a():
    """C2a cutover: environment=0 backtest now routes to BinanceWalletRuntime.
    Position hydration (qty / entry / mark) must still work via the parity
    runtime's canonical ingestion path.
    """
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=0)))

    assert isinstance(wallet, BinanceWalletRuntime)
    # Environment code must reflect the session environment, not the runtime class default.
    assert wallet.environment_code == 0
    # Futures book in parity runtime is keyed by derive_position_key; for
    # one-way positions that's (symbol, 0).
    pos = wallet.futures.positions[("BTCUSDT", 0)]
    assert pos.position_qty == pytest.approx(0.1)
    assert pos.entry_price == pytest.approx(100.0)
    assert pos.mark_price == pytest.approx(110.0)


def test_build_wallet_from_http_backtest_dict_uses_binance_runtime_after_c2a():
    wallet = build_wallet_from_portfolio({
        "futures": {
            "margin_mode": "isolated",
            "position_mode": "one_way",
            "positions": [
                {
                    "symbol": "BTCUSDT",
                    "position_side": "BOTH",
                    "initial_balance": 5_000.0,
                    "leverage": 20.0,
                    "fee_rate": 0.0004,
                    "position_qty": 0.0,
                }
            ],
        },
        "spot": {"assets": []},
    })

    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.environment_code == 0
    assert ("BTCUSDT", 0) in wallet.futures.positions


@pytest.mark.parametrize(
    ("section", "legacy_field", "legacy_value"),
    [
        ("futures_position", "qty", 0.1),
        ("futures_position", "margin_type", "cross"),
        ("spot_wallet", "free", 1.0),
        ("spot_wallet", "locked", 1.0),
        ("spot_asset", "symbol", "BTCUSDT"),
        ("spot_asset", "qty", 1.0),
        ("spot_asset", "free", 1.0),
        ("spot_asset", "locked", 0.0),
        ("spot_asset", "avg_entry_price", 100.0),
        ("spot_asset", "price", 110.0),
        ("futures_position", "unexpected_position_field", True),
        ("futures_wallet", "unexpected_futures_field", True),
        ("spot_wallet", "unexpected_spot_field", True),
        ("spot_asset", "unexpected_asset_field", True),
        ("portfolio", "unexpected_portfolio_field", True),
    ],
)
def test_http_backtest_dict_rejects_removed_wallet_fields(
    section,
    legacy_field,
    legacy_value,
):
    position = {
        "symbol": "BTCUSDT",
        "position_qty": 0.1,
        "margin_mode": "cross",
    }
    spot = {
        "assets": [
            {
                "asset": "BTC",
                "free_decimal": "1",
                "locked_decimal": "0",
            },
        ],
    }
    if section == "futures_position":
        position[legacy_field] = legacy_value
    elif section == "spot_wallet":
        spot[legacy_field] = legacy_value
    elif section == "spot_asset":
        spot["assets"][0][legacy_field] = legacy_value

    portfolio = {
        "futures": {
            "margin_mode": "cross",
            "position_mode": "one_way",
            "positions": [position],
        },
        "spot": spot,
    }
    if section == "futures_wallet":
        portfolio["futures"][legacy_field] = legacy_value
    elif section == "portfolio":
        portfolio[legacy_field] = legacy_value

    with pytest.raises(ValueError, match=legacy_field):
        build_wallet_from_portfolio(portfolio)


def test_http_backtest_futures_positions_require_position_qty():
    with pytest.raises(ValueError, match="position_qty"):
        build_wallet_from_portfolio(
            {
                "futures": {
                    "margin_mode": "cross",
                    "position_mode": "one_way",
                    "positions": [
                        {
                            "symbol": "BTCUSDT",
                            "margin_mode": "cross",
                        }
                    ],
                },
                "spot": {"assets": []},
            }
        )


@pytest.mark.parametrize(
    ("field_name", "value"),
    [
        ("asset", "btc"),
        ("asset", "BTC-USDT"),
        ("free_decimal", 1),
        ("free_decimal", "1e-19"),
        ("free_decimal", "1e39"),
        ("free_decimal", "-1"),
        ("free_decimal", "NaN"),
        ("free_decimal", ""),
        ("free_decimal", "123456789012345678901"),
        ("free_decimal", "0.1234567890123456789"),
        ("locked_decimal", 0),
        ("avg_entry_price_decimal", 0),
        ("price_decimal", 110),
    ],
)
def test_http_backtest_spot_assets_require_canonical_exact_values(field_name, value):
    asset = {
        "asset": "BTC",
        "free_decimal": "1",
        "locked_decimal": "0",
        "avg_entry_price_decimal": "100",
        "price_decimal": "110",
    }
    asset[field_name] = value

    with pytest.raises(ValueError, match=field_name):
        build_wallet_from_portfolio(
            {
                "futures": {"margin_mode": "cross", "position_mode": "one_way"},
                "spot": {"assets": [asset]},
            }
        )


def test_http_backtest_wallet_installs_resolved_target_leverage_metadata():
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
            ]
        ),
        5,
    )

    wallet = build_wallet_from_portfolio(
        {
            "futures": {
                "margin_mode": "cross",
                "position_mode": "one_way",
                "initial_balance": 1000.0,
            },
            "spot": {"assets": []},
        },
        simulated_order_targets=targets,
    )

    assert {
        symbol: (metadata.configured_leverage, metadata.leverage_source)
        for symbol, metadata in wallet.futures.risk_metadata.items()
    } == {
        "BTCUSDT": (5, "strategy_default"),
        "ETHUSDT": (10, "order_target"),
    }


def test_backtest_leverage_overlay_refreshes_position_order_and_portfolio_risk_fields():
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=0,
        total_value=1000.0,
        futures_position_equity=1000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            initial_balance=1000.0,
            wallet_balance=1000.0,
            available_balance=500.0,
            total_margin_balance=1000.0,
            margin_balance=1000.0,
            total_position_initial_margin=500.0,
            total_cross_wallet_balance=1000.0,
            positions=[
                portfolio_service_pb2.FuturesPosition(
                    symbol="BTCUSDT",
                    leverage=20.0,
                    mark_price=100_000.0,
                    qty=0.1,
                    entry_price=100_000.0,
                    position_side=BOTH,
                    margin_mode="cross",
                    notional=10_000.0,
                    initial_margin=500.0,
                    position_initial_margin=500.0,
                )
            ],
            risk_metadata=[
                portfolio_service_pb2.FuturesRiskMetadata(
                    symbol="BTCUSDT",
                    configured_leverage=20.0,
                    configured_margin_mode="cross",
                    price_precision=2,
                    step_size=0.001,
                    brackets=[
                        portfolio_service_pb2.FuturesRiskBracket(
                            bracket=1,
                            notional_cap=1_000_000.0,
                            initial_leverage=20.0,
                            maint_margin_ratio=0.004,
                        )
                    ],
                )
            ],
        ),
        spot=portfolio_service_pb2.SpotWallet(),
    )
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))

    class OpenOrder:
        status = "NEW"
        side = "BUY"
        qty = 0.01
        orig_qty = 0.01
        executed_qty = 0.0
        remaining_qty = 0.01
        price = 100_000.0
        fill_price = 0.0
        order_id = "open-after-overlay"
        position_side = "BOTH"
        reduce_only = False
        fee = 0.0

    wallet.on_order("BTCUSDT", "futures", OpenOrder())
    targets = resolve_order_target_leverages(
        parse_order_targets(
            [
                {
                    "exchange": "binance",
                    "market": "perpetual_futures",
                    "symbol": "BTCUSDT",
                    "leverage": 5,
                }
            ]
        ),
        None,
    )

    install_simulated_target_leverages(wallet, targets)

    futures = wallet.futures
    position = futures.positions[("BTCUSDT", 0)]
    metadata = futures.risk_metadata["BTCUSDT"]
    assert position.leverage == pytest.approx(5.0)
    assert position.position_initial_margin == pytest.approx(2000.0)
    assert position.open_order_initial_margin == pytest.approx(200.0)
    assert position.initial_margin == pytest.approx(2200.0)
    assert futures.total_position_initial_margin == pytest.approx(2000.0)
    assert futures.total_open_order_initial_margin == pytest.approx(200.0)
    assert futures.get_available_balance() == pytest.approx(0.0)
    assert position.maint_margin == pytest.approx(40.0)
    assert futures.total_maint_margin == pytest.approx(40.0)
    assert position.liquidation_price == pytest.approx(
        (10_000.0 - 1000.0) / (0.1 * (1.0 - 0.004))
    )
    assert metadata.configured_margin_mode == "cross"
    assert metadata.price_precision == 2
    assert metadata.step_size == pytest.approx(0.001)
    assert metadata.brackets[0].maint_margin_ratio == pytest.approx(0.004)


@pytest.mark.parametrize("environment_code", [1, 2])
def test_simulated_target_overlay_never_touches_demo_or_live(environment_code):
    class ForbiddenFutures:
        def __getattribute__(self, name):
            raise AssertionError(
                f"environment {environment_code} must not inspect Futures overlay member {name}"
            )

    class Runtime:
        futures = ForbiddenFutures()

        def __init__(self):
            self.environment_code = environment_code

    targets = resolve_order_target_leverages(
        parse_order_targets(
            [
                {
                    "exchange": "binance",
                    "market": "perpetual_futures",
                    "symbol": "BTCUSDT",
                }
            ]
        ),
        5,
    )
    runtime = Runtime()

    assert install_simulated_target_leverages(runtime, targets) is runtime


def test_binance_parity_runtime_preserves_flat_isolated_seed_balances_on_mode0():
    """C2a: what used to be legacy-isolated-seed behavior now runs through
    BinanceWalletRuntime. wallet/margin/available_balance come from the
    canonical ingress; the runtime surfaces them unchanged when no further
    lifecycle events have fired.
    """
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=0,
        total_value=8_000.0,
        spot_estimated_value=0.0,
        futures_position_equity=8_000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="isolated",
            position_mode="one_way",
            wallet_balance=8_000.0,
            available_balance=7_500.0,
            total_unrealized_pnl=0.0,
            unrealized_pnl=0.0,
            total_margin_balance=8_000.0,
            margin_balance=8_000.0,
        ),
        spot=portfolio_service_pb2.SpotWallet(),
    )

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))

    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.environment_code == 0
    assert wallet.get_wallet_balance() == pytest.approx(8_000.0)
    # available_balance from parity's cross/isolated computed path; with no
    # open positions and isolated margin_mode, it collapses to margin_balance.
    # Exact formula: margin_balance - (total_position_IM + total_open_order_IM)
    # = 8000 - (0 + 0) = 8000. Exchange-reported 7500 is no longer authoritative
    # for computed — this is the expected behavior change from legacy.
    assert wallet.get_available_balance() == pytest.approx(8_000.0)


def test_build_wallet_from_portfolio_selects_binance_parity_for_mode2():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=1)))

    assert isinstance(wallet, BinanceWalletRuntime)
    pos = wallet.futures.positions[("BTCUSDT", 0)]
    assert pos.net_qty == pytest.approx(0.1)
    assert pos.avg_entry_price == pytest.approx(100.0)
    assert pos.mark_price == pytest.approx(110.0)
    assert wallet.get_wallet_balance() == pytest.approx(1000.0)
    assert wallet.get_available_balance() == pytest.approx(999.9)
    assert wallet.futures.get_margin_balance() == pytest.approx(1001.0)
    assert wallet.get_total_value() == pytest.approx(1010.0)


def test_binance_parity_wallet_bootstraps_cross_wallet_balance_from_seed():
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=10_000.0,
        spot_estimated_value=0.0,
        futures_position_equity=10_000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            initial_balance=10_000.0,
            wallet_balance=0.0,
            available_balance=0.0,
            total_margin_balance=0.0,
            margin_balance=0.0,
            total_unrealized_pnl=0.0,
            unrealized_pnl=0.0,
        ),
        spot=portfolio_service_pb2.SpotWallet(),
    )

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))

    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.get_wallet_balance() == pytest.approx(10_000.0)
    assert wallet.get_available_balance() == pytest.approx(10_000.0)


def test_binance_parity_wallet_bootstraps_isolated_wallet_balance_from_position_seeds():
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=8_000.0,
        spot_estimated_value=0.0,
        futures_position_equity=8_000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="isolated",
            position_mode="one_way",
            wallet_balance=0.0,
            available_balance=0.0,
            total_margin_balance=0.0,
            margin_balance=0.0,
            total_unrealized_pnl=0.0,
            unrealized_pnl=0.0,
            positions=[
                portfolio_service_pb2.FuturesPosition(
                    symbol="BTCUSDT",
                    initial_balance=5_000.0,
                    leverage=10.0,
                    fee_rate=0.0004,
                    margin_mode="isolated",
                    position_side=BOTH,
                ),
                portfolio_service_pb2.FuturesPosition(
                    symbol="ETHUSDT",
                    initial_balance=3_000.0,
                    leverage=10.0,
                    fee_rate=0.0004,
                    margin_mode="isolated",
                    position_side=BOTH,
                ),
            ],
        ),
        spot=portfolio_service_pb2.SpotWallet(),
    )

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))

    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.get_wallet_balance() == pytest.approx(8_000.0)
    assert wallet.get_available_balance() == pytest.approx(8_000.0)


def test_resolve_target_mode_0_returns_local_backtest():
    assert resolve_target(0) == ("local", "backtest")


def test_resolve_target_mode_1_returns_binance_live_not_registered():
    target = resolve_target(2)
    assert target == ("binance", "live")

    _populate_runtime_registry()
    assert target not in RUNTIME_REGISTRY


def test_resolve_target_demo_environment_returns_binance_demo():
    assert resolve_target(1) == ("binance", "demo")


def test_resolve_target_unsupported_environment_raises():
    with pytest.raises(ValueError, match="unsupported portfolio environment"):
        resolve_target(9)


def test_futures_control_unchanged_build_wallet_backtest_environment_uses_binance_parity_after_c2a():
    """environment=0 backtest is routed to BinanceWalletRuntime."""
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=0)))

    assert isinstance(wallet, BinanceWalletRuntime)
    assert wallet.environment_code == 0


def test_build_wallet_demo_environment_uses_binance_runtime():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=1)))

    assert isinstance(wallet, BinanceWalletRuntime)
    # Alias preserved for one phase (C1), remove in C2b.
    assert isinstance(wallet, BinanceWalletRuntime)


def test_build_wallet_live_environment_fails_closed():
    with pytest.raises(ValueError, match=r"environment=2 is not enabled"):
        build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=2)))


def test_build_wallet_from_portfolio_rejects_unsupported_environment():
    with pytest.raises(ValueError, match="unsupported portfolio environment"):
        build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=9)))


def test_binance_parity_wallet_updates_first_batch_formulas_after_fill():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=1)))
    pos = wallet.futures.positions[("BTCUSDT", 0)]

    class Fill:
        status = "FILLED"
        side = "BUY"
        qty = 0.1
        fill_price = 120.0
        fee = 0.01

    wallet.on_order("BTCUSDT", "futures", Fill())
    wallet.on_market_data("BTCUSDT", "futures", 121.0)

    assert pos.net_qty == pytest.approx(0.2)
    assert pos.avg_entry_price == pytest.approx(110.0)
    assert wallet.get_wallet_balance() == pytest.approx(999.99)
    assert wallet.futures.get_unrealized_pnl() == pytest.approx(2.2)
    assert wallet.futures.get_margin_balance() == pytest.approx(1002.19)
    assert math.isclose(wallet.get_available_balance(), 999.77, rel_tol=1e-9, abs_tol=1e-9)


def test_binance_parity_wallet_uses_risk_metadata_for_maint_margin_and_liquidation():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_isolated_wallet_proto_with_metadata()))
    pos = wallet.futures.positions[("BTCUSDT", 0)]

    assert pos.maint_margin == pytest.approx(0.6)
    assert pos.liquidation_price == pytest.approx(50.0)
    assert wallet.futures.total_maint_margin == pytest.approx(0.6)
    assert len(wallet.futures.risk_metadata) == 1

    wallet.on_market_data("BTCUSDT", "futures", 130.0)

    assert pos.notional == pytest.approx(13.0)
    assert pos.initial_margin == pytest.approx(1.3)
    assert pos.position_initial_margin == pytest.approx(1.3)
    assert pos.maint_margin == pytest.approx(2.6)
    assert pos.liquidation_price == pytest.approx(62.5)
    assert wallet.futures.total_maint_margin == pytest.approx(2.6)


def test_cross_long_liquidation_clamps_to_zero_when_wallet_balance_covers_position():
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=4_996.83849157684,
        spot_estimated_value=0.0,
        futures_position_equity=4_996.83849157684,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=4_996.806361576841,
            available_balance=4_987.06878757684,
            total_unrealized_pnl=0.03213,
            unrealized_pnl=0.03213,
            total_margin_balance=4_996.83849157684,
            margin_balance=4_996.83849157684,
            total_position_initial_margin=9.737574,
            total_cross_wallet_balance=4_996.806361576841,
            total_cross_un_pnl=0.03213,
            positions=[
                portfolio_service_pb2.FuturesPosition(
                    symbol="ETHUSDT",
                    leverage=5.0,
                    fee_rate=0.0004,
                    mark_price=2_318.47,
                    qty=0.021,
                    entry_price=2_316.94,
                    unrealized_pnl=0.03213,
                    position_side=BOTH,
                    margin_mode="cross",
                    notional=48.68787,
                    initial_margin=9.737574,
                    position_initial_margin=9.737574,
                    maint_margin=0.24343935,
                    liquidation_price=0.0,
                    break_even_price=2_316.94,
                ),
            ],
            risk_metadata=[
                portfolio_service_pb2.FuturesRiskMetadata(
                    symbol="ETHUSDT",
                    configured_leverage=5.0,
                    configured_margin_mode="cross",
                    price_precision=2,
                    quantity_precision=3,
                    tick_size=0.01,
                    step_size=0.001,
                    brackets=[
                        portfolio_service_pb2.FuturesRiskBracket(
                            bracket=1,
                            notional_floor=0.0,
                            notional_cap=50_000.0,
                            initial_leverage=5.0,
                            maint_margin_ratio=0.005,
                            cumulative=0.0,
                        ),
                    ],
                ),
            ],
        ),
        spot=portfolio_service_pb2.SpotWallet(),
    )

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    pos = wallet.futures.positions[("ETHUSDT", 0)]

    assert pos.maint_margin == pytest.approx(0.24343935)
    assert pos.liquidation_price == pytest.approx(0.0)


def test_cross_short_liquidation_is_estimated_when_exchange_price_and_brackets_are_missing():
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=1_050.0,
        spot_estimated_value=0.0,
        futures_position_equity=1_050.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=1_000.0,
            available_balance=810.0,
            total_unrealized_pnl=50.0,
            unrealized_pnl=50.0,
            total_margin_balance=1_050.0,
            margin_balance=1_050.0,
            total_position_initial_margin=190.0,
            total_cross_wallet_balance=1_000.0,
            total_cross_un_pnl=50.0,
            positions=[
                portfolio_service_pb2.FuturesPosition(
                    symbol="ETHUSDT",
                    leverage=5.0,
                    fee_rate=0.0004,
                    mark_price=1_900.0,
                    qty=-0.5,
                    entry_price=2_000.0,
                    unrealized_pnl=50.0,
                    position_side=BOTH,
                    margin_mode="cross",
                    notional=-950.0,
                    initial_margin=190.0,
                    position_initial_margin=190.0,
                    maint_margin=0.0,
                    liquidation_price=0.0,
                    break_even_price=2_000.0,
                ),
            ],
        ),
        spot=portfolio_service_pb2.SpotWallet(),
    )

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    pos = wallet.futures.positions[("ETHUSDT", 0)]

    assert pos.position_qty < 0.0
    assert pos.liquidation_price == pytest.approx(4_000.0)


def test_binance_parity_wallet_preserves_exchange_authoritative_open_order_and_oracle_risk_when_metadata_missing():
    wallet_proto = _wallet_proto(environment=1)
    wallet_proto.futures.positions[0].open_order_initial_margin = 0.25
    wallet_proto.futures.total_open_order_initial_margin = 0.25

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    pos = wallet.futures.positions[("BTCUSDT", 0)]

    wallet.on_market_data("BTCUSDT", "futures", 120.0)

    assert pos.open_order_initial_margin == pytest.approx(0.25)
    assert pos.maint_margin == pytest.approx(0.05)
    assert pos.liquidation_price == pytest.approx(80.0)


def test_serialize_future_wallet_preserves_risk_metadata_from_parity_runtime():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_isolated_wallet_proto_with_metadata()))

    proto_fw = _serialize_future_wallet(wallet.futures)

    assert len(proto_fw.risk_metadata) == 1
    assert proto_fw.risk_metadata[0].symbol == "BTCUSDT"
    assert len(proto_fw.risk_metadata[0].brackets) == 2
    assert proto_fw.risk_metadata[0].brackets[0].maint_margin_ratio == pytest.approx(0.1)


def test_funding_income_cursor_replays_only_new_entries_and_serializes():
    wallet = make_testnet_wallet(wallet_balance=100.0)
    wallet.futures.venue_id = 11

    for entry_id, amount in ((7, "-0.1"), (7, "-0.1"), (6, "-0.1"), (8, "0.2")):
        wallet.on_ledger_event(LedgerEvent(
            event_type="funding_fee",
            amount=0.0,
            income_entry_id=entry_id,
            venue_id=11,
            asset="USDT",
            amount_decimal=amount,
            margin_mode="cross",
        ))

    assert wallet.get_wallet_balance() == pytest.approx(100.1)
    assert wallet.futures.last_applied_income_entry_id == 8
    assert _serialize_future_wallet(wallet.futures).last_applied_income_entry_id == 8


def test_funding_income_cursor_survives_canonical_serialize_and_rebuild():
    wallet = make_testnet_wallet(wallet_balance=100.0)
    wallet.futures.venue_id = 11
    wallet.on_ledger_event(LedgerEvent(
        event_type="funding_fee", amount=0.0, income_entry_id=7,
        venue_id=11, asset="USDT", amount_decimal="-0.1", margin_mode="cross",
    ))
    state = _wallet_proto(environment=1)
    state.futures.CopyFrom(_serialize_future_wallet(wallet.futures))

    rebuilt = build_wallet_from_portfolio(proto_to_portfolio_spec(state))

    assert rebuilt.futures.last_applied_income_entry_id == 7


def test_terminal_order_checkpoint_survives_proto_serialize_and_rebuild():
    wallet = make_testnet_wallet(wallet_balance=1000.0)
    terminal = SimpleNamespace(
        order_id="terminal-proto-round-trip",
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
    wallet.on_order("BTCUSDT", "futures", terminal)
    state = _wallet_proto(environment=1)
    state.futures.CopyFrom(_serialize_future_wallet(wallet.futures))

    rebuilt = build_wallet_from_portfolio(proto_to_portfolio_spec(state))
    rebuilt.on_order("BTCUSDT", "futures", terminal)

    assert rebuilt.futures.wallet_balance == pytest.approx(999.96)
    assert rebuilt.futures.positions[("BTCUSDT", 0)].position_qty == pytest.approx(1.0)
    checkpoints = rebuilt.to_canonical_state().futures.order_checkpoints
    assert [(item.order_id, item.executed_qty_decimal, item.terminal) for item in checkpoints] == [
        ("terminal-proto-round-trip", "1.0", True)
    ]


def test_funding_income_isolated_updates_the_specific_leg_wallet():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_isolated_wallet_proto_with_metadata()))
    wallet.futures.venue_id = 11
    pos = wallet.futures.positions[("BTCUSDT", 0)]

    wallet.on_ledger_event(LedgerEvent(
        event_type="funding_fee",
        amount=0.0,
        income_entry_id=7,
        venue_id=11,
        asset="USDT",
        amount_decimal="-0.2",
        symbol="BTCUSDT",
        position_side="BOTH",
        margin_mode="isolated",
    ))

    assert pos.isolated_wallet == pytest.approx(4.8)
    assert wallet.futures.last_applied_income_entry_id == 7


def test_funding_income_rejects_non_usdt_without_mutating_wallet_or_cursor():
    wallet = make_testnet_wallet(wallet_balance=100.0)
    wallet.futures.venue_id = 11

    with pytest.raises(ValueError, match="USDT"):
        wallet.on_ledger_event(LedgerEvent(
            event_type="funding_fee", amount=0.0, income_entry_id=7,
            venue_id=11, asset="BTC", amount_decimal="-0.1", margin_mode="cross",
        ))

    assert wallet.get_wallet_balance() == pytest.approx(100.0)
    assert wallet.futures.last_applied_income_entry_id == 0


def test_funding_income_refresh_error_rolls_back_balance_and_cursor(monkeypatch: pytest.MonkeyPatch):
    wallet = make_testnet_wallet(wallet_balance=100.0)
    wallet.futures.venue_id = 11

    def reject_refresh() -> None:
        raise RuntimeError("refresh failed")

    monkeypatch.setattr(wallet.futures, "_refresh_portfolio_fields", reject_refresh)

    with pytest.raises(RuntimeError, match="refresh failed"):
        wallet.on_ledger_event(LedgerEvent(
            event_type="funding_fee", amount=0.0, income_entry_id=7,
            venue_id=11, asset="USDT", amount_decimal="-0.1", margin_mode="cross",
        ))

    assert wallet.futures.wallet_balance == pytest.approx(100.0)
    assert wallet.futures.last_applied_income_entry_id == 0


def test_binance_wallet_ignores_provider_display_spot_estimate_when_prices_missing():
    """canonical-wallet-display-boundary: the upstream proto's
    ``spot_estimated_value`` is a DISPLAY projection and MUST NOT feed
    runtime-facing state.

    Regression for the removed ``_source_state.spot_estimated_value`` read
    path: previously, when spot assets lacked ``price``, the runtime would
    fall back to the ingress display total. That leaked provider-display
    semantics into the runtime projection. Now the runtime cleanly falls
    back to the canonical cash position (``free + locked``).
    """
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        # Provider display totals supplied on ingress — must be IGNORED.
        total_value=1_055.0,
        spot_estimated_value=55.0,
        futures_position_equity=1_001.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=1_000.0,
            available_balance=998.9,
            total_unrealized_pnl=1.0,
            unrealized_pnl=1.0,
            total_margin_balance=1_001.0,
            margin_balance=1_001.0,
        ),
        spot=portfolio_service_pb2.SpotWallet(
            assets=[
                portfolio_service_pb2.SpotAsset(
                    asset="ETH",
                    free_decimal="1",
                    locked_decimal="0",
                    avg_entry_price_decimal="50",
                    # No `price` set → canonical estimate cannot price this
                    # asset. Runtime must fall back to cash-only, NOT to the
                    # upstream display total.
                ),
            ],
        ),
    )

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    out = wallet.to_canonical_state()

    # Display-ingress value (55.0) was ignored. Unpriced canonical assets do not
    # contribute to the estimate.
    assert out.spot_estimated_value == pytest.approx(0.0)
    # total_value = futures.margin_balance (canonical-recomputed from
    # wallet_balance + local unrealized_pnl = 1000.0 + 0.0 since no positions
    # mean unrealized_pnl evaluates to 0) + spot_estimated_value (0) = 1000.
    assert out.total_value == pytest.approx(1_000.0)


def test_binance_parity_wallet_tracks_open_order_margin_lifecycle_and_total_im():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=1)))
    pos = wallet.futures.positions[("BTCUSDT", 0)]

    class NewOrder:
        order_id = "open-1"
        status = "NEW"
        side = "BUY"
        qty = 0.0
        orig_qty = 0.2
        remaining_qty = 0.2
        price = 120.0

    wallet.on_order("BTCUSDT", "futures", NewOrder())

    assert pos.open_order_initial_margin == pytest.approx(2.2)
    assert pos.position_initial_margin == pytest.approx(1.1)
    assert pos.initial_margin == pytest.approx(3.3)
    assert wallet.futures.total_open_order_initial_margin == pytest.approx(2.2)

    class PartialFill:
        order_id = "open-1"
        status = "PARTIALLY_FILLED"
        side = "BUY"
        qty = 0.05
        orig_qty = 0.2
        remaining_qty = 0.15
        executed_qty = 0.05
        fill_price = 120.0
        price = 120.0
        fee = 0.01

    wallet.on_order("BTCUSDT", "futures", PartialFill())

    assert pos.net_qty == pytest.approx(0.15)
    assert pos.position_initial_margin == pytest.approx(1.65)
    assert pos.open_order_initial_margin == pytest.approx(1.65)
    assert pos.initial_margin == pytest.approx(3.3)
    assert wallet.futures.total_open_order_initial_margin == pytest.approx(1.65)

    class Cancel:
        order_id = "open-1"
        status = "CANCELED"
        side = "BUY"
        orig_qty = 0.2
        executed_qty = 0.05
        remaining_qty = 0.0
        price = 120.0

    wallet.on_order("BTCUSDT", "futures", Cancel())

    assert pos.open_order_initial_margin == pytest.approx(0.0)
    assert pos.initial_margin == pytest.approx(pos.position_initial_margin)
    assert wallet.futures.total_open_order_initial_margin == pytest.approx(0.0)


def test_binance_parity_wallet_rejects_lifecycle_events_without_order_id():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=1)))

    class NewOrder:
        status = "NEW"
        side = "BUY"
        qty = 0.0
        orig_qty = 0.2
        remaining_qty = 0.2
        price = 120.0

    with pytest.raises(ValueError, match="order_id"):
        wallet.on_order("BTCUSDT", "futures", NewOrder())


def test_binance_parity_wallet_reduce_only_order_does_not_consume_opening_margin():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto(environment=1)))
    pos = wallet.futures.positions[("BTCUSDT", 0)]

    class ReduceOnly:
        order_id = "reduce-1"
        status = "NEW"
        side = "SELL"
        qty = 0.0
        orig_qty = 0.05
        remaining_qty = 0.05
        price = 100.0
        reduce_only = True

    wallet.on_order("BTCUSDT", "futures", ReduceOnly())

    assert pos.open_order_initial_margin == pytest.approx(0.0)
    assert wallet.futures.total_open_order_initial_margin == pytest.approx(0.0)


def test_binance_parity_wallet_applies_ledger_events_and_local_isolated_state():
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(_isolated_wallet_proto_with_metadata()))
    pos = wallet.futures.positions[("BTCUSDT", 0)]
    before_wb = wallet.get_wallet_balance()

    class Funding:
        event_type = "funding_fee"
        amount = -0.2
        symbol = "BTCUSDT"
        position_side = "BOTH"
        income_entry_id = 1
        venue_id = 11
        asset = "USDT"
        amount_decimal = "-0.2"
        margin_mode = "isolated"

    wallet.on_ledger_event(Funding())

    assert wallet.get_wallet_balance() == pytest.approx(before_wb)
    assert pos.isolated_wallet == pytest.approx(4.8)
    assert pos.break_even_price == pytest.approx(102.04)
    assert pos.liquidation_price == pytest.approx(52.22)

    wb_after_ledger = wallet.get_wallet_balance()
    wallet.on_market_data("BTCUSDT", "futures", 130.0)
    assert wallet.get_wallet_balance() == pytest.approx(wb_after_ledger)


def test_one_way_short_position_serializes_as_both_not_short():
    wallet = make_testnet_wallet(
        margin_mode="cross",
        position_mode="one_way",
        futures_positions=[{
            "symbol": "ETHUSDT",
            "position_qty": -0.021,
            "entry_price": 2328.08476,
            "mark_price": 2327.5776938,
            "position_side": "BOTH",
            "margin_mode": "cross",
        }],
    )

    positions = wallet.futures.to_canonical().positions
    assert len(positions) == 1
    assert positions[0].symbol == "ETHUSDT"
    assert positions[0].position_qty == pytest.approx(-0.021)
    assert positions[0].position_side == BOTH


def test_serialize_future_wallet_one_way_long_and_short_export_both():
    for qty in (0.021, -0.021):
        wallet = make_testnet_wallet(
            margin_mode="cross",
            position_mode="one_way",
            futures_positions=[{
                "symbol": "ETHUSDT",
                "position_qty": qty,
                "entry_price": 2328.08476,
                "mark_price": 2328.08476,
                "position_side": "BOTH",
                "margin_mode": "cross",
            }],
        )

        proto_fw = _serialize_future_wallet(wallet.futures)

        assert len(proto_fw.positions) == 1
        assert proto_fw.positions[0].qty == pytest.approx(qty)
        assert proto_fw.positions[0].position_side == BOTH


def test_new_position_uses_configured_leverage_for_initial_margin_after_fill():
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=5_000.0,
        spot_estimated_value=0.0,
        futures_position_equity=5_000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=5_000.0,
            available_balance=5_000.0,
            margin_balance=5_000.0,
            total_margin_balance=5_000.0,
            risk_metadata=[
                portfolio_service_pb2.FuturesRiskMetadata(
                    symbol="ETHUSDT",
                    configured_leverage=20.0,
                    configured_margin_mode="cross",
                    price_precision=2,
                    quantity_precision=3,
                    tick_size=0.01,
                    step_size=0.001,
                    brackets=[
                        portfolio_service_pb2.FuturesRiskBracket(
                            bracket=1,
                            notional_floor=0.0,
                            notional_cap=50_000.0,
                            initial_leverage=20.0,
                            maint_margin_ratio=0.004,
                            cumulative=0.0,
                        ),
                    ],
                ),
            ],
        ),
        spot=portfolio_service_pb2.SpotWallet(),
    )
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))

    class OpenShort:
        status = "FILLED"
        side = "SELL"
        position_side = "BOTH"
        qty = 0.021
        fill_price = 2328.08476
        fee = 0.01
        order_id = "eth-open-short-20x"

    wallet.on_order("ETHUSDT", "futures", OpenShort())
    pos = wallet.futures.positions[("ETHUSDT", 0)]
    notional = 0.021 * 2328.08476

    assert pos.leverage == pytest.approx(20.0)
    assert pos.mark_price == pytest.approx(2328.08476)
    assert pos.position_initial_margin == pytest.approx(notional / 20.0)
    assert pos.position_initial_margin != pytest.approx(notional)
    assert pos.maint_margin == pytest.approx(notional * 0.004)


def _mode2_eth_snapshot_without_position_leverage(*, include_risk_metadata: bool) -> portfolio_service_pb2.PortfolioWalletState:
    risk_metadata = []
    if include_risk_metadata:
        risk_metadata = [
            portfolio_service_pb2.FuturesRiskMetadata(
                symbol="ETHUSDT",
                configured_leverage=20.0,
                configured_margin_mode="cross",
            ),
        ]
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=14_996.18,
        spot_estimated_value=9_997.9,
        futures_position_equity=4_998.28,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=4_996.45027746,
            available_balance=4_985.78886717,
            total_unrealized_pnl=1.83484563,
            unrealized_pnl=1.83484563,
            total_margin_balance=4_998.28512309,
            margin_balance=4_998.28512309,
            total_position_initial_margin=12.09891811,
            total_open_order_initial_margin=0.0,
            total_cross_wallet_balance=4_996.45027746,
            total_cross_un_pnl=1.83484563,
            risk_metadata=risk_metadata,
            positions=[
                portfolio_service_pb2.FuturesPosition(
                    symbol="ETHUSDT",
                    leverage=0.0,
                    fee_rate=0.0,
                    mark_price=2304.55582809,
                    qty=-0.105,
                    entry_price=2322.030550747,
                    unrealized_pnl=1.83484587,
                    position_side=BOTH,
                    margin_mode="cross",
                    notional=-241.97836194,
                    initial_margin=12.0989181,
                    position_initial_margin=12.0989181,
                    maint_margin=1.2098918,
                    liquidation_price=49658.97640642,
                    break_even_price=2308.837804304,
                ),
            ],
        ),
        spot=portfolio_service_pb2.SpotWallet(),
    )
    return wallet_proto


def test_mode2_hydration_rejects_exchange_position_without_explicit_leverage_fact():
    wallet_proto = _mode2_eth_snapshot_without_position_leverage(include_risk_metadata=False)

    with pytest.raises(ValueError, match="missing FuturesPosition.leverage"):
        build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))


def test_mode2_hydration_uses_configured_leverage_fact_when_position_leverage_missing():
    wallet_proto = _mode2_eth_snapshot_without_position_leverage(include_risk_metadata=True)

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    pos = wallet.futures.positions[("ETHUSDT", 0)]

    assert pos.leverage == pytest.approx(20.0)
    assert pos.position_initial_margin == pytest.approx(12.0989181)
    assert wallet.futures.total_position_initial_margin == pytest.approx(12.0989181)
    assert wallet.get_available_balance() > 4_980.0
    assert wallet.get_available_balance() < 4_990.0


def test_event_fill_without_prior_mark_uses_fill_price_for_risk_fields():
    wallet = make_testnet_wallet(
        margin_mode="cross",
        position_mode="one_way",
        wallet_balance=5000.0,
        futures_positions=[{
            "symbol": "ETHUSDT",
            "position_qty": 0.0,
            "entry_price": 0.0,
            "mark_price": 0.0,
            "position_side": "BOTH",
            "margin_mode": "cross",
            "leverage": 20.0,
        }],
    )

    class OpenShort:
        status = "FILLED"
        side = "SELL"
        position_side = "BOTH"
        qty = 0.021
        fill_price = 2328.08476
        fee = 0.01
        order_id = "eth-open-short-1"

    wallet.on_order("ETHUSDT", "futures", OpenShort())
    pos = wallet.futures.positions[("ETHUSDT", 0)]

    assert pos.position_qty == pytest.approx(-0.021)
    assert pos.position_side == "BOTH"
    assert pos.mark_price == pytest.approx(2328.08476)
    assert pos.notional == pytest.approx(-0.021 * 2328.08476)
    assert pos.position_initial_margin == pytest.approx(abs(pos.notional) / 20.0)
    assert pos.maint_margin >= 0.0

    proto_fw = _serialize_future_wallet(wallet.futures)
    assert proto_fw.positions[0].notional == pytest.approx(pos.notional)


def test_binance_parity_wallet_applies_each_fill_fee_to_wallet_balance():
    wallet = make_testnet_wallet(
        margin_mode="cross",
        position_mode="one_way",
        wallet_balance=1000.0,
        futures_positions=[{
            "symbol": "ETHUSDT",
            "position_qty": 0.0,
            "entry_price": 0.0,
            "mark_price": 100.0,
            "position_side": "BOTH",
            "margin_mode": "cross",
            "leverage": 10.0,
        }],
    )
    before = wallet.get_wallet_balance()

    class FirstFill:
        order_id = "multi-fill-1"
        status = "PARTIALLY_FILLED"
        side = "BUY"
        qty = 0.1
        orig_qty = 0.3
        executed_qty = 0.1
        remaining_qty = 0.2
        fill_price = 100.0
        price = 100.0
        fee = 0.01

    class SecondFill:
        order_id = "multi-fill-1"
        status = "PARTIALLY_FILLED"
        side = "BUY"
        qty = 0.1
        orig_qty = 0.3
        executed_qty = 0.2
        remaining_qty = 0.1
        fill_price = 101.0
        price = 101.0
        fee = 0.02

    class FinalFill:
        order_id = "multi-fill-1"
        status = "FILLED"
        side = "BUY"
        qty = 0.1
        orig_qty = 0.3
        executed_qty = 0.3
        remaining_qty = 0.0
        fill_price = 102.0
        price = 102.0
        fee = 0.03

    wallet.on_order("ETHUSDT", "futures", FirstFill())
    wallet.on_order("ETHUSDT", "futures", SecondFill())
    wallet.on_order("ETHUSDT", "futures", FinalFill())

    assert wallet.get_wallet_balance() == pytest.approx(before - 0.06)


def test_binance_break_even_open_fee_moves_with_position_direction():
    long_wallet = make_testnet_wallet(wallet_balance=1000.0)

    class OpenLong:
        order_id = "open-long-fee"
        status = "FILLED"
        side = "BUY"
        position_side = "BOTH"
        qty = 2.0
        fill_price = 100.0
        fee = 4.0

    long_wallet.on_order("ETHUSDT", "futures", OpenLong())
    long_pos = long_wallet.futures.positions[("ETHUSDT", 0)]

    assert long_pos.carry_cost == pytest.approx(4.0)
    assert long_pos.break_even_price == pytest.approx(102.0)

    short_wallet = make_testnet_wallet(wallet_balance=1000.0)

    class OpenShort:
        order_id = "open-short-fee"
        status = "FILLED"
        side = "SELL"
        position_side = "BOTH"
        qty = 2.0
        fill_price = 100.0
        fee = 4.0

    short_wallet.on_order("ETHUSDT", "futures", OpenShort())
    short_pos = short_wallet.futures.positions[("ETHUSDT", 0)]

    assert short_pos.carry_cost == pytest.approx(4.0)
    assert short_pos.break_even_price == pytest.approx(98.0)


def test_binance_break_even_partial_close_loss_matches_testnet_sample_carry_delta():
    entry = 2278.080523761
    qty = 0.105
    previous_carry = 6.315676037264979
    wallet = make_testnet_wallet(
        margin_mode="cross",
        position_mode="one_way",
        wallet_balance=5000.0,
        futures_positions=[{
            "symbol": "ETHUSDT",
            "position_qty": qty,
            "entry_price": entry,
            "mark_price": entry,
            "position_side": "BOTH",
            "margin_mode": "cross",
            "leverage": 5.0,
            "break_even_price": _break_even_from_carry(entry, qty, previous_carry),
        }],
    )
    pos = wallet.futures.positions[("ETHUSDT", 0)]
    before_wb = wallet.get_wallet_balance()

    class PartialCloseLoss:
        order_id = "sample-loss-close"
        status = "FILLED"
        side = "SELL"
        position_side = "BOTH"
        qty = 0.021
        fill_price = 2274.43
        fee = 0.01910521

    wallet.on_order("ETHUSDT", "futures", PartialCloseLoss())

    realized = PartialCloseLoss.qty * (PartialCloseLoss.fill_price - entry)
    expected_carry = previous_carry - realized + PartialCloseLoss.fee
    remaining_qty = qty - PartialCloseLoss.qty

    assert realized < 0.0
    assert pos.position_qty == pytest.approx(remaining_qty)
    assert pos.carry_cost == pytest.approx(expected_carry)
    assert pos.break_even_price == pytest.approx(_break_even_from_carry(entry, remaining_qty, expected_carry))
    assert wallet.get_wallet_balance() == pytest.approx(before_wb + realized - PartialCloseLoss.fee)


def test_binance_break_even_partial_close_profit_improves_remaining_position():
    entry = 100.0
    qty = 1.0
    previous_carry = 1.0
    wallet = make_testnet_wallet(
        margin_mode="cross",
        position_mode="one_way",
        wallet_balance=1000.0,
        futures_positions=[{
            "symbol": "BTCUSDT",
            "position_qty": qty,
            "entry_price": entry,
            "mark_price": entry,
            "position_side": "BOTH",
            "margin_mode": "cross",
            "break_even_price": _break_even_from_carry(entry, qty, previous_carry),
        }],
    )
    pos = wallet.futures.positions[("BTCUSDT", 0)]

    class PartialCloseProfit:
        order_id = "profit-close"
        status = "FILLED"
        side = "SELL"
        position_side = "BOTH"
        qty = 0.25
        fill_price = 110.0
        fee = 0.1

    wallet.on_order("BTCUSDT", "futures", PartialCloseProfit())

    realized = PartialCloseProfit.qty * (PartialCloseProfit.fill_price - entry)
    expected_carry = previous_carry - realized + PartialCloseProfit.fee
    remaining_qty = qty - PartialCloseProfit.qty

    assert realized > 0.0
    assert pos.position_qty == pytest.approx(remaining_qty)
    assert pos.carry_cost == pytest.approx(expected_carry)
    assert pos.break_even_price == pytest.approx(_break_even_from_carry(entry, remaining_qty, expected_carry))
    assert pos.break_even_price < entry


def test_binance_break_even_full_close_resets_lifecycle():
    entry = 100.0
    wallet = make_testnet_wallet(
        margin_mode="cross",
        position_mode="one_way",
        wallet_balance=1000.0,
        futures_positions=[{
            "symbol": "BTCUSDT",
            "position_qty": 1.0,
            "entry_price": entry,
            "mark_price": entry,
            "position_side": "BOTH",
            "margin_mode": "cross",
            "break_even_price": 101.0,
        }],
    )
    pos = wallet.futures.positions[("BTCUSDT", 0)]

    class FullClose:
        order_id = "full-close"
        status = "FILLED"
        side = "SELL"
        position_side = "BOTH"
        qty = 1.0
        fill_price = 90.0
        fee = 0.1

    wallet.on_order("BTCUSDT", "futures", FullClose())

    assert pos.position_qty == pytest.approx(0.0)
    assert pos.entry_price == pytest.approx(0.0)
    assert pos.carry_cost == pytest.approx(0.0)
    assert pos.break_even_price == pytest.approx(0.0)


def test_binance_break_even_flip_starts_new_lifecycle_with_open_segment_fee_only():
    entry = 100.0
    wallet = make_testnet_wallet(
        margin_mode="cross",
        position_mode="one_way",
        wallet_balance=1000.0,
        futures_positions=[{
            "symbol": "BTCUSDT",
            "position_qty": 1.0,
            "entry_price": entry,
            "mark_price": entry,
            "position_side": "BOTH",
            "margin_mode": "cross",
            "break_even_price": 101.0,
        }],
    )
    pos = wallet.futures.positions[("BTCUSDT", 0)]
    before_wb = wallet.get_wallet_balance()

    class FlipShort:
        order_id = "flip-short"
        status = "FILLED"
        side = "SELL"
        position_side = "BOTH"
        qty = 1.5
        fill_price = 90.0
        fee = 0.3

    wallet.on_order("BTCUSDT", "futures", FlipShort())

    realized = 1.0 * (FlipShort.fill_price - entry)
    open_qty = 0.5
    expected_open_fee = FlipShort.fee * (open_qty / FlipShort.qty)

    assert pos.position_qty == pytest.approx(-open_qty)
    assert pos.entry_price == pytest.approx(FlipShort.fill_price)
    assert pos.carry_cost == pytest.approx(expected_open_fee)
    assert pos.break_even_price == pytest.approx(
        _break_even_from_carry(FlipShort.fill_price, -open_qty, expected_open_fee)
    )
    assert wallet.get_wallet_balance() == pytest.approx(before_wb + realized - FlipShort.fee)


def test_binance_break_even_cross_funding_without_position_attribution_changes_wallet_only():
    wallet = make_testnet_wallet(
        margin_mode="cross",
        position_mode="one_way",
        wallet_balance=1000.0,
        futures_positions=[{
            "symbol": "BTCUSDT",
            "position_qty": 0.1,
            "entry_price": 100.0,
            "mark_price": 100.0,
            "position_side": "BOTH",
            "margin_mode": "cross",
            "break_even_price": 100.04,
        }],
    )
    pos = wallet.futures.positions[("BTCUSDT", 0)]
    before_wb = wallet.get_wallet_balance()
    before_carry = pos.carry_cost
    before_break_even = pos.break_even_price

    class CrossFunding:
        event_type = "funding_fee"
        amount = -0.2
        symbol = "BTCUSDT"
        income_entry_id = 1
        venue_id = 11
        asset = "USDT"
        amount_decimal = "-0.2"
        margin_mode = "cross"
        # no position_side: mirrors cross PORTFOLIO_UPDATE funding behavior

    wallet.on_ledger_event(CrossFunding())

    assert wallet.get_wallet_balance() == pytest.approx(before_wb - 0.2)
    assert pos.carry_cost == pytest.approx(before_carry)
    assert pos.break_even_price == pytest.approx(before_break_even)


def test_hedge_mode_full_lifecycle_long_and_short_sides_independent():
    """End-to-end hedge coverage: open LONG + SHORT in parallel, push market
    data, verify the two sides track independently through fills, mark
    updates and a partial close. Exercises the branches guarded by
    ``_position_key_from_order`` (hedge routing requires explicit side)
    across the ``on_order`` path, not just the ledger path covered by the
    B1 regression tests.
    """
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=10_000.0,
        spot_estimated_value=0.0,
        futures_position_equity=10_000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="hedge",
            wallet_balance=10_000.0,
            available_balance=10_000.0,
            total_margin_balance=10_000.0,
            margin_balance=10_000.0,
        ),
    )
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    assert wallet.futures.position_mode == "hedge"
    assert wallet.futures.positions == {}

    # ── 1. Open LONG via a BUY on LONG side ─────────────────────────────
    class OpenLong:
        status = "FILLED"
        side = "BUY"
        position_side = "LONG"
        qty = 0.1
        fill_price = 100.0
        fee = 0.01
        order_id = "hedge-long-open-1"

    wallet.on_order("BTCUSDT", "futures", OpenLong())
    long_pos = wallet.futures.positions[("BTCUSDT", 1)]
    assert long_pos.position_qty == pytest.approx(0.1)
    assert long_pos.entry_price == pytest.approx(100.0)

    # ── 2. Open SHORT via a SELL on SHORT side, same symbol ─────────────
    class OpenShort:
        status = "FILLED"
        side = "SELL"
        position_side = "SHORT"
        qty = 0.05
        fill_price = 105.0
        fee = 0.01
        order_id = "hedge-short-open-1"

    wallet.on_order("BTCUSDT", "futures", OpenShort())
    short_pos = wallet.futures.positions[("BTCUSDT", -1)]
    assert short_pos.position_qty == pytest.approx(-0.05)
    assert short_pos.entry_price == pytest.approx(105.0)

    # LONG side untouched by the SHORT fill.
    assert long_pos.position_qty == pytest.approx(0.1)
    assert long_pos.entry_price == pytest.approx(100.0)

    # ── 3. Mark tick — both sides refresh independently ─────────────────
    wallet.on_market_data("BTCUSDT", "futures", 110.0)
    # LONG @100 → @110, qty 0.1 → UPnL = +1.0
    assert long_pos.get_unrealized_pnl() == pytest.approx(1.0)
    # SHORT @105 → @110, net_qty -0.05 → UPnL = -0.25
    assert short_pos.get_unrealized_pnl() == pytest.approx(-0.25)

    # ── 4. Partial close on LONG via a SELL on LONG side ────────────────
    class PartialCloseLong:
        status = "FILLED"
        side = "SELL"
        position_side = "LONG"
        qty = 0.04  # reduce LONG 0.1 → 0.06
        fill_price = 110.0
        fee = 0.01
        order_id = "hedge-long-close-1"

    wallet.on_order("BTCUSDT", "futures", PartialCloseLong())
    assert long_pos.position_qty == pytest.approx(0.06)
    # SHORT still at -0.05, unchanged qty/entry
    assert short_pos.position_qty == pytest.approx(-0.05)
    assert short_pos.entry_price == pytest.approx(105.0)

    # ── 5. Full close on LONG leaves SHORT alive ────────────────────────
    class FullCloseLong:
        status = "FILLED"
        side = "SELL"
        position_side = "LONG"
        qty = 0.06
        fill_price = 110.0
        fee = 0.01
        order_id = "hedge-long-close-2"

    wallet.on_order("BTCUSDT", "futures", FullCloseLong())
    # LONG key may still be in the dict with qty=0 (allowed by C3 post-fix);
    # only requirement is that it reports as flat.
    if ("BTCUSDT", 1) in wallet.futures.positions:
        assert wallet.futures.positions[("BTCUSDT", 1)].position_qty == pytest.approx(0.0)
    # SHORT still alive and unchanged.
    assert ("BTCUSDT", -1) in wallet.futures.positions
    assert wallet.futures.positions[("BTCUSDT", -1)].position_qty == pytest.approx(-0.05)


def test_hedge_mode_ledger_event_without_position_side_does_not_crash():
    """Regression for Phase C hedge-mode crash: on_ledger_event used to raise
    ValueError("hedge-mode parity orders require explicit position_side") when
    the ledger carried a symbol but no position_side — legitimate for
    portfolio-level events (funding_fee without direction, transfers, etc.).

    Fix applies only the wallet-level delta and skips per-position work when
    position_side is absent in hedge mode.
    """
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=10_000.0,
        spot_estimated_value=0.0,
        futures_position_equity=10_000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="hedge",
            wallet_balance=10_000.0,
            available_balance=10_000.0,
            total_unrealized_pnl=0.0,
            unrealized_pnl=0.0,
            total_margin_balance=10_000.0,
            margin_balance=10_000.0,
        ),
    )
    # Hedge-mode position with LONG side so there's a position to potentially
    # attribute; the fix should still skip per-position work when the ledger
    # itself doesn't specify a side.
    long_pos = wallet_proto.futures.positions.add()
    long_pos.symbol = "BTCUSDT"
    long_pos.position_side = LONG
    long_pos.qty = 0.1
    long_pos.entry_price = 45_000.0
    long_pos.mark_price = 45_000.0
    long_pos.leverage = 20.0
    long_pos.margin_mode = "cross"

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    before_wb = wallet.get_wallet_balance()

    class PortfolioLevelFunding:
        event_type = "funding_fee"
        amount = -0.25
        symbol = "BTCUSDT"
        income_entry_id = 1
        venue_id = 11
        asset = "USDT"
        amount_decimal = "-0.25"
        margin_mode = "cross"
        # intentionally no position_side — this is the crash case pre-fix

    # Must NOT raise.
    wallet.on_ledger_event(PortfolioLevelFunding())

    # Wallet-level delta still applied.
    assert wallet.get_wallet_balance() == pytest.approx(before_wb - 0.25)


def test_hedge_mode_ledger_event_with_explicit_position_side_still_routes():
    """Counterpart to the no-side test: when the hedge ledger event does
    carry an explicit LONG/SHORT, the per-position update still fires as
    before — ensures the fix didn't regress the happy path."""
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=10_000.0,
        spot_estimated_value=0.0,
        futures_position_equity=10_000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="isolated",
            position_mode="hedge",
            wallet_balance=10_000.0,
            available_balance=10_000.0,
            total_margin_balance=10_000.0,
            margin_balance=10_000.0,
        ),
    )
    short_pos = wallet_proto.futures.positions.add()
    short_pos.symbol = "BTCUSDT"
    short_pos.position_side = SHORT
    short_pos.qty = -0.1
    short_pos.entry_price = 45_000.0
    short_pos.mark_price = 45_000.0
    short_pos.leverage = 20.0
    short_pos.margin_mode = "isolated"
    short_pos.initial_margin = 225.0
    short_pos.position_initial_margin = 225.0
    short_pos.isolated_wallet = 250.0

    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    pos = wallet.futures.positions[("BTCUSDT", -1)]
    before_wb = wallet.get_wallet_balance()
    before_iso = pos.isolated_wallet

    class ShortSideFunding:
        event_type = "funding_fee"
        amount = -0.1
        symbol = "BTCUSDT"
        position_side = "SHORT"
        income_entry_id = 1
        venue_id = 11
        asset = "USDT"
        amount_decimal = "-0.1"
        margin_mode = "isolated"

    wallet.on_ledger_event(ShortSideFunding())

    # Wallet-level delta applied.
    assert wallet.get_wallet_balance() == pytest.approx(before_wb)
    # Per-position isolated_wallet decremented (isolated funding_fee routing).
    assert pos.isolated_wallet == pytest.approx(before_iso - 0.1)


def test_binance_parity_wallet_spot_lock_lifecycle_tracks_buy_and_sell_orders():
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=1_200.0,
        spot_estimated_value=200.0,
        futures_position_equity=1_000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=1_000.0,
            available_balance=1_000.0,
            total_unrealized_pnl=0.0,
            unrealized_pnl=0.0,
            total_margin_balance=1_000.0,
            margin_balance=1_000.0,
        ),
        spot=portfolio_service_pb2.SpotWallet(assets=[
            portfolio_service_pb2.SpotAsset(
                asset="USDT", free_decimal="1000", locked_decimal="0",
            ),
            portfolio_service_pb2.SpotAsset(
                asset="BTC", free_decimal="1", locked_decimal="0",
                avg_entry_price_decimal="90", price_decimal="100",
            ),
        ]),
    )
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    for symbol, base in (("ETHUSDT", "ETH"), ("BTCUSDT", "BTC")):
        wallet.spot.register_metadata(SpotSymbolMetadata(
            venue_id=1, exchange="binance", market="spot", symbol=symbol,
            status="TRADING", base_asset=base, quote_asset="USDT",
            base_asset_precision=8, quote_asset_precision=8,
            spot_trading_allowed=True,
        ))

    def update(
        *, order_id: str, symbol: str, status: str, side: str,
        orig: str, executed: str, remaining: str, price: str,
        fill_qty: str = "0", fill_price: str = "0", fee: str = "0",
        cumulative_quote: str = "0", trade_id: str = "",
    ) -> OrderResponse:
        quote = str(Decimal(fill_qty) * Decimal(fill_price))
        return OrderResponse(
            symbol=symbol, side=side, qty=float(fill_qty), fill_price=float(fill_price),
            status=status, fee=float(fee), order_id=order_id,
            exchange_order_id=order_id, exchange_trade_id=trade_id,
            venue_id=1, exchange="binance", market="spot", fee_asset="USDT",
            qty_decimal=fill_qty, fill_price_decimal=fill_price,
            fee_decimal=fee, quote_qty_decimal=quote,
            orig_qty_decimal=orig, executed_qty_decimal=executed,
            remaining_qty_decimal=remaining, price_decimal=price,
            cumulative_quote_qty_decimal=cumulative_quote,
        )

    wallet.on_order("ETHUSDT", "spot", update(
        order_id="buy-1", symbol="ETHUSDT", status="NEW", side="BUY",
        orig="1", executed="0", remaining="1", price="100",
    ))
    assert wallet.spot.assets["USDT"].free == Decimal("900")
    assert wallet.spot.assets["USDT"].locked == Decimal("100")

    wallet.on_order("ETHUSDT", "spot", update(
        order_id="buy-1", symbol="ETHUSDT", status="PARTIALLY_FILLED", side="BUY",
        orig="1", executed="0.4", remaining="0.6", price="100",
        fill_qty="0.4", fill_price="95", fee="1", trade_id="buy-trade-1",
        cumulative_quote="38",
    ))
    assert wallet.spot.assets["USDT"].free == Decimal("901")
    assert wallet.spot.assets["USDT"].locked == Decimal("60")
    assert wallet.spot.assets["ETH"].total == Decimal("0.4")
    assert wallet.spot.assets["ETH"].avg_entry_price == Decimal("95.0")

    wallet.on_order("ETHUSDT", "spot", update(
        order_id="buy-1", symbol="ETHUSDT", status="CANCELED", side="BUY",
        orig="1", executed="0.4", remaining="0", price="100",
        cumulative_quote="38",
    ))
    assert wallet.spot.assets["USDT"].free == Decimal("961")
    assert wallet.spot.assets["USDT"].locked == Decimal("0")

    wallet.on_order("BTCUSDT", "spot", update(
        order_id="sell-1", symbol="BTCUSDT", status="NEW", side="SELL",
        orig="0.5", executed="0", remaining="0.5", price="100",
    ))
    assert wallet.spot.assets["BTC"].locked == Decimal("0.5")

    wallet.on_order("BTCUSDT", "spot", update(
        order_id="sell-1", symbol="BTCUSDT", status="PARTIALLY_FILLED", side="SELL",
        orig="0.5", executed="0.2", remaining="0.3", price="100",
        fill_qty="0.2", fill_price="100", trade_id="sell-trade-1",
        cumulative_quote="20",
    ))
    assert wallet.spot.assets["BTC"].total == Decimal("0.8")
    assert wallet.spot.assets["BTC"].locked == Decimal("0.3")
    assert wallet.spot.assets["USDT"].free == Decimal("981")

    wallet.on_order("BTCUSDT", "spot", update(
        order_id="sell-1", symbol="BTCUSDT", status="EXPIRED", side="SELL",
        orig="0.5", executed="0.2", remaining="0", price="100",
        cumulative_quote="20",
    ))
    assert wallet.spot.assets["BTC"].locked == Decimal("0")


def test_binance_parity_wallet_spot_lifecycle_requires_order_id():
    wallet_proto = portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        total_value=1_000.0,
        spot_estimated_value=0.0,
        futures_position_equity=1_000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=1_000.0,
            available_balance=1_000.0,
            total_unrealized_pnl=0.0,
            unrealized_pnl=0.0,
            total_margin_balance=1_000.0,
            margin_balance=1_000.0,
        ),
        spot=portfolio_service_pb2.SpotWallet(assets=[
            portfolio_service_pb2.SpotAsset(
                asset="USDT", free_decimal="1000", locked_decimal="0",
            ),
        ]),
    )
    wallet = build_wallet_from_portfolio(proto_to_portfolio_spec(wallet_proto))
    wallet.spot.register_metadata(SpotSymbolMetadata(
        venue_id=1, exchange="binance", market="spot", symbol="ETHUSDT",
        status="TRADING", base_asset="ETH", quote_asset="USDT",
        base_asset_precision=8, quote_asset_precision=8,
        spot_trading_allowed=True,
    ))

    class NewBuy:
        status = "NEW"
        side = "BUY"
        qty_decimal = "0"
        fill_price_decimal = "0"
        fee_decimal = "0"
        quote_qty_decimal = "0"
        orig_qty_decimal = "1"
        executed_qty_decimal = "0"
        remaining_qty_decimal = "1"
        price_decimal = "100"
        cumulative_quote_qty_decimal = "0"

    with pytest.raises(ValueError, match="order_id"):
        wallet.on_order("ETHUSDT", "spot", NewBuy())


@pytest.mark.parametrize(
    ("field_name", "message"),
    [
        ("multi_assets_mode", "multi-assets mode"),
        ("portfolio_margin", "portfolio margin"),
    ],
)
def test_build_wallet_from_portfolio_rejects_unsupported_binance_margin_modes(field_name, message):
    wallet = _wallet_proto(environment=1)
    setattr(wallet.futures, field_name, True)

    with pytest.raises(ValueError, match=message):
        build_wallet_from_portfolio(proto_to_portfolio_spec(wallet))
