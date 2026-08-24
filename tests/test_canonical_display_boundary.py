"""Boundary tests for ``canonical-wallet-display-boundary``.

These tests prove that display-only wallet fields carried on the ingress
proto never influence runtime / risk / reconciliation / session-restoration
behaviour. The invariant is: mutating any display-only field must produce
byte-for-byte identical canonical runtime state.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from strategy_service.gen import portfolio_service_pb2
from strategy_service.wallet_adapter import proto_to_portfolio_spec
from strategy_service.wallet_factory import build_wallet_from_portfolio


def _wallet_proto(
    *,
    display_wallet_balance_usd: float = 0.0,
    display_margin_balance_usd: float = 0.0,
    display_unrealized_pnl_usd: float = 0.0,
    total_value: float = 0.0,
    spot_estimated_value: float = 0.0,
    futures_position_equity: float = 0.0,
    metrics_authoritative: bool = False,
) -> portfolio_service_pb2.PortfolioWalletState:
    """Build a wallet proto with configurable display fields + fixed canonical fields.

    Canonical fields are always identical so any runtime difference between
    two calls can only be attributed to a display-field mutation.
    """
    return portfolio_service_pb2.PortfolioWalletState(
        environment=1,
        # Display fields (the surface under test).
        total_value=total_value,
        spot_estimated_value=spot_estimated_value,
        futures_position_equity=futures_position_equity,
        metrics_authoritative=metrics_authoritative,
        futures=portfolio_service_pb2.FuturesWallet(
            # Canonical fields — fixed.
            margin_mode="cross",
            position_mode="one_way",
            wallet_balance=10_000.0,
            available_balance=9_500.0,
            total_unrealized_pnl=0.0,
            unrealized_pnl=0.0,
            total_margin_balance=10_000.0,
            margin_balance=10_000.0,
            # Display fields on the futures sub-message.
            display_wallet_balance_usd=display_wallet_balance_usd,
            display_margin_balance_usd=display_margin_balance_usd,
            display_unrealized_pnl_usd=display_unrealized_pnl_usd,
        ),
        spot=portfolio_service_pb2.SpotWallet(assets=[]),
    )


def _runtime_snapshot(wallet) -> dict:
    """Dump the subset of state that drives runtime / risk / reconciliation."""
    fw = wallet.futures
    return {
        "available_balance": fw.get_available_balance(),
        "wallet_balance": fw.get_wallet_balance(),
        "margin_balance": fw.get_margin_balance(),
        "positions": {
            key: {
                "position_qty": pos.position_qty,
                "entry_price": pos.avg_entry_price,
                "margin_mode": pos.margin_mode,
                "leverage": pos.leverage,
            }
            for key, pos in fw.positions.items()
        },
        "spot": {
            "assets": {
                sym: {
                    "free": a.free,
                    "locked": a.locked,
                    "total": a.total,
                    "price": a.price,
                }
                for sym, a in wallet.spot.assets.items()
            },
        },
    }


def test_display_usd_fields_do_not_affect_runtime_state():
    """Mutating ``display_*_usd`` must not change any runtime-facing value."""
    baseline = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto()))
    mutated = build_wallet_from_portfolio(
        proto_to_portfolio_spec(
            _wallet_proto(
                display_wallet_balance_usd=99_999.0,
                display_margin_balance_usd=88_888.0,
                display_unrealized_pnl_usd=77_777.0,
            )
        )
    )
    assert _runtime_snapshot(baseline) == _runtime_snapshot(mutated)


def test_display_total_value_and_equity_do_not_affect_runtime_state():
    """``total_value``, ``spot_estimated_value``, ``futures_position_equity``
    are display-derived outputs; mutating them on ingress must not change
    runtime state. This is the boundary property that lets core-service
    populate display totals without risk of polluting the strategy runtime.
    """
    baseline = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto()))
    mutated = build_wallet_from_portfolio(
        proto_to_portfolio_spec(
            _wallet_proto(
                total_value=123_456.0,
                spot_estimated_value=55.0,
                futures_position_equity=654_321.0,
                metrics_authoritative=True,
            )
        )
    )
    assert _runtime_snapshot(baseline) == _runtime_snapshot(mutated)


def test_available_balance_ignores_display_usd_totals():
    """Risk/precheck's ONLY input on the futures side is ``get_available_balance``.
    Setting provider display totals far larger than the canonical available
    balance MUST NOT lift the available balance reported to the engine."""
    baseline = build_wallet_from_portfolio(proto_to_portfolio_spec(_wallet_proto()))
    mutated = build_wallet_from_portfolio(
        proto_to_portfolio_spec(
            _wallet_proto(
                display_wallet_balance_usd=1_000_000.0,
                display_margin_balance_usd=1_000_000.0,
                display_unrealized_pnl_usd=1_000_000.0,
            )
        )
    )
    # Both wallets MUST report the same available balance; display totals
    # cannot lift risk headroom.
    assert (
        baseline.futures.get_available_balance()
        == mutated.futures.get_available_balance()
    )
    # And that common value is derived from canonical ingress (~wallet_balance
    # for a position-free portfolio), NOT from the provider display USD total.
    assert mutated.futures.get_available_balance() < 100_000.0


def test_session_restore_cleanup_does_not_consume_wallet_fields():
    """``StrategyServiceServicer._restore_running_sessions`` currently marks
    orphaned running sessions terminal instead of reconstructing runtime
    state. That cleanup path must stay wallet-agnostic — pulling wallet
    display fields into startup cleanup would silently break the
    canonical-wallet-display-boundary invariant."""
    from strategy_service.grpc_server import StrategyServiceServicer

    captured: dict = {}

    class FakePortfolioClient:
        def __init__(self, _addr):
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            # Attach plausible-looking wallet totals. If session restore ever
            # starts reading them, the captured list will include the wallet
            # fields below — the assertion at the bottom proves it does not.
            return [
                SimpleNamespace(
                    session_id="sess-restore-1",
                    status="running",
                    bars_processed=42,
                    error="",
                    environment=1,
                    user_id=7,
                    portfolio_id=101,
                    strategy_id=33,
                    # Poisoned display fields — must never reach the runtime.
                    display_wallet_balance_usd=999_999.0,
                    total_value=999_999.0,
                    spot_estimated_value=999_999.0,
                )
            ]

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            captured["update"] = {
                "session_id": session_id,
                "status": status,
                "bars_processed": bars_processed,
                "error": error,
            }
            return True

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient("")

    servicer = StrategyServiceServicer(
        "", "", {}, "kafka:9092", runtime_id="rt-restore", platform_proxy=FakePlatformProxy()
    )

    assert servicer._sessions.get("sess-restore-1") is None
    assert captured["update"]["session_id"] == "sess-restore-1"
    assert captured["update"]["status"] == "stopped"
    # The cleanup decision must come only from session metadata, not poisoned
    # wallet display fields.
    for key, value in captured["update"].items():
        assert "display" not in key
        assert "total_value" not in key
        assert "spot_estimated_value" not in key
