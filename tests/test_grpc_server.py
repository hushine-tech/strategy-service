from __future__ import annotations

import json
import threading
import time
import types
from datetime import datetime, timezone
from types import SimpleNamespace
import sys

import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from strategy_service import grpc_server
from strategy_service.gen import order_service_pb2
from strategy_service.gen import portfolio_service_pb2
from strategy_service.gen import strategy_service_pb2 as pb2
from strategy_service.grpc_server import StrategyServiceServicer
from strategy_service.indicators import IndicatorDefinition, IndicatorFrame
from strategy_service.session import SessionState, StreamBinding
from strategy_service.strategy_imports import (
    StrategyDependencyError,
    StrategySourceGateResult,
    gate_strategy_source,
    prepare_strategy,
    resolve_strategy_source,
)
from strategy_service.strategy import base as strategy_base
from strategy_service.types import OrderUpdateEvent, OrderUpdateFill
from strategy_service.wallet.portfolio import PortfolioWalletRuntime
from strategy_service.wallet.order_types import OrderResponse
from strategy_service.wallet.canonical import CanonicalFuturesRiskMetadata, SpotSymbolMetadata
from tests.helpers.wallet_fixtures import make_testnet_wallet
from tests.helpers.wallet_fixtures import make_backtest_wallet


def _make_fake_client(cls, addr: str):
    try:
        return cls(addr)
    except TypeError:
        return cls()


def _prepare_strategy_code_for_test(path: str, code: str):
    gate = gate_strategy_source(
        resolve_strategy_source(path, code),
        python_invocation_path=sys.executable,
    )
    assert gate.ok and gate.gated_source is not None
    return prepare_strategy(gate.gated_source)


def _mixed_leverage_bootstrap() -> pb2.StrategySessionBootstrap:
    return pb2.StrategySessionBootstrap(
        session_id="1" * 32,
        launch_operation_id="launch-1",
        strategy_source_sha256="a" * 64,
        environment=1,
        confirmed_target_facts=[
            pb2.StrategySessionTargetLeverageFact(
                venue_id=22,
                exchange="binance",
                environment=1,
                market="perpetual_futures",
                symbol="BTCUSDT",
                effective_leverage=2,
                leverage_source="order_target",
                confirmed_leverage=2,
            ),
            pb2.StrategySessionTargetLeverageFact(
                venue_id=22,
                exchange="binance",
                environment=1,
                market="perpetual_futures",
                symbol="ETHUSDT",
                effective_leverage=3,
                leverage_source="strategy_default",
                confirmed_leverage=3,
            ),
        ],
    )


def _mixed_leverage_declarations():
    return SimpleNamespace(
        order_targets=(
            SimpleNamespace(
                exchange="binance",
                market="perpetual_futures",
                symbol="BTCUSDT",
                effective_leverage=2,
                leverage_source="order_target",
            ),
            SimpleNamespace(
                exchange="binance",
                market="perpetual_futures",
                symbol="ETHUSDT",
                effective_leverage=3,
                leverage_source="strategy_default",
            ),
        )
    )


def _mixed_leverage_wallet(*, eth_leverage: float = 3.0) -> PortfolioWalletRuntime:
    return PortfolioWalletRuntime(
        portfolio_id=7,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={
            ("binance", "perpetual_futures", 22): SimpleNamespace(
                environment_code=1,
                futures=SimpleNamespace(
                    risk_metadata={
                        "BTCUSDT": SimpleNamespace(configured_leverage=2.0),
                        "ETHUSDT": SimpleNamespace(configured_leverage=eth_leverage),
                    }
                ),
            )
        },
    )


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        ("digest", "source digest mismatch"),
        ("target_set", "target set mismatch"),
        ("effective", "effective leverage mismatch"),
        ("source", "leverage source mismatch"),
        ("venue", "wallet venue mismatch"),
        ("confirmed", "confirmed leverage mismatch"),
    ],
)
def test_new_session_bootstrap_fails_closed_on_prepared_or_confirmed_fact_mismatch(
    mutation,
    error,
):
    bootstrap = _mixed_leverage_bootstrap()
    declarations = _mixed_leverage_declarations()
    prepared = SimpleNamespace(
        gated_source=SimpleNamespace(
            resolved=SimpleNamespace(source_sha256="a" * 64)
        ),
        declarations=declarations,
    )
    wallet = _mixed_leverage_wallet()
    if mutation == "digest":
        prepared.gated_source.resolved.source_sha256 = "b" * 64
    elif mutation == "target_set":
        del bootstrap.confirmed_target_facts[-1]
    elif mutation == "effective":
        bootstrap.confirmed_target_facts[0].effective_leverage = 9
    elif mutation == "source":
        bootstrap.confirmed_target_facts[0].leverage_source = "platform_default"
    elif mutation == "venue":
        bootstrap.confirmed_target_facts[0].venue_id = 23
    elif mutation == "confirmed":
        bootstrap.confirmed_target_facts[0].confirmed_leverage = 1

    with pytest.raises(grpc_server._SessionBootstrapError, match=error):
        grpc_server._validated_confirmed_target_map(
            bootstrap=bootstrap,
            prepared_strategy=prepared,
            environment=1,
            wallet=wallet,
        )


def test_new_session_bootstrap_rejects_wallet_risk_metadata_mismatch():
    with pytest.raises(
        grpc_server._SessionBootstrapError,
        match="wallet risk metadata mismatch",
    ):
        grpc_server._validated_confirmed_target_map(
            bootstrap=_mixed_leverage_bootstrap(),
            prepared_strategy=SimpleNamespace(
                gated_source=SimpleNamespace(
                    resolved=SimpleNamespace(source_sha256="a" * 64)
                ),
                declarations=_mixed_leverage_declarations(),
            ),
            environment=1,
            wallet=_mixed_leverage_wallet(eth_leverage=9.0),
        )


def test_new_session_state_uses_mixed_target_map_without_scalar_collapse():
    state = SessionState(environment=1)

    state.configure_risk_runtime(
        order_target_keys={
            ("binance", "perpetual_futures", "BTCUSDT"),
            ("binance", "perpetual_futures", "ETHUSDT"),
        },
        max_loss_close_pct=0.3,
        max_loss_close_source="platform_default",
        target_leverage_facts={
            ("binance", "perpetual_futures", "BTCUSDT"): (2, "order_target", 2),
            ("binance", "perpetual_futures", "ETHUSDT"): (3, "strategy_default", 3),
        },
    )

    assert state.leverage_for_target("binance", "perpetual_futures", "BTCUSDT") == 2
    assert state.leverage_for_target("binance", "perpetual_futures", "ETHUSDT") == 3
    assert state.leverage == 1.0


class _NoopMarketDataClient:
    def fetch_backtest_page(self, **kwargs):
        return SimpleNamespace(
            klines=[],
            next_cursor_time_ms=int(kwargs.get("start_after_time_ms", 0) or 0),
            has_more=False,
        )

    def create_session_market_data_subscriptions(self, **_kwargs) -> bool:
        return True

    def release_session_market_data_subscriptions(self, **_kwargs) -> bool:
        return True

    def create_or_renew_market_data_lease(self, **_kwargs) -> bool:
        return True

    def release_market_data_lease(self, **_kwargs) -> bool:
        return True


class _NoopOrderClient:
    pass


def _install_portfolio_client(monkeypatch, fake_cls) -> None:
    monkeypatch.setattr(
        StrategyServiceServicer,
        "_require_platform_proxy",
        lambda self, context, operation: True,
    )
    monkeypatch.setattr(
        StrategyServiceServicer,
        "_require_market_data_execution_path",
        lambda self, context, operation, profile: True,
    )
    monkeypatch.setattr(
        StrategyServiceServicer,
        "_portfolio_client",
        lambda self: _make_fake_client(fake_cls, self._portfolio_addr),
    )
    monkeypatch.setattr(
        StrategyServiceServicer,
        "_marketdata_client",
        lambda self: _NoopMarketDataClient(),
    )
    monkeypatch.setattr(
        StrategyServiceServicer,
        "_order_client",
        lambda self: _NoopOrderClient(),
    )


def _install_order_client(monkeypatch, fake_cls) -> None:
    monkeypatch.setattr(
        StrategyServiceServicer,
        "_order_client",
        lambda self: _make_fake_client(fake_cls, self._order_addr),
    )


def _install_marketdata_client(monkeypatch, fake_cls) -> None:
    monkeypatch.setattr(
        StrategyServiceServicer,
        "_require_platform_proxy",
        lambda self, context, operation: True,
    )
    monkeypatch.setattr(
        StrategyServiceServicer,
        "_require_market_data_execution_path",
        lambda self, context, operation, profile: True,
    )
    monkeypatch.setattr(
        StrategyServiceServicer,
        "_marketdata_client",
        lambda self: _make_fake_client(fake_cls, self._market_data_addr),
    )


def test_session_state_has_no_legacy_live_loop_hook():
    state = SessionState(environment=1, user_id=17, portfolio_id=404)

    assert not hasattr(state, "live_loop")


def _wallet_with_futures_slot():
    """Build a environment=1 testnet wallet with one isolated BTCUSDT position slot.

    Post-Phase-C2b this goes through the canonical proto path; the returned
    ``BinanceWalletRuntime`` exposes the same ``wallet.futures.positions``
    / ``wallet.spot`` surface the gRPC handlers already depend on.
    """
    return make_testnet_wallet(
        margin_mode="isolated",
        position_mode="one_way",
        futures_positions=[
            {
                "symbol": "BTCUSDT",
                "position_qty": 0.0,
                "entry_price": 0.0,
                "mark_price": 0.0,
                "leverage": 20.0,
                "initial_balance": 10_000.0,
                "fee_rate": 0.0004,
                "margin_mode": "isolated",
            },
        ],
    )


def _current_timestamp() -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(datetime.now(timezone.utc))
    return ts


def test_backtest_snapshot_sync_pushes_local_wallet_state():
    calls: list[dict[str, object]] = []
    wallet = SimpleNamespace(
        futures=SimpleNamespace(wallet_balance=1234.5, available_balance=1200.0),
        spot=None,
    )

    class FakePortfolioClient:
        def update_portfolio_wallet_state(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace()

        def update_portfolio_snapshot(self, **_kwargs):
            raise AssertionError("backtest wallet sync must push local wallet state")

    grpc_server._sync_strategy_snapshot(
        FakePortfolioClient(),
        portfolio_id=407,
        user_id=17,
        environment=0,
        wallet=wallet,
        snapshot_reason=grpc_server.SNAPSHOT_REASON_EVENT,
        strategy_id=43,
        session_id="sess-wallet-sync",
        snapshot_time=1780274580000,
    )

    assert len(calls) == 1
    assert calls[0]["portfolio_id"] == 407
    assert calls[0]["user_id"] == 17
    assert calls[0]["future_wallet"] is wallet.futures
    assert calls[0]["snapshot_reason"] == grpc_server.SNAPSHOT_REASON_EVENT
    assert calls[0]["session_id"] == "sess-wallet-sync"


def test_exchange_snapshot_sync_pushes_local_wallet_state_for_reconciliation():
    calls: list[dict[str, object]] = []
    wallet = SimpleNamespace(
        futures=SimpleNamespace(wallet_balance=1234.5, available_balance=1200.0),
        spot=None,
    )

    class FakePortfolioClient:
        def update_portfolio_wallet_state(self, **kwargs):
            calls.append(kwargs)
            return SimpleNamespace()

        def update_portfolio_snapshot(self, **_kwargs):
            raise AssertionError("exchange wallet sync must include local wallet for reconciliation")

    grpc_server._sync_strategy_snapshot(
        FakePortfolioClient(),
        portfolio_id=407,
        user_id=17,
        environment=1,
        wallet=wallet,
        snapshot_reason=grpc_server.SNAPSHOT_REASON_EVENT,
        strategy_id=43,
        session_id="sess-wallet-sync",
        snapshot_time=1780274580000,
    )

    assert len(calls) == 1
    assert calls[0]["portfolio_id"] == 407
    assert calls[0]["user_id"] == 17
    assert calls[0]["future_wallet"] is wallet.futures
    assert calls[0]["snapshot_reason"] == grpc_server.SNAPSHOT_REASON_EVENT
    assert calls[0]["session_id"] == "sess-wallet-sync"


def test_backtest_snapshot_sync_fails_when_wallet_state_not_persisted():
    wallet = SimpleNamespace(
        futures=SimpleNamespace(wallet_balance=1234.5, available_balance=1200.0),
        spot=None,
    )

    class FakePortfolioClient:
        def update_portfolio_wallet_state(self, **_kwargs):
            return None

    with pytest.raises(RuntimeError, match="UpdatePortfolioWalletState returned no response"):
        grpc_server._sync_strategy_snapshot(
            FakePortfolioClient(),
            portfolio_id=407,
            user_id=17,
            environment=0,
            wallet=wallet,
            snapshot_reason=grpc_server.SNAPSHOT_REASON_EVENT,
            strategy_id=43,
            session_id="sess-wallet-sync",
        )


def make_portfolio_snapshot_with_binance_perp_and_spot(
    portfolio_id: int,
    *,
    user_id: int = 17,
    environment: int = 0,
):
    futures_wallet = portfolio_service_pb2.PortfolioWalletState(
        environment=environment,
        total_value=1000.0,
        futures=portfolio_service_pb2.FuturesWallet(
            margin_mode="cross",
            position_mode="one_way",
            initial_balance=1000.0,
            wallet_balance=1000.0,
            available_balance=900.0,
            total_margin_balance=1000.0,
            margin_balance=1000.0,
            positions=[
                portfolio_service_pb2.FuturesPosition(
                    symbol="BTCUSDT",
                    position_side="BOTH",
                    position_qty=0.0,
                    qty=0.0,
                    entry_price=0.0,
                    mark_price=50_000.0,
                    leverage=20.0,
                    margin_mode="cross",
                    margin_type="cross",
                )
            ],
        ),
    )
    spot_wallet = portfolio_service_pb2.PortfolioWalletState(
        environment=environment,
        total_value=1000.0,
        spot=portfolio_service_pb2.SpotWallet(
            free=900.0,
            locked=100.0,
            assets=[
                portfolio_service_pb2.SpotAsset(
                    symbol="ETH",
                    qty=1.0,
                    locked=0.0,
                    avg_entry_price=2000.0,
                    price=2000.0,
                )
            ],
        ),
    )
    perp_venue = portfolio_service_pb2.VenueSnapshot(
        venue_id=1001,
        exchange=1,
        environment=environment,
        market=2,
        total_value=1000.0,
        wallet_balance=1000.0,
        available_balance=900.0,
    )
    perp_venue.wallet.CopyFrom(futures_wallet)
    spot_venue = portfolio_service_pb2.VenueSnapshot(
        venue_id=1002,
        exchange=1,
        environment=environment,
        market=1,
        total_value=1000.0,
        wallet_balance=1000.0,
        available_balance=900.0,
        balances=[
            portfolio_service_pb2.BalanceEntry(
                asset="USDT",
                wallet_balance=1000.0,
                available_balance=900.0,
                locked=100.0,
            ),
        ],
    )
    spot_venue.wallet.CopyFrom(spot_wallet)
    snapshot = portfolio_service_pb2.PortfolioSnapshot(
        portfolio_id=portfolio_id,
        user_id=user_id,
        total_value=2000.0,
        wallet_balance=2000.0,
        available_balance=1800.0,
        venues=[perp_venue, spot_venue],
    )
    snapshot.wallet.CopyFrom(futures_wallet)
    return snapshot


def _phase3_strategy_code() -> str:
    return (
        "from strategy_service.types import OrderDecision, OrderSide\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        '    ORDER_TARGETS = [{"exchange": "binance", "market": "spot", "symbol": "ETH"}]\n'
        "    def on_market_data(self, data, wallet): return None\n"
    )


def _phase3_strategy_code_with_target_leverage() -> str:
    return (
        "from strategy_service.types import OrderDecision, OrderSide\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        '    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "leverage": 7}, {"exchange": "binance", "market": "spot", "symbol": "ETH"}]\n'
        "    def on_market_data(self, data, wallet): return None\n"
    )


def test_session_thread_otel_context_is_inherited_for_platform_proxy_calls():
    try:
        from opentelemetry import context as otel_context
        from opentelemetry import propagate, trace
        from opentelemetry.propagate import get_global_textmap, set_global_textmap
        from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    except ImportError:
        return

    old_textmap = get_global_textmap()
    set_global_textmap(TraceContextTextMapPropagator())
    parent_span = NonRecordingSpan(SpanContext(
        trace_id=int("4bf92f3577b34da6a3ce929d0e0e4736", 16),
        span_id=int("00f067aa0ba902b7", 16),
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
        trace_state=TraceState(),
    ))
    token = otel_context.attach(trace.set_span_in_context(parent_span))
    try:
        parent_context = grpc_server._capture_otel_context()
    finally:
        otel_context.detach(token)

    carrier: dict[str, str] = {}
    errors: list[BaseException] = []

    def worker():
        try:
            grpc_server._run_in_otel_context(
                parent_context,
                "StrategySession/test",
                lambda: propagate.inject(carrier),
            )
        except BaseException as exc:
            errors.append(exc)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join(timeout=1)
    set_global_textmap(old_textmap)

    if errors:
        raise errors[0]
    assert carrier["traceparent"].startswith("00-4bf92f3577b34da6a3ce929d0e0e4736-")


class _FakeContext:
    def __init__(self) -> None:
        self.code = None
        self.details = ""
        self.runtime_dependency_error = None

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details

    def set_runtime_dependency_error(self, detail) -> None:
        self.runtime_dependency_error = detail


class _AsyncTestThread:
    def __init__(self, target) -> None:
        self._thread = threading.Thread(target=target, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout=None) -> None:
        self._thread.join(timeout=timeout)

    def is_alive(self) -> bool:
        return self._thread.is_alive()


def test_validate_strategy_source_returns_runtime_profile_without_session():
    servicer = StrategyServiceServicer(
        "", "", {}, "", bound_user_id=7, runtime_id="rt-1",
        restore_running_sessions=False,
    )
    context = _FakeContext()
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )

    response = servicer.ValidateStrategySource(
        pb2.ValidateStrategySourceRequest(source=source, user_id=7, runtime_id="rt-1"),
        context,
    )

    assert context.code is None
    assert response.ok is True
    assert response.runtime_profile.contract_sha256
    assert response.runtime_profile.profile_name
    assert servicer._sessions.list_ids() == ()


def test_validate_strategy_source_returns_runtime_prepared_declarations_without_session():
    servicer = StrategyServiceServicer(
        "", "", {}, "", bound_user_id=7, runtime_id="rt-1",
        restore_running_sessions=False,
    )
    context = _FakeContext()
    source = '''
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    def __init__(self):
        type(self).INPUTS = [
            {"stream_id": "btc-kline", "exchange": "binance", "market": "spot", "kind": "kline", "symbol": "BTCUSDT", "interval": "1m"},
            {"stream_id": "btc-mark", "exchange": "binance", "market": "spot", "kind": "mark_price", "symbol": "BTCUSDT", "interval": "1m"},
        ]
        type(self).ORDER_TARGETS = [
            {"exchange": "binance", "market": "spot", "symbol": "BTCUSDT"},
        ]
    def on_market_data(self, data, wallet):
        return None
'''

    response = servicer.ValidateStrategySource(
        pb2.ValidateStrategySourceRequest(
            source=source,
            user_id=7,
            runtime_id="rt-1",
            include_declarations=True,
        ),
        context,
    )

    assert context.code is None
    assert response.ok is True
    assert [
        (item.stream_id, item.exchange, item.market, item.kind, item.symbol, item.interval)
        for item in response.declared_inputs
    ] == [
        ("btc-kline", "binance", "spot", "kline", "BTCUSDT", "1m"),
        ("btc-mark", "binance", "spot", "mark_price", "BTCUSDT", "1m"),
    ]
    assert [
        (item.exchange, item.market, item.symbol)
        for item in response.declared_order_targets
    ] == [("binance", "spot", "BTCUSDT")]
    assert servicer._sessions.list_ids() == ()


def test_prepare_strategy_extracts_resolved_target_leverage_facts():
    prepared = _prepare_strategy_code_for_test(
        "<db:leverage-declarations>",
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = [\n"
        '        {"exchange": "binance", "market": "spot", "symbol": "BTCUSDT"},\n'
        '        {"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT"},\n'
        '        {"exchange": "okx", "market": "delivery_futures", "symbol": "BTCUSD", "leverage": 7},\n'
        "    ]\n"
        "    LEVERAGE = 3\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n",
    )

    assert [
        (
            target.market,
            target.symbol,
            target.leverage,
            target.effective_leverage,
            target.leverage_source,
        )
        for target in prepared.declarations.order_targets
    ] == [
        ("spot", "BTCUSDT", None, None, None),
        ("perpetual_futures", "ETHUSDT", None, 3, "strategy_default"),
        ("delivery_futures", "BTCUSD", 7, 7, "order_target"),
    ]


def test_validate_strategy_source_shapes_resolved_target_leverage_facts():
    servicer = StrategyServiceServicer(
        "", "", {}, "", bound_user_id=7, runtime_id="rt-1",
        restore_running_sessions=False,
    )
    context = _FakeContext()
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = [\n"
        '        {"exchange": "binance", "market": "spot", "symbol": "BTCUSDT"},\n'
        '        {"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT"},\n'
        '        {"exchange": "okx", "market": "delivery_futures", "symbol": "BTCUSD", "leverage": 7},\n'
        "    ]\n"
        "    LEVERAGE = 3\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )

    response = servicer.ValidateStrategySource(
        pb2.ValidateStrategySourceRequest(
            source=source,
            user_id=7,
            runtime_id="rt-1",
            include_declarations=True,
        ),
        context,
    )

    assert context.code is None
    assert response.ok is True
    assert [
        (target.market, target.symbol, target.effective_leverage, target.leverage_source)
        for target in response.declared_order_targets
    ] == [
        ("spot", "BTCUSDT", 0, ""),
        ("perpetual_futures", "ETHUSDT", 3, "strategy_default"),
        ("delivery_futures", "BTCUSD", 7, "order_target"),
    ]


def test_validate_strategy_source_does_not_execute_source_without_declaration_opt_in():
    servicer = StrategyServiceServicer(
        "", "", {}, "", bound_user_id=7, runtime_id="rt-1",
        restore_running_sessions=False,
    )
    context = _FakeContext()
    source = '''
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "spot", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    def __init__(self):
        raise RuntimeError("must not execute")
    def on_market_data(self, data, wallet):
        return None
'''

    response = servicer.ValidateStrategySource(
        pb2.ValidateStrategySourceRequest(source=source, user_id=7, runtime_id="rt-1"),
        context,
    )

    assert context.code is None
    assert response.ok is True
    assert list(response.declared_inputs) == []
    assert list(response.declared_order_targets) == []
    assert servicer._sessions.list_ids() == ()


@pytest.mark.parametrize(
    ("source", "expected_code"),
    [
        ("", "STRATEGY_SOURCE_REQUIRED"),
        ("class Broken(:\n    pass\n", "syntax_error"),
    ],
)
def test_validate_strategy_source_reports_source_issues_without_transport_error(
    source,
    expected_code,
):
    servicer = StrategyServiceServicer(
        "", "", {}, "", bound_user_id=7, runtime_id="rt-1",
        restore_running_sessions=False,
    )
    context = _FakeContext()

    response = servicer.ValidateStrategySource(
        pb2.ValidateStrategySourceRequest(source=source, user_id=7, runtime_id="rt-1"),
        context,
    )

    assert context.code is None
    assert response.ok is False
    assert [issue.code for issue in response.issues] == [expected_code]
    assert response.runtime_profile.contract_sha256
    assert servicer._sessions.list_ids() == ()


def test_dependency_gate_attaches_typed_runtime_detail(monkeypatch):
    detail = StrategyDependencyError(
        code="STRATEGY_DEPENDENCY_UNAVAILABLE",
        module="google.cloud",
        runtime_profile="platform-python-3.13",
        runtime_profile_version="1.0.0",
        image_build_id="build-1",
        message="dependency unavailable",
    )
    monkeypatch.setattr(
        grpc_server,
        "_resolve_and_gate_strategy_source",
        lambda *_args, **_kwargs: StrategySourceGateResult(
            ok=False,
            issues=(),
            runtime_profile=detail.runtime_profile,
            runtime_profile_version=detail.runtime_profile_version,
            contract_sha256="a" * 64,
            image_build_id=detail.image_build_id,
            dependency_error=detail,
        ),
    )
    context = _FakeContext()

    prepared = grpc_server._prepare_gated_strategy_for_rpc(
        strategy_path="<db:test>",
        strategy_code="import google.cloud",
        hot_reload=False,
        context=context,
        operation="RunStrategy",
    )

    assert prepared is None
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.runtime_dependency_error.module == "google.cloud"


def test_run_strategy_returns_not_found_when_portfolio_lookup_fails(monkeypatch):
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    request = SimpleNamespace(
        portfolio_id=101,
        user_id=17,
        strategy_path="strategies.buy_once",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 101
            assert user_id == 17
            return None

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.NOT_FOUND
    assert "portfolio 101 not found" in context.details


def test_run_strategy_rejects_wallet_schema_mismatch(monkeypatch):
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    request = SimpleNamespace(
        portfolio_id=202,
        user_id=17,
        strategy_path="strategies.buy_once",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 202
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def preflight_strategy_session(self, **_kwargs):
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, _portfolio_id: int):
            return SimpleNamespace(
                strategy_id=7,
                code=_phase3_strategy_code(),
                name="phase3",
                version="v1",
            )

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    monkeypatch.setattr(
        grpc_server,
        "build_portfolio_wallet_from_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("schema mismatch: invalid margin_mode")),
    )

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "schema mismatch" in context.details


def test_run_strategy_builds_wallet_from_portfolio_snapshot(monkeypatch):
    calls = {"portfolio": 0, "preflight": 0, "wallet_update": 0}

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int = 0):
            calls["portfolio"] += 1
            assert portfolio_id == 404
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def preflight_strategy_session(self, **_kwargs):
            calls["preflight"] += 1
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, _portfolio_id: int):
            return SimpleNamespace(
                strategy_id=42,
                code=_phase3_strategy_code(),
                name="phase3",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            return True

        def update_session(self, **_kwargs) -> bool:
            return True

        def update_portfolio_wallet_state(self, *_args, **_kwargs):
            calls["wallet_update"] += 1
            return SimpleNamespace()

    class FakeThread:
        def __init__(self, target=None, args=(), daemon=None) -> None:
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self) -> None:
            return None

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    monkeypatch.setattr(grpc_server, "_create_session_thread", _AsyncTestThread)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        market_data_policy={"preflight_enabled": False},
        runtime_id="rt-test",
        restore_running_sessions=False,
    )
    request = SimpleNamespace(
        portfolio_id=404,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id != ""
    assert context.code is None
    state = servicer._sessions.get(resp.session_id)
    assert state is not None
    state.thread.join(timeout=1.0)
    assert calls["portfolio"] == 2
    assert calls["wallet_update"] == 3


def test_run_strategy_fails_start_when_backtest_wallet_sync_is_missing(monkeypatch):
    calls = {"portfolio": 0, "preflight": 0, "wallet_update": 0, "update_session": []}

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int = 0):
            calls["portfolio"] += 1
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def preflight_strategy_session(self, **_kwargs):
            calls["preflight"] += 1
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, _portfolio_id: int):
            return SimpleNamespace(
                strategy_id=42,
                code=_phase3_strategy_code(),
                name="phase3",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            return True

        def update_portfolio_wallet_state(self, *_args, **_kwargs):
            calls["wallet_update"] += 1
            return None

        def update_session(self, **kwargs) -> bool:
            calls["update_session"].append(dict(kwargs))
            return True

    class FakeThread:
        def __init__(self, target=None, args=(), daemon=None) -> None:
            pass

        def start(self) -> None:
            raise AssertionError("session thread must not start when startup wallet sync fails")

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    monkeypatch.setattr(grpc_server, "_create_session_thread", _AsyncTestThread)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        market_data_policy={"preflight_enabled": False},
        runtime_id="rt-test",
        restore_running_sessions=False,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(SimpleNamespace(
        portfolio_id=404,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    ), context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.UNAVAILABLE
    assert "failed to persist strategy_start snapshot" in context.details
    assert calls["wallet_update"] == 1
    assert calls["update_session"][0]["status"] == "failed"
    assert calls["update_session"][0]["error"] == "failed to persist strategy_start snapshot"


def test_run_strategy_preflight_sends_required_routes_and_symbols(monkeypatch):
    captured: dict[str, object] = {}
    wallet_calls = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int = 0, required_symbols=None):
            captured.setdefault("snapshots", []).append(required_symbols)
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def preflight_strategy_session(self, **kwargs):
            captured["preflight"] = kwargs
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, _portfolio_id: int):
            return SimpleNamespace(
                strategy_id=42,
                code=_phase3_strategy_code_with_target_leverage(),
                name="phase3",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            return True

        def update_session(self, **_kwargs) -> bool:
            return True

        def update_portfolio_wallet_state(self, *_args, **kwargs):
            wallet_calls.append(kwargs)
            return SimpleNamespace()

    class FakeThread:
        def __init__(self, target=None, args=(), daemon=None) -> None:
            self.target = target

        def start(self) -> None:
            return None

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    monkeypatch.setattr(grpc_server, "_create_session_thread", _AsyncTestThread)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        market_data_policy={"preflight_enabled": False},
        runtime_id="rt-test",
        restore_running_sessions=False,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(SimpleNamespace(
        portfolio_id=405,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    ), context)

    assert resp.session_id != ""
    assert context.code is None
    state = servicer._sessions.get(resp.session_id)
    assert state is not None
    state.thread.join(timeout=1.0)
    assert [item["snapshot_reason"] for item in wallet_calls] == [
        grpc_server.SNAPSHOT_REASON_STRATEGY_START,
        grpc_server.SNAPSHOT_REASON_STRATEGY_END,
        0,
    ]
    req = captured["preflight"]
    assert set(req["required_routes"]) == {
        ("binance", "perpetual_futures"),
        ("binance", "spot"),
    }
    assert set(req["required_symbols"]) == {
        ("binance", "perpetual_futures", "BTCUSDT"),
        ("binance", "spot", "ETH"),
    }
    targets = {
        (target.exchange, target.market, target.symbol): target
        for target in req["order_targets"]
    }
    assert targets[("binance", "perpetual_futures", "BTCUSDT")].effective_leverage == 7
    assert targets[("binance", "perpetual_futures", "BTCUSDT")].leverage_source == "order_target"
    assert targets[("binance", "spot", "ETH")].effective_leverage is None
    assert targets[("binance", "spot", "ETH")].leverage_source is None
    assert req["leverage"] == 7
    assert captured["snapshots"][0] is None
    assert set(captured["snapshots"][1]) == {
        ("binance", "perpetual_futures", "BTCUSDT"),
        ("binance", "spot", "ETH"),
    }


def test_run_session_order_callback_updates_portfolio_wallet_state(monkeypatch):
    calls = {"wallet_update": 0}
    captured: list[dict[str, object]] = []
    state = SessionState(environment=0, portfolio_id=406, strategy_id=42)
    wallet = make_portfolio_snapshot_with_binance_perp_and_spot(406)

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def update_portfolio_wallet_state(self, **kwargs):
            calls["wallet_update"] += 1
            captured.append(kwargs)
            return SimpleNamespace()

        def update_session(self, **_kwargs):
            return True

    class FakeStrategy:
        def __init__(self) -> None:
            self.on_order_callback = None

    fake_strategy = FakeStrategy()

    class FakeEngine:
        def create_strategy(self, **_kwargs):
            return fake_strategy

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
    )

    def fake_run_backtest(*_args, **_kwargs):
        assert fake_strategy.on_order_callback is not None
        fake_strategy.on_order_callback()
        state.transition("stopped", bars=1)

    monkeypatch.setattr(servicer, "_run_backtest", fake_run_backtest)

    from strategy_service.wallet.portfolio_adapter import build_portfolio_wallet_from_snapshot

    portfolio_wallet = build_portfolio_wallet_from_snapshot(
        wallet,
        allowed_routes={("binance", "perpetual_futures"), ("binance", "spot")},
    )
    fake_strategy.on_order_callback = lambda: grpc_server._sync_strategy_snapshot(
        servicer._portfolio_client(),
        portfolio_id=406,
        user_id=17,
        environment=0,
        wallet=portfolio_wallet,
        snapshot_reason=grpc_server.SNAPSHOT_REASON_EVENT,
        strategy_id=42,
        session_id="sess-portfolio",
    )
    servicer._run_session(
        "sess-portfolio",
        state,
        SimpleNamespace(),
        portfolio_wallet,
        0,
        406,
        17,
        [],
        FakeEngine(),
        fake_strategy,
        42,
    )

    assert calls["wallet_update"] >= 1
    order_update = next(item for item in captured if item["snapshot_reason"] == 1)
    assert order_update["portfolio_id"] == 406
    assert order_update["strategy_id"] == 42
    assert order_update["session_id"] == "sess-portfolio"


def test_backtest_run_persists_wallet_snapshots(monkeypatch):
    calls = {"wallet_update": 0}
    state = SessionState(environment=0, portfolio_id=407, strategy_id=43, user_id=17)
    snapshot = make_portfolio_snapshot_with_binance_perp_and_spot(407, user_id=17)

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int = 0):
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def update_portfolio_wallet_state(self, *args, **kwargs):
            del args
            calls["wallet_update"] += 1
            assert kwargs["portfolio_id"] == 407
            assert kwargs["user_id"] == 17
            return SimpleNamespace()

        def update_session(self, **_kwargs):
            return True

    class FakeStrategy:
        def __init__(self) -> None:
            self.on_order_callback = None

    fake_strategy = FakeStrategy()

    class FakeEngine:
        def create_strategy(self, **_kwargs):
            return fake_strategy

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
    )

    def fake_run_backtest(*_args, **_kwargs):
        assert fake_strategy.on_order_callback is not None
        fake_strategy.on_order_callback()
        state.transition("finished", bars=1)

    monkeypatch.setattr(servicer, "_run_backtest", fake_run_backtest)

    from strategy_service.wallet.portfolio_adapter import build_portfolio_wallet_from_snapshot

    wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "perpetual_futures"), ("binance", "spot")},
    )
    fake_strategy.on_order_callback = lambda: grpc_server._sync_strategy_snapshot(
        servicer._portfolio_client(),
        portfolio_id=407,
        user_id=17,
        environment=0,
        wallet=wallet,
        snapshot_reason=grpc_server.SNAPSHOT_REASON_EVENT,
        strategy_id=43,
        session_id="sess-portfolio-sync",
    )
    servicer._run_session(
        "sess-portfolio-sync",
        state,
        SimpleNamespace(),
        wallet,
        0,
        407,
        17,
        [],
        FakeEngine(),
        fake_strategy,
        43,
    )

    assert calls["wallet_update"] >= 2


def test_backtest_run_restores_portfolio_wallet_state_after_finish(monkeypatch):
    wallet_updates: list[dict[str, object]] = []
    session_updates: list[dict[str, object]] = []
    created: dict[str, object] = {}

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int = 0):
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def preflight_strategy_session(self, **_kwargs):
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, _portfolio_id: int):
            return SimpleNamespace(
                strategy_id=43,
                code=_phase3_strategy_code(),
                name="phase3",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            return True

        def update_portfolio_wallet_state(self, **kwargs):
            future_wallet = kwargs["future_wallet"]
            wallet_updates.append({
                "snapshot_reason": kwargs["snapshot_reason"],
                "wallet_balance": future_wallet.get_wallet_balance(),
                "session_id": kwargs["session_id"],
            })
            return SimpleNamespace()

        def update_session(self, **kwargs):
            session_updates.append(dict(kwargs))
            return True

    class FakeStrategy:
        def __init__(self) -> None:
            self.on_order_callback = None
            self.last_market_time = 1780274580000

        def activate_order_event_cursor(self) -> None:
            return None

    fake_strategy = FakeStrategy()

    class FakeEngine:
        def create_strategy(self, **kwargs):
            created["wallet"] = kwargs["wallet"]
            return fake_strategy

    class InlineThread:
        def __init__(self, target=None, args=(), daemon=None) -> None:
            del args, daemon
            self.target = target

        def start(self) -> None:
            self.target()

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())
    monkeypatch.setattr(grpc_server, "_create_session_thread", _AsyncTestThread)

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        market_data_policy={"preflight_enabled": False},
        runtime_id="rt-test",
        restore_running_sessions=False,
    )

    def fake_run_backtest(*_args, **_kwargs):
        wallet = created["wallet"]
        route_wallet = wallet.get("binance", "perpetual_futures")
        route_wallet.futures.wallet_balance = 777.0
        route_wallet.futures._refresh_portfolio_fields()
        state = _args[1]
        state.transition("finished", bars=3)

    monkeypatch.setattr(servicer, "_run_backtest", fake_run_backtest)

    context = _FakeContext()
    resp = servicer.RunStrategy(SimpleNamespace(
        portfolio_id=407,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    ), context)

    assert resp.session_id != ""
    assert context.code is None
    state = servicer._sessions.get(resp.session_id)
    assert state is not None
    state.thread.join(timeout=1.0)
    assert [item["snapshot_reason"] for item in wallet_updates] == [
        grpc_server.SNAPSHOT_REASON_STRATEGY_START,
        grpc_server.SNAPSHOT_REASON_STRATEGY_END,
        0,
    ]
    assert wallet_updates[-2]["wallet_balance"] == pytest.approx(777.0)
    assert wallet_updates[-1]["wallet_balance"] == pytest.approx(1000.0)
    assert session_updates[-1]["status"] == "finished"


def test_backtest_final_snapshot_failure_marks_session_recoverable(monkeypatch):
    session_updates: list[dict[str, object]] = []
    state = SessionState(environment=0, portfolio_id=407, strategy_id=43, user_id=17)
    snapshot = make_portfolio_snapshot_with_binance_perp_and_spot(407, user_id=17)

    class FakePortfolioClient:
        def update_portfolio_wallet_state(self, **kwargs):
            if kwargs["snapshot_reason"] == grpc_server.SNAPSHOT_REASON_STRATEGY_END:
                raise RuntimeError("strategy_end store timeout")
            return SimpleNamespace()

        def update_session(self, **kwargs):
            session_updates.append(dict(kwargs))
            return True

    class FakeProxy:
        def __init__(self) -> None:
            self.portfolio = FakePortfolioClient()

        def portfolio_client(self):
            return self.portfolio

        def order_client(self):
            return SimpleNamespace()

        def marketdata_client(self):
            return SimpleNamespace()

    class FakeStrategy:
        def __init__(self) -> None:
            self.on_order_callback = None
            self.last_market_time = 1780274580000

    class FakeEngine:
        def create_strategy(self, **_kwargs):
            return FakeStrategy()

    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())

    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
        platform_proxy=FakeProxy(),
    )

    def fake_run_backtest(*_args, **_kwargs):
        state.transition("finished", bars=3)

    monkeypatch.setattr(servicer, "_run_backtest", fake_run_backtest)

    from strategy_service.wallet.portfolio_adapter import build_portfolio_wallet_from_snapshot

    wallet = build_portfolio_wallet_from_snapshot(
        snapshot,
        allowed_routes={("binance", "perpetual_futures"), ("binance", "spot")},
    )
    servicer._run_session(
        "sess-finalization",
        state,
        SimpleNamespace(end_time_ms=1780274580000),
        wallet,
        0,
        407,
        17,
        [],
        FakeEngine(),
        FakeStrategy(),
        43,
    )

    assert session_updates[-1]["status"] == "recoverable"
    assert "failed to persist strategy_end snapshot" in session_updates[-1]["error"]
    assert "strategy_end store timeout" in session_updates[-1]["error"]


def test_run_strategy_returns_internal_when_session_persist_fails(monkeypatch):
    # Test focuses on session-persist failure path. Disable preflight so we
    # don't also need to fake TimescaleDB for the backtest profile.
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        market_data_policy={"preflight_enabled": False},
        restore_running_sessions=False,
    )
    request = SimpleNamespace(
        portfolio_id=303,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
        runtime_id="rt-test",
    )
    context = _FakeContext()

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 303
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def preflight_strategy_session(self, **_kwargs):
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, portfolio_id: int):
            assert portfolio_id == 303
            return SimpleNamespace(
                strategy_id=7,
                code=(
                    "class MyStrategy:\n"
                    '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
                    '    ORDER_TARGETS = []\n'
                    "    def on_market_data(self, data, wallet): return None\n"
                ),
                name="buy_once",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            return False

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.INTERNAL
    assert "failed to persist session" in context.details


def test_run_strategy_rejects_empty_runtime_binding_before_persist(monkeypatch):
    request = SimpleNamespace(
        portfolio_id=303,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()
    calls = {"save_session": 0}

    class FakePortfolioClient:
        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 303
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def preflight_strategy_session(self, **_kwargs):
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, _portfolio_id: int):
            return SimpleNamespace(
                strategy_id=7,
                code=(
                    "class MyStrategy:\n"
                    '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
                    '    ORDER_TARGETS = []\n'
                    "    def on_market_data(self, data, wallet): return None\n"
                ),
                name="buy_once",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            calls["save_session"] += 1
            return True

        def update_session(self, **_kwargs) -> bool:
            return True

        def update_session(self, **_kwargs) -> bool:
            return True

    class FakeMarketDataClient:
        def fetch_klines(self, **_kwargs):
            return []

        def fetch_backtest_page(self, **_kwargs):
            return SimpleNamespace(klines=[], next_cursor_time_ms=0, has_more=False)

    class FakePlatformProxy:
        def __init__(self) -> None:
            self.portfolio = FakePortfolioClient()
            self.marketdata = FakeMarketDataClient()

        def portfolio_client(self):
            return self.portfolio

        def marketdata_client(self):
            return self.marketdata

        def order_client(self):
            return SimpleNamespace()

    class FakeRuntimeDataSource:
        def iter_dataset_klines(self, **_kwargs):
            return iter(())

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        market_data_policy={"preflight_enabled": False},
        restore_running_sessions=False,
        platform_proxy=FakePlatformProxy(),
    )
    servicer.set_runtime_data_source(FakeRuntimeDataSource())
    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "runtime_id is required" in context.details
    assert calls["save_session"] == 0


def test_live_consumer_group_uses_strategy_and_session():
    group = grpc_server._live_consumer_group(7, "sess-123")
    assert group == "strategy-session-7-sess-123"


def test_run_live_initializes_runtime_channel_delivery(monkeypatch):
    events: list[tuple] = []

    class FakePortfolioClient:
        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            events.append(("session_update", session_id, status, bars_processed, error, runtime_id))
            return True

    class FakeMarketDataClient:
        def create_or_renew_market_data_lease(self, **kwargs) -> bool:
            events.append(("lease", kwargs))
            return True

    class FakeProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

        def marketdata_client(self):
            return FakeMarketDataClient()

        def order_client(self):
            return _NoopOrderClient()

    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "kafka-1:9092,kafka-2",
        platform_proxy=FakeProxy(),
        restore_running_sessions=False,
    )
    state = SessionState(environment=1)
    state.configure_live_runtime(
        portfolio_id=101,
        strategy_id=77,
        required_streams=[
            StreamBinding(11, "binance", "futures", "kline", "BTCUSDT", "1m", canonical_market="perpetual_futures"),
            StreamBinding(12, "binance", "spot", "kline", "ETHUSDT", "1m", canonical_market="spot"),
        ],
        consumer_group="strategy-session-77-sess-live-route",
    )

    class FakeEvent:
        def __init__(self) -> None:
            self._set = False

        def wait(self, timeout=None) -> bool:
            events.append(("wait", timeout))
            return self._set

        def set(self) -> None:
            events.append(("set",))
            self._set = True

        def is_set(self) -> bool:
            return self._set

    class FakeThread:
        def __init__(self, target=None, args=(), daemon=None) -> None:
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self) -> None:
            events.append(("lease_thread_start", self.daemon))

    monkeypatch.setitem(
        sys.modules,
        "strategy_service.marketdata_adapter",
        types.SimpleNamespace(
            _adapt_kline=lambda kline, market=None: SimpleNamespace(
                symbol=kline.symbol,
                market=market,
                interval=kline.interval,
                price=kline.close,
            ),
        ),
    )
    monkeypatch.setattr(threading, "Event", FakeEvent)
    monkeypatch.setattr(threading, "Thread", FakeThread)

    class FakeDelivery:
        def iter_session_events(self, *, session_id, required_streams, stop_event):
            events.append(("iter", session_id, [(s.market, s.symbol, s.interval, s.canonical_market) for s in required_streams]))
            yield SimpleNamespace(
                kind="kline",
                payload=SimpleNamespace(
                    symbol="BTCUSDT",
                    interval="1m",
                    close=1.5,
                    market="futures",
                ),
            )
            state.transition("stopped")
            stop_event.set()

    class FakeEngine:
        def running_strategy(self, market_data):
            events.append(("strategy", market_data.symbol, market_data.market, market_data.interval, market_data.price))
            return True

    servicer.set_runtime_data_source(FakeDelivery())

    servicer._run_live(
        "sess-live-route",
        state,
        engine=FakeEngine(),
        declared_inputs=[],
        strategy_id=77,
    )

    assert events == [
        ("lease", {"session_id": "sess-live-route", "strategy_id": 77, "portfolio_id": 101, "stream_id": 11, "ttl_seconds": servicer._lease_ttl_seconds}),
        ("lease", {"session_id": "sess-live-route", "strategy_id": 77, "portfolio_id": 101, "stream_id": 12, "ttl_seconds": servicer._lease_ttl_seconds}),
        ("lease_thread_start", True),
        ("iter", "sess-live-route", [("futures", "BTCUSDT", "1m", "perpetual_futures"), ("spot", "ETHUSDT", "1m", "spot")]),
        ("strategy", "BTCUSDT", "perpetual_futures", "1m", 1.5),
        ("session_update", "sess-live-route", "running", 1, "", ""),
        ("set",),
    ]
    assert state.status == "stopped"


def test_run_live_uses_resolved_stream_bindings_instead_of_declared_inputs(monkeypatch):
    events: list[tuple] = []

    class FakeProxy:
        def portfolio_client(self):
            return SimpleNamespace(update_session=lambda **_kwargs: True)

        def marketdata_client(self):
            return _NoopMarketDataClient()

        def order_client(self):
            return _NoopOrderClient()

    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "kafka-1:9092",
        platform_proxy=FakeProxy(),
        market_data_policy={"lease_management_enabled": False},
        restore_running_sessions=False,
    )
    state = SessionState(environment=1)
    state.configure_live_runtime(
        portfolio_id=101,
        strategy_id=77,
        required_streams=[
            StreamBinding(11, "binance", "futures", "kline", "ETHUSDT", "1m", canonical_market="perpetual_futures"),
        ],
        consumer_group="strategy-session-77-sess-live-okx",
    )

    monkeypatch.setitem(
        sys.modules,
        "strategy_service.data_loop",
        types.SimpleNamespace(_adapt_kline=lambda kline, market=None: kline),
    )

    class FakeDelivery:
        def iter_session_events(self, *, session_id, required_streams, stop_event):
            events.append(("iter", session_id, [(s.exchange, s.market, s.symbol, s.interval) for s in required_streams]))
            state.transition("stopped")
            return iter(())

    servicer.set_runtime_data_source(FakeDelivery())

    from strategy_service.inputs import StrategyInput
    servicer._run_live(
        "sess-live-okx",
        state,
        engine=object(),
        declared_inputs=[StrategyInput("okx", "futures", "ETHUSDT", "1m")],
        strategy_id=77,
    )

    assert events == [
        ("iter", "sess-live-okx", [("binance", "futures", "ETHUSDT", "1m")]),
    ]
    assert state.status == "stopped"


def test_run_strategy_rejects_strategy_missing_inputs_declaration(monkeypatch):
    """Pre_C3 contract: a strategy without a valid INPUTS declaration MUST
    be rejected at RPC entry, not deferred to a background session failure."""
    servicer = StrategyServiceServicer("", "", {}, "127.0.0.1:9092")
    request = SimpleNamespace(
        portfolio_id=405,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=0,
        end_time_ms=0,
        runtime_id="rt-test",
    )
    context = _FakeContext()
    calls = {"save_session": 0}

    bad_code = (
        "class MyStrategy:\n"
        "    # No INPUTS declared — must be rejected at the RPC boundary.\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return None\n"
    )

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def get_active_strategy(self, portfolio_id: int):
            return SimpleNamespace(strategy_id=5, code=bad_code, name="bad", version="v1")

        def save_session(self, **_kwargs) -> bool:
            calls["save_session"] += 1
            return True

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    resp = servicer.RunStrategy(request, context)

    # No session created, no save_session call, RPC returns FAILED_PRECONDITION.
    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details.startswith("strategy code validation failed: ")
    assert '"code":"missing_inputs"' in context.details
    assert calls["save_session"] == 0
    assert servicer._sessions._sessions == {}


def test_run_strategy_mode2_preflight_rejects_before_session_creation(monkeypatch):
    servicer = StrategyServiceServicer("", "", {}, "127.0.0.1:9092")
    request = SimpleNamespace(
        portfolio_id=404,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=0,
        end_time_ms=0,
        runtime_id="rt-test",
    )
    context = _FakeContext()
    calls = {"save_session": 0}

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 404
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id, environment=1)

        def preflight_strategy_session(self, **_kwargs):
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, portfolio_id: int):
            assert portfolio_id == 404
            return SimpleNamespace(
                strategy_id=8,
                code=(
                    "class MyStrategy:\n"
                    '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
                    '    ORDER_TARGETS = []\n'
                    "    def on_market_data(self, data, wallet): return None\n"
                ),
                name="live_ready",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            calls["save_session"] += 1
            return True

    # Phase D2: GetMarketDataStreamStatus moved to MarketDataClient. Returning
    # None here means preflight will fail with "stream missing", which is what
    # this test asserts on.
    class FakeMarketDataClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_market_data_stream_status(self, **_kwargs):
            return None

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    _install_marketdata_client(monkeypatch, FakeMarketDataClient)
    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "demo profile preflight failed" in context.details
    assert "[stream]" in context.details
    assert calls["save_session"] == 0
    assert servicer._sessions._sessions == {}


def test_run_strategy_mode2_preflight_disabled_still_resolves_stream_bindings(monkeypatch):
    """preflight_enabled=False disables READINESS gating but MUST still
    resolve stream bindings for environment=1 — otherwise lease management silently
    no-ops while the Kafka consumer subscribes to topics the control plane
    doesn't know about (see canonical-wallet-display-boundary review #1)."""
    from strategy_service.preflight import RuntimeSourceProfile

    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "127.0.0.1:9092",
        market_data_policy={"preflight_enabled": False},
    )
    request = SimpleNamespace(
        portfolio_id=405,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=0,
        end_time_ms=0,
        runtime_id="rt-test",
    )
    context = _FakeContext()
    calls = {"save_session": 0}

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 405
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id, environment=1)

        def preflight_strategy_session(self, **_kwargs):
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, portfolio_id: int):
            assert portfolio_id == 405
            return SimpleNamespace(
                strategy_id=9,
                code=(
                    "class MyStrategy:\n"
                    '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
                    '    ORDER_TARGETS = []\n'
                    "    def on_market_data(self, data, wallet): return None\n"
                ),
                name="live_skip_preflight",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            calls["save_session"] += 1
            return True

        def update_session(self, **_kwargs) -> bool:
            return True

        def update_portfolio_wallet_state(self, *args, **kwargs):
            del args, kwargs
            return SimpleNamespace()

    class FakeThread:
        def __init__(self, target=None, args=(), daemon=None) -> None:
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self) -> None:
            return None

    class FakeMarketDataClient:
        def __init__(self, _addr: str) -> None:
            pass

        def create_session_market_data_subscriptions(self, **_kwargs) -> bool:
            return True

    captured_preflight_args: dict = {}

    def capturing_preflight(**kwargs):
        from strategy_service.preflight import PreflightResult
        from strategy_service.session import StreamBinding
        captured_preflight_args.update(kwargs)
        # Simulate the real evaluator's "bindings are resolved even when
        # readiness gating is off" behavior: return a StreamBinding for the
        # single declared input, no failures.
        return PreflightResult(
            profile=kwargs["profile"],
            required_streams=[
                StreamBinding(
                    stream_id=1001,
                    exchange="binance",
                    market="futures",
                    kind="kline",
                    symbol="BTCUSDT",
                    interval="1m",
                ),
            ],
        )

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    _install_marketdata_client(monkeypatch, FakeMarketDataClient)
    monkeypatch.setattr(grpc_server, "_live_consumer_group", lambda strategy_id, session_id: f"cg-{strategy_id}-{session_id}")
    monkeypatch.setattr(grpc_server, "_create_session_thread", _AsyncTestThread)
    monkeypatch.setattr(servicer, "_run_profile_preflight", capturing_preflight)

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id != ""
    assert context.code is None
    assert calls["save_session"] == 1
    # Critical invariants:
    # (a) preflight WAS called (bindings must be resolved).
    assert captured_preflight_args, "preflight must run even when readiness is bypassed"
    # (b) readiness gating was explicitly disabled.
    assert captured_preflight_args["require_readiness"] is False
    # (c) the resulting bindings were threaded into the live-runtime state so
    # lease management can find stream_id=1001.
    state = servicer._sessions.get(resp.session_id)
    assert state is not None
    assert [b.stream_id for b in state.required_streams] == [1001]


def test_run_strategy_mode2_uses_strategy_declared_symbols_for_preflight(monkeypatch):
    servicer = StrategyServiceServicer("", "", {}, "127.0.0.1:9092")
    request = SimpleNamespace(
        portfolio_id=406,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=0,
        end_time_ms=0,
        runtime_id="rt-test",
    )
    context = _FakeContext()
    captured: dict[str, list[tuple[str, str]]] = {}
    strategy_code = """
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        return None
"""

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 406
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id, environment=1)

        def preflight_strategy_session(self, **_kwargs):
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, portfolio_id: int):
            assert portfolio_id == 406
            return SimpleNamespace(
                strategy_id=10,
                code=strategy_code,
                name="eth_declared",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            return True

        def update_session(self, **_kwargs) -> bool:
            return True

        def update_portfolio_wallet_state(self, *args, **kwargs):
            del args, kwargs
            return SimpleNamespace()

    class FakeThread:
        def __init__(self, target=None, args=(), daemon=None) -> None:
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self) -> None:
            return None

    class FakeMarketDataClient:
        def __init__(self, _addr: str) -> None:
            pass

        def create_session_market_data_subscriptions(self, **_kwargs) -> bool:
            return True

    def fake_preflight(**kwargs):
        from strategy_service.preflight import PreflightResult, RuntimeSourceProfile
        from strategy_service.session import StreamBinding
        captured["declared_inputs"] = [
            (inp.market, inp.symbol, inp.interval) for inp in kwargs["declared_inputs"]
        ]
        captured["profile"] = kwargs["profile"]
        return PreflightResult(
            profile=RuntimeSourceProfile.DEMO,
            required_streams=[
                StreamBinding(
                    stream_id=1002,
                    exchange="binance",
                    market="futures",
                    kind="kline",
                    symbol="ETHUSDT",
                    interval="1m",
                ),
            ],
        )

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    _install_marketdata_client(monkeypatch, FakeMarketDataClient)
    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)
    monkeypatch.setattr(grpc_server, "_create_session_thread", _AsyncTestThread)

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id != ""
    assert context.code is None
    # Preflight sees ONLY the declared input — wallet-side USDC asset is ignored.
    assert captured["declared_inputs"] == [("perpetual_futures", "ETHUSDT", "1m")]
    from strategy_service.preflight import RuntimeSourceProfile
    assert captured["profile"] is RuntimeSourceProfile.DEMO


def test_run_strategy_mode2_creates_subscriptions_from_required_streams(monkeypatch):
    from strategy_service.preflight import PreflightResult, RuntimeSourceProfile

    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=1,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    def fake_preflight(**_kwargs):
        return PreflightResult(
            profile=RuntimeSourceProfile.DEMO,
            required_streams=[
                StreamBinding(1002, "binance", "futures", "kline", "ETHUSDT", "1m"),
            ],
        )

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)
    request = SimpleNamespace(
        portfolio_id=203,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=0,
        end_time_ms=0,
        runtime_id="rt-test",
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id != ""
    assert context.code is None
    assert calls["session_subscriptions"] == [
        {
            "user_id": 17,
            "session_id": resp.session_id,
            "runtime_id": "rt-test",
            "environment": 1,
            "streams": [StreamBinding(1002, "binance", "futures", "kline", "ETHUSDT", "1m")],
        }
    ]


def test_subscription_baseexception_fails_pending_activation_and_releases(monkeypatch):
    from strategy_service.preflight import PreflightResult, RuntimeSourceProfile

    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=1,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
            "    ORDER_TARGETS = []\n"
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )
    monkeypatch.setattr(
        servicer,
        "_run_profile_preflight",
        lambda **_kwargs: PreflightResult(
            profile=RuntimeSourceProfile.DEMO,
            required_streams=[
                StreamBinding(1002, "binance", "futures", "kline", "ETHUSDT", "1m")
            ],
        ),
    )
    released: list[dict] = []
    monkeypatch.setattr(
        servicer._platform_proxy.marketdata,
        "create_session_market_data_subscriptions",
        lambda **_kwargs: (_ for _ in ()).throw(SystemExit("subscription-secret")),
    )
    monkeypatch.setattr(
        servicer._platform_proxy.marketdata,
        "release_session_market_data_subscriptions",
        lambda **kwargs: released.append(dict(kwargs)) or True,
    )
    context = _FakeContext()

    response = servicer.RunStrategy(
        SimpleNamespace(
            portfolio_id=203,
            user_id=17,
            strategy_path="",
            interval="1m",
            start_time_ms=0,
            end_time_ms=0,
            runtime_id="rt-test",
        ),
        context,
    )

    assert response.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details == "strategy activation failed"
    assert calls["save_session"] == 1
    assert calls["save_kwargs"][0]["initial_status"] == "pending"
    assert calls["update_session"][-1]["status"] == "failed"
    assert servicer._sessions.list_ids() == ()
    assert len(released) == 1


def test_renew_stream_leases_once_updates_heartbeat(monkeypatch):
    servicer = StrategyServiceServicer("", "", {}, "127.0.0.1:9092")
    state = SessionState(environment=1, user_id=17)
    state.configure_live_runtime(
        portfolio_id=101,
        strategy_id=202,
        required_streams=[
            StreamBinding(
                stream_id=11,
                exchange="binance",
                market="futures",
                kind="kline",
                symbol="BTCUSDT",
                interval="1m",
            )
        ],
        consumer_group="strategy-session-202-sess-lease",
    )
    calls: list[tuple[str, int, int, int, int]] = []

    # Phase D2: market-data RPCs moved from core-service to control-panel-service.
    # The lease-renewal path now talks to MarketDataClient, not PortfolioClient.
    class FakeMarketDataClient:
        def __init__(self, _addr: str) -> None:
            pass

        def create_or_renew_market_data_lease(
            self,
            *,
            session_id: str,
            strategy_id: int = 0,
            portfolio_id: int = 0,
            stream_id: int,
            ttl_seconds: int,
        ) -> bool:
            calls.append((session_id, strategy_id, portfolio_id, stream_id, ttl_seconds))
            return True

    _install_marketdata_client(monkeypatch, FakeMarketDataClient)

    assert servicer._renew_stream_leases_once("sess-lease", state) is True
    assert calls == [("sess-lease", 202, 101, 11, servicer._lease_ttl_seconds)]
    assert state.lease_heartbeat_at_ms > 0


def test_lease_heartbeat_loop_renews_until_stop(monkeypatch):
    servicer = StrategyServiceServicer("", "", {}, "127.0.0.1:9092")
    state = SessionState(environment=1)
    calls: list[str] = []

    class FakeStopEvent:
        def __init__(self) -> None:
            self._waits = 0

        def wait(self, timeout=None) -> bool:
            calls.append(f"wait:{timeout}")
            self._waits += 1
            return self._waits >= 2

    monkeypatch.setattr(
        servicer,
        "_renew_stream_leases_once",
        lambda session_id, inner_state: calls.append(f"renew:{session_id}") or True,
    )
    servicer._lease_heartbeat_seconds = 7

    servicer._lease_heartbeat_loop("sess-heartbeat", state, FakeStopEvent())

    assert calls == ["wait:7", "renew:sess-heartbeat", "wait:7"]


def test_release_stream_leases_releases_each_binding(monkeypatch):
    servicer = StrategyServiceServicer("", "", {}, "127.0.0.1:9092")
    state = SessionState(environment=1)
    state.configure_live_runtime(
        portfolio_id=101,
        strategy_id=202,
        required_streams=[
            StreamBinding(11, "binance", "futures", "kline", "BTCUSDT", "1m"),
            StreamBinding(12, "binance", "spot", "kline", "ETHUSDT", "1m"),
        ],
        consumer_group="strategy-session-202-sess-release",
    )
    calls: list[tuple[str, int]] = []

    # Phase D2: market-data RPCs are on MarketDataClient now.
    class FakeMarketDataClient:
        def __init__(self, _addr: str) -> None:
            pass

        def release_market_data_lease(self, *, session_id: str, stream_id: int) -> bool:
            calls.append((session_id, stream_id))
            return True

    _install_marketdata_client(monkeypatch, FakeMarketDataClient)

    servicer._release_stream_leases("sess-release", state)

    assert calls == [("sess-release", 11), ("sess-release", 12)]


def test_run_live_skips_lease_management_when_disabled(monkeypatch):
    events: list[tuple] = []

    class FakeProxy:
        def portfolio_client(self):
            return SimpleNamespace(update_session=lambda **_kwargs: True)

        def marketdata_client(self):
            return _NoopMarketDataClient()

        def order_client(self):
            return _NoopOrderClient()

    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "kafka-1:9092",
        market_data_policy={"lease_management_enabled": False},
        platform_proxy=FakeProxy(),
        restore_running_sessions=False,
    )
    state = SessionState(environment=1)
    state.configure_live_runtime(
        portfolio_id=101,
        strategy_id=202,
        required_streams=[StreamBinding(11, "binance", "futures", "kline", "BTCUSDT", "1m")],
        consumer_group="strategy-session-202-sess-disabled",
    )

    monkeypatch.setitem(
        sys.modules,
        "strategy_service.data_loop",
        types.SimpleNamespace(_adapt_kline=lambda kline, market=None: kline),
    )
    monkeypatch.setattr(
        servicer,
        "_renew_stream_leases_once",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("lease renew should be skipped")),
    )

    class FakeDelivery:
        def iter_session_events(self, *, session_id, required_streams, stop_event):
            events.append(("iter", session_id, [(s.market, s.symbol, s.interval) for s in required_streams]))
            state.transition("stopped")
            return iter(())

    servicer.set_runtime_data_source(FakeDelivery())

    from strategy_service.inputs import StrategyInput
    servicer._run_live(
        "sess-disabled",
        state,
        engine=object(),
        declared_inputs=[StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")],
        strategy_id=202,
    )

    assert events == [
        ("iter", "sess-disabled", [("futures", "BTCUSDT", "1m")]),
    ]
    assert state.status == "stopped"


def test_run_session_backtest_persists_order_fill_before_strategy_end(monkeypatch):
    wallet = _wallet_with_futures_slot()
    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    state = SessionState(environment=0)
    request = SimpleNamespace(interval="1m", start_time_ms=1, end_time_ms=2)
    snapshot_time = 1780274580000
    events: list[tuple] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def update_portfolio_wallet_state(
            self,
            portfolio_id,
            user_id=0,
            future_wallet=None,
            spot_wallet=None,
            snapshot_reason=0,
            strategy_id=0,
            session_id="",
            snapshot_time=None,
        ):
            del future_wallet, spot_wallet
            events.append((
                "wallet_sync",
                snapshot_reason,
                strategy_id,
                session_id,
                portfolio_id,
                user_id,
                snapshot_time,
            ))
            return SimpleNamespace()

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            events.append(("session_update", status, bars_processed, error, session_id))
            return True

    class FakeOrderClient:
        def __init__(self, _addr: str) -> None:
            pass

    class FakeMarketDataClient:
        def release_session_market_data_subscriptions(self, **_kwargs):
            return True

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient("acct:1")

        def order_client(self):
            return FakeOrderClient("order:1")

        def marketdata_client(self):
            return FakeMarketDataClient()

    fake_user = SimpleNamespace(on_order_callback=None)

    class FakeEngine:
        def create_strategy(self, **_kwargs):
            return fake_user

    def fake_run_backtest(session_id, inner_state, engine, req, declared_inputs):
        assert [(i.market, i.symbol, i.interval) for i in declared_inputs] == [
            ("perpetual_futures", "BTCUSDT", "1m"),
        ]
        fake_user.last_market_time = snapshot_time
        fake_user.on_order_callback()
        inner_state.transition("finished", bars=17)

    servicer.set_platform_proxy(FakePlatformProxy())
    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())
    monkeypatch.setattr(servicer, "_run_backtest", fake_run_backtest)

    from strategy_service.inputs import StrategyInput
    fake_user.on_order_callback = lambda: grpc_server._sync_strategy_snapshot(
        servicer._portfolio_client(),
        portfolio_id=101,
        user_id=17,
        environment=0,
        wallet=wallet,
        snapshot_reason=grpc_server.SNAPSHOT_REASON_EVENT,
        strategy_id=202,
        session_id="sess-backtest",
        snapshot_time=getattr(fake_user, "last_market_time", None),
    )
    servicer._run_session(
        session_id="sess-backtest",
        state=state,
        request=request,
        wallet=wallet,
        environment=0,
        portfolio_id=101,
        user_id=17,
        declared_inputs=[StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")],
        engine=FakeEngine(),
        user_strategy=fake_user,
        strategy_id=202,
    )

    assert events == [
        ("wallet_sync", 1, 202, "sess-backtest", 101, 17, snapshot_time),
        ("wallet_sync", 3, 202, "sess-backtest", 101, 17, snapshot_time),
        ("session_update", "finished", 17, "", "sess-backtest"),
    ]


def test_run_session_snapshot_failure_marks_session_recoverable(monkeypatch):
    wallet = _wallet_with_futures_slot()
    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    state = SessionState(environment=0)
    request = SimpleNamespace(interval="1m", start_time_ms=1, end_time_ms=2)
    events: list[tuple] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def update_portfolio_wallet_state(self, **_kwargs):
            raise RuntimeError("runtime platform request timed out")

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            events.append(("session_update", status, bars_processed, error, session_id))
            return True

    class FakeOrderClient:
        def __init__(self, _addr: str) -> None:
            pass

    class FakeMarketDataClient:
        def release_session_market_data_subscriptions(self, **_kwargs):
            return True

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient("acct:1")

        def order_client(self):
            return FakeOrderClient("order:1")

        def marketdata_client(self):
            return FakeMarketDataClient()

    fake_user = SimpleNamespace(on_order_callback=None)

    class FakeEngine:
        def create_strategy(self, **_kwargs):
            return fake_user

    def fake_run_backtest(session_id, inner_state, engine, req, declared_inputs):
        inner_state.transition("finished", bars=9)

    servicer.set_platform_proxy(FakePlatformProxy())
    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())
    monkeypatch.setattr(servicer, "_run_backtest", fake_run_backtest)

    from strategy_service.inputs import StrategyInput
    servicer._run_session(
        session_id="sess-finished-with-snapshot-timeout",
        state=state,
        request=request,
        wallet=wallet,
        environment=0,
        portfolio_id=101,
        user_id=17,
        declared_inputs=[StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")],
        engine=FakeEngine(),
        user_strategy=fake_user,
        strategy_id=202,
    )

    assert state.status == "recoverable"
    assert "failed to persist strategy_end snapshot" in state.error
    assert "runtime platform request timed out" in state.error
    assert events == [
        ("session_update", "recoverable", 9, state.error, "sess-finished-with-snapshot-timeout"),
    ]


def test_finalizer_baseexception_emits_one_terminal_result(monkeypatch) -> None:
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    state = SessionState(environment=0, portfolio_id=101, strategy_id=202, user_id=17)
    terminal_updates: list[tuple[str, str, str]] = []
    terminal_marks: list[tuple[str, SessionState]] = []
    finalizer_calls: list[str] = []

    class FakeEngine:
        @staticmethod
        def raise_if_user_code_fatal() -> None:
            return None

    monkeypatch.setattr(
        servicer,
        "_run_backtest",
        lambda *_args, **_kwargs: state.transition("finished", bars=3),
    )
    monkeypatch.setattr(
        servicer,
        "_release_stream_leases",
        lambda *_args: finalizer_calls.append("release_leases"),
    )
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda *_args: finalizer_calls.append("release_subscriptions"),
    )
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: object())

    def final_snapshot_fatal(*_args, **kwargs):
        reason = int(kwargs["snapshot_reason"])
        finalizer_calls.append(f"snapshot:{reason}")
        if reason == grpc_server.SNAPSHOT_REASON_STRATEGY_END:
            raise SystemExit("finalizer-controlled secret")
        raise KeyboardInterrupt("second-finalizer-controlled secret")

    monkeypatch.setattr(grpc_server, "_sync_strategy_snapshot", final_snapshot_fatal)
    monkeypatch.setattr(
        servicer,
        "_persist_session_status",
        lambda session_id, inner_state, **_kwargs: terminal_updates.append(
            (session_id, inner_state.status, inner_state.error)
        ) or True,
    )
    monkeypatch.setattr(
        servicer._sessions,
        "mark_terminal",
        lambda session_id, inner_state: terminal_marks.append((session_id, inner_state)) or True,
    )

    servicer._run_session(
        session_id="sess-finalizer-fatal",
        state=state,
        request=SimpleNamespace(end_time_ms=2),
        wallet=_wallet_with_futures_slot(),
        environment=0,
        portfolio_id=101,
        user_id=17,
        declared_inputs=[],
        engine=FakeEngine(),
        user_strategy=SimpleNamespace(last_market_time=None),
        strategy_id=202,
        backtest_restore_wallet=object(),
    )

    assert state.status == "failed"
    assert state.error == "strategy session terminated"
    assert terminal_updates == [
        ("sess-finalizer-fatal", "failed", "strategy session terminated"),
    ]
    assert terminal_marks == [("sess-finalizer-fatal", state)]
    assert finalizer_calls == [
        "release_leases",
        "release_subscriptions",
        f"snapshot:{grpc_server.SNAPSHOT_REASON_STRATEGY_END}",
        "snapshot:0",
    ]


def test_backtest_stop_and_close_finalizer_does_not_restore_pre_run_wallet(monkeypatch) -> None:
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    state = SessionState(environment=0, portfolio_id=101, strategy_id=202, user_id=17)
    state.remember_stop_operation_id("stop-operation-1")
    runtime_wallet = object()
    pre_run_wallet = object()
    snapshot_calls: list[tuple[int, object]] = []

    class FakeEngine:
        @staticmethod
        def raise_if_user_code_fatal() -> None:
            return None

    monkeypatch.setattr(
        servicer,
        "_run_backtest",
        lambda *_args, **_kwargs: state.transition("stopped"),
    )
    monkeypatch.setattr(servicer, "_release_stream_leases", lambda *_args: True)
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda *_args: True,
    )
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: object())
    monkeypatch.setattr(
        grpc_server,
        "_sync_strategy_snapshot",
        lambda *_args, **kwargs: snapshot_calls.append(
            (int(kwargs["snapshot_reason"]), kwargs["wallet"])
        ),
    )
    monkeypatch.setattr(servicer, "_persist_session_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(servicer._sessions, "mark_terminal", lambda *_args: True)

    servicer._run_session(
        session_id="sess-stop-close-finalizer",
        state=state,
        request=SimpleNamespace(end_time_ms=2),
        wallet=runtime_wallet,
        environment=0,
        portfolio_id=101,
        user_id=17,
        declared_inputs=[],
        engine=FakeEngine(),
        user_strategy=SimpleNamespace(last_market_time=None),
        strategy_id=202,
        backtest_restore_wallet=pre_run_wallet,
    )

    assert snapshot_calls == [
        (grpc_server.SNAPSHOT_REASON_STRATEGY_END, runtime_wallet),
    ]


def test_user_fatal_overrides_recoverable_finalizer_and_second_fatal_once(
    monkeypatch,
    caplog,
) -> None:
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    state = SessionState(environment=0, portfolio_id=101, strategy_id=202, user_id=17)
    state.latch_user_code_fatal("callback")
    fatal = strategy_base.StrategyUserCodeFatalError(stage="callback")

    class FatalEngine:
        @staticmethod
        def raise_if_user_code_fatal() -> None:
            raise fatal

    second_fatal_results: list[bool] = []

    def ordinary_finalizer_failure(*_args) -> None:
        second_fatal_results.append(state.latch_user_code_fatal("attribute"))
        raise RuntimeError("ordinary-finalizer-log-canary")

    monkeypatch.setattr(servicer, "_release_stream_leases", ordinary_finalizer_failure)
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda *_args: True,
    )
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: object())
    monkeypatch.setattr(grpc_server, "_sync_strategy_snapshot", lambda *_args, **_kwargs: None)
    terminal_updates: list[tuple[str, str, str]] = []
    terminal_marks: list[tuple[str, SessionState]] = []
    monkeypatch.setattr(
        servicer,
        "_persist_session_status",
        lambda session_id, inner_state, **_kwargs: terminal_updates.append(
            (session_id, inner_state.status, inner_state.error)
        ) or True,
    )
    monkeypatch.setattr(
        servicer._sessions,
        "mark_terminal",
        lambda session_id, inner_state: terminal_marks.append(
            (session_id, inner_state)
        ) or True,
    )

    with caplog.at_level("WARNING"):
        servicer._run_session(
            session_id="e" * 32,
            state=state,
            request=SimpleNamespace(end_time_ms=2),
            wallet=_wallet_with_futures_slot(),
            environment=0,
            portfolio_id=101,
            user_id=17,
            declared_inputs=[],
            engine=FatalEngine(),
            user_strategy=SimpleNamespace(last_market_time=None),
            strategy_id=202,
        )

    assert second_fatal_results == [False]
    assert state.user_code_fatal_stage == "callback"
    assert state.status == "failed"
    assert state.error == "strategy user code terminated"
    assert terminal_updates == [
        ("e" * 32, "failed", "strategy user code terminated")
    ]
    assert terminal_marks == [("e" * 32, state)]
    assert [record.getMessage() for record in caplog.records] == [
        f"STRATEGY_SESSION_STREAM_LEASE_RELEASE_FAILED session={'e' * 32}",
        (
            f"STRATEGY_USER_CODE_FATAL session={'e' * 32} "
            "portfolio_id=101 strategy_id=202"
        ),
    ]
    assert "ordinary-finalizer-log-canary" not in caplog.text
    assert "Traceback" not in caplog.text


def test_user_fatal_survives_finalizer_baseexception_and_all_cleanup_is_attempted(
    monkeypatch,
    caplog,
) -> None:
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    state = SessionState(environment=0, portfolio_id=101, strategy_id=202, user_id=17)
    fatal = strategy_base.StrategyUserCodeFatalError(stage="callback")
    cleanup_calls: list[str] = []

    class FatalEngine:
        @staticmethod
        def raise_if_user_code_fatal() -> None:
            raise fatal

    def fatal_lease_release(*_args) -> None:
        cleanup_calls.append("release_leases")
        raise SystemExit("finalizer-fatal-log-canary")

    monkeypatch.setattr(servicer, "_release_stream_leases", fatal_lease_release)
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda *_args: cleanup_calls.append("release_subscriptions") or True,
    )
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: object())
    monkeypatch.setattr(
        grpc_server,
        "_sync_strategy_snapshot",
        lambda *_args, **_kwargs: cleanup_calls.append("snapshot"),
    )
    terminal_updates: list[tuple[str, str, str]] = []
    terminal_marks: list[tuple[str, SessionState]] = []
    monkeypatch.setattr(
        servicer,
        "_persist_session_status",
        lambda session_id, inner_state, **_kwargs: terminal_updates.append(
            (session_id, inner_state.status, inner_state.error)
        ) or True,
    )
    monkeypatch.setattr(
        servicer._sessions,
        "mark_terminal",
        lambda session_id, inner_state: terminal_marks.append(
            (session_id, inner_state)
        ) or True,
    )

    with caplog.at_level("ERROR"):
        servicer._run_session(
            session_id="d" * 32,
            state=state,
            request=SimpleNamespace(end_time_ms=2),
            wallet=_wallet_with_futures_slot(),
            environment=0,
            portfolio_id=101,
            user_id=17,
            declared_inputs=[],
            engine=FatalEngine(),
            user_strategy=SimpleNamespace(last_market_time=None),
            strategy_id=202,
        )

    assert cleanup_calls == ["release_leases", "release_subscriptions", "snapshot"]
    assert state.status == "failed"
    assert state.error == "strategy user code terminated"
    assert terminal_updates == [
        ("d" * 32, "failed", "strategy user code terminated")
    ]
    assert terminal_marks == [("d" * 32, state)]
    assert [record.getMessage() for record in caplog.records] == [
        (
            f"STRATEGY_USER_CODE_FATAL session={'d' * 32} "
            "portfolio_id=101 strategy_id=202"
        )
    ]
    assert "finalizer-fatal-log-canary" not in caplog.text
    assert "Traceback" not in caplog.text


@pytest.mark.parametrize("fatal_type", [RuntimeError, SystemExit])
def test_user_fatal_survives_terminal_persist_baseexception(
    monkeypatch,
    caplog,
    fatal_type,
) -> None:
    patches = []
    platform_proxy = SimpleNamespace(
        send_session_status_patch=lambda **kwargs: patches.append(dict(kwargs))
    )
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
        platform_proxy=platform_proxy,
    )
    state = SessionState(environment=0, portfolio_id=101, strategy_id=202, user_id=17)
    state.latch_user_code_fatal("callback")
    fatal = strategy_base.StrategyUserCodeFatalError(stage="callback")
    updates = []

    class FatalEngine:
        @staticmethod
        def raise_if_user_code_fatal() -> None:
            raise fatal

    class FailingPortfolioClient:
        @staticmethod
        def update_session(**kwargs):
            updates.append(dict(kwargs))
            raise fatal_type("terminal-persist-log-canary")

    monkeypatch.setattr(servicer, "_portfolio_client", FailingPortfolioClient)
    monkeypatch.setattr(servicer, "_release_stream_leases", lambda *_args: None)
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda *_args: True,
    )
    monkeypatch.setattr(grpc_server, "_sync_strategy_snapshot", lambda *_args, **_kwargs: None)
    terminal_marks = []
    monkeypatch.setattr(
        servicer._sessions,
        "mark_terminal",
        lambda session_id, inner_state: terminal_marks.append(
            (session_id, inner_state)
        ) or True,
    )

    with caplog.at_level("ERROR"):
        servicer._run_session(
            session_id="9" * 32,
            state=state,
            request=SimpleNamespace(end_time_ms=2),
            wallet=_wallet_with_futures_slot(),
            environment=0,
            portfolio_id=101,
            user_id=17,
            declared_inputs=[],
            engine=FatalEngine(),
            user_strategy=SimpleNamespace(last_market_time=None),
            strategy_id=202,
        )

    assert state.status == "failed"
    assert state.error == "strategy user code terminated"
    assert state.user_code_fatal_stage == "callback"
    assert updates == [
        {
            "session_id": "9" * 32,
            "status": "failed",
            "bars_processed": 0,
            "error": "strategy user code terminated",
            "runtime_id": "",
        }
    ]
    assert patches == updates
    assert terminal_marks == [("9" * 32, state)]
    assert [record.getMessage() for record in caplog.records] == [
        (
            f"STRATEGY_USER_CODE_FATAL session={'9' * 32} "
            "portfolio_id=101 strategy_id=202"
        ),
        (
            f"STRATEGY_SESSION_TERMINAL_PERSIST_FAILED session={'9' * 32} "
            "portfolio_id=101 strategy_id=202"
        ),
    ]
    assert "terminal-persist-log-canary" not in caplog.text
    assert "Traceback" not in caplog.text


def test_run_session_ordinary_failures_log_only_fixed_events(monkeypatch, caplog) -> None:
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    state = SessionState(environment=0, portfolio_id=101, strategy_id=202, user_id=17)

    class FailingStopEvent:
        @staticmethod
        def set() -> None:
            raise RuntimeError("lease-stop-log-canary")

    class FakeEngine:
        @staticmethod
        def raise_if_user_code_fatal() -> None:
            return None

    state.lease_stop_event = FailingStopEvent()
    monkeypatch.setattr(
        servicer,
        "_run_backtest",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("business-log-canary")
        ),
    )
    monkeypatch.setattr(
        servicer,
        "_release_stream_leases",
        lambda *_args: (_ for _ in ()).throw(
            RuntimeError("stream-release-log-canary")
        ),
    )
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda *_args: (_ for _ in ()).throw(
            RuntimeError("subscription-release-log-canary")
        ),
    )
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: object())

    def fail_snapshot(*_args, **kwargs):
        if int(kwargs["snapshot_reason"]) == grpc_server.SNAPSHOT_REASON_STRATEGY_END:
            raise RuntimeError("end-snapshot-log-canary")
        raise RuntimeError("restore-snapshot-log-canary")

    monkeypatch.setattr(grpc_server, "_sync_strategy_snapshot", fail_snapshot)
    terminal_updates: list[tuple[str, str, str]] = []
    terminal_marks: list[tuple[str, SessionState]] = []
    monkeypatch.setattr(
        servicer,
        "_persist_session_status",
        lambda session_id, inner_state, **_kwargs: terminal_updates.append(
            (session_id, inner_state.status, inner_state.error)
        ) or True,
    )
    monkeypatch.setattr(
        servicer._sessions,
        "mark_terminal",
        lambda session_id, inner_state: terminal_marks.append(
            (session_id, inner_state)
        ) or True,
    )

    with caplog.at_level("WARNING"):
        servicer._run_session(
            session_id="b" * 32,
            state=state,
            request=SimpleNamespace(end_time_ms=2),
            wallet=_wallet_with_futures_slot(),
            environment=0,
            portfolio_id=101,
            user_id=17,
            declared_inputs=[],
            engine=FakeEngine(),
            user_strategy=SimpleNamespace(last_market_time=None),
            strategy_id=202,
            backtest_restore_wallet=object(),
        )

    assert [record.getMessage() for record in caplog.records] == [
        f"STRATEGY_SESSION_ERROR session={'b' * 32} portfolio_id=101 strategy_id=202",
        f"STRATEGY_SESSION_LEASE_STOP_FAILED session={'b' * 32}",
        f"STRATEGY_SESSION_STREAM_LEASE_RELEASE_FAILED session={'b' * 32}",
        f"STRATEGY_SESSION_SUBSCRIPTION_RELEASE_FAILED session={'b' * 32}",
        f"STRATEGY_SESSION_END_SNAPSHOT_FAILED session={'b' * 32}",
        f"STRATEGY_SESSION_BACKTEST_RESTORE_FAILED session={'b' * 32}",
    ]
    for canary in (
        "business-log-canary",
        "lease-stop-log-canary",
        "stream-release-log-canary",
        "subscription-release-log-canary",
        "end-snapshot-log-canary",
        "restore-snapshot-log-canary",
        "tests/test_grpc_server.py",
        "raise RuntimeError",
        "Traceback",
    ):
        assert canary not in caplog.text
    assert len(terminal_updates) == 1
    assert terminal_marks == [("b" * 32, state)]


def test_run_session_finalizer_client_failure_log_is_fixed(monkeypatch, caplog) -> None:
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    state = SessionState(environment=0, portfolio_id=101, strategy_id=202, user_id=17)

    class FakeEngine:
        @staticmethod
        def raise_if_user_code_fatal() -> None:
            return None

    monkeypatch.setattr(
        servicer,
        "_run_backtest",
        lambda *_args, **_kwargs: state.transition("finished", bars=1),
    )
    monkeypatch.setattr(servicer, "_release_stream_leases", lambda *_args: None)
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda *_args: True,
    )
    portfolio_client_calls = 0

    def finalizer_client_failure():
        nonlocal portfolio_client_calls
        portfolio_client_calls += 1
        if portfolio_client_calls == 1:
            return object()
        raise RuntimeError("finalizer-client-log-canary")

    monkeypatch.setattr(servicer, "_portfolio_client", finalizer_client_failure)
    monkeypatch.setattr(servicer, "_persist_session_status", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(servicer._sessions, "mark_terminal", lambda *_args: True)

    with caplog.at_level("WARNING"):
        servicer._run_session(
            session_id="c" * 32,
            state=state,
            request=SimpleNamespace(end_time_ms=2),
            wallet=_wallet_with_futures_slot(),
            environment=0,
            portfolio_id=101,
            user_id=17,
            declared_inputs=[],
            engine=FakeEngine(),
            user_strategy=SimpleNamespace(last_market_time=None),
            strategy_id=202,
        )

    assert [record.getMessage() for record in caplog.records] == [
        f"STRATEGY_SESSION_FINALIZER_CLIENT_FAILED session={'c' * 32}"
    ]
    assert "finalizer-client-log-canary" not in caplog.text
    assert "Traceback" not in caplog.text


def test_indicator_collection_failure_logs_are_fixed_and_redacted(monkeypatch, caplog) -> None:
    definition = IndicatorDefinition(
        key="alpha",
        name="Alpha",
        type="line",
        pane="strategy",
    )
    state = SessionState(environment=1, user_id=17, strategy_id=202)

    def make_servicer_and_strategy():
        servicer = StrategyServiceServicer(
            "acct:1",
            "order:1",
            {},
            "127.0.0.1:9092",
            restore_running_sessions=False,
        )
        strategy = SimpleNamespace(
            indicator_definitions=[definition],
            on_indicator_frame=None,
        )
        return servicer, strategy, SimpleNamespace(strategies={"active": strategy})

    client_servicer, _client_strategy, client_engine = make_servicer_and_strategy()

    def unavailable_client():
        raise RuntimeError("indicator-client-log-canary")

    monkeypatch.setattr(client_servicer, "_portfolio_client", unavailable_client)

    sink_servicer, sink_strategy, sink_engine = make_servicer_and_strategy()
    monkeypatch.setattr(sink_servicer, "_portfolio_client", lambda: object())

    def failing_sink(**_kwargs):
        raise RuntimeError("indicator-sink-log-canary")

    sink_servicer._indicator_frame_sink = failing_sink

    missing_save_servicer, missing_save_strategy, missing_save_engine = (
        make_servicer_and_strategy()
    )
    monkeypatch.setattr(missing_save_servicer, "_portfolio_client", lambda: object())

    save_servicer, save_strategy, save_engine = make_servicer_and_strategy()

    class FailingSaveClient:
        @staticmethod
        def save_strategy_indicators(**_kwargs):
            raise RuntimeError("indicator-save-log-canary")

    monkeypatch.setattr(save_servicer, "_portfolio_client", FailingSaveClient)

    with caplog.at_level("WARNING"):
        client_servicer._install_indicator_collection(
            "indicator-log-session",
            state,
            client_engine,
        )
        sink_servicer._install_indicator_collection(
            "indicator-log-session",
            state,
            sink_engine,
        )
        with pytest.raises(RuntimeError, match="indicator-sink-log-canary"):
            sink_strategy.on_indicator_frame(
                "indicator-stream-key-log-canary",
                0,
                1,
                1,
                IndicatorFrame(values={"alpha": 1.0}),
            )
        missing_save_servicer._install_indicator_collection(
            "indicator-log-session",
            state,
            missing_save_engine,
        )
        missing_save_strategy.on_indicator_frame(
            "indicator-stream-key-log-canary",
            0,
            1,
            1,
            IndicatorFrame(values={"alpha": 1.0}),
        )
        save_servicer._install_indicator_collection(
            "indicator-log-session",
            state,
            save_engine,
        )
        save_strategy.on_indicator_frame(
            "indicator-stream-key-log-canary",
            0,
            1,
            1,
            IndicatorFrame(values={"alpha": 1.0}),
        )

    assert [record.getMessage() for record in caplog.records] == [
        "STRATEGY_INDICATOR_CLIENT_UNAVAILABLE session=indicator-log-session strategy_id=202",
        "STRATEGY_INDICATOR_SAVE_UNAVAILABLE session=indicator-log-session strategy_id=202",
        "STRATEGY_INDICATOR_SAVE_FAILED session=indicator-log-session strategy_id=202",
    ]
    assert all(record.exc_info is None for record in caplog.records)
    for canary in (
        "indicator-client-log-canary",
        "indicator-sink-log-canary",
        "indicator-save-log-canary",
        "indicator-stream-key-log-canary",
        "Traceback",
    ):
        assert canary not in caplog.text


def test_agent_indicator_sink_uses_v2_sequence_and_retries_first_definitions(monkeypatch):
    definition = IndicatorDefinition(
        key="alpha",
        name="Alpha",
        type="line",
        pane="strategy",
    )
    state = SessionState(environment=1, user_id=17, strategy_id=202)
    strategy = SimpleNamespace(
        indicator_definitions=[definition],
        on_indicator_frame=None,
    )
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    monkeypatch.setattr(
        servicer,
        "_portfolio_client",
        lambda: (_ for _ in ()).throw(
            AssertionError("agent-managed V2 must not acquire portfolio client")
        ),
    )
    calls = []

    def sink(**kwargs):
        calls.append(kwargs)
        if len(calls) == 1:
            raise RuntimeError("first transport attempt failed")

    servicer._indicator_frame_sink = sink
    servicer._install_indicator_collection(
        "indicator-v2-session",
        state,
        SimpleNamespace(strategies={"active": strategy}),
    )

    with pytest.raises(RuntimeError, match="first transport attempt failed"):
        strategy.on_indicator_frame(
            "binance:perpetual_futures:TESTUSDT:1m",
            0,
            1_000,
            60_000,
            IndicatorFrame(values={"alpha": 1.0}),
        )
    strategy.on_indicator_frame(
        "binance:perpetual_futures:TESTUSDT:1m",
        0,
        1_000,
        60_000,
        IndicatorFrame(values={"alpha": 1.0}),
    )
    strategy.on_indicator_frame(
        "binance:perpetual_futures:TESTUSDT:1m",
        1,
        61_000,
        60_000,
        IndicatorFrame(),
    )

    assert [call["stream_sequence"] for call in calls] == [0, 0, 1]
    assert len(calls[0]["definitions"]) == 1
    assert len(calls[1]["definitions"]) == 1
    assert calls[2]["definitions"] == []


def test_backtest_indicator_flush_baseexception_preserves_exact_user_fatal(
    monkeypatch,
) -> None:
    from market_data.models import MarketKline
    from strategy_service.inputs import StrategyInput

    fatal = strategy_base.StrategyUserCodeFatalError(stage="callback")
    flush_calls: list[str] = []

    class OneBarMarketDataClient:
        @staticmethod
        def fetch_backtest_page(**_kwargs):
            return SimpleNamespace(
                klines=[
                    MarketKline(
                        symbol="BTCUSDT",
                        interval="1m",
                        open_time=1,
                        close_time=2,
                        open=1.0,
                        high=2.0,
                        low=0.5,
                        close=1.5,
                        volume=10.0,
                        timestamp=2,
                        market="futures",
                    )
                ],
                next_cursor_time_ms=1,
                has_more=False,
            )

    class FatalEngine:
        strategies = {}

        @staticmethod
        def raise_if_user_code_fatal() -> None:
            raise fatal

    def fatal_flush() -> None:
        flush_calls.append("flush")
        raise SystemExit("indicator-flush-canary")

    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    monkeypatch.setattr(servicer, "_marketdata_client", OneBarMarketDataClient)
    monkeypatch.setattr(
        servicer,
        "_install_indicator_collection",
        lambda *_args, **_kwargs: fatal_flush,
    )

    with pytest.raises(strategy_base.StrategyUserCodeFatalError) as captured:
        servicer._run_backtest_via_platform_proxy(
            session_id="backtest-indicator-fatal",
            state=SessionState(environment=0),
            engine=FatalEngine(),
            request=SimpleNamespace(start_time_ms=1, end_time_ms=2, user_id=17),
            declared_inputs=[
                StrategyInput(
                    "binance",
                    "perpetual_futures",
                    "BTCUSDT",
                    "1m",
                )
            ],
        )

    assert captured.value is fatal
    assert flush_calls == ["flush"]


def test_live_indicator_flush_baseexception_preserves_exact_user_fatal(
    monkeypatch,
) -> None:
    fatal = strategy_base.StrategyUserCodeFatalError(stage="callback")
    flush_calls: list[str] = []

    class OneEventDelivery:
        @staticmethod
        def iter_session_events(**_kwargs):
            yield SimpleNamespace(kind="kline", payload=object())

    class FatalEngine:
        strategies = {}

        @staticmethod
        def raise_if_user_code_fatal() -> None:
            raise fatal

    def fatal_flush() -> None:
        flush_calls.append("flush")
        raise GeneratorExit("indicator-flush-canary")

    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    servicer._lease_management_enabled = False
    servicer.set_runtime_data_source(OneEventDelivery())
    monkeypatch.setattr(
        servicer,
        "_install_indicator_collection",
        lambda *_args, **_kwargs: fatal_flush,
    )

    with pytest.raises(strategy_base.StrategyUserCodeFatalError) as captured:
        servicer._run_live_via_platform_proxy(
            "live-indicator-fatal",
            SessionState(environment=1),
            FatalEngine(),
        )

    assert captured.value is fatal
    assert flush_calls == ["flush"]


def test_live_progress_persist_failure_log_is_fixed_and_redacted(monkeypatch, caplog) -> None:
    from market_data.models import MarketKline

    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    servicer._lease_management_enabled = False

    class OneBarDelivery:
        @staticmethod
        def iter_session_events(**_kwargs):
            yield SimpleNamespace(
                kind="kline",
                payload=MarketKline(
                    symbol="BTCUSDT",
                    interval="1m",
                    open_time=1,
                    close_time=2,
                    open=1.0,
                    high=2.0,
                    low=0.5,
                    close=1.5,
                    volume=10.0,
                    timestamp=2,
                    market="futures",
                ),
            )

    class FakeEngine:
        strategies = {}

        @staticmethod
        def running_strategy(_market_data):
            return True

    state = SessionState(environment=1, portfolio_id=101, strategy_id=202)
    state.configure_live_runtime(
        portfolio_id=101,
        strategy_id=202,
        required_streams=[],
        consumer_group="strategy-session-202-live-log-session",
    )
    servicer.set_runtime_data_source(OneBarDelivery())

    def fail_progress_persist(*_args, **_kwargs):
        raise RuntimeError("live-progress-log-canary")

    monkeypatch.setattr(servicer, "_persist_session_status", fail_progress_persist)

    with caplog.at_level("WARNING"):
        servicer._run_live_via_platform_proxy("live-log-session", state, FakeEngine())

    assert [record.getMessage() for record in caplog.records] == [
        "STRATEGY_LIVE_PROGRESS_PERSIST_FAILED session=live-log-session strategy_id=202"
    ]
    assert caplog.records[0].exc_info is None
    assert "live-progress-log-canary" not in caplog.text
    assert "Traceback" not in caplog.text


def test_run_session_live_finalizes_strategy_end_before_session_update(monkeypatch):
    wallet = _wallet_with_futures_slot()
    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    state = SessionState(environment=1)
    request = SimpleNamespace(interval="1m", start_time_ms=0, end_time_ms=0)
    events: list[tuple] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def update_portfolio_wallet_state(
            self,
            portfolio_id,
            user_id=0,
            future_wallet=None,
            spot_wallet=None,
            snapshot_reason=0,
            strategy_id=0,
            session_id="",
            snapshot_time=None,
        ):
            del future_wallet, spot_wallet, snapshot_time
            events.append(("wallet_sync", snapshot_reason, strategy_id, session_id, portfolio_id, user_id))
            return SimpleNamespace()

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            events.append(("session_update", status, bars_processed, error, session_id))
            return True

    class FakeOrderClient:
        def __init__(self, _addr: str) -> None:
            pass

    class FakeMarketDataClient:
        def release_session_market_data_subscriptions(self, **_kwargs):
            return True

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient("acct:1")

        def order_client(self):
            return FakeOrderClient("order:1")

        def marketdata_client(self):
            return FakeMarketDataClient()

    fake_user = SimpleNamespace(on_order_callback=None)

    class FakeEngine:
        def create_strategy(self, **_kwargs):
            return fake_user

        def running_strategy(self, _md):
            pass

    def fake_run_live(session_id, inner_state, engine, declared_inputs, strategy_id):
        assert [(i.market, i.symbol, i.interval) for i in declared_inputs] == [
            ("perpetual_futures", "BTCUSDT", "1m"),
        ]
        assert strategy_id == 404
        inner_state.transition("stopped")

    servicer.set_platform_proxy(FakePlatformProxy())
    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())
    monkeypatch.setattr(servicer, "_run_live", fake_run_live)

    from strategy_service.inputs import StrategyInput
    servicer._run_session(
        session_id="sess-live",
        state=state,
        request=request,
        wallet=wallet,
        environment=1,
        portfolio_id=303,
        user_id=17,
        declared_inputs=[StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")],
        engine=FakeEngine(),
        user_strategy=fake_user,
        strategy_id=404,
    )

    assert events == [
        ("wallet_sync", 3, 404, "sess-live", 303, 17),
        ("session_update", "stopped", 0, "", "sess-live"),
    ]


def test_run_session_failure_persists_failed_status_and_error(monkeypatch):
    wallet = _wallet_with_futures_slot()
    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    state = SessionState(environment=0)
    request = SimpleNamespace(interval="1m", start_time_ms=1, end_time_ms=2)
    events: list[tuple] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def update_portfolio_wallet_state(
            self,
            portfolio_id,
            user_id=0,
            future_wallet=None,
            spot_wallet=None,
            snapshot_reason=0,
            strategy_id=0,
            session_id="",
            snapshot_time=None,
        ):
            del future_wallet, spot_wallet, snapshot_time
            events.append(("wallet_sync", snapshot_reason, strategy_id, session_id, portfolio_id, user_id))
            return SimpleNamespace()

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            events.append(("session_update", status, bars_processed, error, session_id))
            return True

    class FakeOrderClient:
        def __init__(self, _addr: str) -> None:
            pass

    fake_user = SimpleNamespace(on_order_callback=None)

    class FakeEngine:
        def create_strategy(self, **_kwargs):
            return fake_user

    def fake_run_backtest(session_id, inner_state, engine, req, declared_inputs):
        raise RuntimeError("schema mismatch from downstream feed")

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    _install_order_client(monkeypatch, FakeOrderClient)
    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())
    monkeypatch.setattr(servicer, "_run_backtest", fake_run_backtest)

    from strategy_service.inputs import StrategyInput
    servicer._run_session(
        session_id="sess-failed",
        state=state,
        request=request,
        wallet=wallet,
        environment=0,
        portfolio_id=505,
        user_id=17,
        declared_inputs=[StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")],
        engine=FakeEngine(),
        user_strategy=fake_user,
        strategy_id=606,
    )

    assert state.status == "failed"
    assert state.error == "schema mismatch from downstream feed"
    assert events == [
        ("wallet_sync", 3, 606, "sess-failed", 505, 17),
        ("session_update", "failed", 0, "schema mismatch from downstream feed", "sess-failed"),
    ]


def test_get_strategy_status_hides_other_users_session():
    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    session_id, _ = servicer._sessions.create(environment=0, user_id=17)
    context = _FakeContext()

    resp = servicer.GetStrategyStatus(
        SimpleNamespace(session_id=session_id, user_id=99),
        context,
    )

    assert resp.status == ""
    assert context.code == grpc.StatusCode.NOT_FOUND
    assert "not found" in context.details


def test_get_strategy_status_surfaces_running_session_error_without_stopping():
    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    session_id, state = servicer._sessions.create(
        environment=1,
        user_id=17,
        runtime_id="bare-17-debug-local",
        runtime_source="bare",
    )
    state.record_runtime_error("user strategy on_market_data failed: NameError: name 'b' is not defined")
    context = _FakeContext()

    resp = servicer.GetStrategyStatus(
        SimpleNamespace(session_id=session_id, user_id=17, runtime_id="bare-17-debug-local"),
        context,
    )

    assert resp.status == "running"
    assert resp.error == "user strategy on_market_data failed: NameError: name 'b' is not defined"
    assert context.code is None


def test_stop_strategy_hides_other_users_session():
    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    session_id, _ = servicer._sessions.create(environment=0, user_id=17)
    context = _FakeContext()

    resp = servicer.StopStrategy(
        SimpleNamespace(session_id=session_id, user_id=99),
        context,
    )

    assert resp.stopped is False
    assert context.code == grpc.StatusCode.NOT_FOUND
    assert "not found" in context.details


def test_run_strategy_maps_portfolio_active_session_conflict(monkeypatch):
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        market_data_policy={"preflight_enabled": False},
        restore_running_sessions=False,
        runtime_id="rt-test",
    )
    request = SimpleNamespace(
        portfolio_id=303,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    class ActiveSessionRpcError(grpc.RpcError):
        def code(self):
            return grpc.StatusCode.FAILED_PRECONDITION

        def details(self):
            return "portfolio already has an active session"

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 303
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

        def preflight_strategy_session(self, **_kwargs):
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, _portfolio_id: int):
            return SimpleNamespace(
                strategy_id=7,
                code=(
                    "class MyStrategy:\n"
                    '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
                    '    ORDER_TARGETS = []\n'
                    "    def on_market_data(self, data, wallet): return None\n"
                ),
                name="buy_once",
                version="v1",
            )

        def require_save_session(self, **_kwargs) -> None:
            raise ActiveSessionRpcError()

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "already has an active session" in context.details


def test_futures_stop_paths_unchanged_stop_strategy_stop_only_persists_state_and_halts_runtime():
    updates: list[tuple[str, str, int, str]] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            updates.append((session_id, status, bars_processed, error))
            return True

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient("acct:1")

    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092")
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(environment=1, user_id=17, portfolio_id=404)
    state.configure_stop_runtime(order_client=SimpleNamespace(
        list_order_lifecycle_events=lambda **_kwargs: [],
    ))
    stop_event = threading.Event()
    state._stop_event = stop_event  # type: ignore[attr-defined]
    state.lease_stop_event = threading.Event()
    context = _FakeContext()

    resp = servicer.StopStrategy(
        SimpleNamespace(
            session_id=session_id,
            user_id=17,
            stop_action=pb2.STOP_ACTION_STOP_ONLY,
        ),
        context,
    )

    assert resp.stopped is True
    assert context.code is None
    assert state.status == "stopped"
    assert stop_event.is_set() is True
    assert state.lease_stop_event is not None and state.lease_stop_event.is_set() is True
    assert updates[-1] == (session_id, "stopped", 0, "")


def test_stop_strategy_persists_runtime_guard(monkeypatch):
    updates: list[tuple[str, str]] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            updates.append((session_id, runtime_id))
            return True

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        runtime_id="rt-owned",
        restore_running_sessions=False,
    )
    session_id, state = servicer._sessions.create(
        environment=1,
        user_id=17,
        portfolio_id=404,
        runtime_id="rt-owned",
    )
    state.configure_stop_runtime(order_client=SimpleNamespace(
        list_order_lifecycle_events=lambda **_kwargs: [],
    ))
    context = _FakeContext()

    resp = servicer.StopStrategy(
        SimpleNamespace(
            session_id=session_id,
            user_id=17,
            runtime_id="rt-owned",
            stop_action=pb2.STOP_ACTION_STOP_ONLY,
        ),
        context,
    )

    assert resp.stopped is True
    assert context.code is None
    assert updates == [
        (session_id, "rt-owned"),
        (session_id, "rt-owned"),
    ]


def test_stop_strategy_terminal_runtime_state_is_idempotent():
    updates: list[tuple[str, str, int, str, str]] = []

    class FakePortfolioClient:
        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            updates.append((session_id, status, bars_processed, error, runtime_id))
            return True

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        runtime_id="rt-owned",
        restore_running_sessions=False,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(
        environment=0,
        user_id=17,
        portfolio_id=404,
        runtime_id="rt-owned",
    )
    state.transition("finished", bars=2047)
    context = _FakeContext()

    resp = servicer.StopStrategy(
        SimpleNamespace(
            session_id=session_id,
            user_id=17,
            runtime_id="rt-owned",
            stop_action=pb2.STOP_ACTION_STOP_ONLY,
        ),
        context,
    )

    assert resp.stopped is True
    assert context.code is None
    assert state.status == "finished"
    assert updates[-1] == (session_id, "finished", 2047, "", "rt-owned")


def test_status_rejects_non_owning_runtime():
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        runtime_id="rt-owned",
        restore_running_sessions=False,
    )
    session_id, _state = servicer._sessions.create(
        environment=1,
        user_id=17,
        portfolio_id=404,
        runtime_id="rt-owned",
    )
    context = _FakeContext()

    resp = servicer.GetStrategyStatus(
        SimpleNamespace(session_id=session_id, user_id=17, runtime_id="rt-other"),
        context,
    )

    assert resp.status == ""
    assert context.code == grpc.StatusCode.PERMISSION_DENIED
    assert "runtime_id mismatch" in context.details


def test_stop_strategy_finish_persists_finished_and_halts_runtime():
    updates: list[tuple[str, str, int, str]] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            updates.append((session_id, status, bars_processed, error))
            return True

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient("acct:1")

    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092")
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(environment=1, user_id=17, portfolio_id=404)
    stop_event = threading.Event()
    state._stop_event = stop_event  # type: ignore[attr-defined]
    state.lease_stop_event = threading.Event()
    context = _FakeContext()

    resp = servicer.StopStrategy(
        SimpleNamespace(
            session_id=session_id,
            user_id=17,
            stop_action=pb2.STOP_ACTION_FINISH,
        ),
        context,
    )

    assert resp.stopped is True
    assert context.code is None
    assert state.status == "finished"
    assert stop_event.is_set() is True
    assert state.lease_stop_event is not None and state.lease_stop_event.is_set() is True
    assert updates[0][1] == "stopping"
    assert updates[-1][1] == "finished"


def test_futures_stop_paths_unchanged_stop_strategy_stop_and_close_backtest_futures_flattens_wallet(monkeypatch):
    route_wallet = make_backtest_wallet(
        futures_positions=[{
            "symbol": "ETHUSDT",
            "position_qty": 0.02,
            "entry_price": 2300.0,
            "mark_price": 2310.0,
            "margin_mode": "cross",
        }],
    )
    wallet = PortfolioWalletRuntime(
        portfolio_id=505,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): route_wallet},
    )
    updates: list[tuple[str, str, int, str]] = []
    wallet_syncs: list[tuple[int, int, str]] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            updates.append((session_id, status, bars_processed, error))
            return True

        def update_portfolio_wallet_state(
            self,
            portfolio_id: int,
            user_id: int = 0,
            future_wallet=None,
            spot_wallet=None,
            snapshot_reason: int = 0,
            strategy_id: int = 0,
            session_id: str = "",
            snapshot_time=None,
        ) -> bool:
            del user_id, future_wallet, spot_wallet, strategy_id, snapshot_time
            wallet_syncs.append((portfolio_id, snapshot_reason, session_id))
            return True

    class FakeOrderClient:
        def place_order(
            self,
            portfolio_id,
            decision,
            mark_price,
            *,
            portfolio_symbol=None,
            strategy_id=0,
            market="futures",
            session_id="",
            intent_id="",
            market_time=None,
        ):
            del market_time
            assert portfolio_id == 505
            assert decision.exchange == "binance"
            assert decision.market == "perpetual_futures"
            assert market == "perpetual_futures"
            assert decision.symbol == "ETHUSDT"
            assert decision.side == "SELL"
            assert abs(float(decision.qty) - 0.02) < 1e-12
            assert decision.reduce_only is True
            return OrderResponse(
                symbol=portfolio_symbol or decision.symbol,
                side="SELL",
                qty=float(decision.qty),
                fill_price=mark_price,
                status="FILLED",
                order_id="close-1",
            )

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    session_id, state = servicer._sessions.create(environment=0, user_id=17, portfolio_id=505)
    state.strategy_id = 606
    state.configure_risk_runtime(
        order_target_keys={("binance", "perpetual_futures", "ETHUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
        initial_margin_balance=1000.0,
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())
    context = _FakeContext()

    resp = servicer.StopStrategy(
        SimpleNamespace(
            session_id=session_id,
            user_id=17,
            stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        ),
        context,
    )

    assert resp.stopped is True
    assert context.code is None
    assert state.status == "stopped"
    assert abs(route_wallet.futures.positions[("ETHUSDT", 0)].position_qty) <= 1e-12
    assert updates[0][1] == "stopping"
    assert updates[-1][1] == "stopped"
    assert wallet_syncs[-1] == (505, 1, session_id)


def test_stop_strategy_stop_and_close_only_closes_declared_order_targets(monkeypatch):
    route_wallet = make_backtest_wallet(
        futures_positions=[
            {
                "symbol": "ETHUSDT",
                "position_qty": 0.02,
                "entry_price": 2300.0,
                "mark_price": 2310.0,
                "margin_mode": "cross",
            },
            {
                "symbol": "BTCUSDT",
                "position_qty": -0.01,
                "entry_price": 70000.0,
                "mark_price": 69000.0,
                "margin_mode": "cross",
            },
        ],
    )
    wallet = PortfolioWalletRuntime(
        portfolio_id=515,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): route_wallet},
    )
    placed: list[str] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            return True

        def update_portfolio_wallet_state(self, **_kwargs) -> bool:
            return True

    class FakeOrderClient:
        def place_order(self, portfolio_id, decision, mark_price, **kwargs):
            del portfolio_id, kwargs
            placed.append(decision.symbol)
            return OrderResponse(
                symbol=decision.symbol,
                side=decision.side,
                qty=float(decision.qty),
                fill_price=mark_price,
                status="FILLED",
                order_id=f"close-{decision.symbol}",
                reduce_only=decision.reduce_only,
            )

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    session_id, state = servicer._sessions.create(environment=0, user_id=17, portfolio_id=515)
    state.strategy_id = 616
    state.configure_risk_runtime(
        order_target_keys={("binance", "perpetual_futures", "ETHUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
        initial_margin_balance=1000.0,
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    resp = servicer.StopStrategy(
        SimpleNamespace(
            session_id=session_id,
            user_id=17,
            stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        ),
        _FakeContext(),
    )

    assert resp.stopped is True
    assert placed == ["ETHUSDT"]
    assert abs(route_wallet.futures.positions[("ETHUSDT", 0)].position_qty) <= 1e-12
    assert abs(route_wallet.futures.positions[("BTCUSDT", 0)].position_qty + 0.01) <= 1e-12


def test_stop_strategy_stop_and_close_quantizes_futures_qty(monkeypatch):
    route_wallet = make_backtest_wallet(
        futures_positions=[
            {
                "symbol": "ETHUSDT",
                "position_qty": 0.010000000000000002,
                "entry_price": 2300.0,
                "mark_price": 2310.0,
                "margin_mode": "cross",
            },
        ],
    )
    route_wallet.futures.risk_metadata["ETHUSDT"] = CanonicalFuturesRiskMetadata(
        symbol="ETHUSDT",
        quantity_precision=3,
        step_size=0.001,
    )
    wallet = PortfolioWalletRuntime(
        portfolio_id=516,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): route_wallet},
    )
    placed: list[str] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            return True

        def update_portfolio_wallet_state(self, **_kwargs) -> bool:
            return True

    class FakeOrderClient:
        def place_order(self, portfolio_id, decision, mark_price, **kwargs):
            del portfolio_id, kwargs
            placed.append(decision.qty)
            return OrderResponse(
                symbol=decision.symbol,
                side=decision.side,
                qty=float(decision.qty),
                fill_price=mark_price,
                status="FILLED",
                order_id="close-eth",
                reduce_only=decision.reduce_only,
            )

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    session_id, state = servicer._sessions.create(environment=0, user_id=17, portfolio_id=516)
    state.strategy_id = 617
    state.configure_risk_runtime(
        order_target_keys={("binance", "perpetual_futures", "ETHUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
        initial_margin_balance=1000.0,
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    resp = servicer.StopStrategy(
        SimpleNamespace(
            session_id=session_id,
            user_id=17,
            stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        ),
        _FakeContext(),
    )

    assert resp.stopped is True
    assert placed == ["0.01"]


def test_max_loss_guard_stops_and_closes_target_position(monkeypatch):
    route_wallet = make_backtest_wallet(
        wallet_balance=1000.0,
        futures_positions=[{
            "symbol": "ETHUSDT",
            "position_qty": 1.0,
            "entry_price": 2300.0,
            "mark_price": 1900.0,
            "margin_mode": "cross",
        }],
    )
    wallet = PortfolioWalletRuntime(
        portfolio_id=525,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): route_wallet},
    )
    updates: list[tuple[str, str, str]] = []
    placed: list[tuple[str, bool]] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            del bars_processed, runtime_id
            updates.append((session_id, status, error))
            return True

        def update_portfolio_wallet_state(self, **_kwargs) -> bool:
            return True

    class FakeOrderClient:
        def place_order(self, portfolio_id, decision, mark_price, **kwargs):
            del portfolio_id, kwargs
            placed.append((decision.symbol, decision.reduce_only))
            return OrderResponse(
                symbol=decision.symbol,
                side=decision.side,
                qty=float(decision.qty),
                fill_price=mark_price,
                status="FILLED",
                order_id="risk-close-1",
                reduce_only=decision.reduce_only,
            )

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    session_id, state = servicer._sessions.create(environment=0, user_id=17, portfolio_id=525)
    state.strategy_id = 626
    state.configure_risk_runtime(
        order_target_keys={("binance", "perpetual_futures", "ETHUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="strategy",
        initial_margin_balance=1000.0,
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    servicer._maybe_trigger_max_loss_close(
        session_id=session_id,
        state=state,
        wallet=wallet,
    )

    assert state.status == "stopped"
    assert state.max_loss_close_triggered is True
    assert state.current_stop_operation_id() == grpc_server._stop_operation_id(
        session_id,
        pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        "",
    )
    assert "max_loss_close_triggered" in state.error
    assert placed == [("ETHUSDT", True)]
    assert updates[0][1] == "stopping"
    assert updates[-1][1] == "stopped"
    assert abs(route_wallet.futures.positions[("ETHUSDT", 0)].position_qty) <= 1e-12


def test_max_loss_close_exception_transitions_to_stop_failed(monkeypatch):
    route_wallet = make_backtest_wallet(
        wallet_balance=1000.0,
        futures_positions=[{
            "symbol": "ETHUSDT",
            "position_qty": 1.0,
            "entry_price": 2300.0,
            "mark_price": 1900.0,
            "margin_mode": "cross",
        }],
    )
    wallet = PortfolioWalletRuntime(
        portfolio_id=5251,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): route_wallet},
    )

    class FakePortfolioClient:
        @staticmethod
        def update_session(**_kwargs):
            return True

        @staticmethod
        def update_portfolio_wallet_state(**_kwargs):
            return SimpleNamespace()

    class FailingOrderClient:
        @staticmethod
        def place_order(*_args, **_kwargs):
            raise RuntimeError("exchange transport unavailable")

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    session_id, state = servicer._sessions.create(
        environment=0,
        user_id=17,
        portfolio_id=5251,
    )
    state.strategy_id = 6261
    state.configure_risk_runtime(
        order_target_keys={("binance", "perpetual_futures", "ETHUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="strategy",
        initial_margin_balance=1000.0,
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FailingOrderClient())

    servicer._maybe_trigger_max_loss_close(
        session_id=session_id,
        state=state,
        wallet=wallet,
    )

    assert state.status == "stop_failed"
    assert state.current_stop_operation_id()


def test_max_loss_guard_ignores_unowned_position_drawdown(monkeypatch):
    route_wallet = make_backtest_wallet(
        wallet_balance=1000.0,
        futures_positions=[
            {
                "symbol": "ETHUSDT",
                "position_qty": 0.02,
                "entry_price": 2300.0,
                "mark_price": 2310.0,
                "margin_mode": "cross",
            },
            {
                "symbol": "BTCUSDT",
                "position_qty": 1.0,
                "entry_price": 70000.0,
                "mark_price": 69000.0,
                "margin_mode": "cross",
            },
        ],
    )
    wallet = PortfolioWalletRuntime(
        portfolio_id=526,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): route_wallet},
    )
    updates: list[tuple[str, str, str]] = []
    placed: list[tuple[str, bool]] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            del bars_processed, runtime_id
            updates.append((session_id, status, error))
            return True

    class FakeOrderClient:
        def place_order(self, portfolio_id, decision, mark_price, **kwargs):
            del portfolio_id, mark_price, kwargs
            placed.append((decision.symbol, decision.reduce_only))
            raise AssertionError("non-target drawdown must not trigger close orders")

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    session_id, state = servicer._sessions.create(environment=0, user_id=17, portfolio_id=526)
    state.strategy_id = 627
    state.configure_risk_runtime(
        order_target_keys={("binance", "perpetual_futures", "ETHUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="strategy",
        initial_margin_balance=1000.0,
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    servicer._maybe_trigger_max_loss_close(
        session_id=session_id,
        state=state,
        wallet=wallet,
    )

    assert state.status == "running"
    assert state.max_loss_close_triggered is False
    assert placed == []
    assert updates == []


def test_stop_strategy_spot_exit_fails_structurally_when_core_close_unavailable(monkeypatch):
    route_wallet = make_testnet_wallet(
        spot_assets=[{
            "symbol": "BTC",
            "qty": 0.01,
            "locked": 0.0,
            "avg_entry_price": 70000.0,
            "price": 71000.0,
        }],
        spot_free=1000.0,
    )
    wallet = PortfolioWalletRuntime(
        portfolio_id=707,
        allowed_routes={("binance", "spot")},
        wallets={("binance", "spot", 22): route_wallet},
    )
    updates: list[tuple[str, str, int, str]] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            updates.append((session_id, status, bars_processed, error))
            return True

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    servicer = StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)
    session_id, state = servicer._sessions.create(environment=1, user_id=17, portfolio_id=707)
    state.strategy_id = 808
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
        initial_margin_balance=1000.0,
    )
    state.configure_stop_runtime(wallet=wallet, order_client=object())
    stop_event = threading.Event()
    state._stop_event = stop_event  # type: ignore[attr-defined]
    context = _FakeContext()

    resp = servicer.StopStrategy(
        SimpleNamespace(
            session_id=session_id,
            user_id=17,
            stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        ),
        context,
    )

    assert resp.stopped is False
    assert context.code is None
    assert resp.status == "stop_failed"
    assert resp.code == "SPOT_CLOSE_UNAVAILABLE"
    assert state.status == "stop_failed"
    assert updates[0][1] == "stopping"
    assert updates[-1][1] == "stop_failed"
    assert stop_event.is_set() is True


def _spot_close_final_snapshot(*, venue_id: int, environment: int, btc_free: str, usdt_free: str):
    venue = portfolio_service_pb2.VenueSnapshot(
        venue_id=venue_id,
        exchange=1,
        environment=environment,
        market=1,
    )
    venue.wallet.CopyFrom(portfolio_service_pb2.PortfolioWalletState(
        environment=environment,
        spot=portfolio_service_pb2.SpotWallet(assets=[
            portfolio_service_pb2.SpotAsset(
                asset="BTC",
                free=float(btc_free),
                free_decimal=btc_free,
                locked_decimal="0",
            ),
            portfolio_service_pb2.SpotAsset(
                asset="USDT",
                free=float(usdt_free),
                free_decimal=usdt_free,
                locked_decimal="0",
            ),
        ]),
    ))
    return venue


def _spot_stop_wallet(*, environment: int, portfolio_id: int, venue_id: int):
    factory = make_backtest_wallet if environment == 0 else make_testnet_wallet
    route_wallet = factory(
        spot_assets=[{
            "symbol": "BTC",
            "qty": 0.01,
            "locked": 0.0,
            "avg_entry_price": 50_000.0,
            "price": 51_000.0,
        }],
        spot_free=1_000.0,
    )
    route_wallet.spot.register_metadata(SpotSymbolMetadata(
        venue_id=venue_id,
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
        status="TRADING",
        base_asset="BTC",
        quote_asset="USDT",
        base_asset_precision=8,
        quote_asset_precision=8,
        spot_trading_allowed=True,
    ))
    return PortfolioWalletRuntime(
        portfolio_id=portfolio_id,
        allowed_routes={("binance", "spot")},
        wallets={("binance", "spot", venue_id): route_wallet},
    )


def test_spot_close_result_uses_core_authority_when_worker_metadata_is_missing():
    wallet = _spot_stop_wallet(environment=0, portfolio_id=706, venue_id=21)
    wallet.wallets[("binance", "spot", 21)].spot.symbol_metadata.clear()
    results = [order_service_pb2.SpotCloseTargetResult(
        target=order_service_pb2.SpotCloseTarget(
            venue_id=21,
            exchange=1,
            market=1,
            symbol="BTCUSDT",
        ),
        base_asset="BTC",
        status="terminal",
    )]

    reason = StrategyServiceServicer._validate_spot_close_result_targets(
        [{
            "venue_id": 21,
            "exchange": "binance",
            "market": "spot",
            "symbol": "BTCUSDT",
        }],
        results,
    )

    assert reason == ""


def test_spot_close_final_snapshot_must_explicitly_include_core_result_asset():
    wallet = _spot_stop_wallet(environment=0, portfolio_id=706, venue_id=21)
    results = [order_service_pb2.SpotCloseTargetResult(
        target=order_service_pb2.SpotCloseTarget(
            venue_id=21,
            exchange=1,
            market=1,
            symbol="BTCUSDT",
        ),
        base_asset="ETH",
        status="terminal",
    )]

    flat, reason = StrategyServiceServicer._spot_close_results_are_flat(wallet, results)

    assert flat is False
    assert reason == "stop_and_close_failed:missing_core_spot_final_asset:ETH"


def test_stop_operation_id_is_stable_across_worker_reconstruction():
    first = grpc_server._stop_operation_id(
        "0123456789abcdef0123456789abcdef",
        pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        "",
    )
    reconstructed = grpc_server._stop_operation_id(
        "0123456789abcdef0123456789abcdef",
        pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        "",
    )

    assert first == reconstructed
    assert str(grpc_server.uuid.UUID(first)) == first
    assert grpc_server._stop_operation_id(
        "fedcba9876543210fedcba9876543210",
        pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        "",
    ) != first
    assert grpc_server._stop_operation_id(
        "0123456789abcdef0123456789abcdef",
        pb2.STOP_ACTION_STOP_ONLY,
        "",
    ) != first
    assert grpc_server._stop_operation_id(
        "0123456789abcdef0123456789abcdef",
        pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        "caller-operation",
    ) == "caller-operation"


def test_second_stop_request_does_not_execute_close_while_first_is_in_progress():
    wallet = _spot_stop_wallet(environment=0, portfolio_id=7061, venue_id=211)
    close_calls = 0

    class FakePortfolioClient:
        def update_session(self, **_kwargs):
            return True

    class FakeOrderClient:
        def close_spot_targets(self, **_kwargs):
            nonlocal close_calls
            close_calls += 1
            raise AssertionError("a second stop request must not execute the close")

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    session_id, state = servicer._sessions.create(
        environment=0,
        user_id=17,
        portfolio_id=7061,
    )
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())
    assert state.transition("stopping") is True

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
    ), _FakeContext())

    assert response.stopped is False
    assert response.status == "stopping"
    assert response.code == "STOP_IN_PROGRESS"
    assert response.operation_id == grpc_server._stop_operation_id(
        session_id,
        pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
        "",
    )
    assert close_calls == 0


def test_stop_only_waits_for_accepted_order_without_trading_or_mutating_wallet(monkeypatch):
    wallet = _spot_stop_wallet(environment=1, portfolio_id=707, venue_id=22)
    original_btc = wallet.wallets[("binance", "spot", 22)].spot.assets["BTC"].free
    lifecycle_calls: list[int] = []
    handled_statuses: list[str] = []

    def user_order_update_handler(event):
        handled_statuses.append(event.order_status)
        raise AssertionError("STOP_ONLY lifecycle scans must not invoke user callbacks")

    class FakePortfolioClient:
        def update_session(self, **_kwargs):
            return True

    class FakeOrderClient:
        def list_order_lifecycle_events(
            self,
            *,
            session_id,
            after_event_id=0,
            limit=100,
            timeout_seconds=None,
        ):
            del session_id, limit, timeout_seconds
            lifecycle_calls.append(after_event_id)
            status = "NEW" if len(lifecycle_calls) == 1 else "FILLED"
            return [SimpleNamespace(
                event_id=len(lifecycle_calls),
                order_status=status,
                order_id="accepted-order-1",
                exchange_order_id="exchange-order-1",
                attempt_id="attempt-1",
                intent_id="intent-1",
                exchange="binance",
                market="spot",
                symbol="BTCUSDT",
            )]

        def place_order(self, *_args, **_kwargs):
            raise AssertionError("STOP_ONLY must not place an order")

        def close_spot_targets(self, **_kwargs):
            raise AssertionError("STOP_ONLY must not close Spot targets")

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_POLL_SECONDS", 0.0)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(environment=1, user_id=17, portfolio_id=707)
    state.strategy_id = 808
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(
        wallet=wallet,
        order_client=FakeOrderClient(),
        order_update_handler=user_order_update_handler,
    )

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_ONLY,
    ), _FakeContext())

    assert response.stopped is True
    assert response.status == "stopped"
    assert response.code == "STOPPED"
    assert lifecycle_calls == [0, 1]
    assert handled_statuses == []
    assert wallet.wallets[("binance", "spot", 22)].spot.assets["BTC"].free == original_btc


def test_stop_only_exhausts_lifecycle_pages_before_declaring_no_pending_orders(monkeypatch):
    wallet = _spot_stop_wallet(environment=1, portfolio_id=7070, venue_id=220)
    lifecycle_calls: list[int] = []

    class FakePortfolioClient:
        @staticmethod
        def update_session(**_kwargs):
            return True

    class FakeOrderClient:
        @staticmethod
        def list_order_lifecycle_events(
            *,
            session_id,
            after_event_id=0,
            limit=100,
            timeout_seconds=None,
        ):
            del session_id, timeout_seconds
            lifecycle_calls.append(after_event_id)
            assert limit == 500
            if after_event_id == 0:
                return [SimpleNamespace(
                    event_id=event_id,
                    order_status="FILLED",
                    order_id=f"historical-{event_id}",
                    exchange_order_id=f"historical-exchange-{event_id}",
                    attempt_id=f"historical-attempt-{event_id}",
                    intent_id=f"historical-intent-{event_id}",
                    exchange="binance",
                    market="spot",
                    symbol="BTCUSDT",
                ) for event_id in range(1, 501)]
            if after_event_id == 500:
                return [SimpleNamespace(
                    event_id=501,
                    order_status="NEW",
                    order_id="accepted-order-501",
                    exchange_order_id="exchange-order-501",
                    attempt_id="attempt-501",
                    intent_id="intent-501",
                    exchange="binance",
                    market="spot",
                    symbol="BTCUSDT",
                )]
            if after_event_id == 501:
                return [SimpleNamespace(
                    event_id=502,
                    order_status="FILLED",
                    order_id="accepted-order-501",
                    exchange_order_id="exchange-order-501",
                    attempt_id="attempt-501",
                    intent_id="intent-501",
                    exchange="binance",
                    market="spot",
                    symbol="BTCUSDT",
                )]
            return []

    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_POLL_SECONDS", 0.0)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    session_id, state = servicer._sessions.create(
        environment=1,
        user_id=17,
        portfolio_id=7070,
    )
    state.strategy_id = 8080
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_ONLY,
    ), _FakeContext())

    assert response.stopped is True
    assert response.status == "stopped"
    assert response.code == "STOPPED"
    assert lifecycle_calls == [0, 500, 501]


def test_stop_only_propagates_remaining_deadline_to_lifecycle_reader(monkeypatch):
    observed_timeouts: list[float] = []

    class DeadlineOrderClient:
        @staticmethod
        def list_order_lifecycle_events(*, timeout_seconds, **_kwargs):
            observed_timeouts.append(timeout_seconds)
            raise TimeoutError("simulated lifecycle deadline")

    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_TIMEOUT_SECONDS", 0.05)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    session_id, state = servicer._sessions.create(
        environment=1,
        user_id=17,
        portfolio_id=70701,
    )
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(
        wallet=_spot_stop_wallet(environment=1, portfolio_id=70701, venue_id=2201),
        order_client=DeadlineOrderClient(),
    )

    with pytest.raises(TimeoutError, match="simulated lifecycle deadline"):
        servicer._wait_for_accepted_orders(session_id, state)

    assert len(observed_timeouts) == 1
    assert 0 < observed_timeouts[0] <= 0.05


def test_stop_only_hard_deadline_releases_session_from_blocking_lifecycle_reader(monkeypatch):
    release_reader = threading.Event()
    observed_timeouts: list[float] = []

    class BlockingOrderClient:
        @staticmethod
        def list_order_lifecycle_events(*, timeout_seconds, **_kwargs):
            observed_timeouts.append(timeout_seconds)
            release_reader.wait(timeout=0.2)
            return []

    class FakePortfolioClient:
        @staticmethod
        def update_session(**_kwargs):
            return True

    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_TIMEOUT_SECONDS", 0.01)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    session_id, state = servicer._sessions.create(
        environment=1,
        user_id=17,
        portfolio_id=707011,
    )
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(
        wallet=_spot_stop_wallet(environment=1, portfolio_id=707011, venue_id=22011),
        order_client=BlockingOrderClient(),
    )

    started_at = time.monotonic()
    try:
        response = servicer.StopStrategy(pb2.StopStrategyRequest(
            session_id=session_id,
            user_id=17,
            stop_action=pb2.STOP_ACTION_STOP_ONLY,
        ), _FakeContext())
    finally:
        release_reader.set()

    assert time.monotonic() - started_at < 0.1
    assert len(observed_timeouts) == 1
    assert response.stopped is False
    assert response.status == "stop_failed"
    assert response.code == "STOP_LIFECYCLE_UNAVAILABLE"
    assert state.status == "stop_failed"


def test_stop_only_fails_closed_when_full_lifecycle_page_does_not_advance(monkeypatch):
    lifecycle_calls: list[int] = []
    page = [SimpleNamespace(
        event_id=event_id,
        order_status="FILLED",
        order_id=f"historical-{event_id}",
        exchange_order_id=f"historical-exchange-{event_id}",
        exchange="binance",
        market="spot",
        symbol="BTCUSDT",
    ) for event_id in range(1, 501)]

    class StalePageOrderClient:
        @staticmethod
        def list_order_lifecycle_events(*, after_event_id, **_kwargs):
            lifecycle_calls.append(after_event_id)
            return page

    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_TIMEOUT_SECONDS", 1.0)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    session_id, state = servicer._sessions.create(
        environment=1,
        user_id=17,
        portfolio_id=70702,
    )
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(
        wallet=_spot_stop_wallet(environment=1, portfolio_id=70702, venue_id=2202),
        order_client=StalePageOrderClient(),
    )

    with pytest.raises(RuntimeError, match="pagination did not advance"):
        servicer._wait_for_accepted_orders(session_id, state)

    assert lifecycle_calls == [0, 500]


def test_stop_only_waits_for_inflight_strategy_decision_before_lifecycle_scan(monkeypatch):
    from market_data.models import MarketKline
    from strategy_service.inputs import StrategyInput

    callback_entered = threading.Event()
    release_callback = threading.Event()
    lifecycle_read = threading.Event()

    class OneBarMarketDataClient:
        @staticmethod
        def fetch_backtest_page(**_kwargs):
            return SimpleNamespace(
                klines=[MarketKline(
                    symbol="BTCUSDT",
                    interval="1m",
                    open_time=1,
                    close_time=2,
                    open=1.0,
                    high=2.0,
                    low=0.5,
                    close=1.5,
                    volume=10.0,
                    timestamp=2,
                    market="spot",
                )],
                next_cursor_time_ms=1,
                has_more=False,
            )

    class BlockingEngine:
        strategies = {}

        @staticmethod
        def running_strategy(_market_data):
            callback_entered.set()
            assert release_callback.wait(timeout=2)
            return True

    class FakePortfolioClient:
        @staticmethod
        def update_session(**_kwargs):
            return True

    class FakeOrderClient:
        @staticmethod
        def list_order_lifecycle_events(**_kwargs):
            lifecycle_read.set()
            return []

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    monkeypatch.setattr(servicer, "_marketdata_client", lambda: OneBarMarketDataClient())
    monkeypatch.setattr(servicer, "_install_indicator_collection", lambda *_args, **_kwargs: lambda: None)
    session_id, state = servicer._sessions.create(
        environment=0,
        user_id=17,
        portfolio_id=7071,
    )
    state.configure_stop_runtime(
        wallet=_spot_stop_wallet(environment=0, portfolio_id=7071, venue_id=221),
        order_client=FakeOrderClient(),
    )

    run_thread = threading.Thread(target=servicer._run_backtest_via_platform_proxy, kwargs={
        "session_id": session_id,
        "state": state,
        "engine": BlockingEngine(),
        "request": SimpleNamespace(start_time_ms=1, end_time_ms=2, user_id=17),
        "declared_inputs": [StrategyInput("binance", "spot", "BTCUSDT", "1m")],
    })
    run_thread.start()
    assert callback_entered.wait(timeout=1)

    result: dict[str, object] = {}
    stop_thread = threading.Thread(target=lambda: result.setdefault(
        "response",
        servicer.StopStrategy(pb2.StopStrategyRequest(
            session_id=session_id,
            user_id=17,
            stop_action=pb2.STOP_ACTION_STOP_ONLY,
        ), _FakeContext()),
    ))
    stop_thread.start()

    assert lifecycle_read.wait(timeout=0.05) is False
    assert stop_thread.is_alive()

    release_callback.set()
    run_thread.join(timeout=2)
    stop_thread.join(timeout=2)

    assert not run_thread.is_alive()
    assert not stop_thread.is_alive()
    assert lifecycle_read.is_set()
    response = result["response"]
    assert response.stopped is True
    assert response.status == "stopped"


def test_stop_only_fails_closed_when_lifecycle_reader_is_unavailable():
    class FakePortfolioClient:
        @staticmethod
        def update_session(**_kwargs):
            return True

    class FailingOrderClient:
        @staticmethod
        def list_order_lifecycle_events(**_kwargs):
            raise RuntimeError("lifecycle transport unavailable")

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    session_id, state = servicer._sessions.create(
        environment=1,
        user_id=17,
        portfolio_id=7072,
    )
    state.configure_stop_runtime(
        wallet=_spot_stop_wallet(environment=1, portfolio_id=7072, venue_id=222),
        order_client=FailingOrderClient(),
    )

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_ONLY,
    ), _FakeContext())

    assert response.stopped is False
    assert response.status == "stop_failed"
    assert response.code == "STOP_LIFECYCLE_UNAVAILABLE"
    assert state.status == "stop_failed"


def test_stop_status_persist_exception_does_not_strand_session_in_stopping():
    class FailingPortfolioClient:
        @staticmethod
        def update_session(**_kwargs):
            raise RuntimeError("portfolio transport unavailable")

    class FakeOrderClient:
        @staticmethod
        def list_order_lifecycle_events(**_kwargs):
            raise AssertionError("lifecycle scan must not run after status persistence fails")

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FailingPortfolioClient(),
    ))
    session_id, state = servicer._sessions.create(
        environment=1,
        user_id=17,
        portfolio_id=7073,
    )
    state.configure_stop_runtime(
        wallet=_spot_stop_wallet(environment=1, portfolio_id=7073, venue_id=223),
        order_client=FakeOrderClient(),
    )

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_ONLY,
    ), _FakeContext())

    assert response.stopped is False
    assert response.status == "stop_failed"
    assert response.code == "STOP_STATUS_PERSIST_FAILED"
    assert state.status == "stop_failed"


def test_futures_close_exception_does_not_strand_session_in_stopping():
    route_wallet = make_backtest_wallet(futures_positions=[{
        "symbol": "ETHUSDT",
        "position_qty": 0.02,
        "entry_price": 2300.0,
        "mark_price": 2310.0,
        "margin_mode": "cross",
    }])
    wallet = PortfolioWalletRuntime(
        portfolio_id=7074,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 224): route_wallet},
    )

    class FakePortfolioClient:
        @staticmethod
        def update_session(**_kwargs):
            return True

        @staticmethod
        def update_portfolio_wallet_state(**_kwargs):
            return SimpleNamespace()

    class FailingOrderClient:
        @staticmethod
        def place_order(*_args, **_kwargs):
            raise RuntimeError("exchange transport unavailable")

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    session_id, state = servicer._sessions.create(
        environment=0,
        user_id=17,
        portfolio_id=7074,
    )
    state.strategy_id = 8084
    state.configure_risk_runtime(
        order_target_keys={("binance", "perpetual_futures", "ETHUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FailingOrderClient())

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
    ), _FakeContext())

    assert response.stopped is False
    assert response.status == "stop_failed"
    assert response.code == "STOP_EXECUTION_FAILED"
    assert state.status == "stop_failed"


def test_stop_only_timeout_reports_pending_order_without_trading(monkeypatch):
    wallet = _spot_stop_wallet(environment=1, portfolio_id=708, venue_id=23)

    class FakePortfolioClient:
        def update_session(self, **_kwargs):
            return True

    class FakeOrderClient:
        def list_order_lifecycle_events(self, **_kwargs):
            return [SimpleNamespace(
                event_id=1,
                order_status="NEW",
                order_id="accepted-order-timeout",
                exchange_order_id="exchange-order-timeout",
                attempt_id="attempt-timeout",
                intent_id="intent-timeout",
                exchange="binance",
                market="spot",
                symbol="BTCUSDT",
            )]

        def place_order(self, *_args, **_kwargs):
            raise AssertionError("STOP_ONLY must not place an order")

        def close_spot_targets(self, **_kwargs):
            raise AssertionError("STOP_ONLY must not close Spot targets")

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_TIMEOUT_SECONDS", 0.001)
    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_POLL_SECONDS", 0.0)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(environment=1, user_id=17, portfolio_id=708)
    state.strategy_id = 809
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_ONLY,
    ), _FakeContext())

    assert response.stopped is False
    assert response.status == "stop_failed"
    assert response.code == "STOP_PENDING_ORDERS_TIMEOUT"
    assert [(item.symbol, item.status) for item in response.target_results] == [
        ("BTCUSDT", "pending"),
    ]
    assert state.status == "stop_failed"


def test_stop_only_matches_terminal_event_by_exchange_order_identity(monkeypatch):
    wallet = _spot_stop_wallet(environment=1, portfolio_id=7081, venue_id=231)
    spot = wallet.wallets[("binance", "spot", 231)].spot
    spot.open_orders[("binance", "spot", 231, "exchange-order-1")] = SimpleNamespace(
        symbol="BTCUSDT",
        status="NEW",
        order_id="local-wallet-order",
        order_identity="exchange-order-1",
        exchange_order_id="exchange-order-1",
    )

    class FakePortfolioClient:
        def update_session(self, **_kwargs):
            return True

    class FakeOrderClient:
        def list_order_lifecycle_events(self, **_kwargs):
            return [SimpleNamespace(
                event_id=1,
                order_status="FILLED",
                order_id="local-core-order",
                exchange_order_id="exchange-order-1",
                exchange="binance",
                market="spot",
                symbol="BTCUSDT",
            )]

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_TIMEOUT_SECONDS", 0.001)
    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_POLL_SECONDS", 0.0)
    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(environment=1, user_id=17, portfolio_id=7081)
    state.strategy_id = 8091
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(
        wallet=wallet,
        order_client=FakeOrderClient(),
        order_update_handler=lambda _event: spot.open_orders.clear(),
    )

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_ONLY,
    ), _FakeContext())

    assert response.stopped is True
    assert response.status == "stopped"


def test_stop_only_matches_order_lifecycle_when_exchange_identity_arrives_late(monkeypatch):
    wallet = _spot_stop_wallet(environment=1, portfolio_id=7082, venue_id=232)
    calls = 0

    class FakePortfolioClient:
        def update_session(self, **_kwargs):
            return True

    class FakeOrderClient:
        def list_order_lifecycle_events(self, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                return [SimpleNamespace(
                    event_id=1,
                    order_status="NEW",
                    order_id="local-order-1",
                    exchange_order_id="",
                    attempt_id="attempt-1",
                    intent_id="intent-1",
                    exchange="binance",
                    market="spot",
                    symbol="BTCUSDT",
                )]
            return [SimpleNamespace(
                event_id=2,
                order_status="FILLED",
                order_id="local-order-1",
                exchange_order_id="exchange-order-1",
                attempt_id="attempt-1",
                intent_id="intent-1",
                exchange="binance",
                market="spot",
                symbol="BTCUSDT",
            )]

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    session_id, state = servicer._sessions.create(
        environment=1,
        user_id=17,
        portfolio_id=7082,
    )
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())
    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_TIMEOUT_SECONDS", 0.01)
    monkeypatch.setattr(grpc_server, "DEFAULT_STOP_ONLY_POLL_SECONDS", 0.0)

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_ONLY,
    ), _FakeContext())

    assert response.stopped is True
    assert response.status == "stopped"


def test_backtest_spot_stop_syncs_before_core_close_and_applies_final_snapshot(monkeypatch):
    wallet = _spot_stop_wallet(environment=0, portfolio_id=709, venue_id=24)
    events: list[str] = []
    seen_operation_ids: list[str] = []

    class FakePortfolioClient:
        def update_session(self, **_kwargs):
            events.append("session:" + str(_kwargs["status"]))
            return True

        def update_portfolio_wallet_state(self, **_kwargs):
            events.append("wallet_sync")
            return SimpleNamespace()

    class FakeOrderClient:
        def close_spot_targets(self, **kwargs):
            events.append("core_close")
            seen_operation_ids.append(kwargs["operation_id"])
            assert kwargs["targets"] == [{
                "venue_id": 24,
                "exchange": "binance",
                "market": "spot",
                "symbol": "BTCUSDT",
            }]
            return order_service_pb2.CloseSpotTargetsResponse(
                status="stopped",
                operation_id=kwargs["operation_id"],
                results=[order_service_pb2.SpotCloseTargetResult(
                    target=order_service_pb2.SpotCloseTarget(
                        venue_id=24, exchange=1, market=1, symbol="BTCUSDT",
                    ),
                    base_asset="BTC",
                    status="terminal",
                )],
                final_snapshots=[_spot_close_final_snapshot(
                    venue_id=24,
                    environment=0,
                    btc_free="0",
                    usdt_free="1510.00000000",
                )],
            )

        def place_order(self, *_args, **_kwargs):
            raise AssertionError("Spot close must execute in core-service")

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(environment=0, user_id=17, portfolio_id=709)
    state.strategy_id = 810
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
    ), _FakeContext())

    # A transport retry after the first response must address and echo the same
    # durable core operation without issuing a second close.
    retry = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
    ), _FakeContext())

    assert response.stopped is True
    assert response.status == "stopped"
    assert response.operation_id == seen_operation_ids[0]
    assert retry.stopped is True
    assert retry.code == "ALREADY_TERMINAL"
    assert retry.operation_id == response.operation_id
    assert seen_operation_ids == [response.operation_id]
    assert str(grpc_server.uuid.UUID(response.operation_id)) == response.operation_id
    assert events == ["session:stopping", "wallet_sync", "core_close", "wallet_sync"]
    final_spot = wallet.wallets[("binance", "spot", 24)].spot
    assert final_spot.assets["BTC"].free == grpc_server.Decimal("0")
    assert final_spot.assets["USDT"].free == grpc_server.Decimal("1510.00000000")


def test_backtest_spot_final_sync_failure_keeps_local_wallet_at_last_persisted_state():
    wallet = _spot_stop_wallet(environment=0, portfolio_id=7090, venue_id=240)
    sync_calls = 0

    class FakePortfolioClient:
        @staticmethod
        def update_portfolio_wallet_state(**_kwargs):
            nonlocal sync_calls
            sync_calls += 1
            if sync_calls == 2:
                raise RuntimeError("final wallet persistence unavailable")
            return SimpleNamespace()

    class FakeOrderClient:
        @staticmethod
        def close_spot_targets(**kwargs):
            return order_service_pb2.CloseSpotTargetsResponse(
                status="stopped",
                operation_id=kwargs["operation_id"],
                results=[order_service_pb2.SpotCloseTargetResult(
                    target=order_service_pb2.SpotCloseTarget(
                        venue_id=240, exchange=1, market=1, symbol="BTCUSDT",
                    ),
                    base_asset="BTC",
                    status="terminal",
                )],
                final_snapshots=[_spot_close_final_snapshot(
                    venue_id=240,
                    environment=0,
                    btc_free="0",
                    usdt_free="1510.00000000",
                )],
            )

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    state = SessionState(
        environment=0,
        user_id=17,
        portfolio_id=7090,
        strategy_id=8100,
    )
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    result = servicer._stop_and_close_portfolio("session-1", state, "operation-1")

    assert result.ok is False
    assert result.code == "SPOT_CLOSE_FINAL_WALLET_SYNC_FAILED"
    assert sync_calls == 2
    spot = wallet.wallets[("binance", "spot", 240)].spot
    assert spot.assets["BTC"].free == grpc_server.Decimal("0.01")
    assert spot.assets["USDT"].free == grpc_server.Decimal("1000.0")


def test_spot_stop_rejects_success_response_for_a_different_target(monkeypatch):
    wallet = _spot_stop_wallet(environment=0, portfolio_id=7091, venue_id=241)

    class FakePortfolioClient:
        def update_session(self, **_kwargs):
            return True

        def update_portfolio_wallet_state(self, **_kwargs):
            return SimpleNamespace()

    class FakeOrderClient:
        def close_spot_targets(self, **kwargs):
            return order_service_pb2.CloseSpotTargetsResponse(
                status="stopped",
                operation_id=kwargs["operation_id"],
                results=[order_service_pb2.SpotCloseTargetResult(
                    target=order_service_pb2.SpotCloseTarget(
                        venue_id=241, exchange=1, market=1, symbol="ETHUSDT",
                    ),
                    base_asset="ETH",
                    status="terminal",
                )],
                final_snapshots=[_spot_close_final_snapshot(
                    venue_id=241,
                    environment=0,
                    btc_free="0",
                    usdt_free="1510.00000000",
                )],
            )

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(
        environment=0,
        user_id=17,
        portfolio_id=7091,
    )
    state.strategy_id = 8101
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
    ), _FakeContext())

    assert response.stopped is False
    assert response.status == "stop_failed"
    assert response.code == "SPOT_CLOSE_RESULT_MISMATCH"
    spot = wallet.wallets[("binance", "spot", 241)].spot
    assert spot.assets["BTC"].free == grpc_server.Decimal("0.01")


def test_spot_stop_rejects_nonterminal_result_in_success_response(monkeypatch):
    wallet = _spot_stop_wallet(environment=0, portfolio_id=7092, venue_id=242)

    class FakePortfolioClient:
        def update_portfolio_wallet_state(self, **_kwargs):
            return SimpleNamespace()

    class FakeOrderClient:
        def close_spot_targets(self, **kwargs):
            return order_service_pb2.CloseSpotTargetsResponse(
                status="stopped",
                operation_id=kwargs["operation_id"],
                results=[order_service_pb2.SpotCloseTargetResult(
                    target=order_service_pb2.SpotCloseTarget(
                        venue_id=242, exchange=1, market=1, symbol="BTCUSDT",
                    ),
                    base_asset="BTC",
                    status="failed",
                )],
                final_snapshots=[_spot_close_final_snapshot(
                    venue_id=242,
                    environment=0,
                    btc_free="0",
                    usdt_free="1510.00000000",
                )],
            )

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    state = SessionState(
        environment=0,
        user_id=17,
        portfolio_id=7092,
        strategy_id=8102,
    )
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    result = servicer._stop_and_close_portfolio("sess-result-status", state, "operation-1")

    assert result.ok is False
    assert result.code == "SPOT_CLOSE_RESULT_MISMATCH"
    assert wallet.wallets[("binance", "spot", 242)].spot.assets["BTC"].free == grpc_server.Decimal("0.01")


def test_spot_stop_invalid_extra_snapshot_does_not_partially_replace_wallet(monkeypatch):
    wallet = _spot_stop_wallet(environment=0, portfolio_id=7093, venue_id=243)

    class FakePortfolioClient:
        def update_portfolio_wallet_state(self, **_kwargs):
            return SimpleNamespace()

    class FakeOrderClient:
        def close_spot_targets(self, **kwargs):
            return order_service_pb2.CloseSpotTargetsResponse(
                status="stopped",
                operation_id=kwargs["operation_id"],
                results=[order_service_pb2.SpotCloseTargetResult(
                    target=order_service_pb2.SpotCloseTarget(
                        venue_id=243, exchange=1, market=1, symbol="BTCUSDT",
                    ),
                    base_asset="BTC",
                    status="terminal",
                )],
                final_snapshots=[
                    _spot_close_final_snapshot(
                        venue_id=243,
                        environment=0,
                        btc_free="0",
                        usdt_free="1510.00000000",
                    ),
                    _spot_close_final_snapshot(
                        venue_id=999,
                        environment=0,
                        btc_free="0",
                        usdt_free="1510.00000000",
                    ),
                ],
            )

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(SimpleNamespace(
        portfolio_client=lambda: FakePortfolioClient(),
    ))
    state = SessionState(
        environment=0,
        user_id=17,
        portfolio_id=7093,
        strategy_id=8103,
    )
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    result = servicer._stop_and_close_portfolio("sess-snapshot-atomic", state, "operation-1")

    assert result.ok is False
    assert result.code == "SPOT_CLOSE_FINAL_SNAPSHOT_INVALID"
    assert wallet.wallets[("binance", "spot", 243)].spot.assets["BTC"].free == grpc_server.Decimal("0.01")


def test_demo_spot_stop_uses_core_snapshot_and_surfaces_reconciliation(monkeypatch):
    wallet = _spot_stop_wallet(environment=1, portfolio_id=710, venue_id=25)
    updates: list[str] = []

    class FakePortfolioClient:
        def update_session(self, **kwargs):
            updates.append(kwargs["status"])
            return True

        def update_portfolio_wallet_state(self, **_kwargs):
            raise AssertionError("Demo Spot close must plan from core's fresh account read")

    class FakeOrderClient:
        def close_spot_targets(self, **kwargs):
            return order_service_pb2.CloseSpotTargetsResponse(
                status="stop_failed",
                code="SPOT_CLOSE_RESIDUAL_BALANCE",
                operation_id=kwargs["operation_id"],
                reconciliation_run_id="recon-25",
                reconciliation_required=True,
                results=[order_service_pb2.SpotCloseTargetResult(
                    target=order_service_pb2.SpotCloseTarget(
                        venue_id=25, exchange=1, market=1, symbol="BTCUSDT",
                    ),
                    base_asset="BTC",
                    status="failed",
                    code="SPOT_CLOSE_RESIDUAL_BALANCE",
                    message="authoritative residual balance remains",
                )],
            )

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(environment=1, user_id=17, portfolio_id=710)
    state.strategy_id = 811
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
    ), _FakeContext())

    assert response.stopped is False
    assert response.status == "stop_failed"
    assert response.code == "SPOT_CLOSE_RESIDUAL_BALANCE"
    assert response.reconciliation_run_id == "recon-25"
    assert response.target_results[0].code == "SPOT_CLOSE_RESIDUAL_BALANCE"
    assert state.status == "stop_failed"
    assert state.reconciliation_run_id == "recon-25"
    assert updates == ["stopping"]


def test_demo_spot_stop_replaces_stale_wallet_with_core_final_snapshot(monkeypatch):
    wallet = _spot_stop_wallet(environment=1, portfolio_id=7101, venue_id=251)
    events: list[str] = []

    class FakePortfolioClient:
        def update_session(self, **kwargs):
            events.append("session:" + kwargs["status"])
            return True

        def update_portfolio_wallet_state(self, **_kwargs):
            raise AssertionError("Demo Spot close must not publish the stale local wallet")

    class FakeOrderClient:
        def close_spot_targets(self, **kwargs):
            events.append("core_close")
            return order_service_pb2.CloseSpotTargetsResponse(
                status="stopped",
                operation_id=kwargs["operation_id"],
                results=[order_service_pb2.SpotCloseTargetResult(
                    target=order_service_pb2.SpotCloseTarget(
                        venue_id=251, exchange=1, market=1, symbol="BTCUSDT",
                    ),
                    base_asset="BTC",
                    status="terminal",
                )],
                final_snapshots=[_spot_close_final_snapshot(
                    venue_id=251,
                    environment=1,
                    btc_free="0",
                    usdt_free="1600.00000000",
                )],
            )

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(environment=1, user_id=17, portfolio_id=7101)
    state.strategy_id = 8111
    state.configure_risk_runtime(
        order_target_keys={("binance", "spot", "BTCUSDT")},
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
    ), _FakeContext())

    assert response.stopped is True
    assert response.status == "stopped"
    assert events == ["session:stopping", "core_close"]
    final_spot = wallet.wallets[("binance", "spot", 251)].spot
    assert final_spot.assets["BTC"].free == grpc_server.Decimal("0")
    assert final_spot.assets["USDT"].free == grpc_server.Decimal("1600.00000000")


def test_backtest_mixed_futures_and_spot_stop_closes_every_declared_target(monkeypatch):
    futures_wallet = make_backtest_wallet(futures_positions=[{
        "symbol": "ETHUSDT",
        "position_qty": 0.02,
        "entry_price": 2300.0,
        "mark_price": 2310.0,
        "margin_mode": "cross",
    }])
    spot_wallet = _spot_stop_wallet(
        environment=0,
        portfolio_id=7102,
        venue_id=253,
    ).wallets[("binance", "spot", 253)]
    wallet = PortfolioWalletRuntime(
        portfolio_id=7102,
        allowed_routes={("binance", "perpetual_futures"), ("binance", "spot")},
        wallets={
            ("binance", "perpetual_futures", 252): futures_wallet,
            ("binance", "spot", 253): spot_wallet,
        },
    )
    events: list[str] = []

    class FakePortfolioClient:
        def update_session(self, **kwargs):
            events.append("session:" + kwargs["status"])
            return True

        def update_portfolio_wallet_state(self, **_kwargs):
            events.append("wallet_sync")
            return SimpleNamespace()

    class FakeOrderClient:
        def place_order(self, _portfolio_id, decision, mark_price, **_kwargs):
            events.append("futures_close")
            return OrderResponse(
                symbol=decision.symbol,
                side=decision.side,
                qty=float(decision.qty),
                fill_price=mark_price,
                status="FILLED",
                order_id="close-eth",
                reduce_only=True,
            )

        def close_spot_targets(self, **kwargs):
            events.append("core_spot_close")
            return order_service_pb2.CloseSpotTargetsResponse(
                status="stopped",
                operation_id=kwargs["operation_id"],
                results=[order_service_pb2.SpotCloseTargetResult(
                    target=order_service_pb2.SpotCloseTarget(
                        venue_id=253, exchange=1, market=1, symbol="BTCUSDT",
                    ),
                    base_asset="BTC",
                    status="terminal",
                )],
                final_snapshots=[_spot_close_final_snapshot(
                    venue_id=253,
                    environment=0,
                    btc_free="0",
                    usdt_free="1510.00000000",
                )],
            )

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(
        environment=0,
        user_id=17,
        portfolio_id=7102,
    )
    state.strategy_id = 8112
    state.configure_risk_runtime(
        order_target_keys={
            ("binance", "perpetual_futures", "ETHUSDT"),
            ("binance", "spot", "BTCUSDT"),
        },
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
    ), _FakeContext())

    assert response.stopped is True
    assert response.status == "stopped"
    assert {(item.market, item.symbol, item.status) for item in response.target_results} == {
        (2, "ETHUSDT", "terminal"),
        (1, "BTCUSDT", "terminal"),
    }
    assert futures_wallet.futures.positions[("ETHUSDT", 0)].position_qty == 0
    assert (
        wallet.wallets[("binance", "spot", 253)].spot.assets["BTC"].free
        == grpc_server.Decimal("0")
    )
    assert events == [
        "session:stopping",
        "futures_close",
        "wallet_sync",
        "wallet_sync",
        "core_spot_close",
        "wallet_sync",
    ]


def test_live_spot_guard_runs_before_any_mixed_route_close_side_effect(monkeypatch):
    futures_wallet = make_testnet_wallet(futures_positions=[{
        "symbol": "ETHUSDT",
        "position_qty": 0.02,
        "entry_price": 2300.0,
        "mark_price": 2310.0,
        "margin_mode": "cross",
    }])
    spot_wallet = make_testnet_wallet(
        spot_assets=[{
            "symbol": "BTC",
            "qty": 0.01,
            "locked": 0.0,
            "avg_entry_price": 50_000.0,
            "price": 51_000.0,
        }],
        spot_free=1000.0,
    )
    wallet = PortfolioWalletRuntime(
        portfolio_id=7102,
        allowed_routes={("binance", "perpetual_futures"), ("binance", "spot")},
        wallets={
            ("binance", "perpetual_futures", 252): futures_wallet,
            ("binance", "spot", 253): spot_wallet,
        },
    )

    class FakePortfolioClient:
        def update_session(self, **_kwargs):
            return True

    class FakeOrderClient:
        def place_order(self, *_args, **_kwargs):
            raise AssertionError("Live Spot guard must run before Futures close")

        def close_spot_targets(self, **_kwargs):
            raise AssertionError("Live Spot must never reach core close")

    class FakePlatformProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "127.0.0.1:9092",
        restore_running_sessions=False,
        agent_managed_final_status=True,
    )
    servicer.set_platform_proxy(FakePlatformProxy())
    session_id, state = servicer._sessions.create(environment=2, user_id=17, portfolio_id=7102)
    state.strategy_id = 8112
    state.configure_risk_runtime(
        order_target_keys={
            ("binance", "perpetual_futures", "ETHUSDT"),
            ("binance", "spot", "BTCUSDT"),
        },
        max_loss_close_pct=0.30,
        max_loss_close_source="platform_default",
    )
    state.configure_stop_runtime(wallet=wallet, order_client=FakeOrderClient())

    response = servicer.StopStrategy(pb2.StopStrategyRequest(
        session_id=session_id,
        user_id=17,
        stop_action=pb2.STOP_ACTION_STOP_AND_CLOSE_POSITIONS,
    ), _FakeContext())

    assert response.stopped is False
    assert response.status == "stop_failed"
    assert response.code == "SPOT_LIVE_ROLLOUT_GUARD"


def test_restore_running_sessions_marks_orphaned_sessions_terminal(monkeypatch):
    updates: list[tuple[str, str, int, str, str]] = []
    listed_runtime_ids: list[str] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            listed_runtime_ids.append(runtime_id)
            return [
                SimpleNamespace(
                    session_id="sess-running",
                    status="running",
                    bars_processed=9,
                    error="",
                    environment=2,
                    user_id=17,
                ),
                SimpleNamespace(
                    session_id="sess-stopping",
                    status="stopping",
                    bars_processed=3,
                    error="",
                    environment=1,
                    user_id=18,
                ),
            ]

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            updates.append((session_id, status, bars_processed, error, runtime_id))
            return True

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        runtime_id="rt-owned",
    )
    assert servicer._sessions.get("sess-running") is None
    assert servicer._sessions.get("sess-stopping") is None
    assert listed_runtime_ids == ["rt-owned"]
    assert updates == [
        ("sess-running", "stopped", 9, "session orphaned after strategy-service restart; runtime recovery is not implemented", "rt-owned"),
        ("sess-stopping", "stop_failed", 3, "session stop interrupted by strategy-service restart; runtime recovery is not implemented", "rt-owned"),
    ]


def test_restore_running_sessions_ignores_recoverable_sessions(monkeypatch):
    updates: list[tuple[str, str]] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            assert runtime_id == "rt-owned"
            return [
                SimpleNamespace(
                    session_id="sess-recoverable",
                    status="recoverable",
                    bars_processed=9,
                    error="runtime failed",
                    environment=1,
                    user_id=17,
                )
            ]

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            updates.append((session_id, status))
            return True

    _install_portfolio_client(monkeypatch, FakePortfolioClient)

    StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        runtime_id="rt-owned",
    )
    assert updates == []


def test_restore_running_sessions_fails_visible_when_list_fails(monkeypatch):
    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def require_running_sessions(self, runtime_id: str = ""):
            assert runtime_id == "rt-owned"
            raise RuntimeError("core-service unavailable")

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    monkeypatch.setattr(grpc_server, "RESTORE_RUNNING_SESSIONS_RETRIES", 2)
    monkeypatch.setattr(grpc_server, "RESTORE_RUNNING_SESSIONS_RETRY_SECONDS", 0)

    with pytest.raises(RuntimeError, match="startup session recovery failed"):
        StrategyServiceServicer(
            "acct:1",
            "order:1",
            {},
            "127.0.0.1:9092",
            runtime_id="rt-owned",
        )


def test_live_stream_preflight_requires_running_live_and_fresh_streams():
    """Module-level preflight: per-declared-input lookup, running + fresh ⇒ ok."""
    from strategy_service.inputs import StrategyInput
    from strategy_service.preflight import RuntimeSourceProfile, live_stream_preflight

    stream = SimpleNamespace(
        stream_id=31,
        actual_state="running",
        effective_live_delivery=True,
        last_data_at=_current_timestamp(),
        last_error="",
        key=SimpleNamespace(
            exchange="binance",
            market="futures",
            kind="kline",
            symbol="BTCUSDT",
            interval="1m",
        ),
        HasField=lambda field: field == "last_data_at",
    )

    looked_up: list[tuple[str, str, str]] = []

    def lookup(market: str, symbol: str, interval: str):
        looked_up.append((market, symbol, interval))
        return stream

    result = live_stream_preflight(
        [StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")],
        profile=RuntimeSourceProfile.DEMO,
        lookup_stream=lookup,
        freshness_grace_seconds=30,
    )

    assert result.ok
    assert result.failures == []
    assert looked_up == [("futures", "BTCUSDT", "1m")]
    assert [(b.stream_id, b.symbol, b.market) for b in result.required_streams] == [
        (31, "BTCUSDT", "futures")
    ]


def test_record_unroutable_live_kline_updates_session_state(caplog):
    servicer = StrategyServiceServicer("", "", {}, "127.0.0.1:9092")
    state = SessionState(environment=1)

    with caplog.at_level("WARNING"):
        servicer._record_unroutable_live_kline(
            "sess-unroutable",
            state,
            SimpleNamespace(symbol="BTCUSDT", market="futures", interval="1m"),
        )

    assert state.unroutable_events == 1
    assert state.last_unroutable_reason == "unroutable live kline: BTCUSDT futures 1m"
    assert "session sess-unroutable: unroutable live kline: BTCUSDT futures 1m" in caplog.text


# ── pre_C3 gate 2 regression tests (runtime-source-profile-preflight) ───────
#
# Each test maps to a scenario in
# ``openspec/changes/runtime-source-profile-preflight/specs/runtime-source-profile-preflight/spec.md``.


def test_portfolio_preflight_preserves_structured_issue_facts():
    class FakePortfolioClient:
        @staticmethod
        def preflight_strategy_session(**_kwargs):
            return SimpleNamespace(
                ok=False,
                issues=[SimpleNamespace(
                    code="SPOT_MIN_NOTIONAL",
                    message="notional below minimum",
                    exchange=1,
                    market=1,
                    symbol="BTCUSDT",
                    venue_id=77,
                    filter_type="MIN_NOTIONAL",
                    environment=1,
                    retryable=False,
                    source="preflight",
                )],
            )

    issues = StrategyServiceServicer._run_portfolio_preflight(
        acct_client=FakePortfolioClient(),
        portfolio_id=9,
        user_id=42,
        required_routes={("binance", "spot")},
        required_symbols={("binance", "spot", "BTCUSDT")},
        environment=1,
    )

    assert issues is not None and len(issues) == 1
    issue = issues[0]
    assert issue.code == "SPOT_MIN_NOTIONAL"
    assert issue.message == "notional below minimum"
    assert (issue.exchange, issue.market, issue.symbol, issue.venue_id) == (1, 1, "BTCUSDT", 77)
    assert issue.filter_type == "MIN_NOTIONAL"
    assert issue.environment == 1
    assert issue.retryable is False
    assert issue.source == "preflight"


def test_preflight_failure_proto_structured_field_numbers_are_additive():
    fields = pb2.PreflightFailureProto.DESCRIPTOR.fields_by_name
    assert {name: fields[name].number for name in (
        "kind", "reason", "input_key", "code", "exchange", "market", "symbol",
        "venue_id", "filter_type", "environment", "retryable", "source",
    )} == {
        "kind": 1,
        "reason": 2,
        "input_key": 3,
        "code": 4,
        "exchange": 5,
        "market": 6,
        "symbol": 7,
        "venue_id": 8,
        "filter_type": 9,
        "environment": 10,
        "retryable": 11,
        "source": 12,
    }


def _build_servicer_with_faked_preflight_deps(
    *,
    monkeypatch,
    environment: int,
    strategy_code: str | None,
    save_session_ok: bool = True,
    record_calls: dict | None = None,
    market_data_policy: dict | None = None,
    portfolio_preflight_response: object | None = None,
):
    """Shared scaffolding for RunStrategy integration tests.

    Wires FakePortfolioClient / portfolio snapshot runtime / Thread so the
    tests only need to vary the bits that matter (strategy INPUTS, stream
    readiness, time range, etc.).
    """
    calls = record_calls if record_calls is not None else {}
    calls.setdefault("save_session", 0)
    calls.setdefault("save_kwargs", [])
    calls.setdefault("update_portfolio", 0)
    calls.setdefault("update_wallet", 0)
    calls.setdefault("snapshot_reads", 0)
    calls.setdefault("thread_created", 0)
    calls.setdefault("update_session", [])

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def get_portfolio_snapshot(self, _portfolio_id: int, _user_id: int):
            calls["snapshot_reads"] += 1
            return make_portfolio_snapshot_with_binance_perp_and_spot(
                _portfolio_id,
                user_id=_user_id,
                environment=environment,
            )

        def preflight_strategy_session(self, **kwargs):
            calls.setdefault("portfolio_preflight", []).append(dict(kwargs))
            if portfolio_preflight_response is not None:
                return portfolio_preflight_response
            return SimpleNamespace(ok=True, issues=[])

        def get_active_strategy(self, _portfolio_id: int):
            if strategy_code is None:
                return None
            return SimpleNamespace(
                strategy_id=42,
                code=strategy_code,
                name="regression",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
            calls["save_session"] += 1
            calls["save_kwargs"].append(dict(_kwargs))
            return save_session_ok

        def update_session(self, **kwargs) -> bool:
            calls["update_session"].append(dict(kwargs))
            return True

        def update_portfolio_snapshot(self, *_args, **_kwargs):
            calls["update_portfolio"] += 1
            return SimpleNamespace()

        def update_portfolio_wallet_state(self, *_args, **_kwargs):
            calls["update_wallet"] += 1
            return SimpleNamespace()

    # Phase D2: GetMarketDataStreamStatus moved out of PortfolioClient. Default
    # behaviour (no stream) preserved so tests that don't care about D2-specific
    # state still see the original "stream missing" preflight outcome.
    class FakeMarketDataClient:
        def __init__(self, _addr: str) -> None:
            pass

        def get_market_data_stream_status(self, **_kwargs):
            return None

        def fetch_klines(self, **_kwargs):
            return []

        def fetch_backtest_page(self, **_kwargs):
            return SimpleNamespace(klines=[], next_cursor_time_ms=0, has_more=False)

        def create_or_renew_market_data_lease(self, **_kwargs) -> bool:
            return True

        def release_market_data_lease(self, **_kwargs) -> bool:
            return True

        def create_session_market_data_subscriptions(self, **kwargs) -> bool:
            calls.setdefault("session_subscriptions", []).append(dict(kwargs))
            return True

        def release_session_market_data_subscriptions(self, **kwargs) -> bool:
            calls.setdefault("release_session_subscriptions", []).append(dict(kwargs))
            return True

    real_thread = threading.Thread

    def create_test_thread(target):
        calls["thread_created"] += 1
        return real_thread(target=target, daemon=True)

    class FakePlatformProxy:
        def __init__(self) -> None:
            self.portfolio = FakePortfolioClient("")
            self.marketdata = FakeMarketDataClient("")

        def portfolio_client(self):
            return self.portfolio

        def marketdata_client(self):
            return self.marketdata

        def order_client(self):
            return SimpleNamespace()

    class FakeRuntimeDataSource:
        def iter_dataset_klines(self, **_kwargs):
            return iter(())

        def iter_live_klines(self, **_kwargs):
            return iter(())

    monkeypatch.setattr(grpc_server, "_create_session_thread", create_test_thread)

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "kafka:9092",
        market_data_policy=market_data_policy,
        runtime_id="rt-test",
        platform_proxy=FakePlatformProxy(),
    )
    servicer.set_runtime_data_source(FakeRuntimeDataSource())
    return servicer, calls


def test_prepare_run_strategy_start_is_read_only_and_never_runs_callbacks(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet):\n"
        "        raise AssertionError('callback must not run during preparation')\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )

    class ForbiddenEngine:
        def __init__(self):
            raise AssertionError("preparation must not construct the execution engine")

    monkeypatch.setattr(grpc_server, "StrategyEngine", ForbiddenEngine)
    context = _FakeContext()
    response = servicer.PrepareRunStrategyStart(
        pb2.PrepareRunStrategyStartRequest(
            run_request=pb2.RunStrategyRequest(
                portfolio_id=701,
                user_id=17,
                runtime_id="rt-test",
                interval="1m",
                start_time_ms=1,
                end_time_ms=2,
            ),
            session_id="a" * 32,
            launch_operation_id="launch-1",
        ),
        context,
    )

    assert context.code is None
    assert response.ok is True
    assert response.session.session_id == "a" * 32
    assert response.strategy_source_sha256
    assert calls["save_session"] == 0
    assert calls["thread_created"] == 0
    assert servicer._sessions.list_ids() == ()


class _PublicationContext(_FakeContext):
    def __init__(self, start_session_id: str) -> None:
        super().__init__()
        self.start_session_id = start_session_id
        self.running_publication = None

    def bind_running_publication(self, session_id, state) -> None:
        assert self.running_publication is None
        self.running_publication = (session_id, state)


def test_run_uses_canonical_id_and_waits_behind_pending_publication(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    monkeypatch.setattr(
        grpc_server,
        "_create_session_thread",
        lambda target: threading.Thread(target=target, daemon=True),
    )
    canonical_id = "a" * 32
    context = _PublicationContext(canonical_id)

    response = servicer.RunStrategy(
        SimpleNamespace(
            portfolio_id=701,
            user_id=17,
            runtime_id="rt-test",
            strategy_path="",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
            max_loss_close_pct=0.0,
            leverage=0.0,
        ),
        context,
    )

    assert context.code is None
    assert response.session_id == canonical_id
    assert calls["save_session"] == 1
    assert calls["save_kwargs"][0]["session_id"] == canonical_id
    assert calls["save_kwargs"][0]["initial_status"] == "pending"
    assert [item["status"] for item in calls["update_session"]] == ["running"]
    state = servicer._sessions.get(canonical_id)
    assert state is not None
    assert state.status == "running"
    assert state.publication_state() == "READY"
    assert context.running_publication == (canonical_id, state)
    startup = state.startup_result()
    assert startup.worker_ready.is_set()
    assert startup.commit.is_set()
    assert startup.activation_ready.is_set()
    assert not startup.release.is_set()

    assert servicer._sessions.claim_running_publication(canonical_id, state)
    assert state.complete_running_publication_submission()
    startup.release.set()
    state.thread.join(timeout=1.0)
    assert not state.thread.is_alive()


def test_committed_bootstrap_publishes_running_without_second_save(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    digest = _prepare_strategy_code_for_test("<committed-bootstrap>", source).gated_source.resolved.source_sha256
    canonical_id = "d" * 32
    servicer._require_session_bootstrap = True
    servicer._session_bootstrap = pb2.StrategySessionBootstrap(
        session_id=canonical_id,
        launch_operation_id="launch-committed",
        strategy_source_sha256=digest,
        environment=0,
    )
    context = _PublicationContext(canonical_id)

    response = servicer.RunStrategy(
        pb2.RunStrategyRequest(
            portfolio_id=704,
            user_id=17,
            runtime_id="rt-test",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
        ),
        context,
    )

    assert context.code is None
    assert response.ok is True
    assert response.session_id == canonical_id
    assert calls["save_session"] == 0
    assert [item["status"] for item in calls["update_session"]] == ["running"]
    assert calls["update_session"][0]["expected_status"] == "pending"
    state = servicer._sessions.get(canonical_id)
    assert state is not None
    state.startup_result().release.set()
    state.thread.join(timeout=1.0)


def test_committed_running_publish_exception_never_sends_unguarded_failed(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    digest = _prepare_strategy_code_for_test(
        "<committed-running-ambiguous>", source
    ).gated_source.resolved.source_sha256
    canonical_id = "f" * 32
    servicer._require_session_bootstrap = True
    servicer._session_bootstrap = pb2.StrategySessionBootstrap(
        session_id=canonical_id,
        launch_operation_id="launch-running-ambiguous",
        strategy_source_sha256=digest,
        environment=0,
    )

    def lose_running_ack(**kwargs):
        calls["update_session"].append(dict(kwargs))
        if kwargs["status"] == "running":
            raise RuntimeError("running acknowledgement lost")
        return True

    servicer._platform_proxy.portfolio.update_session = lose_running_ack
    context = _PublicationContext(canonical_id)
    response = servicer.RunStrategy(
        pb2.RunStrategyRequest(
            portfolio_id=704,
            user_id=17,
            runtime_id="rt-test",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
        ),
        context,
    )

    assert response.ok is False
    assert context.code == grpc.StatusCode.UNAVAILABLE
    assert len(calls["update_session"]) == 1
    assert calls["update_session"][0]["status"] == "running"
    assert calls["update_session"][0]["expected_status"] == "pending"
    assert calls["update_session"][0]["strict"] is True


def test_committed_empty_fact_bootstrap_rejects_environment_mismatch(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "spot", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    digest = _prepare_strategy_code_for_test(
        "<committed-empty-fact-env>", source
    ).gated_source.resolved.source_sha256
    canonical_id = "e" * 32
    servicer._require_session_bootstrap = True
    servicer._session_bootstrap = pb2.StrategySessionBootstrap(
        session_id=canonical_id,
        launch_operation_id="launch-env-mismatch",
        strategy_source_sha256=digest,
        environment=1,
    )
    context = _PublicationContext(canonical_id)

    response = servicer.RunStrategy(
        pb2.RunStrategyRequest(
            portfolio_id=704,
            user_id=17,
            runtime_id="rt-test",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
        ),
        context,
    )

    assert response.ok is False
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "environment mismatch" in context.details
    assert calls["save_session"] == 0
    assert calls["update_session"] == []


def test_worker_readiness_timeout_discards_without_durable_row(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    entered = threading.Event()
    allow_exit = threading.Event()

    def blocked_runner(*_args, **_kwargs):
        entered.set()
        allow_exit.wait(timeout=1.0)

    monkeypatch.setattr(servicer, "_run_session", blocked_runner)
    monkeypatch.setattr(
        grpc_server,
        "_create_session_thread",
        lambda target: threading.Thread(target=target, daemon=True),
    )
    servicer._session_start_timeout_seconds = 0.02
    canonical_id = "b" * 32
    context = _PublicationContext(canonical_id)

    response = servicer.RunStrategy(
        SimpleNamespace(
            portfolio_id=702,
            user_id=17,
            runtime_id="rt-test",
            strategy_path="",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
            max_loss_close_pct=0.0,
            leverage=0.0,
        ),
        context,
    )
    allow_exit.set()

    assert entered.is_set()
    assert response.session_id == ""
    assert context.code == grpc.StatusCode.DEADLINE_EXCEEDED
    assert context.details == "strategy worker readiness timed out"
    assert calls["save_session"] == 0
    assert calls["update_session"] == []
    assert servicer._sessions.get(canonical_id) is None


def test_activation_failure_keeps_durable_session_non_running(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    monkeypatch.setattr(
        grpc_server,
        "_create_session_thread",
        lambda target: threading.Thread(target=target, daemon=True),
    )
    monkeypatch.setattr(
        strategy_base.BaseStrategy,
        "activate_order_event_cursor",
        lambda _self: (_ for _ in ()).throw(RuntimeError("cursor failed")),
    )
    canonical_id = "c" * 32
    context = _PublicationContext(canonical_id)

    response = servicer.RunStrategy(
        SimpleNamespace(
            portfolio_id=703,
            user_id=17,
            runtime_id="rt-test",
            strategy_path="",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
            max_loss_close_pct=0.0,
            leverage=0.0,
        ),
        context,
    )

    assert response.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert calls["save_kwargs"][0]["initial_status"] == "pending"
    assert [item["status"] for item in calls["update_session"]] == ["failed"]
    state = servicer._sessions.get(canonical_id)
    assert state is None or state.status == "failed"


def test_late_activation_after_timeout_releases_new_subscription(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=1,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    monkeypatch.setattr(
        servicer,
        "_run_profile_preflight",
        lambda **_kwargs: grpc_server.PreflightResult(
            profile=grpc_server.RuntimeSourceProfile.DEMO,
            required_streams=[
                StreamBinding(
                    stream_id=1,
                    exchange="binance",
                    market="perpetual_futures",
                    kind="kline",
                    symbol="BTCUSDT",
                    interval="1m",
                )
            ],
        ),
    )
    monkeypatch.setattr(
        strategy_base.BaseStrategy,
        "activate_order_event_cursor",
        lambda _self: None,
    )
    create_started = threading.Event()
    allow_create = threading.Event()
    events: list[str] = []

    def create_subscription(**_kwargs):
        events.append("create:start")
        create_started.set()
        allow_create.wait(timeout=1.0)
        events.append("create:end")
        return True

    def release_subscription(**_kwargs):
        events.append("release")
        return True

    monkeypatch.setattr(
        servicer._platform_proxy.marketdata,
        "create_session_market_data_subscriptions",
        create_subscription,
    )
    monkeypatch.setattr(
        servicer._platform_proxy.marketdata,
        "release_session_market_data_subscriptions",
        release_subscription,
    )
    threads: list[threading.Thread] = []

    def create_thread(target):
        thread = threading.Thread(target=target, daemon=True)
        threads.append(thread)
        return thread

    monkeypatch.setattr(grpc_server, "_create_session_thread", create_thread)
    servicer._session_start_timeout_seconds = 0.02
    context = _PublicationContext("d" * 32)

    response = servicer.RunStrategy(
        SimpleNamespace(
            portfolio_id=704,
            user_id=17,
            runtime_id="rt-test",
            strategy_path="",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
            max_loss_close_pct=0.0,
            leverage=0.0,
        ),
        context,
    )

    assert create_started.is_set()
    assert response.session_id == ""
    assert context.code == grpc.StatusCode.DEADLINE_EXCEEDED
    assert context.details == "strategy activation timed out"
    allow_create.set()
    for thread in threads:
        thread.join(timeout=1.0)
        assert not thread.is_alive()
    assert events[-2:] == ["create:end", "release"]
    assert calls["update_session"][-1]["status"] == "failed"


@pytest.mark.parametrize("method_name", ["RunStrategy", "PreviewRunStrategy"])
def test_bare_materialization_failure_is_symmetric_and_stops_before_gate(
    monkeypatch,
    method_name,
):
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=_phase3_strategy_code(),
        market_data_policy={"preflight_enabled": False},
    )
    servicer._runtime_source = "bare"

    def fail_materialization(**_kwargs):
        raise grpc_server.DebugStrategySourceError("materialization_failed")

    monkeypatch.setattr(servicer, "_debug_strategy_source_for_db_code", fail_materialization)
    monkeypatch.setattr(
        grpc_server,
        "_prepare_gated_strategy_for_rpc",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("gate must not run after Bare materialization failure")
        ),
    )
    request = SimpleNamespace(
        portfolio_id=900,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    response = getattr(servicer, method_name)(request, context)

    assert getattr(response, "session_id", "") == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details == "failed to materialize bare debug strategy source"
    assert calls["snapshot_reads"] == 1
    assert calls["save_session"] == 0
    assert calls["thread_created"] == 0
    assert servicer._sessions.list_ids() == ()


@pytest.mark.parametrize("method_name", ["RunStrategy", "PreviewRunStrategy"])
def test_dependency_gate_rejects_unsupported_import_before_side_effects(monkeypatch, method_name):
    source = (
        "import talib\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    request = SimpleNamespace(
        portfolio_id=901,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    response = getattr(servicer, method_name)(request, context)

    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details.startswith("STRATEGY_DEPENDENCY_ERROR:")
    payload = json.loads(context.details.removeprefix("STRATEGY_DEPENDENCY_ERROR:"))
    assert payload["code"] == "UNSUPPORTED_STRATEGY_DEPENDENCY"
    assert payload["module"] == "talib"
    assert calls["snapshot_reads"] == 1
    assert calls["save_session"] == 0
    assert calls["thread_created"] == 0
    assert servicer._sessions.list_ids() == ()
    assert getattr(response, "session_id", "") == ""


@pytest.mark.parametrize("method_name", ["RunStrategy", "PreviewRunStrategy"])
def test_strategy_source_gate_rejects_missing_allowed_child_symmetrically(monkeypatch, method_name):
    source = (
        "import google.hushine_missing\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    request = SimpleNamespace(
        portfolio_id=902,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    getattr(servicer, method_name)(request, context)

    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details.startswith("STRATEGY_DEPENDENCY_ERROR:")
    payload = json.loads(context.details.removeprefix("STRATEGY_DEPENDENCY_ERROR:"))
    assert payload["code"] == "STRATEGY_DEPENDENCY_UNAVAILABLE"
    assert payload["module"] == "google.hushine_missing"
    assert calls["snapshot_reads"] == 1
    assert calls["save_session"] == 0
    assert calls["thread_created"] == 0
    assert servicer._sessions.list_ids() == ()


@pytest.mark.parametrize("method_name", ["RunStrategy", "PreviewRunStrategy"])
def test_strategy_source_gate_internal_failure_is_fixed_and_side_effect_free(monkeypatch, method_name):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )

    def fail_gate(*_args, **_kwargs):
        raise RuntimeError("gate-canary-secret")

    monkeypatch.setattr(grpc_server, "_resolve_and_gate_strategy_source", fail_gate, raising=False)
    request = SimpleNamespace(
        portfolio_id=903,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    getattr(servicer, method_name)(request, context)

    assert context.code == grpc.StatusCode.INTERNAL
    assert context.details == "strategy source gate failed"
    assert "canary" not in context.details
    assert calls["snapshot_reads"] == 1
    assert calls["save_session"] == 0
    assert calls["thread_created"] == 0
    assert servicer._sessions.list_ids() == ()


@pytest.mark.parametrize("method_name", ["RunStrategy", "PreviewRunStrategy"])
@pytest.mark.parametrize(
    ("serializer_name", "source"),
    [
        (
            "_dependency_error_details",
            "import talib\n"
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            "    ORDER_TARGETS = []\n"
            "    def on_market_data(self, data, wallet): return None\n",
        ),
        (
            "_gate_validation_details",
            "class MyStrategy:\n"
            "    ORDER_TARGETS = []\n"
            "    def on_market_data(self, data, wallet): return None\n",
        ),
    ],
)
def test_failed_gate_serializer_baseexception_is_symmetric_internal_and_side_effect_free(
    monkeypatch,
    caplog,
    method_name,
    serializer_name,
    source,
):
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )

    def fail_serializer(_value):
        raise SystemExit("gate-serializer-log-canary")

    monkeypatch.setattr(grpc_server, serializer_name, fail_serializer)
    request = SimpleNamespace(
        portfolio_id=903,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    with caplog.at_level("ERROR"):
        response = getattr(servicer, method_name)(request, context)

    assert getattr(response, "session_id", "") == ""
    assert context.code == grpc.StatusCode.INTERNAL
    assert context.details == "strategy source gate failed"
    assert calls["snapshot_reads"] == 1
    assert calls["save_session"] == 0
    assert calls["thread_created"] == 0
    assert servicer._sessions.list_ids() == ()
    assert [record.getMessage() for record in caplog.records] == [
        f"STRATEGY_SOURCE_GATE_INTERNAL operation={method_name}"
    ]
    assert "gate-serializer-log-canary" not in caplog.text
    assert "Traceback" not in caplog.text


@pytest.mark.parametrize("method_name", ["RunStrategy", "PreviewRunStrategy"])
def test_strategy_source_resolution_internal_failure_is_distinct_and_side_effect_free(
    monkeypatch,
    method_name,
    caplog,
):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )

    def fail_resolution(*_args, **_kwargs):
        raise SystemExit("resolution-canary-secret")

    monkeypatch.setattr(grpc_server, "resolve_strategy_source", fail_resolution)
    request = SimpleNamespace(
        portfolio_id=903,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    with caplog.at_level("ERROR"):
        getattr(servicer, method_name)(request, context)

    assert context.code == grpc.StatusCode.INTERNAL
    assert context.details == "strategy source resolution failed"
    assert [record.getMessage() for record in caplog.records] == [
        f"STRATEGY_SOURCE_RESOLUTION_INTERNAL operation={method_name}"
    ]
    assert "resolution-canary-secret" not in caplog.text
    assert "Traceback" not in caplog.text
    assert calls["snapshot_reads"] == 1
    assert calls["save_session"] == 0
    assert calls["thread_created"] == 0
    assert servicer._sessions.list_ids() == ()


def test_strategy_source_gate_run_claims_once_before_register(monkeypatch):
    import os

    os.environ["HUSHINE_RUN_TOP_LEVEL_COUNT"] = "0"
    os.environ["HUSHINE_RUN_CONSTRUCTOR_COUNT"] = "0"
    source = (
        "import os\n"
        "os.environ['HUSHINE_RUN_TOP_LEVEL_COUNT'] = "
        "str(int(os.environ['HUSHINE_RUN_TOP_LEVEL_COUNT']) + 1)\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def __init__(self):\n"
        "        os.environ['HUSHINE_RUN_CONSTRUCTOR_COUNT'] = "
        "str(int(os.environ['HUSHINE_RUN_CONSTRUCTOR_COUNT']) + 1)\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    try:
        servicer, calls = _build_servicer_with_faked_preflight_deps(
            monkeypatch=monkeypatch,
            environment=0,
            strategy_code=source,
            market_data_policy={"preflight_enabled": False},
        )
        request = SimpleNamespace(
            portfolio_id=904,
            user_id=17,
            runtime_id="rt-test",
            strategy_path="",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
        )
        context = _FakeContext()

        response = servicer.RunStrategy(request, context)

        assert context.code is None
        assert response.session_id
        assert calls["save_session"] == 1
        assert calls["thread_created"] == 1
        assert os.environ["HUSHINE_RUN_TOP_LEVEL_COUNT"] == "1"
        assert os.environ["HUSHINE_RUN_CONSTRUCTOR_COUNT"] == "1"
        assert calls["portfolio_preflight"][0]["session_id"] == response.session_id
        assert servicer._sessions.get(response.session_id) is not None
    finally:
        del os.environ["HUSHINE_RUN_TOP_LEVEL_COUNT"]
        del os.environ["HUSHINE_RUN_CONSTRUCTOR_COUNT"]


def test_strategy_source_gate_binding_failure_is_invisible(monkeypatch, caplog):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def __setattr__(self, name, value):\n"
        "        if name == 'notify': raise SystemExit('binding-canary')\n"
        "        object.__setattr__(self, name, value)\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    request = SimpleNamespace(
        portfolio_id=905,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    with caplog.at_level("ERROR"):
        response = servicer.RunStrategy(request, context)

    assert response.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details == "strategy could not be loaded"
    assert calls["save_session"] == 0
    assert calls["thread_created"] == 0
    assert servicer._sessions.list_ids() == ()
    assert [record.getMessage() for record in caplog.records] == [
        "STRATEGY_SOURCE_LOAD_FAILED operation=RunStrategy"
    ]
    assert "binding-canary" not in caplog.text
    assert "raise SystemExit" not in caplog.text
    assert "Traceback" not in caplog.text


def test_strategy_source_gate_cursor_failure_updates_failed_without_thread(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )

    class FailingOrderClient:
        def list_order_lifecycle_events(self, **_kwargs):
            raise RuntimeError("order-canary")

    monkeypatch.setattr(servicer._platform_proxy, "order_client", lambda: FailingOrderClient())
    request = SimpleNamespace(
        portfolio_id=906,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    response = servicer.RunStrategy(request, context)

    assert response.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details == "strategy activation failed"
    assert calls["save_session"] == 1
    assert calls["thread_created"] == 1
    assert calls["update_session"][-1]["status"] == "failed"
    assert servicer._sessions.list_ids() == ()


@pytest.mark.parametrize("update_failure", ["false", "baseexception"])
def test_cursor_startup_failure_retains_owner_when_terminal_update_is_unconfirmed(
    monkeypatch,
    update_failure,
):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )

    class FailingOrderClient:
        def list_order_lifecycle_events(self, **_kwargs):
            raise SystemExit("cursor-startup-secret")

    monkeypatch.setattr(servicer._platform_proxy, "order_client", lambda: FailingOrderClient())
    if update_failure == "false":
        monkeypatch.setattr(
            servicer._platform_proxy.portfolio,
            "update_session",
            lambda **_kwargs: False,
        )
    else:
        monkeypatch.setattr(
            servicer._platform_proxy.portfolio,
            "update_session",
            lambda **_kwargs: (_ for _ in ()).throw(
                KeyboardInterrupt("status-update-secret")
            ),
        )
    context = _FakeContext()

    response = servicer.RunStrategy(
        SimpleNamespace(
            portfolio_id=906,
            user_id=17,
            runtime_id="rt-test",
            strategy_path="",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
        ),
        context,
    )

    assert response.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details == "strategy activation failed"
    session_id = calls["save_kwargs"][0]["session_id"]
    retained = servicer._sessions.get(session_id)
    assert retained is not None
    assert retained.status == "failed"
    assert retained.error == "strategy activation failed"
    assert calls["thread_created"] == 1


@pytest.mark.parametrize(
    ("failure_seam", "expected_code", "expected_detail"),
    [
        (
            "snapshot",
            grpc.StatusCode.UNAVAILABLE,
            "failed to persist strategy_start snapshot",
        ),
        ("cursor", grpc.StatusCode.FAILED_PRECONDITION, "strategy activation failed"),
        (
            "thread_registration",
            grpc.StatusCode.INTERNAL,
            "session thread registration failed",
        ),
        ("thread_start", grpc.StatusCode.INTERNAL, "session thread start failed"),
    ],
)
@pytest.mark.parametrize("update_failure", ["false", "baseexception"])
def test_post_save_startup_failure_matrix_retains_unconfirmed_owner(
    monkeypatch,
    failure_seam,
    expected_code,
    expected_detail,
    update_failure,
):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    if update_failure == "false":
        monkeypatch.setattr(
            servicer._platform_proxy.portfolio,
            "update_session",
            lambda **_kwargs: False,
        )
    else:
        monkeypatch.setattr(
            servicer._platform_proxy.portfolio,
            "update_session",
            lambda **_kwargs: (_ for _ in ()).throw(
                KeyboardInterrupt("terminal-update-secret")
            ),
        )

    if failure_seam == "snapshot":
        monkeypatch.setattr(
            grpc_server,
            "_sync_strategy_snapshot",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                SystemExit("snapshot-secret")
            ),
        )
    elif failure_seam == "cursor":
        monkeypatch.setattr(
            strategy_base.BaseStrategy,
            "activate_order_event_cursor",
            lambda _self: (_ for _ in ()).throw(GeneratorExit("cursor-secret")),
        )
    elif failure_seam == "thread_registration":
        monkeypatch.setattr(servicer._sessions, "set_thread", lambda *_args: False)
    else:
        class FailingThread:
            def __init__(self, target):
                self.target = target

            def start(self):
                raise SystemExit("thread-start-secret")

        monkeypatch.setattr(grpc_server, "_create_session_thread", FailingThread)

    context = _FakeContext()
    response = servicer.RunStrategy(
        SimpleNamespace(
            portfolio_id=909,
            user_id=17,
            runtime_id="rt-test",
            strategy_path="",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
        ),
        context,
    )

    assert response.session_id == ""
    assert context.code == expected_code
    assert context.details == expected_detail
    if failure_seam in {"thread_registration", "thread_start"}:
        assert calls["save_kwargs"] == []
        assert calls["update_session"] == []
        assert servicer._sessions.list_ids() == ()
        return
    session_id = calls["save_kwargs"][0]["session_id"]
    state = servicer._sessions.get(session_id)
    assert state is not None
    assert state.status == "failed"
    assert state.error == expected_detail


@pytest.mark.parametrize("release_failure", [False, None, "baseexception"])
def test_abort_persisted_startup_retains_owner_until_subscription_release_is_confirmed(
    monkeypatch,
    release_failure,
):
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    session_id, state = servicer._sessions.prepare(
        session_id="d" * 32,
        environment=1,
        runtime_id="rt-test",
    )
    servicer._sessions.register(session_id, state)
    monkeypatch.setattr(servicer, "_persist_session_status", lambda *_args, **_kwargs: True)
    if release_failure == "baseexception":
        monkeypatch.setattr(
            servicer,
            "_release_session_market_data_subscriptions",
            lambda *_args: (_ for _ in ()).throw(
                SystemExit("release-result-log-canary")
            ),
        )
    else:
        monkeypatch.setattr(
            servicer,
            "_release_session_market_data_subscriptions",
            lambda *_args: release_failure,
        )
    context = _FakeContext()

    response = servicer._abort_persisted_startup(
        session_id=session_id,
        state=state,
        environment=1,
        context=context,
        error="strategy activation failed",
        status_code=grpc.StatusCode.FAILED_PRECONDITION,
        detail="strategy activation failed",
    )

    assert response.session_id == ""
    assert servicer._sessions.get(session_id) is state
    assert state.status == "failed"
    assert state.error == "strategy activation failed"


@pytest.mark.parametrize(
    ("core_result", "owner_retained", "patch_count"),
    [
        (True, False, 0),
        (False, True, 1),
    ],
)
def test_agent_managed_startup_abort_still_requires_real_core_confirmation(
    monkeypatch,
    core_result,
    owner_retained,
    patch_count,
):
    updates = []
    patches = []
    platform_proxy = SimpleNamespace(
        send_session_status_patch=lambda **kwargs: patches.append(dict(kwargs))
    )
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
        platform_proxy=platform_proxy,
        agent_managed_final_status=True,
    )
    portfolio_client = SimpleNamespace(
        update_session=lambda **kwargs: updates.append(dict(kwargs)) or core_result
    )
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: portfolio_client)
    session_id, state = servicer._sessions.prepare(
        session_id="e" * 32,
        environment=0,
        runtime_id="rt-test",
    )
    servicer._sessions.register(session_id, state)

    servicer._abort_persisted_startup(
        session_id=session_id,
        state=state,
        environment=0,
        context=_FakeContext(),
        error="strategy activation failed",
        status_code=grpc.StatusCode.FAILED_PRECONDITION,
        detail="strategy activation failed",
    )

    assert len(updates) == 1
    assert updates[0]["status"] == "failed"
    assert len(patches) == patch_count
    if owner_retained:
        assert servicer._sessions.get(session_id) is state
    else:
        assert servicer._sessions.get(session_id) is None


@pytest.mark.parametrize(
    "core_result",
    [
        False,
        None,
        1,
        pytest.param(object(), id="truthy-object"),
    ],
)
def test_startup_status_patch_submission_never_confirms_core_persistence(
    monkeypatch,
    core_result,
):
    updates = []
    patches = []
    platform_proxy = SimpleNamespace(
        send_session_status_patch=lambda **kwargs: patches.append(dict(kwargs))
    )
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
        platform_proxy=platform_proxy,
    )
    portfolio_client = SimpleNamespace(
        update_session=lambda **kwargs: updates.append(dict(kwargs)) or core_result
    )
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: portfolio_client)
    session_id, state = servicer._sessions.prepare(
        session_id="f" * 32,
        environment=0,
        runtime_id="rt-test",
    )
    servicer._sessions.register(session_id, state)

    servicer._abort_persisted_startup(
        session_id=session_id,
        state=state,
        environment=0,
        context=_FakeContext(),
        error="strategy activation failed",
        status_code=grpc.StatusCode.FAILED_PRECONDITION,
        detail="strategy activation failed",
    )

    assert len(updates) == 1
    assert len(patches) == 1
    assert servicer._sessions.get(session_id) is state


@pytest.mark.parametrize("fatal_type", [RuntimeError, SystemExit])
def test_startup_terminal_update_failure_still_submits_patch_and_retains_owner(
    monkeypatch,
    fatal_type,
):
    patches = []
    platform_proxy = SimpleNamespace(
        send_session_status_patch=lambda **kwargs: patches.append(dict(kwargs))
    )
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
        platform_proxy=platform_proxy,
    )
    portfolio_client = SimpleNamespace(
        update_session=lambda **_kwargs: (_ for _ in ()).throw(
            fatal_type("startup-terminal-update-canary")
        )
    )
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: portfolio_client)
    session_id, state = servicer._sessions.prepare(
        session_id="1" * 32,
        environment=0,
        runtime_id="rt-test",
    )
    servicer._sessions.register(session_id, state)

    servicer._abort_persisted_startup(
        session_id=session_id,
        state=state,
        environment=0,
        context=_FakeContext(),
        error="strategy activation failed",
        status_code=grpc.StatusCode.FAILED_PRECONDITION,
        detail="strategy activation failed",
    )

    assert len(patches) == 1
    assert servicer._sessions.get(session_id) is state


@pytest.mark.parametrize("fatal_type", [RuntimeError, SystemExit])
def test_startup_terminal_client_acquisition_failure_still_patches_releases_and_retains(
    monkeypatch,
    fatal_type,
):
    patches = []
    releases = []
    platform_proxy = SimpleNamespace(
        send_session_status_patch=lambda **kwargs: patches.append(dict(kwargs))
    )
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
        platform_proxy=platform_proxy,
    )

    def fail_client_acquisition():
        raise fatal_type("startup-terminal-client-canary")

    monkeypatch.setattr(servicer, "_portfolio_client", fail_client_acquisition)
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda session_id, state: releases.append((session_id, state)) or True,
    )
    session_id, state = servicer._sessions.prepare(
        session_id="3" * 32,
        environment=1,
        runtime_id="rt-test",
    )
    servicer._sessions.register(session_id, state)

    servicer._abort_persisted_startup(
        session_id=session_id,
        state=state,
        environment=1,
        context=_FakeContext(),
        error="strategy activation failed",
        status_code=grpc.StatusCode.FAILED_PRECONDITION,
        detail="strategy activation failed",
    )

    assert patches == [
        {
            "session_id": session_id,
            "status": "failed",
            "bars_processed": 0,
            "error": "strategy activation failed",
            "runtime_id": "rt-test",
        }
    ]
    assert releases == [(session_id, state)]
    assert servicer._sessions.get(session_id) is state


@pytest.mark.parametrize(
    "release_result",
    [1, pytest.param(object(), id="truthy-object")],
)
def test_startup_abort_rejects_truthy_non_boolean_subscription_release(
    monkeypatch,
    release_result,
):
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    monkeypatch.setattr(servicer, "_persist_session_status", lambda *_args, **_kwargs: True)
    marketdata_client = SimpleNamespace(
        release_session_market_data_subscriptions=lambda **_kwargs: release_result
    )
    monkeypatch.setattr(servicer, "_marketdata_client", lambda: marketdata_client)
    session_id, state = servicer._sessions.prepare(
        session_id="2" * 32,
        environment=1,
        runtime_id="rt-test",
    )
    servicer._sessions.register(session_id, state)

    servicer._abort_persisted_startup(
        session_id=session_id,
        state=state,
        environment=1,
        context=_FakeContext(),
        error="strategy activation failed",
        status_code=grpc.StatusCode.FAILED_PRECONDITION,
        detail="strategy activation failed",
    )

    assert servicer._sessions.get(session_id) is state


def test_session_thread_registration_failure_never_starts_or_leaks_session(monkeypatch):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )
    thread_calls = {"created": 0, "started": 0}

    class FakeThread:
        def __init__(self, target):
            thread_calls["created"] += 1
            self.target = target

        def start(self):
            thread_calls["started"] += 1

    monkeypatch.setattr(grpc_server, "_create_session_thread", FakeThread)
    monkeypatch.setattr(servicer._sessions, "set_thread", lambda *_args: False)
    context = _FakeContext()

    response = servicer.RunStrategy(
        SimpleNamespace(
            portfolio_id=907,
            user_id=17,
            runtime_id="rt-test",
            strategy_path="",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
        ),
        context,
    )

    assert response.session_id == ""
    assert context.code == grpc.StatusCode.INTERNAL
    assert context.details == "session thread registration failed"
    assert thread_calls == {"created": 1, "started": 0}
    assert calls["save_session"] == 0
    assert calls["update_session"] == []
    assert servicer._sessions.list_ids() == ()


@pytest.mark.parametrize("fatal_stage", ["before_business", "after_business"])
def test_otel_baseexception_is_owned_by_one_session_terminal_boundary(
    monkeypatch,
    fatal_stage,
):
    source = (
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=source,
        market_data_policy={"preflight_enabled": False},
    )

    class InlineThread:
        def __init__(self, target):
            self.target = target

        def start(self):
            self.target()

    def fatal_instrumentation(_parent, _span_name, fn):
        if fatal_stage == "before_business":
            raise SystemExit("otel-before-secret")
        fn()
        raise GeneratorExit("otel-after-secret")

    monkeypatch.setattr(grpc_server, "_create_session_thread", _AsyncTestThread)
    monkeypatch.setattr(grpc_server, "_run_in_otel_context", fatal_instrumentation)
    context = _FakeContext()

    response = servicer.RunStrategy(
        SimpleNamespace(
            portfolio_id=908,
            user_id=17,
            runtime_id="rt-test",
            strategy_path="",
            interval="1m",
            start_time_ms=1,
            end_time_ms=2,
        ),
        context,
    )

    assert context.code is None
    state = servicer._sessions.get(response.session_id)
    assert state is not None
    state.thread.join(timeout=1.0)
    assert state.status == "failed"
    assert state.error == "strategy session terminated"
    terminal_updates = [
        item for item in calls["update_session"] if item["session_id"] == response.session_id
    ]
    assert [item["status"] for item in terminal_updates] == ["running", "failed"]
    assert terminal_updates[-1]["error"] == "strategy session terminated"


@pytest.mark.parametrize(
    "otel_stage",
    ["tracer_lookup", "attach", "span_start", "span_enter", "span_exit", "detach"],
)
@pytest.mark.parametrize("fatal_type", [SystemExit, KeyboardInterrupt, GeneratorExit])
def test_each_otel_stage_baseexception_reaches_one_terminal_owner(
    monkeypatch,
    caplog,
    otel_stage,
    fatal_type,
):
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    state = SessionState(environment=0, portfolio_id=101, strategy_id=202, user_id=17)
    business_calls: list[str] = []

    class FakeEngine:
        @staticmethod
        def raise_if_user_code_fatal() -> None:
            return None

    def raise_stage(stage: str) -> None:
        if otel_stage == stage:
            raise fatal_type("otel-stage-log-canary")

    class FakeSpan:
        def __enter__(self):
            raise_stage("span_enter")
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            raise_stage("span_exit")
            return False

    class FakeTracer:
        @staticmethod
        def start_as_current_span(_name):
            raise_stage("span_start")
            return FakeSpan()

    class FakeTrace:
        @staticmethod
        def get_tracer(_name):
            raise_stage("tracer_lookup")
            return FakeTracer()

    class FakeContext:
        @staticmethod
        def attach(_parent):
            raise_stage("attach")
            return object()

        @staticmethod
        def detach(_token):
            raise_stage("detach")

    monkeypatch.setattr(grpc_server, "_OTEL_AVAILABLE", True)
    monkeypatch.setattr(grpc_server, "_otel_trace", FakeTrace())
    monkeypatch.setattr(grpc_server, "_otel_context", FakeContext())
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: object())
    monkeypatch.setattr(
        servicer,
        "_run_backtest",
        lambda *_args, **_kwargs: (
            business_calls.append("business"),
            state.transition("finished", bars=1),
        ),
    )
    monkeypatch.setattr(servicer, "_release_stream_leases", lambda *_args: None)
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda *_args: True,
    )
    monkeypatch.setattr(grpc_server, "_sync_strategy_snapshot", lambda *_args, **_kwargs: None)
    terminal_updates: list[tuple[str, str, str]] = []
    terminal_marks: list[tuple[str, SessionState]] = []
    monkeypatch.setattr(
        servicer,
        "_persist_session_status",
        lambda session_id, inner_state, **_kwargs: terminal_updates.append(
            (session_id, inner_state.status, inner_state.error)
        ) or True,
    )
    monkeypatch.setattr(
        servicer._sessions,
        "mark_terminal",
        lambda session_id, inner_state: terminal_marks.append(
            (session_id, inner_state)
        ) or True,
    )

    with caplog.at_level("ERROR"):
        servicer._run_session(
            session_id="f" * 32,
            state=state,
            request=SimpleNamespace(end_time_ms=2),
            wallet=_wallet_with_futures_slot(),
            environment=0,
            portfolio_id=101,
            user_id=17,
            declared_inputs=[],
            engine=FakeEngine(),
            user_strategy=SimpleNamespace(last_market_time=None),
            strategy_id=202,
            otel_parent_context=object(),
        )

    expected_business_calls = 1 if otel_stage in {"span_exit", "detach"} else 0
    assert len(business_calls) == expected_business_calls
    assert state.status == "failed"
    assert state.error == "strategy session terminated"
    assert terminal_updates == [
        ("f" * 32, "failed", "strategy session terminated")
    ]
    assert terminal_marks == [("f" * 32, state)]
    assert [record.getMessage() for record in caplog.records] == [
        (
            f"STRATEGY_SESSION_FATAL session={'f' * 32} "
            "portfolio_id=101 strategy_id=202"
        )
    ]
    assert "otel-stage-log-canary" not in caplog.text
    assert "Traceback" not in caplog.text


@pytest.mark.parametrize("teardown_stage", ["span_exit", "detach"])
def test_user_fatal_survives_otel_teardown_baseexception(
    monkeypatch,
    caplog,
    teardown_stage,
) -> None:
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        restore_running_sessions=False,
    )
    state = SessionState(environment=0, portfolio_id=101, strategy_id=202, user_id=17)
    fatal = strategy_base.StrategyUserCodeFatalError(stage="callback")
    instrumentation_calls: list[str] = []
    cleanup_calls: list[str] = []

    class FatalEngine:
        @staticmethod
        def raise_if_user_code_fatal() -> None:
            instrumentation_calls.append("business")
            raise fatal

    class FakeSpan:
        def __enter__(self):
            instrumentation_calls.append("span_enter")
            return self

        def __exit__(self, _exc_type, _exc, _traceback):
            instrumentation_calls.append("span_exit")
            if teardown_stage == "span_exit":
                raise SystemExit("span-exit-log-canary")
            return False

    class FakeTracer:
        @staticmethod
        def start_as_current_span(_name):
            return FakeSpan()

    class FakeTrace:
        @staticmethod
        def get_tracer(_name):
            return FakeTracer()

    class FakeContext:
        @staticmethod
        def attach(_parent):
            instrumentation_calls.append("attach")
            return object()

        @staticmethod
        def detach(_token):
            instrumentation_calls.append("detach")
            if teardown_stage == "detach":
                raise KeyboardInterrupt("detach-log-canary")

    monkeypatch.setattr(grpc_server, "_OTEL_AVAILABLE", True)
    monkeypatch.setattr(grpc_server, "_otel_trace", FakeTrace())
    monkeypatch.setattr(grpc_server, "_otel_context", FakeContext())
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: object())
    monkeypatch.setattr(
        servicer,
        "_release_stream_leases",
        lambda *_args: cleanup_calls.append("release_leases"),
    )
    monkeypatch.setattr(
        servicer,
        "_release_session_market_data_subscriptions",
        lambda *_args: cleanup_calls.append("release_subscriptions") or True,
    )
    monkeypatch.setattr(
        grpc_server,
        "_sync_strategy_snapshot",
        lambda *_args, **_kwargs: cleanup_calls.append("snapshot"),
    )
    terminal_updates: list[tuple[str, str, str]] = []
    terminal_marks: list[tuple[str, SessionState]] = []
    monkeypatch.setattr(
        servicer,
        "_persist_session_status",
        lambda session_id, inner_state, **_kwargs: terminal_updates.append(
            (session_id, inner_state.status, inner_state.error)
        ) or True,
    )
    monkeypatch.setattr(
        servicer._sessions,
        "mark_terminal",
        lambda session_id, inner_state: terminal_marks.append(
            (session_id, inner_state)
        ) or True,
    )

    with caplog.at_level("ERROR"):
        servicer._run_session(
            session_id="c" * 32,
            state=state,
            request=SimpleNamespace(end_time_ms=2),
            wallet=_wallet_with_futures_slot(),
            environment=0,
            portfolio_id=101,
            user_id=17,
            declared_inputs=[],
            engine=FatalEngine(),
            user_strategy=SimpleNamespace(last_market_time=None),
            strategy_id=202,
            otel_parent_context=object(),
        )

    assert instrumentation_calls == [
        "attach",
        "span_enter",
        "business",
        "span_exit",
        "detach",
    ]
    assert cleanup_calls == ["release_leases", "release_subscriptions", "snapshot"]
    assert state.status == "failed"
    assert state.error == "strategy user code terminated"
    assert terminal_updates == [
        ("c" * 32, "failed", "strategy user code terminated")
    ]
    assert terminal_marks == [("c" * 32, state)]
    assert [record.getMessage() for record in caplog.records] == [
        (
            f"STRATEGY_USER_CODE_FATAL session={'c' * 32} "
            "portfolio_id=101 strategy_id=202"
        )
    ]
    assert "log-canary" not in caplog.text
    assert "Traceback" not in caplog.text


def test_run_strategy_rejects_mode1_as_unsupported_profile(monkeypatch):
    """Scenario: Unsupported profile fails as a profile error (LIVE today).

    environment=2 MUST produce a FAILED_PRECONDITION with a structured PROFILE
    failure — NOT a 'failed to build wallet: no wallet runtime registered'
    leak from the wallet-factory registry.
    """
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=2,  # live
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    request = SimpleNamespace(
        portfolio_id=101, user_id=17,
        strategy_path="", interval="1m",
        start_time_ms=0, end_time_ms=0,
    )
    context = _FakeContext()
    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "profile" in context.details.lower()
    assert "live" in context.details.lower()
    # No session created.
    assert calls["save_session"] == 0
    assert servicer._sessions._sessions == {}


def test_run_strategy_persists_runtime_binding(monkeypatch):
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )
    servicer._runtime_id = "rt-hosted"
    servicer._runtime_source = "hosted"
    servicer._runtime_name = "default"

    def fake_preflight(**_kwargs):
        from strategy_service.preflight import PreflightResult, RuntimeSourceProfile
        return PreflightResult(profile=RuntimeSourceProfile.BACKTEST)

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=201,
        user_id=17,
        runtime_id="rt-hosted",
        strategy_path="",
        interval="1m",
        start_time_ms=1_700_000_000_000,
        end_time_ms=1_700_000_060_000,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id != ""
    assert context.code is None
    assert calls["save_kwargs"][-1]["runtime_id"] == "rt-hosted"
    assert calls["save_kwargs"][-1]["runtime_source"] == "hosted"
    assert calls["save_kwargs"][-1]["runtime_name"] == "default"
    assert servicer._sessions.get(resp.session_id).runtime_id == "rt-hosted"


def test_run_strategy_stores_effective_risk_controls(monkeypatch):
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT"}]\n'
            "    LEVERAGE = 6\n"
            '    RISK_CONTROLS = {"max_loss_close_pct": 0.2}\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
        market_data_policy={"preflight_enabled": False},
    )

    request = SimpleNamespace(
        portfolio_id=201,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
        max_loss_close_pct=0.25,
        leverage=3,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(request, context)

    assert context.code is None
    state = servicer._sessions.get(resp.session_id)
    assert state.max_loss_close_pct == 0.2
    assert state.max_loss_close_source == "strategy"
    assert state.leverage == 6
    assert state.leverage_source == "strategy_default"
    assert state.initial_margin_balance == 1000.0
    assert state.order_target_keys == {("binance", "perpetual_futures", "BTCUSDT")}
    assert calls["save_kwargs"][-1]["leverage"] == 6


def test_effective_risk_controls_excludes_leverage_authority():
    declarations = SimpleNamespace(risk_controls=SimpleNamespace(max_loss_close_pct=None))

    effective = grpc_server._effective_risk_controls_from_request(
        declarations,
        0.25,
    )

    assert effective.max_loss_close_pct == 0.25
    assert effective.max_loss_close_source == "request_default"
    assert not hasattr(effective, "leverage")
    assert not hasattr(effective, "leverage_source")


def test_run_strategy_portfolio_preflight_passes_persistence_session_id(monkeypatch):
    calls: dict = {}
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
        record_calls=calls,
        market_data_policy={"preflight_enabled": False},
    )

    request = SimpleNamespace(
        portfolio_id=501,
        user_id=17,
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(request, context)

    assert context.code is None
    assert resp.session_id != ""
    preflight = calls["portfolio_preflight"][0]
    assert preflight["session_id"]
    assert preflight["session_id"] == resp.session_id
    assert preflight["strategy_id"] == 42


def test_run_strategy_rejects_runtime_id_mismatch_before_internal_calls(monkeypatch):
    calls = {"portfolio_client": 0}

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            calls["portfolio_client"] += 1

    _install_portfolio_client(monkeypatch, FakePortfolioClient)
    servicer = StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "kafka:9092",
        runtime_id="rt-owned",
        restore_running_sessions=False,
    )
    request = SimpleNamespace(
        portfolio_id=201,
        user_id=17,
        runtime_id="rt-other",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.PERMISSION_DENIED
    assert "runtime_id mismatch" in context.details
    assert calls["portfolio_client"] == 0


def test_run_strategy_proxy_only_fails_closed_before_internal_calls(monkeypatch):
    calls = {"portfolio_client": 0}

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            calls["portfolio_client"] += 1

    monkeypatch.setattr(
        StrategyServiceServicer,
        "_portfolio_client",
        lambda self: _make_fake_client(FakePortfolioClient, self._portfolio_addr),
    )
    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        platform_access_mode=grpc_server.PLATFORM_ACCESS_PROXY_ONLY,
        restore_running_sessions=False,
    )
    request = SimpleNamespace(
        portfolio_id=201,
        user_id=17,
        runtime_id="rt-self",
        strategy_path="",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "platform proxy client is not configured" in context.details
    assert calls["portfolio_client"] == 0


def test_proxy_only_uses_platform_proxy_client_factories():
    class FakeProxy:
        def __init__(self) -> None:
            self.portfolio = object()
            self.order = object()
            self.marketdata = object()

        def portfolio_client(self):
            return self.portfolio

        def order_client(self):
            return self.order

        def marketdata_client(self):
            return self.marketdata

    proxy = FakeProxy()
    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        platform_access_mode=grpc_server.PLATFORM_ACCESS_PROXY_ONLY,
        platform_proxy=proxy,
        restore_running_sessions=False,
    )
    context = _FakeContext()

    assert servicer._require_platform_proxy(context, "RunStrategy") is True
    assert servicer._portfolio_client() is proxy.portfolio
    assert servicer._order_client() is proxy.order
    assert servicer._marketdata_client() is proxy.marketdata
    assert context.code is None


def test_proxy_only_with_proxy_fails_closed_before_market_data_source_ready():
    class FakePortfolioClient:
        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 201
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id)

    class FakeProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

        def order_client(self):
            raise AssertionError("order client should not be built")

        def marketdata_client(self):
            return object()

    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        platform_access_mode=grpc_server.PLATFORM_ACCESS_PROXY_ONLY,
        platform_proxy=FakeProxy(),
        restore_running_sessions=False,
    )
    request = SimpleNamespace(
        portfolio_id=201,
        user_id=17,
        runtime_id="rt-self",
        strategy_path="strategies.buy_once",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "paged backtest data proxy is not configured" in context.details
    assert "FetchKlines fallback is disabled" in context.details


def test_proxy_only_mode2_without_live_delivery_fails_closed_before_session():
    class FakePortfolioClient:
        def get_portfolio_snapshot(self, portfolio_id: int, user_id: int):
            assert portfolio_id == 201
            assert user_id == 17
            return make_portfolio_snapshot_with_binance_perp_and_spot(portfolio_id, user_id=user_id, environment=1)

    class FakeProxy:
        def portfolio_client(self):
            return FakePortfolioClient()

        def order_client(self):
            raise AssertionError("order client should not be built")

        def marketdata_client(self):
            raise AssertionError("marketdata client should not be built before live delivery guard")

    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        platform_access_mode=grpc_server.PLATFORM_ACCESS_PROXY_ONLY,
        platform_proxy=FakeProxy(),
        restore_running_sessions=False,
    )
    request = SimpleNamespace(
        portfolio_id=201,
        user_id=17,
        runtime_id="rt-self",
        strategy_path="strategies.buy_once",
        interval="1m",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "platform live delivery is not configured" in context.details
    assert "FetchKlines fallback is disabled" in context.details


def test_proxy_only_backtest_reads_paged_data_without_runtime_dataset_delivery():
    from market_data.models import MarketKline
    from strategy_service.inputs import StrategyInput

    class FakeMarketDataClient:
        def __init__(self) -> None:
            self.page_calls = []
            self.rows = [
                MarketKline(
                    symbol="ETHUSDT",
                    interval="1m",
                    open_time=60_000 * (idx + 1),
                    close_time=60_000 * (idx + 2) - 1,
                    open=1.0,
                    high=2.0,
                    low=0.5,
                    close=1.5,
                    volume=10.0,
                    timestamp=60_000 * (idx + 2) - 1,
                    market="futures",
                )
                for idx in range(2880)
            ]

        def fetch_klines(self, **kwargs):
            raise AssertionError("execution must not use legacy FetchKlines polling")

        def deliver_dataset_klines(self, **kwargs):
            raise AssertionError("formal backtest must not use RuntimeChannel dataset delivery")

        def fetch_backtest_page(self, **kwargs):
            self.page_calls.append(kwargs)
            start_after = int(kwargs["start_after_time_ms"])
            end = int(kwargs["end_time_ms"])
            rows = [row for row in self.rows if row.open_time > start_after and row.open_time < end]
            rows = rows[:8192]
            return SimpleNamespace(
                stream_key="binance/futures/kline/ETHUSDT/1m",
                klines=rows,
                next_cursor_time_ms=rows[-1].open_time if rows else start_after,
                has_more=False,
            )

    class FakeProxy:
        def __init__(self) -> None:
            self.marketdata = FakeMarketDataClient()

        def marketdata_client(self):
            return self.marketdata

    class FakeEngine:
        def __init__(self) -> None:
            self.rows = []

        def running_strategy(self, market_data):
            self.rows.append(market_data)
            return True

    proxy = FakeProxy()
    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        platform_access_mode=grpc_server.PLATFORM_ACCESS_PROXY_ONLY,
        platform_proxy=proxy,
        restore_running_sessions=False,
    )
    state = SessionState(environment=0)
    engine = FakeEngine()

    servicer._run_backtest(
        "sess-1",
        state,
        engine,
        SimpleNamespace(start_time_ms=60_000, end_time_ms=60_000 * 2881),
        [StrategyInput(exchange="binance", market="perpetual_futures", symbol="ETHUSDT", interval="1m")],
    )

    assert state.status == "finished"
    assert state.bars_processed == 2880
    assert len(engine.rows) == 2880
    assert engine.rows[0].symbol == "ETHUSDT"
    assert engine.rows[0].market == "perpetual_futures"
    assert proxy.marketdata.page_calls == [{
        "exchange": "binance",
        "market": "futures",
        "kind": "kline",
        "symbol": "ETHUSDT",
        "interval": "1m",
        "start_after_time_ms": 0,
        "end_time_ms": 60_000 * 2881,
    }]


def test_proxy_only_backtest_flushes_custom_indicator_chunks():
    from market_data.models import MarketKline
    from strategy_service.inputs import StrategyInput
    from strategy_service.service import StrategyEngine

    class FakeMarketDataClient:
        def fetch_backtest_page(self, **kwargs):
            start_after = int(kwargs["start_after_time_ms"])
            rows = [
                MarketKline(
                    symbol="ETHUSDT",
                    interval="1m",
                    open_time=60_000,
                    close_time=119_999,
                    open=1.0,
                    high=2.0,
                    low=0.5,
                    close=10.0,
                    volume=10.0,
                    timestamp=60_000,
                    market="futures",
                ),
                MarketKline(
                    symbol="ETHUSDT",
                    interval="1m",
                    open_time=120_000,
                    close_time=179_999,
                    open=10.0,
                    high=12.0,
                    low=9.5,
                    close=11.0,
                    volume=11.0,
                    timestamp=120_000,
                    market="futures",
                ),
            ]
            rows = [row for row in rows if row.open_time > start_after]
            return SimpleNamespace(
                stream_key="binance/futures/kline/ETHUSDT/1m",
                klines=rows,
                next_cursor_time_ms=rows[-1].open_time if rows else start_after,
                has_more=False,
            )

    class FakePortfolioClient:
        def __init__(self) -> None:
            self.indicator_saves = []

        def save_strategy_indicators(self, **kwargs):
            self.indicator_saves.append(kwargs)
            return (len(kwargs.get("definitions") or []), len(kwargs.get("chunks") or []))

    class FakeProxy:
        def __init__(self) -> None:
            self.marketdata = FakeMarketDataClient()
            self.portfolio = FakePortfolioClient()

        def marketdata_client(self):
            return self.marketdata

        def portfolio_client(self):
            return self.portfolio

    route_wallet = make_backtest_wallet()
    wallet = PortfolioWalletRuntime(
        portfolio_id=505,
        allowed_routes={("binance", "perpetual_futures")},
        wallets={("binance", "perpetual_futures", 11): route_wallet},
    )
    strategy_code = (
        "class MyStrategy:\n"
        "    INPUTS = [{\"exchange\": \"binance\", \"market\": \"perpetual_futures\", \"symbol\": \"ETHUSDT\", \"interval\": \"1m\"}]\n"
        "    ORDER_TARGETS = []\n"
        "    INDICATORS = {\"alpha_score\": {\"type\": \"line\", \"pane\": \"strategy\"}}\n"
        "    def on_market_data(self, data, wallet):\n"
        "        self.indicators.set(\"alpha_score\", data.price)\n"
        "        return None\n"
    )
    engine = StrategyEngine()
    engine.create_strategy(
        "u1",
        _prepare_strategy_code_for_test("<db:indicator_backtest>", strategy_code),
        wallet,
        session_id="sess-indicators",
    )

    proxy = FakeProxy()
    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        platform_access_mode=grpc_server.PLATFORM_ACCESS_PROXY_ONLY,
        platform_proxy=proxy,
        restore_running_sessions=False,
    )
    state = SessionState(environment=0)

    servicer._run_backtest(
        "sess-indicators",
        state,
        engine,
        SimpleNamespace(start_time_ms=60_000, end_time_ms=180_000, user_id=6),
        [StrategyInput(exchange="binance", market="perpetual_futures", symbol="ETHUSDT", interval="1m")],
    )

    chunk_saves = [call for call in proxy.portfolio.indicator_saves if call.get("chunks")]
    assert state.status == "finished"
    assert proxy.portfolio.indicator_saves[0]["definitions"][0].stream_key == "binance:perpetual_futures:ETHUSDT:1m"
    assert chunk_saves[-1]["chunks"][0].values_json["values"] == [10.0, 11.0]


def test_proxy_only_mode2_live_uses_runtime_delivery_not_fetch_klines():
    from market_data.models import MarketKline

    class FakeMarketDataClient:
        def __init__(self) -> None:
            self.lease_calls = []

        def create_or_renew_market_data_lease(self, **kwargs) -> bool:
            self.lease_calls.append(kwargs)
            return True

        def fetch_klines(self, **_kwargs):
            raise AssertionError("environment=1 proxy-only live path must not poll FetchKlines")

    class FakePortfolioClient:
        def __init__(self) -> None:
            self.session_updates = []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            self.session_updates.append((session_id, status, bars_processed, error, runtime_id))
            return True

    class FakeProxy:
        def __init__(self) -> None:
            self.marketdata = FakeMarketDataClient()
            self.portfolio = FakePortfolioClient()

        def marketdata_client(self):
            return self.marketdata

        def portfolio_client(self):
            return self.portfolio

    class FakeDelivery:
        def iter_live_klines(self, *, session_id, required_streams, stop_event):
            assert session_id == "sess-live"
            assert [(s.market, s.symbol, s.interval) for s in required_streams] == [
                ("futures", "BTCUSDT", "1m")
            ]
            yield MarketKline(
                symbol="BTCUSDT",
                interval="1m",
                open_time=1,
                close_time=2,
                open=1.0,
                high=2.0,
                low=0.5,
                close=1.5,
                volume=10.0,
                timestamp=2,
                market="futures",
            )
            stop_event.set()

    class FakeEngine:
        def __init__(self) -> None:
            self.rows = []

        def running_strategy(self, market_data):
            self.rows.append(market_data)
            return True

    proxy = FakeProxy()
    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        platform_access_mode=grpc_server.PLATFORM_ACCESS_PROXY_ONLY,
        platform_proxy=proxy,
        restore_running_sessions=False,
    )
    servicer.set_runtime_data_source(FakeDelivery())
    state = SessionState(environment=1)
    state.configure_live_runtime(
        portfolio_id=101,
        strategy_id=202,
        required_streams=[
            StreamBinding(
                11,
                "binance",
                "futures",
                "kline",
                "BTCUSDT",
                "1m",
                canonical_market="perpetual_futures",
            )
        ],
        consumer_group="strategy-session-202-sess-live",
    )
    engine = FakeEngine()

    servicer._run_live_via_platform_proxy("sess-live", state, engine)

    assert proxy.marketdata.lease_calls == [
        {
            "session_id": "sess-live",
            "strategy_id": 202,
            "portfolio_id": 101,
            "stream_id": 11,
            "ttl_seconds": servicer._lease_ttl_seconds,
        }
    ]
    assert [row.symbol for row in engine.rows] == ["BTCUSDT"]
    assert [row.market for row in engine.rows] == ["perpetual_futures"]
    assert state.bars_processed == 1
    assert proxy.portfolio.session_updates == [
        ("sess-live", "running", 1, "", "")
    ]


def test_proxy_only_mode2_live_delivers_order_updates_before_next_kline():
    from market_data.models import MarketKline

    class FakeMarketDataClient:
        def __init__(self) -> None:
            self.lease_calls = []

        def create_or_renew_market_data_lease(self, **kwargs) -> bool:
            self.lease_calls.append(kwargs)
            return True

    class FakePortfolioClient:
        def __init__(self) -> None:
            self.session_updates = []

        def update_session(self, session_id: str, status: str, bars_processed: int = 0, error: str = "", runtime_id: str = "") -> bool:
            self.session_updates.append((session_id, status, bars_processed, error, runtime_id))
            return True

    class FakeProxy:
        def __init__(self) -> None:
            self.marketdata = FakeMarketDataClient()
            self.portfolio = FakePortfolioClient()

        def marketdata_client(self):
            return self.marketdata

        def portfolio_client(self):
            return self.portfolio

    class FakeDelivery:
        def iter_session_events(self, *, session_id, required_streams, stop_event):
            assert session_id == "sess-live"
            assert [(s.market, s.symbol, s.interval) for s in required_streams] == [
                ("futures", "BTCUSDT", "1m")
            ]
            yield SimpleNamespace(
                kind="order_update",
                payload=OrderUpdateEvent(
                    event_id=44,
                    session_id="sess-live",
                    portfolio_id=101,
                    venue_id=11,
                    exchange="binance",
                    market="perpetual_futures",
                    side="BUY",
                    position_side="both",
                    event_type="fill",
                    order_status="FILLED",
                    order_id="order-44",
                    fill=OrderUpdateFill(symbol="BTCUSDT", qty=0.01, fill_price=50_000.0),
                ),
            )
            yield SimpleNamespace(
                kind="kline",
                payload=MarketKline(
                    symbol="BTCUSDT",
                    interval="1m",
                    open_time=1,
                    close_time=2,
                    open=1.0,
                    high=2.0,
                    low=0.5,
                    close=1.5,
                    volume=10.0,
                    timestamp=2,
                    market="futures",
                ),
            )
            stop_event.set()

        def iter_live_klines(self, **_kwargs):
            raise AssertionError("iter_session_events should be preferred when available")

    class FakeEngine:
        def __init__(self) -> None:
            self.rows = []
            self.order_updates = []

        def running_strategy(self, market_data):
            self.rows.append(market_data)
            return True

        def handle_order_update(self, event):
            self.order_updates.append(event)
            return True

    proxy = FakeProxy()
    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        platform_access_mode=grpc_server.PLATFORM_ACCESS_PROXY_ONLY,
        platform_proxy=proxy,
        restore_running_sessions=False,
    )
    servicer.set_runtime_data_source(FakeDelivery())
    state = SessionState(environment=1)
    state.configure_live_runtime(
        portfolio_id=101,
        strategy_id=202,
        required_streams=[
            StreamBinding(
                11,
                "binance",
                "futures",
                "kline",
                "BTCUSDT",
                "1m",
                canonical_market="perpetual_futures",
            )
        ],
        consumer_group="strategy-session-202-sess-live",
    )
    engine = FakeEngine()

    servicer._run_live_via_platform_proxy("sess-live", state, engine)

    assert [event.event_id for event in engine.order_updates] == [44]
    assert [row.symbol for row in engine.rows] == ["BTCUSDT"]
    assert state.bars_processed == 1
    assert proxy.portfolio.session_updates == [
        ("sess-live", "running", 1, "", "")
    ]


def test_run_strategy_backtest_allows_empty_wallet_when_data_available(monkeypatch):
    """Scenario: Empty wallet can start when the profile is ready.

    A backtest portfolio with zero holdings must still start when every declared
    input has historical data in the requested range.
    """
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,  # backtest
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    # Fake availability: always has data.
    def fake_preflight(**kwargs):
        from strategy_service.preflight import PreflightResult, RuntimeSourceProfile
        return PreflightResult(profile=RuntimeSourceProfile.BACKTEST)

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=201, user_id=17,
        strategy_path="", interval="1m",
        start_time_ms=1_700_000_000_000,
        end_time_ms=1_700_000_060_000,
    )
    context = _FakeContext()
    resp = servicer.RunStrategy(request, context)

    assert resp.session_id != ""
    assert context.code is None
    assert calls["save_session"] == 1


def test_run_strategy_backtest_rejects_when_historical_data_missing(monkeypatch):
    """Scenario: Missing historical data blocks startup.

    Backtest preflight reports a HISTORICAL_DATA failure identifying the
    declared input that has no usable data; no session is created.
    """
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "NOPEUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    def fake_preflight(**kwargs):
        from strategy_service.preflight import (
            PreflightFailure,
            PreflightFailureKind,
            PreflightResult,
            RuntimeSourceProfile,
        )
        return PreflightResult(
            profile=RuntimeSourceProfile.BACKTEST,
            failures=[
                PreflightFailure(
                    kind=PreflightFailureKind.HISTORICAL_DATA,
                    reason="no historical kline data in requested range",
                    input_key=("futures", "NOPEUSDT", "1m"),
                )
            ],
        )

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=202, user_id=17,
        strategy_path="", interval="1m",
        start_time_ms=1, end_time_ms=2,
    )
    context = _FakeContext()
    resp = servicer.RunStrategy(request, context)

    assert resp.session_id == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "backtest profile preflight failed" in context.details
    assert "NOPEUSDT" in context.details
    assert "[historical_data]" in context.details
    assert calls["save_session"] == 0
    assert servicer._sessions._sessions == {}


def test_run_strategy_live_preflight_ignores_undeclared_wallet_holdings(monkeypatch):
    """Scenario: Unrelated holdings do not trigger extra stream checks.

    A environment=1 portfolio with spot assets (USDC) + declared futures ETHUSDT must
    only trigger readiness lookups for ETHUSDT futures — the wallet USDC must
    NOT expand the preflight universe.
    """
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=1,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    captured: dict = {}

    def fake_preflight(**kwargs):
        from strategy_service.preflight import PreflightResult, RuntimeSourceProfile
        captured["declared"] = [
            (inp.market, inp.symbol, inp.interval) for inp in kwargs["declared_inputs"]
        ]
        return PreflightResult(
            profile=RuntimeSourceProfile.DEMO,
            required_streams=[
                StreamBinding(1003, "binance", "futures", "kline", "ETHUSDT", "1m"),
            ],
        )

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=203, user_id=17,
        strategy_path="", interval="1m",
        start_time_ms=0, end_time_ms=0,
    )
    context = _FakeContext()
    resp = servicer.RunStrategy(request, context)

    assert resp.session_id != ""
    assert context.code is None
    # Only the declared input fed the preflight — USDC spot holding ignored.
    assert captured["declared"] == [("perpetual_futures", "ETHUSDT", "1m")]


def test_run_strategy_backtest_distinct_intervals_are_preserved(monkeypatch):
    """Scenario: Distinct declared intervals are evaluated distinctly.

    Strategy declares BTCUSDT futures 1m AND BTCUSDT futures 5m. Preflight
    must receive both entries as separate (market, symbol, interval) tuples,
    not be collapsed to a single (market, symbol) pair.
    """
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            "    INPUTS = [\n"
            '        {"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"},\n'
            '        {"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "5m"},\n'
            "    ]\n"
            "    ORDER_TARGETS = []\n"
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    captured: dict = {}

    def fake_preflight(**kwargs):
        from strategy_service.preflight import PreflightResult, RuntimeSourceProfile
        captured["declared"] = [
            (inp.market, inp.symbol, inp.interval) for inp in kwargs["declared_inputs"]
        ]
        return PreflightResult(profile=RuntimeSourceProfile.BACKTEST)

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=204, user_id=17,
        strategy_path="", interval="1m",
        start_time_ms=1, end_time_ms=2,
    )
    servicer.RunStrategy(request, _FakeContext())

    assert captured["declared"] == [
        ("perpetual_futures", "BTCUSDT", "1m"),
        ("perpetual_futures", "BTCUSDT", "5m"),
    ]


def test_preview_run_strategy_reports_backtest_availability(monkeypatch):
    """Scenario: Backtest readiness reports historical-data availability.

    PreviewRunStrategy uses the same evaluator as RunStrategy — UI must see
    historical-data failures for backtest (not stream-readiness).
    """
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    def fake_preflight(**kwargs):
        from strategy_service.preflight import (
            PreflightFailure,
            PreflightFailureKind,
            PreflightResult,
            RuntimeSourceProfile,
        )
        return PreflightResult(
            profile=RuntimeSourceProfile.BACKTEST,
            failures=[
                PreflightFailure(
                    kind=PreflightFailureKind.HISTORICAL_DATA,
                    reason="no historical kline data in requested range",
                    input_key=("futures", "BTCUSDT", "1m"),
                )
            ],
        )

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=301, user_id=17,
        strategy_path="",
        start_time_ms=1, end_time_ms=2,
    )
    context = _FakeContext()
    resp = servicer.PreviewRunStrategy(request, context)

    assert context.code is None
    assert resp.profile == "backtest"
    assert resp.supported is True
    assert resp.ok is False
    assert len(resp.failures) == 1
    assert resp.failures[0].kind == "historical_data"
    assert resp.failures[0].input_key.symbol == "BTCUSDT"
    assert resp.failures[0].input_key.interval == "1m"
    # required_streams is empty — backtest preflight never reports streams.
    assert list(resp.required_streams) == []


def test_preview_run_strategy_portfolio_preflight_does_not_persist_session(monkeypatch):
    calls: dict = {}
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "leverage": 5}]\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
        record_calls=calls,
    )

    def fake_preflight(**kwargs):
        from strategy_service.preflight import PreflightResult, RuntimeSourceProfile
        return PreflightResult(profile=RuntimeSourceProfile.BACKTEST)

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=501,
        user_id=17,
        strategy_path="",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    resp = servicer.PreviewRunStrategy(request, context)

    assert context.code is None
    assert resp.ok is True
    preflight = calls["portfolio_preflight"][0]
    assert preflight.get("session_id", "") == ""
    assert preflight.get("strategy_id", 0) == 42
    assert preflight["leverage"] == 0
    assert len(preflight["order_targets"]) == 1
    target = preflight["order_targets"][0]
    assert (target.exchange, target.market, target.symbol) == (
        "binance",
        "perpetual_futures",
        "BTCUSDT",
    )
    assert target.effective_leverage == 5
    assert target.leverage_source == "order_target"


def test_preview_run_strategy_preserves_structured_portfolio_preflight_failure(monkeypatch):
    issue = SimpleNamespace(
        code="SPOT_MIN_NOTIONAL",
        message="notional below minimum",
        exchange=1,
        market=1,
        symbol="BTCUSDT",
        venue_id=77,
        filter_type="MIN_NOTIONAL",
        environment=1,
        retryable=False,
        source="preflight",
    )
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=1,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "spot", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = [{"exchange": "binance", "market": "spot", "symbol": "BTCUSDT"}]\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
        portfolio_preflight_response=SimpleNamespace(ok=False, issues=[issue]),
    )

    response = servicer.PreviewRunStrategy(SimpleNamespace(
        portfolio_id=502,
        user_id=17,
        strategy_path="",
        start_time_ms=1,
        end_time_ms=2,
    ), _FakeContext())

    assert response.ok is False
    assert len(response.failures) == 1
    failure = response.failures[0]
    assert failure.kind == "portfolio"
    assert failure.reason == "notional below minimum"
    assert failure.code == "SPOT_MIN_NOTIONAL"
    assert (failure.exchange, failure.market, failure.symbol, failure.venue_id) == (1, 1, "BTCUSDT", 77)
    assert failure.filter_type == "MIN_NOTIONAL"
    assert failure.environment == 1
    assert failure.retryable is False
    assert failure.source == "preflight"


def test_preview_run_strategy_returns_declared_inputs_for_backtest(monkeypatch):
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT"}]\n'
            "    LEVERAGE = 3\n"
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    def fake_preflight(**kwargs):
        from strategy_service.preflight import PreflightResult, RuntimeSourceProfile
        return PreflightResult(profile=RuntimeSourceProfile.BACKTEST)

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=301, user_id=17,
        strategy_path="",
        start_time_ms=1_779_033_600_000,
        end_time_ms=1_779_037_200_000,
        leverage=9,
    )
    context = _FakeContext()
    resp = servicer.PreviewRunStrategy(request, context)

    assert context.code is None
    assert len(resp.declared_inputs) == 1
    assert resp.declared_inputs[0].exchange == "binance"
    assert resp.declared_inputs[0].kind == "kline"
    assert resp.declared_inputs[0].symbol == "ETHUSDT"
    assert resp.declared_inputs[0].market == "perpetual_futures"
    assert resp.declared_inputs[0].interval == "1m"
    assert len(resp.declared_order_targets) == 1
    assert resp.declared_order_targets[0].exchange == "binance"
    assert resp.declared_order_targets[0].market == "perpetual_futures"
    assert resp.declared_order_targets[0].symbol == "ETHUSDT"
    assert resp.declared_order_targets[0].effective_leverage == 3
    assert resp.declared_order_targets[0].leverage_source == "strategy_default"
    assert {(r.exchange, r.market) for r in resp.required_routes} == {
        ("binance", "perpetual_futures")
    }


def test_preview_run_strategy_returns_effective_risk_controls(monkeypatch):
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT"}]\n'
            "    LEVERAGE = 6\n"
            '    RISK_CONTROLS = {"max_loss_close_pct": 0.2}\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    def fake_preflight(**kwargs):
        from strategy_service.preflight import PreflightResult, RuntimeSourceProfile
        return PreflightResult(profile=RuntimeSourceProfile.BACKTEST)

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=301,
        user_id=17,
        strategy_path="",
        start_time_ms=1,
        end_time_ms=2,
        max_loss_close_pct=0.25,
        leverage=4,
    )
    context = _FakeContext()
    resp = servicer.PreviewRunStrategy(request, context)

    assert context.code is None
    assert resp.risk_controls.max_loss_close_pct == 0.2
    assert resp.risk_controls.max_loss_close_source == "strategy"
    assert resp.risk_controls.leverage == 0
    assert resp.risk_controls.leverage_source == ""
    assert resp.declared_order_targets[0].effective_leverage == 6
    assert resp.declared_order_targets[0].leverage_source == "strategy_default"
    assert calls["portfolio_preflight"][-1]["leverage"] == 0


def test_preview_run_strategy_preserves_mixed_target_leverage_without_scalar_collapse(
    monkeypatch,
):
    servicer, calls = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            "    ORDER_TARGETS = [\n"
            '        {"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "leverage": 2},\n'
            '        {"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "leverage": 3},\n'
            "    ]\n"
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    monkeypatch.setattr(
        servicer,
        "_run_profile_preflight",
        lambda **_kwargs: grpc_server.PreflightResult(
            profile=grpc_server.RuntimeSourceProfile.BACKTEST
        ),
    )

    context = _FakeContext()
    response = servicer.PreviewRunStrategy(
        SimpleNamespace(
            portfolio_id=301,
            user_id=17,
            strategy_path="",
            start_time_ms=1,
            end_time_ms=2,
            leverage=20,
        ),
        context,
    )

    assert response.ok is True
    assert context.code is None
    assert calls["portfolio_preflight"][-1]["leverage"] == 0
    assert [
        (target.symbol, target.effective_leverage, target.leverage_source)
        for target in calls["portfolio_preflight"][-1]["order_targets"]
    ] == [
        ("BTCUSDT", 2, "order_target"),
        ("ETHUSDT", 3, "order_target"),
    ]


def test_preview_run_strategy_uses_request_risk_default_when_strategy_omits(monkeypatch):
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    def fake_preflight(**kwargs):
        from strategy_service.preflight import PreflightResult, RuntimeSourceProfile
        return PreflightResult(profile=RuntimeSourceProfile.BACKTEST)

    monkeypatch.setattr(servicer, "_run_profile_preflight", fake_preflight)

    request = SimpleNamespace(
        portfolio_id=301,
        user_id=17,
        strategy_path="",
        start_time_ms=1,
        end_time_ms=2,
        max_loss_close_pct=0.25,
        leverage=8,
    )
    context = _FakeContext()
    resp = servicer.PreviewRunStrategy(request, context)

    assert context.code is None
    assert resp.risk_controls.max_loss_close_pct == 0.25
    assert resp.risk_controls.max_loss_close_source == "request_default"
    assert resp.risk_controls.leverage == 0
    assert resp.risk_controls.leverage_source == ""


def test_preview_run_strategy_reports_unsupported_live_profile(monkeypatch):
    """Scenario: Unsupported profile fails as a profile error (preview path).

    PreviewRunStrategy must classify environment=2 as unsupported LIVE — not leak
    the wallet-registry miss or misclassify as a strategy mismatch.
    """
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=2,  # live → unsupported
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    request = SimpleNamespace(
        portfolio_id=302, user_id=17,
        strategy_path="",
        start_time_ms=0, end_time_ms=0,
    )
    context = _FakeContext()
    resp = servicer.PreviewRunStrategy(request, context)

    assert context.code is None  # preview returns structured, not RPC error
    assert resp.profile == "live"
    assert resp.supported is False
    assert resp.ok is False
    assert len(resp.failures) == 1
    assert resp.failures[0].kind == "profile"


def test_preview_run_strategy_mirrors_wallet_build_failure(monkeypatch):
    """Review finding #3: Preview must also build the same portfolio wallet.

    Before this fix, a environment=1 portfolio stored with ``multi_assets_mode=True``
    would make ``RunStrategy`` fail (``INVALID_ARGUMENT: failed to build wallet``)
    while Preview reported ``ok=true, profile=testnet`` — classic drift.
    Preview now runs the exact same wallet build so this surface stays
    consistent."""
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=1,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    # Wallet builder raises the same way it would for an unsupported portfolio snapshot.
    def fail_wallet(*_args, **_kwargs):
        raise ValueError("unsupported wallet state: multi-assets mode is disabled in Phase B3")

    monkeypatch.setattr(grpc_server, "build_portfolio_wallet_from_snapshot", fail_wallet)

    request = SimpleNamespace(
        portfolio_id=401, user_id=17,
        strategy_path="",
        start_time_ms=0, end_time_ms=0,
    )
    context = _FakeContext()
    resp = servicer.PreviewRunStrategy(request, context)

    # Same RPC contract as RunStrategy: INVALID_ARGUMENT with wallet error.
    assert resp.profile == ""
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT
    assert "failed to build wallet" in context.details
    assert "multi-assets" in context.details


def test_preview_run_strategy_honours_preflight_enabled_bypass(monkeypatch):
    """When ``preflight_enabled=False``, RunStrategy and PreviewRunStrategy
    both call ``_run_profile_preflight`` with ``require_readiness=False`` —
    readiness gating is skipped but structural binding resolution still runs.
    This keeps Preview and Run aligned: Preview says 'ok' exactly when Run
    would succeed, including the operator bypass case."""
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = []\n'
            "    def on_market_data(self, data, wallet): return None\n"
        ),
        market_data_policy={"preflight_enabled": False},
    )

    from strategy_service.preflight import PreflightResult

    captured: dict = {}

    def capturing_preflight(**kwargs):
        captured.update(kwargs)
        # Bypass path: binding resolution is a no-op for backtest, so an
        # ok result is what the real evaluator returns here too.
        return PreflightResult(profile=kwargs["profile"])

    monkeypatch.setattr(servicer, "_run_profile_preflight", capturing_preflight)

    request = SimpleNamespace(
        portfolio_id=402, user_id=17,
        strategy_path="",
        start_time_ms=1, end_time_ms=2,
    )
    context = _FakeContext()
    resp = servicer.PreviewRunStrategy(request, context)

    assert context.code is None
    assert resp.profile == "backtest"
    assert resp.supported is True
    assert resp.ok is True
    assert list(resp.failures) == []
    # Critical: preflight still ran, with readiness gating OFF.
    assert captured["require_readiness"] is False


def test_preview_run_strategy_rejects_invalid_declaration(monkeypatch):
    """Declaration errors still surface as FAILED_PRECONDITION in preview path."""
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            "    # No INPUTS declared.\n"
            "    def on_market_data(self, data, wallet): return None\n"
        ),
    )

    request = SimpleNamespace(
        portfolio_id=303, user_id=17,
        strategy_path="",
        start_time_ms=1, end_time_ms=2,
    )
    context = _FakeContext()
    resp = servicer.PreviewRunStrategy(request, context)

    assert resp.profile == ""
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details.startswith("strategy code validation failed: ")
    assert '"code":"missing_inputs"' in context.details


@pytest.mark.parametrize("method_name", ["RunStrategy", "PreviewRunStrategy"])
def test_run_and_preview_reject_saved_code_with_same_validation_error(monkeypatch, method_name):
    strategy_code = (
        "import talib\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]\n'
        "    ORDER_TARGETS = []\n"
        "    def on_market_data(self, data, wallet): return None\n"
    )
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=strategy_code,
    )
    request = SimpleNamespace(
        portfolio_id=303,
        user_id=17,
        runtime_id="rt-test",
        strategy_path="",
        start_time_ms=1,
        end_time_ms=2,
    )
    context = _FakeContext()

    getattr(servicer, method_name)(request, context)

    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert context.details.startswith("STRATEGY_DEPENDENCY_ERROR:")
    assert json.loads(context.details.removeprefix("STRATEGY_DEPENDENCY_ERROR:"))["code"] == (
        "UNSUPPORTED_STRATEGY_DEPENDENCY"
    )
