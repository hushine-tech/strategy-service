from __future__ import annotations

import threading
import types
from datetime import datetime, timezone
from types import SimpleNamespace
import sys

import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from strategy_service import grpc_server
from strategy_service.gen import portfolio_service_pb2
from strategy_service.gen import strategy_service_pb2 as pb2
from strategy_service.grpc_server import StrategyServiceServicer
from strategy_service.session import SessionState, StreamBinding
from strategy_service.types import OrderUpdateEvent, OrderUpdateFill
from strategy_service.wallet.portfolio import PortfolioWalletRuntime
from strategy_service.wallet.order_types import OrderResponse
from strategy_service.wallet.canonical import CanonicalFuturesRiskMetadata
from tests.helpers.wallet_fixtures import make_testnet_wallet
from tests.helpers.wallet_fixtures import make_backtest_wallet


def _make_fake_client(cls, addr: str):
    try:
        return cls(addr)
    except TypeError:
        return cls()


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

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


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
    monkeypatch.setattr(threading, "Thread", FakeThread)
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
    assert calls["portfolio"] == 2
    assert calls["wallet_update"] == 1


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
    monkeypatch.setattr(threading, "Thread", FakeThread)
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
    assert "UpdatePortfolioWalletState returned no response" in calls["update_session"][0]["error"]


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
                code=_phase3_strategy_code(),
                name="phase3",
                version="v1",
            )

        def save_session(self, **_kwargs) -> bool:
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
    monkeypatch.setattr(threading, "Thread", FakeThread)
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
    assert len(wallet_calls) == 1
    req = captured["preflight"]
    assert set(req["required_routes"]) == {
        ("binance", "perpetual_futures"),
        ("binance", "spot"),
    }
    assert set(req["required_symbols"]) == {
        ("binance", "perpetual_futures", "BTCUSDT"),
        ("binance", "spot", "ETH"),
    }
    assert req["leverage"] == 1
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
    servicer._run_session(
        "sess-portfolio",
        state,
        SimpleNamespace(),
        portfolio_wallet,
        0,
        406,
        17,
        [],
        "<db:phase3@v1>",
        42,
        _phase3_strategy_code(),
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
    servicer._run_session(
        "sess-portfolio-sync",
        state,
        SimpleNamespace(),
        wallet,
        0,
        407,
        17,
        [],
        "<db:phase3@v1>",
        43,
        _phase3_strategy_code(),
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
    monkeypatch.setattr(threading, "Thread", InlineThread)

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
        "<db:phase3@v1>",
        43,
        _phase3_strategy_code(),
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
    assert "input declaration invalid" in context.details
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
    monkeypatch.setattr(threading, "Thread", FakeThread)
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
    monkeypatch.setattr(threading, "Thread", FakeThread)

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
    servicer._run_session(
        session_id="sess-backtest",
        state=state,
        request=request,
        wallet=wallet,
        environment=0,
        portfolio_id=101,
        user_id=17,
        declared_inputs=[StrategyInput("binance", "perpetual_futures", "BTCUSDT", "1m")],
        strategy_path="strategies.buy_once",
        strategy_id=202,
        strategy_code=None,
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
        strategy_path="strategies.buy_once",
        strategy_id=202,
        strategy_code=None,
    )

    assert state.status == "recoverable"
    assert "failed to persist strategy_end snapshot" in state.error
    assert "runtime platform request timed out" in state.error
    assert events == [
        ("session_update", "recoverable", 9, state.error, "sess-finished-with-snapshot-timeout"),
    ]


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
        strategy_path="strategies.buy_once",
        strategy_id=404,
        strategy_code=None,
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
        strategy_path="strategies.buy_once",
        strategy_id=606,
        strategy_code=None,
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


def test_stop_strategy_stop_only_persists_state_and_halts_runtime():
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
    assert updates == [(session_id, "rt-owned")]


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


def test_stop_strategy_stop_and_close_backtest_futures_flattens_wallet(monkeypatch):
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
    assert "max_loss_close_triggered" in state.error
    assert placed == [("ETHUSDT", True)]
    assert updates[0][1] == "stopping"
    assert updates[-1][1] == "stopped"
    assert abs(route_wallet.futures.positions[("ETHUSDT", 0)].position_qty) <= 1e-12


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


def test_stop_strategy_stop_and_close_mode2_fails_closed_for_spot_exit(monkeypatch):
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
    assert context.code == grpc.StatusCode.FAILED_PRECONDITION
    assert "spot_liquidation_not_supported" in context.details
    assert state.status == "stop_failed"
    assert updates[0][1] == "stopping"
    assert updates[-1][1] == "stop_failed"
    assert stop_event.is_set() is True


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


def test_get_live_consumption_diagnostics_reports_active_mode2_sessions():
    servicer = StrategyServiceServicer("", "", {}, "127.0.0.1:9092")
    session_id, state = servicer._sessions.create(environment=1, user_id=17)
    state.bars_processed = 12
    state.configure_live_runtime(
        portfolio_id=303,
        strategy_id=404,
        required_streams=[
            StreamBinding(21, "binance", "futures", "kline", "BTCUSDT", "1m")
        ],
        consumer_group="strategy-session-404-session-live",
    )
    state.note_lease_heartbeat(now_ms=1_700_000_000_000)
    state.record_unroutable("unroutable live kline: BTCUSDT futures 1m", now_ms=1_700_000_000_100)
    servicer._sessions.create(environment=0, user_id=18)

    resp = servicer.GetLiveConsumptionDiagnostics(pb2.GetLiveConsumptionDiagnosticsRequest(), _FakeContext())

    assert len(resp.sessions) == 1
    session = resp.sessions[0]
    assert session.session_id == session_id
    assert session.user_id == 17
    assert session.portfolio_id == 303
    assert session.strategy_id == 404
    assert session.consumer_group == "strategy-session-404-session-live"
    assert session.unroutable_events == 1
    assert session.last_unroutable_reason == "unroutable live kline: BTCUSDT futures 1m"
    assert session.last_lease_heartbeat_at_ms == 1_700_000_000_000
    assert [(stream.stream_id, stream.symbol, stream.market) for stream in session.streams] == [
        (21, "BTCUSDT", "futures")
    ]


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


def _build_servicer_with_faked_preflight_deps(
    *,
    monkeypatch,
    environment: int,
    strategy_code: str | None,
    save_session_ok: bool = True,
    record_calls: dict | None = None,
    market_data_policy: dict | None = None,
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

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def list_running_sessions(self, runtime_id: str = ""):
            return []

        def get_portfolio_snapshot(self, _portfolio_id: int, _user_id: int):
            return make_portfolio_snapshot_with_binance_perp_and_spot(
                _portfolio_id,
                user_id=_user_id,
                environment=environment,
            )

        def preflight_strategy_session(self, **kwargs):
            calls.setdefault("portfolio_preflight", []).append(dict(kwargs))
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

    class FakeThread:
        def __init__(self, target=None, args=(), daemon=None) -> None:
            self.target = target
            self.args = args
            self.daemon = daemon

        def start(self) -> None:
            return None

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

    monkeypatch.setattr(threading, "Thread", FakeThread)

    servicer = StrategyServiceServicer(
        "acct:1", "order:1", {}, "kafka:9092",
        market_data_policy=market_data_policy,
        runtime_id="rt-test",
        platform_proxy=FakePlatformProxy(),
    )
    servicer.set_runtime_data_source(FakeRuntimeDataSource())
    return servicer, calls


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
    assert state.leverage == 3
    assert state.leverage_source == "request_default"
    assert state.initial_margin_balance == 1000.0
    assert state.order_target_keys == {("binance", "perpetual_futures", "BTCUSDT")}
    assert calls["save_kwargs"][-1]["leverage"] == 3


def test_effective_risk_controls_rejects_fractional_leverage():
    declarations = SimpleNamespace(risk_controls=SimpleNamespace(max_loss_close_pct=None))

    with pytest.raises(grpc_server.StrategyDeclarationError, match="leverage must be a positive whole number"):
        grpc_server._effective_risk_controls_from_request(declarations, 0.25, 1.5)


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
    assert preflight["session_id"] != resp.session_id
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
        "<db:indicator_backtest>",
        wallet,
        session_id="sess-indicators",
        strategy_code=strategy_code,
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
            '    ORDER_TARGETS = []\n'
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


def test_preview_run_strategy_returns_declared_inputs_for_backtest(monkeypatch):
    servicer, _ = _build_servicer_with_faked_preflight_deps(
        monkeypatch=monkeypatch,
        environment=0,
        strategy_code=(
            "class MyStrategy:\n"
            '    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]\n'
            '    ORDER_TARGETS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT"}]\n'
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
    assert resp.risk_controls.leverage == 4
    assert resp.risk_controls.leverage_source == "request_default"
    assert calls["portfolio_preflight"][-1]["leverage"] == 4


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
    )
    context = _FakeContext()
    resp = servicer.PreviewRunStrategy(request, context)

    assert context.code is None
    assert resp.risk_controls.max_loss_close_pct == 0.25
    assert resp.risk_controls.max_loss_close_source == "request_default"
    assert resp.risk_controls.leverage == 1
    assert resp.risk_controls.leverage_source == "platform_default"


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
    assert "input declaration invalid" in context.details
