"""Tests for Phase C hybrid PeriodicSample trigger in strategy-service.

Covers:
- environment=1 fires after N bars (bar-count threshold)
- environment=1 fires after wall-clock idle (time threshold) even with few bars
- environment=0 NEVER fires regardless of bar count or elapsed time
- counter reset semantics after fire
- failures in the wallet push MUST NOT interrupt strategy execution
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from strategy_service import grpc_server
from strategy_service.grpc_server import (
    DEFAULT_PERIODIC_SAMPLE_EVERY_BARS,
    DEFAULT_PERIODIC_SAMPLE_MAX_IDLE_SECONDS,
    SNAPSHOT_REASON_PERIODIC_SAMPLE,
    StrategyServiceServicer,
)
from strategy_service.service import StrategyEngine
from tests.helpers.wallet_fixtures import make_testnet_wallet


def _wallet():
    """Build a environment=1 testnet wallet with one isolated BTCUSDT position slot.

    The periodic-sample trigger is a Phase C `environment=1` feature; returning a
    environment=1-tagged runtime keeps ``wallet.environment_code`` aligned with the scenario
    being tested. For tests that simulate environment=0 / environment=2 paths the trigger
    installation is gated on ``portfolio_mode`` (a separate test parameter),
    not on ``wallet.environment_code``, so this wallet instance is fine across all cases.
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


class _RecordingPortfolioClient:
    """Captures PeriodicSample wallet sync calls."""

    def __init__(self, fail: bool = False) -> None:
        self.calls: list[dict[str, Any]] = []
        self._fail = fail

    def update_portfolio_wallet_state(
        self,
        portfolio_id: int,
        user_id: int = 0,
        future_wallet: Any | None = None,
        spot_wallet: Any | None = None,
        snapshot_reason: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        snapshot_time: object | None = None,
    ) -> None:
        self.calls.append({
            "kind": "wallet",
            "portfolio_id": portfolio_id,
            "user_id": user_id,
            "future_wallet": future_wallet,
            "spot_wallet": spot_wallet,
            "snapshot_reason": snapshot_reason,
            "strategy_id": strategy_id,
            "session_id": session_id,
            "snapshot_time": snapshot_time,
        })
        if self._fail:
            raise RuntimeError("simulated transport failure")

class _FakeClock:
    """Injectable monotonic clock for deterministic idle-threshold testing."""

    def __init__(self, start: float = 0.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


# ── helpers ──────────────────────────────────────────────────────────────────


def _make_servicer() -> StrategyServiceServicer:
    return StrategyServiceServicer("acct:1", "order:1", {}, "127.0.0.1:9092", restore_running_sessions=False)


def _install_and_get_engine(
    *,
    servicer: StrategyServiceServicer,
    portfolio_client: _RecordingPortfolioClient,
    clock: _FakeClock,
    every_n_bars: int = DEFAULT_PERIODIC_SAMPLE_EVERY_BARS,
    max_idle_seconds: float = DEFAULT_PERIODIC_SAMPLE_MAX_IDLE_SECONDS,
    original_calls: list[Any] | None = None,
) -> StrategyEngine:
    """Build an engine whose running_strategy is a no-op (records calls),
    then install the PeriodicSample trigger wrapping it."""
    engine = StrategyEngine()

    def _original(md: Any) -> None:
        if original_calls is not None:
            original_calls.append(md)

    engine.running_strategy = _original  # type: ignore[assignment]

    servicer._install_periodic_sample_trigger(
        engine=engine,
        portfolio_id=101,
        user_id=17,
        strategy_id=202,
        session_id="sess-test",
        wallet=_wallet(),
        portfolio_client=portfolio_client,
        every_n_bars=every_n_bars,
        max_idle_seconds=max_idle_seconds,
        now_fn=clock,
    )
    return engine


def _fake_md(i: int = 0) -> Any:
    """Minimal MarketData stand-in — the trigger doesn't inspect payload."""
    return SimpleNamespace(symbol="BTCUSDT", market="futures", price=50_000.0 + i, timestamp=i)


# ── tests ────────────────────────────────────────────────────────────────────


def test_periodic_sample_fires_after_n_bars_under_time_limit():
    """environment=1 path: 20 bars processed in <5min → trigger fires exactly once, counters reset."""
    servicer = _make_servicer()
    portfolio_client = _RecordingPortfolioClient()
    clock = _FakeClock()
    original_calls: list[Any] = []

    engine = _install_and_get_engine(
        servicer=servicer,
        portfolio_client=portfolio_client,
        clock=clock,
        original_calls=original_calls,
    )

    # 20 bars in 20 seconds — under the 300s idle threshold.
    for i in range(20):
        engine.running_strategy(_fake_md(i))
        clock.advance(1.0)

    # Original called every bar.
    assert len(original_calls) == 20
    # Exactly one PeriodicSample push.
    assert len(portfolio_client.calls) == 1
    call = portfolio_client.calls[0]
    assert call["kind"] == "wallet"
    assert call["snapshot_reason"] == SNAPSHOT_REASON_PERIODIC_SAMPLE
    assert call["portfolio_id"] == 101
    assert call["user_id"] == 17
    assert call["strategy_id"] == 202
    assert call["session_id"] == "sess-test"

    # Additional 19 bars: should NOT fire again (counter reset).
    for i in range(19):
        engine.running_strategy(_fake_md(i))
        clock.advance(1.0)
    assert len(portfolio_client.calls) == 1

    # 20th bar after reset → fires again.
    engine.running_strategy(_fake_md(100))
    assert len(portfolio_client.calls) == 2

def test_periodic_sample_fires_after_idle_threshold_with_few_bars():
    """environment=1 path: 3 bars but 6 minutes elapse → trigger fires on the bar after the idle trip."""
    servicer = _make_servicer()
    portfolio_client = _RecordingPortfolioClient()
    clock = _FakeClock()

    engine = _install_and_get_engine(
        servicer=servicer,
        portfolio_client=portfolio_client,
        clock=clock,
    )

    # Bar 1 immediately — no idle elapsed yet, no fire.
    engine.running_strategy(_fake_md(0))
    assert len(portfolio_client.calls) == 0

    # Advance 6 minutes (> 300s idle threshold).
    clock.advance(360.0)

    # Bar 2 after long idle → fires on this bar.
    engine.running_strategy(_fake_md(1))
    assert len(portfolio_client.calls) == 1
    assert portfolio_client.calls[0]["snapshot_reason"] == SNAPSHOT_REASON_PERIODIC_SAMPLE

    # Bar 3 right after: counter was reset, no immediate re-fire.
    engine.running_strategy(_fake_md(2))
    assert len(portfolio_client.calls) == 1

    # Advance another 6 minutes → next bar fires again.
    clock.advance(360.0)
    engine.running_strategy(_fake_md(3))
    assert len(portfolio_client.calls) == 2


def test_periodic_sample_never_fires_on_mode_0(monkeypatch):
    """environment=0 backtest MUST NOT trigger PeriodicSample under any bar count or elapsed time.

    Wiring this check at `_run_session` level means the trigger installer is never
    invoked for environment=0 — we verify by running a fake backtest path and confirming
    zero PeriodicSample pushes regardless of bars or time.
    """
    wallet = _wallet()
    servicer = _make_servicer()
    state = SimpleNamespace(
        environment=0,
        status="running",
        bars_processed=0,
        error="",
        transition=lambda status, bars=None, error=None: True,
        configure_stop_runtime=lambda **_kwargs: None,
        # New fields added for environment=1 lease management; environment=0/1 don't use them
        # but _run_session's finally block reads lease_stop_event / required_streams
        # unconditionally, so the stub must have them present.
        lease_stop_event=None,
        required_streams=[],
    )
    request = SimpleNamespace(interval="1m", start_time_ms=1, end_time_ms=2)
    snapshot_calls: list[tuple] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def update_portfolio_wallet_state(self, **kwargs):
            snapshot_calls.append(("wallet_sync", kwargs.get("snapshot_reason")))
            return SimpleNamespace(ok=True)

        def update_session(self, **_kwargs) -> bool:
            return True

    class FakeOrderClient:
        def __init__(self, _addr: str) -> None:
            pass

    fake_user = SimpleNamespace(on_order_callback=None)

    class FakeEngine:
        def create_strategy(self, **_kwargs):
            return fake_user

        # Record that no one ever wrapped running_strategy with periodic trigger.
        running_strategy = staticmethod(lambda md: None)

    captured_engine: list[Any] = []

    def fake_run_backtest(session_id, inner_state, engine, req, declared_inputs):
        captured_engine.append(engine)
        # Simulate heavy bar traffic that WOULD fire PeriodicSample if the trigger
        # were wired (20 bars is well past the default 20-bar threshold).
        for i in range(50):
            engine.running_strategy(_fake_md(i))

    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: FakePortfolioClient(""))
    monkeypatch.setattr(servicer, "_order_client", lambda: FakeOrderClient(""))
    monkeypatch.setattr(servicer, "_run_backtest", fake_run_backtest)

    from strategy_service.inputs import StrategyInput
    servicer._run_session(
        session_id="sess-mode0",
        state=state,
        request=request,
        wallet=wallet,
        environment=0,
        portfolio_id=101,
        user_id=17,
        declared_inputs=[StrategyInput("binance", "futures", "BTCUSDT", "1m")],
        strategy_path="strategies.buy_once",
        strategy_id=202,
        strategy_code=None,
    )

    # No PeriodicSample (reason=6) ever pushed. strategy_end (reason=3) is expected
    # in the finally block, but never 6.
    reasons = [r for _, r in snapshot_calls]
    assert SNAPSHOT_REASON_PERIODIC_SAMPLE not in reasons

    # Sanity: the engine's running_strategy must be the raw class method (no closure
    # wrapper installed for environment=0).
    assert captured_engine, "fake_run_backtest should have been called"
    eng = captured_engine[0]
    assert eng.running_strategy.__name__ != "wrapped"


def test_periodic_sample_never_fires_on_mode_1(monkeypatch):
    """environment=2 remains fail-closed in Phase C and MUST NOT install PeriodicSample."""
    wallet = _wallet()
    servicer = _make_servicer()
    state = SimpleNamespace(
        environment=2,
        status="running",
        bars_processed=0,
        error="",
        transition=lambda status, bars=None, error=None: True,
        configure_stop_runtime=lambda **_kwargs: None,
        lease_stop_event=None,
        required_streams=[],
    )
    request = SimpleNamespace(interval="1m", start_time_ms=1, end_time_ms=2)
    snapshot_calls: list[tuple] = []

    class FakePortfolioClient:
        def __init__(self, _addr: str) -> None:
            pass

        def update_portfolio_wallet_state(self, **kwargs):
            snapshot_calls.append(("wallet_sync", kwargs.get("snapshot_reason")))
            return SimpleNamespace(ok=True)

        def update_session(self, **_kwargs) -> bool:
            return True

    class FakeOrderClient:
        def __init__(self, _addr: str) -> None:
            pass

    fake_user = SimpleNamespace(on_order_callback=None)

    class FakeEngine:
        def create_strategy(self, **_kwargs):
            return fake_user

        running_strategy = staticmethod(lambda md: None)

    captured_engine: list[Any] = []

    def fake_run_live(session_id, inner_state, engine, declared_inputs, strategy_id):
        captured_engine.append(engine)
        for i in range(50):
            engine.running_strategy(_fake_md(i))

    monkeypatch.setattr(grpc_server, "StrategyEngine", lambda: FakeEngine())
    monkeypatch.setattr(servicer, "_portfolio_client", lambda: FakePortfolioClient(""))
    monkeypatch.setattr(servicer, "_order_client", lambda: FakeOrderClient(""))
    monkeypatch.setattr(servicer, "_run_live", fake_run_live)

    from strategy_service.inputs import StrategyInput
    servicer._run_session(
        session_id="sess-mode1",
        state=state,
        request=request,
        wallet=wallet,
        environment=2,
        portfolio_id=101,
        user_id=17,
        declared_inputs=[StrategyInput("binance", "futures", "BTCUSDT", "1m")],
        strategy_path="strategies.buy_once",
        strategy_id=202,
        strategy_code=None,
    )

    reasons = [r for _, r in snapshot_calls]
    assert SNAPSHOT_REASON_PERIODIC_SAMPLE not in reasons
    assert not captured_engine, "live environment is fail-closed and must not enter _run_live"


def test_periodic_sample_push_failure_does_not_interrupt_strategy():
    """Push failure MUST be swallowed (logged as warn) and counters MUST still reset."""
    servicer = _make_servicer()
    portfolio_client = _RecordingPortfolioClient(fail=True)
    clock = _FakeClock()
    original_calls: list[Any] = []

    engine = _install_and_get_engine(
        servicer=servicer,
        portfolio_client=portfolio_client,
        clock=clock,
        original_calls=original_calls,
    )

    # Process 40 bars: two firings, both throwing — must not raise.
    for i in range(40):
        engine.running_strategy(_fake_md(i))
        clock.advance(1.0)

    # Original still called all 40 times (strategy not interrupted).
    assert len(original_calls) == 40
    # Push attempted twice (once per 20-bar window, counter reset on failure).
    assert len(portfolio_client.calls) == 2
