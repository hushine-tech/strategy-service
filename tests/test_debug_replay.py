from __future__ import annotations

from dataclasses import dataclass
import sys
import types

import pytest

import strategy_service.debug_replay as debug_replay_module
from strategy_service.debug_control_server import DebugReplayRequest
from strategy_service.debug_replay import DebugReplayRunner, _with_debug_inputs_if_missing
from strategy_service.gen import account_service_pb2, control_panel_service_pb2
from strategy_service.runtime_agent import DebugDataset, RuntimeBusyError, _kline_from_mapping
from tests.helpers.wallet_fixtures import _build_wallet_proto


def test_debug_replay_runs_cached_dataset_with_injected_inputs(tmp_path):
    strategy_file = tmp_path / "self_hosted_strategy.py"
    strategy_file.write_text(
        """
class MyStrategy:
    def __init__(self):
        self.seen = 0

    def on_market_data(self, data, wallet):
        self.seen += 1
        return None
""".strip(),
        encoding="utf-8",
    )
    dataset = DebugDataset(
        dataset_id="dbg-1",
        user_id=7,
        account_id=10,
        runtime_id="rt-debug",
        market="perpetual_futures",
        symbol="ETHUSDT",
        interval="1m",
        start_time_ms=1000,
        end_time_ms=121000,
        loaded_at_ms=999000,
        klines=[
            _kline_from_mapping({
                "symbol": "ETHUSDT",
                "interval": "1m",
                    "market": "perpetual_futures",
                "open_time_ms": 1000,
                "close_time_ms": 60999,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1.0,
            }),
            _kline_from_mapping({
                "symbol": "ETHUSDT",
                "interval": "1m",
                    "market": "perpetual_futures",
                "open_time_ms": 61000,
                "close_time_ms": 120999,
                "open": 100.5,
                "high": 102.0,
                "low": 100.0,
                "close": 101.5,
                "volume": 2.0,
            }),
        ],
    )
    agent = _FakeAgent(dataset)
    account = _FakeAccountClient()
    proxy = _FakePlatformProxy(account=account, dataset=dataset)
    runner = DebugReplayRunner(agent=agent, platform_proxy=proxy, workspace_path=str(tmp_path), progress_every_bars=1)

    result = runner.run(DebugReplayRequest(name="manual-debug"))

    assert result.session_id == "debug-session-1"
    assert result.status == "finished"
    assert result.bars_processed == 2
    assert agent.released is True
    assert account.saved["session_type"] == "debugging"
    assert account.saved["strategy_id"] == 0
    assert account.saved["session_name"] == "manual-debug"
    final_session_idx = account.events.index(("session", "finished"))
    final_portfolio_idx = account.events.index(("portfolio", 3))
    assert final_portfolio_idx < final_session_idx
    assert account.updates[-1]["status"] == "finished"
    assert account.updates[-1]["bars_processed"] == 2
    assert proxy.invocations[0].method == "debug.StartDebugReplay"


def test_debug_replay_rejects_concurrent_run(tmp_path):
    (tmp_path / "self_hosted_strategy.py").write_text("class MyStrategy:\n    pass\n", encoding="utf-8")
    dataset = DebugDataset(
        dataset_id="dbg-1",
        user_id=7,
        account_id=10,
        runtime_id="rt-debug",
        market="perpetual_futures",
        symbol="ETHUSDT",
        interval="1m",
        start_time_ms=1000,
        end_time_ms=61000,
        loaded_at_ms=999000,
        klines=[_kline_from_mapping({
            "symbol": "ETHUSDT",
            "interval": "1m",
            "market": "perpetual_futures",
            "open_time_ms": 1000,
            "close_time_ms": 60999,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1.0,
        })],
    )
    agent = _FakeAgent(dataset, acquire=False)
    runner = DebugReplayRunner(
        agent=agent,
        platform_proxy=_FakePlatformProxy(account=_FakeAccountClient(), dataset=dataset),
        workspace_path=str(tmp_path),
    )

    with pytest.raises(RuntimeBusyError):
        runner.run(DebugReplayRequest())


def test_debug_input_injection_preserves_declared_inputs():
    code = """
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "spot", "symbol": "BTCUSDT", "interval": "1m"}]
    def on_market_data(self, data, wallet):
        return None
"""

    updated = _with_debug_inputs_if_missing(code, market="perpetual_futures", symbol="ETHUSDT", interval="1m")

    assert updated != code
    assert "MyStrategy.INPUTS" not in updated
    assert "MyStrategy.ORDER_TARGETS = []" in updated


def test_activate_pycharm_debugger_uses_current_pydevd_keywords(monkeypatch):
    calls: dict[str, object] = {}

    def settrace(host, **kwargs):
        calls["host"] = host
        calls["kwargs"] = kwargs

    monkeypatch.setitem(sys.modules, "pydevd_pycharm", types.SimpleNamespace(settrace=settrace))

    DebugReplayRunner._activate_debugger(DebugReplayRequest(
        debugger="pycharm",
        host="host.docker.internal",
        port=5680,
        wait=True,
    ))

    assert calls["host"] == "host.docker.internal"
    kwargs = calls["kwargs"]
    assert kwargs["port"] == 5680
    assert kwargs["stdout_to_server"] is True
    assert kwargs["stderr_to_server"] is True
    assert kwargs["suspend"] is True
    assert kwargs["trace_only_current_thread"] is True
    assert "stdoutToServer" not in kwargs
    assert "stderrToServer" not in kwargs


def test_activate_vscode_alias_uses_debugpy(monkeypatch):
    calls: dict[str, object] = {}

    debugpy = types.SimpleNamespace(
        listen=lambda endpoint: calls.setdefault("listen", endpoint),
        wait_for_client=lambda: calls.setdefault("wait", True),
    )
    monkeypatch.setitem(sys.modules, "debugpy", debugpy)
    monkeypatch.setattr(debug_replay_module, "_DEBUGPY_LISTEN_ENDPOINT", None)

    DebugReplayRunner._activate_debugger(DebugReplayRequest(
        debugger="vscode",
        host="0.0.0.0",
        port=5678,
        wait=True,
    ))

    assert calls["listen"] == ("0.0.0.0", 5678)
    assert calls["wait"] is True


def test_activate_debugpy_reuses_existing_listener(monkeypatch):
    calls: dict[str, int] = {"listen": 0, "wait": 0}

    def listen(_endpoint):
        calls["listen"] += 1

    def wait_for_client():
        calls["wait"] += 1

    debugpy = types.SimpleNamespace(listen=listen, wait_for_client=wait_for_client)
    monkeypatch.setitem(sys.modules, "debugpy", debugpy)
    monkeypatch.setattr(debug_replay_module, "_DEBUGPY_LISTEN_ENDPOINT", None)

    request = DebugReplayRequest(debugger="debugpy", host="0.0.0.0", port=5678, wait=True)
    DebugReplayRunner._activate_debugger(request)
    DebugReplayRunner._activate_debugger(request)

    assert calls == {"listen": 1, "wait": 2}


def test_activate_pycharm_debugger_suppresses_existing_thread_attach(monkeypatch):
    calls: dict[str, object] = {"noop_called": False}

    def original_set_trace_to_threads(_tracing_func):
        raise AssertionError("existing runtime threads must not be patched for debugger replay")

    tracing_module = types.SimpleNamespace(set_trace_to_threads=original_set_trace_to_threads)

    def settrace(_host, **_kwargs):
        tracing_module.set_trace_to_threads(object())
        calls["during_settrace"] = tracing_module.set_trace_to_threads is not original_set_trace_to_threads

    monkeypatch.setitem(sys.modules, "pydevd_tracing", tracing_module)
    monkeypatch.setitem(sys.modules, "pydevd_pycharm", types.SimpleNamespace(settrace=settrace))

    DebugReplayRunner._activate_debugger(DebugReplayRequest(
        debugger="pycharm",
        host="host.docker.internal",
        port=5680,
        wait=True,
    ))

    assert calls["during_settrace"] is True
    assert tracing_module.set_trace_to_threads is original_set_trace_to_threads


def test_activate_pycharm_debugger_resets_stale_connected_state(monkeypatch):
    calls: dict[str, object] = {}
    pydevd_module = types.SimpleNamespace(
        connected=True,
        get_global_debugger=lambda: None,
    )

    def settrace(_host, **_kwargs):
        calls["connected_during_settrace"] = pydevd_module.connected

    monkeypatch.setitem(sys.modules, "pydevd", pydevd_module)
    monkeypatch.setitem(sys.modules, "pydevd_pycharm", types.SimpleNamespace(settrace=settrace))

    DebugReplayRunner._activate_debugger(DebugReplayRequest(
        debugger="pycharm",
        host="host.docker.internal",
        port=5680,
    ))

    assert calls["connected_during_settrace"] is False


def test_deactivate_pycharm_debugger_stops_trace_and_resets_connected(monkeypatch):
    calls: dict[str, object] = {"stoptrace": 0}

    def stoptrace():
        calls["stoptrace"] = int(calls["stoptrace"]) + 1

    pydevd_module = types.SimpleNamespace(
        connected=True,
        get_global_debugger=lambda: object(),
        stoptrace=stoptrace,
    )
    monkeypatch.setitem(sys.modules, "pydevd", pydevd_module)

    DebugReplayRunner._deactivate_debugger(DebugReplayRequest(debugger="pycharm"))

    assert calls["stoptrace"] == 1
    assert pydevd_module.connected is False


@dataclass
class _Invocation:
    method: str
    request: object


class _FakeAgent:
    def __init__(self, dataset: DebugDataset | None, acquire: bool = True) -> None:
        self._dataset = dataset
        self._acquire = acquire
        self.released = False

    def active_debug_dataset(self):
        return self._dataset

    def try_acquire_debug_replay(self):
        return self._acquire

    def release_debug_replay(self):
        self.released = True


class _FakePlatformProxy:
    def __init__(self, *, account: "_FakeAccountClient", dataset: DebugDataset) -> None:
        self._account = account
        self._dataset = dataset
        self.invocations: list[_Invocation] = []

    def invoke(self, method, request, response_type, *, timeout_seconds=30.0):
        del timeout_seconds
        self.invocations.append(_Invocation(method=method, request=request))
        assert response_type is control_panel_service_pb2.StartDebugReplayResponse
        ds = control_panel_service_pb2.DebugDatasetState(
            dataset_id=self._dataset.dataset_id,
            user_id=self._dataset.user_id,
            account_id=self._dataset.account_id,
            runtime_id=self._dataset.runtime_id,
            market=self._dataset.market,
            symbol=self._dataset.symbol,
            interval=self._dataset.interval,
            start_time_ms=self._dataset.start_time_ms,
            end_time_ms=self._dataset.end_time_ms,
            bar_count=len(self._dataset.klines),
            coverage_status="complete",
            state="active",
        )
        return control_panel_service_pb2.StartDebugReplayResponse(
            session_id="debug-session-1",
            session_name=request.requested_name or "debug-auto",
            dataset=ds,
        )

    def account_client(self):
        return self._account

    def order_client(self):
        return _FakeOrderClient()

    def notification_client(self):
        return None


class _FakeAccountClient:
    def __init__(self) -> None:
        self.saved = {}
        self.updates = []
        self.portfolio_updates = []
        self.events = []

    def get_portfolio_snapshot(self, account_id: int, user_id: int):
        assert account_id == 10
        assert user_id == 7
        return account_service_pb2.PortfolioSnapshot(
            account_id=account_id,
            user_id=user_id,
            wallet=_build_wallet_proto(
                environment=0,
                margin_mode="cross",
                position_mode="one_way",
                wallet_balance=10_000.0,
                available_balance=None,
                initial_balance=10_000.0,
                deposit_sum=0.0,
                withdrawal_sum=0.0,
                futures_positions=[
                    {
                        "symbol": "ETHUSDT",
                        "position_qty": 0.0,
                        "entry_price": 0.0,
                        "mark_price": 100.0,
                        "leverage": 10.0,
                        "margin_mode": "cross",
                    },
                ],
                spot_assets=None,
                spot_free=0.0,
                spot_locked=0.0,
            ),
        )

    def require_save_session(self, **kwargs):
        self.saved = kwargs
        return account_service_pb2.SaveSessionResponse()

    def update_session(self, **kwargs):
        self.updates.append(kwargs)
        self.events.append(("session", kwargs.get("status")))
        return True

    def update_portfolio_snapshot(self, **kwargs):
        self.portfolio_updates.append(kwargs)
        self.events.append(("portfolio", kwargs.get("snapshot_reason")))
        return None


class _FakeOrderClient:
    pass
