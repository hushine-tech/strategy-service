from __future__ import annotations

import sys
import threading

from google.protobuf.struct_pb2 import Struct
import pytest

from strategy_service.notification import (
    ControlPanelNotificationClient,
    NoopNotificationClient,
    StrategyNotifier,
)
from strategy_service.strategy import base as strategy_base
from strategy_service.strategy.base import BaseStrategy
from strategy_service.strategy_imports import (
    gate_strategy_source,
    prepare_strategy,
    resolve_strategy_source,
)
from strategy_service.types import Exchange, Market, MarketData
from strategy_service.wallet.portfolio import PortfolioWalletRuntime


class _Wallet:
    def on_market_data(self, symbol: str, market: str, price: float) -> None:
        self.last = (symbol, market, price)


def _portfolio_wallet() -> PortfolioWalletRuntime:
    return PortfolioWalletRuntime(
        7,
        {(Exchange.BINANCE, Market.PERPETUAL_FUTURES)},
        {(Exchange.BINANCE, Market.PERPETUAL_FUTURES, 1001): _Wallet()},
    )


def _prepare_strategy(code: str):
    gate = gate_strategy_source(
        resolve_strategy_source("notification_test.py", code),
        python_invocation_path=sys.executable,
    )
    assert gate.ok and gate.gated_source is not None
    return prepare_strategy(gate.gated_source)


class _NotificationClient:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[dict] = []

    def publish(self, **kwargs) -> bool:
        self.calls.append(kwargs)
        return not self.fail


def test_self_notify_injected_into_user_strategy() -> None:
    client = _NotificationClient()
    strategy = BaseStrategy(
        _prepare_strategy("""
from strategy_service.types import Exchange, Market

class MyStrategy:
    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.PERPETUAL_FUTURES, "symbol": "ETHUSDT", "interval": "1m"}]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        self.notify.warn("threshold reached", title="Risk")
        return None
"""),
        _portfolio_wallet(),
        portfolio_id=7,
        strategy_id=9,
        session_id="sess-1",
        notifier=StrategyNotifier(client),
    )

    strategy.running_strategy(
        MarketData(
            exchange=Exchange.BINANCE,
            symbol="ETHUSDT",
            price=2500,
            timestamp=1,
            market=Market.PERPETUAL_FUTURES,
            interval="1m",
        )
    )

    assert len(client.calls) == 1
    assert client.calls[0]["severity"] == "warn"
    assert client.calls[0]["message"] == "threshold reached"
    assert client.calls[0]["title"] == "Risk"
    assert client.calls[0]["portfolio_id"] == 7
    assert client.calls[0]["strategy_id"] == 9
    assert client.calls[0]["session_id"] == "sess-1"


def test_callback_thread_fatal_latch_wakes_the_session_loop_once() -> None:
    wake = threading.Event()
    fatal_notifications: list[BaseException] = []
    strategy = BaseStrategy(
        _prepare_strategy("""
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    callback_calls = 0

    def on_market_data(self, data, wallet):
        return None

    def on_order_update(self, event, wallet):
        type(self).callback_calls += 1
        raise SystemExit("user-controlled secret")
"""),
        _portfolio_wallet(),
        session_id="sess-fatal",
        on_user_code_fatal=lambda fatal: (
            fatal_notifications.append(fatal),
            wake.set(),
        ),
    )

    first = threading.Thread(target=strategy._notify_order_update, args=(object(),))
    first.start()
    first.join(timeout=2)

    assert not first.is_alive()
    assert wake.wait(timeout=1)
    assert len(fatal_notifications) == 1
    assert str(fatal_notifications[0]) == "strategy user code terminated"

    second = threading.Thread(target=strategy._notify_order_update, args=(object(),))
    second.start()
    second.join(timeout=2)

    assert not second.is_alive()
    assert strategy._get_strategy().callback_calls == 1
    assert len(fatal_notifications) == 1
    fatal_type = getattr(strategy_base, "StrategyUserCodeFatalError")
    with pytest.raises(fatal_type) as captured:
        strategy.raise_if_user_code_fatal()
    assert captured.value.stage == "callback"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None


def test_external_fatal_wins_before_main_bar_admission_without_side_effects() -> None:
    strategy = BaseStrategy(
        _prepare_strategy("""
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "ETHUSDT", "interval": "1m"}]
    ORDER_TARGETS = []
    market_calls = 0

    def on_market_data(self, data, wallet):
        type(self).market_calls += 1
        return None

    def on_order_update(self, event, wallet):
        raise SystemExit("fatal-first-secret")
"""),
        _portfolio_wallet(),
        session_id="sess-fatal-first",
    )
    main_precheck_complete = threading.Event()
    release_main = threading.Event()
    original_check = strategy.raise_if_user_code_fatal
    check_calls = 0

    def controlled_check() -> None:
        nonlocal check_calls
        original_check()
        check_calls += 1
        if check_calls == 1:
            main_precheck_complete.set()
            assert release_main.wait(timeout=2)

    strategy.raise_if_user_code_fatal = controlled_check
    main_errors: list[BaseException] = []

    def run_main_bar() -> None:
        try:
            strategy.running_strategy(
                MarketData(
                    exchange=Exchange.BINANCE,
                    symbol="ETHUSDT",
                    price=2500,
                    timestamp=1,
                    market=Market.PERPETUAL_FUTURES,
                    interval="1m",
                )
            )
        except BaseException as error:
            main_errors.append(error)

    main = threading.Thread(target=run_main_bar)
    main.start()
    assert main_precheck_complete.wait(timeout=2)

    external = threading.Thread(target=strategy._notify_order_update, args=(object(),))
    external.start()
    external.join(timeout=2)
    assert not external.is_alive()
    assert strategy.has_user_code_fatal()

    release_main.set()
    main.join(timeout=2)

    assert not main.is_alive()
    assert len(main_errors) == 1
    assert isinstance(main_errors[0], strategy_base.StrategyUserCodeFatalError)
    assert strategy._get_strategy().market_calls == 0
    assert strategy.last_market_time is None
    assert strategy._view.trigger is None


def test_self_notify_swallow_transport_error_and_returns_false() -> None:
    client = _NotificationClient(fail=True)
    notifier = StrategyNotifier(client)

    assert notifier.info("hello", portfolio_id=1, strategy_id=2, session_id="sess") is False


def test_noop_notification_client_returns_false() -> None:
    assert NoopNotificationClient().publish(message="hello") is False


class _CPStub:
    def __init__(self) -> None:
        self.requests = []

    def PublishRuntimeNotification(self, req, timeout=None):
        self.requests.append((req, timeout))
        return type("Resp", (), {"accepted": True})()


def test_hosted_notification_client_calls_control_panel_publish_runtime_notification() -> None:
    stub = _CPStub()
    client = ControlPanelNotificationClient(
        stub,
        user_id=42,
        runtime_id="rt-1",
        timeout_seconds=1.5,
    )

    assert client.publish(
        portfolio_id=7,
        strategy_id=9,
        session_id="sess-1",
        severity="error",
        title="Risk",
        message="threshold reached",
    ) is True
    req, timeout = stub.requests[0]
    assert timeout == 1.5
    assert req.user_id == 42
    assert req.runtime_id == "rt-1"
    assert req.portfolio_id == 7
    assert req.strategy_id == 9
    assert req.session_id == "sess-1"
    assert req.category == "custom"
    assert req.severity == "error"
    assert req.title == "Risk"
    assert req.message == "threshold reached"


class _Proxy:
    def __init__(self) -> None:
        self.calls = []

    def invoke(self, method, request, response_type, *, timeout_seconds=30.0):
        self.calls.append((method, request, response_type, timeout_seconds))
        resp = Struct()
        resp.update({"accepted": True})
        return resp


def test_proxy_notification_client_uses_runtime_channel_method() -> None:
    from strategy_service.platform_proxy import ProxyNotificationClient

    proxy = _Proxy()
    client = ProxyNotificationClient(proxy)

    assert client.publish(
        portfolio_id=7,
        strategy_id=9,
        session_id="sess-1",
        severity="info",
        title="Note",
        message="hello",
    ) is True
    method, request, response_type, timeout = proxy.calls[0]
    assert method == "notification.Publish"
    assert response_type is Struct
    assert timeout == 2.0
    assert request["category"] == "custom"
    assert request["severity"] == "info"
    assert request["message"] == "hello"
    assert request["portfolio_id"] == 7
    assert request["strategy_id"] == 9
    assert request["session_id"] == "sess-1"
