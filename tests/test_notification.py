from __future__ import annotations

from google.protobuf.struct_pb2 import Struct

from strategy_service.notification import (
    ControlPanelNotificationClient,
    NoopNotificationClient,
    StrategyNotifier,
)
from strategy_service.strategy.base import BaseStrategy
from strategy_service.types import MarketData


class _Wallet:
    def on_market_data(self, symbol: str, market: str, price: float) -> None:
        self.last = (symbol, market, price)


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
        "ignored",
        _Wallet(),
        account_id=7,
        strategy_id=9,
        session_id="sess-1",
        notifier=StrategyNotifier(client),
        strategy_code="""
class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "futures", "symbol": "ETHUSDT", "interval": "1m"}]
    def on_market_data(self, data, wallet):
        self.notify.warn("threshold reached", title="Risk")
        return None
""",
    )

    strategy.running_strategy(MarketData(symbol="ETHUSDT", price=2500, timestamp=1, market="futures", interval="1m"))

    assert len(client.calls) == 1
    assert client.calls[0]["severity"] == "warn"
    assert client.calls[0]["message"] == "threshold reached"
    assert client.calls[0]["title"] == "Risk"
    assert client.calls[0]["account_id"] == 7
    assert client.calls[0]["strategy_id"] == 9
    assert client.calls[0]["session_id"] == "sess-1"


def test_self_notify_swallow_transport_error_and_returns_false() -> None:
    client = _NotificationClient(fail=True)
    notifier = StrategyNotifier(client)

    assert notifier.info("hello", account_id=1, strategy_id=2, session_id="sess") is False


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
        account_id=7,
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
    assert req.account_id == 7
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
        account_id=7,
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
    assert request["account_id"] == 7
    assert request["strategy_id"] == 9
    assert request["session_id"] == "sess-1"
