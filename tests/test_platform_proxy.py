from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from strategy_service.gen import account_service_pb2, marketdata_service_pb2, order_service_pb2
from strategy_service.platform_proxy import (
    ACCOUNT_PREFLIGHT_STRATEGY_SESSION,
    ACCOUNT_GET_PORTFOLIO,
    ACCOUNT_SAVE_SESSION,
    ACCOUNT_UPDATE_WALLET_STATE,
    LOGS_EMIT,
    MARKETDATA_FETCH_KLINES,
    MARKETDATA_GET_STATUS,
    ORDER_PLACE,
    RuntimeChannelLogHandler,
    RuntimeChannelPlatformProxy,
)
from strategy_service.types import OrderDecision


def test_proxy_account_client_sends_save_session_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    proxy = RuntimeChannelPlatformProxy(runtime)

    ok = proxy.account_client().save_session(
        session_id="sess-1",
        account_id=7,
        strategy_id=9,
        environment=1,
        runtime_id="runtime-1",
        runtime_source="self_hosted",
        runtime_name="desk",
    )

    assert ok is True
    method, req = runtime.calls[-1]
    assert method == ACCOUNT_SAVE_SESSION
    assert req.session_id == "sess-1"
    assert req.runtime_id == "runtime-1"


def test_proxy_account_client_fetches_portfolio_snapshot_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[ACCOUNT_GET_PORTFOLIO] = account_service_pb2.GetPortfolioSnapshotResponse(
        snapshot=account_service_pb2.PortfolioSnapshot(account_id=7, user_id=3)
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    snapshot = proxy.account_client().get_portfolio_snapshot(account_id=7, user_id=3)

    method, req = runtime.calls[-1]
    assert method == ACCOUNT_GET_PORTFOLIO
    assert req.account_id == 7
    assert req.user_id == 3
    assert snapshot.account_id == 7


def test_proxy_account_client_updates_portfolio_snapshot_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    proxy = RuntimeChannelPlatformProxy(runtime)

    with pytest.raises(RuntimeError, match="deprecated"):
        proxy.account_client().update_portfolio_snapshot(
            account_id=7,
            user_id=3,
            snapshot_reason=2,
            strategy_id=9,
            session_id="sess-1",
            snapshot_time=1780274580000,
        )

    assert runtime.calls == []


def test_proxy_account_client_updates_backtest_wallet_state_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[ACCOUNT_UPDATE_WALLET_STATE] = account_service_pb2.UpdateAccountWalletStateResponse(
        wallet=account_service_pb2.AccountWalletState(total_value=1200)
    )
    proxy = RuntimeChannelPlatformProxy(runtime)
    future_wallet = SimpleNamespace(
        margin_mode="cross",
        position_mode="one_way",
        positions={},
        wallet_balance=1100.0,
        available_balance=1000.0,
        margin_balance=1200.0,
    )

    wallet = proxy.account_client().update_account_wallet_state(
        account_id=7,
        user_id=3,
        future_wallet=future_wallet,
        snapshot_reason=1,
        strategy_id=9,
        session_id="sess-1",
        snapshot_time=1780274580000,
    )

    method, req = runtime.calls[-1]
    assert method == ACCOUNT_UPDATE_WALLET_STATE
    assert req.account_id == 7
    assert req.user_id == 3
    assert req.futures.wallet_balance == 1100.0
    assert req.total_value == 1200.0
    assert req.snapshot_reason == 1
    assert req.session_id == "sess-1"
    assert wallet.total_value == 1200


def test_proxy_account_client_surfaces_wallet_state_update_errors():
    runtime = _FakeRuntimeChannel()
    runtime.errors[ACCOUNT_UPDATE_WALLET_STATE] = RuntimeError(
        "Unavailable: save snapshot: duplicate key"
    )
    proxy = RuntimeChannelPlatformProxy(runtime)
    future_wallet = SimpleNamespace(
        margin_mode="cross",
        position_mode="one_way",
        positions={},
        wallet_balance=1100.0,
        available_balance=1000.0,
        margin_balance=1200.0,
    )

    with pytest.raises(RuntimeError, match="duplicate key"):
        proxy.account_client().update_account_wallet_state(
            account_id=7,
            user_id=3,
            future_wallet=future_wallet,
            snapshot_reason=1,
            strategy_id=9,
            session_id="sess-1",
        )


def test_proxy_account_client_preflight_sends_session_metadata_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[ACCOUNT_PREFLIGHT_STRATEGY_SESSION] = (
        account_service_pb2.PreflightStrategySessionResponse(ok=True)
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    resp = proxy.account_client().preflight_strategy_session(
        account_id=7,
        user_id=3,
        required_routes={("binance", "perpetual_futures")},
        required_symbols={("binance", "perpetual_futures", "btcusdt")},
        session_id="preflight-session-1",
        strategy_id=9,
    )

    method, req = runtime.calls[-1]
    assert method == ACCOUNT_PREFLIGHT_STRATEGY_SESSION
    assert resp.ok is True
    assert req.session_id == "preflight-session-1"
    assert req.strategy_id == 9


def test_proxy_order_client_places_order_without_direct_stub():
    runtime = _FakeRuntimeChannel()
    runtime.responses[ORDER_PLACE] = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-1",
        attempt_id="attempt-1",
        attempt_status="ACCEPTED",
        order=order_service_pb2.ExchangeOrderEntry(
            order_id="order-1",
            symbol="ETHUSDT",
            side="BUY",
            status="FILLED",
            orig_qty=1,
            executed_qty=1,
            avg_price=2000,
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                qty=1,
                fill_price=2000,
                fee=0.8,
                status="CONFIRMED",
            )
        ],
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    feedback = proxy.order_client().place_order(
        7,
        OrderDecision(
            exchange="binance",
            market="perpetual_futures",
            symbol="ETHUSDT",
            side="BUY",
            qty="1",
            order_type="MARKET",
        ),
        2000,
        strategy_id=9,
        session_id="sess-1",
    )

    method, req = runtime.calls[-1]
    assert method == ORDER_PLACE
    assert req.account_id == 7
    assert req.session_id == "sess-1"
    assert req.exchange == 1
    assert req.market == 2
    assert req.position_side == 0
    assert feedback.attempt_status == "ACCEPTED"
    assert feedback.order.order_id == "order-1"


def test_proxy_marketdata_client_uses_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[MARKETDATA_GET_STATUS] = marketdata_service_pb2.GetMarketDataStreamStatusResponse(
        stream=marketdata_service_pb2.MarketDataStream(
            stream_id=11,
            key=marketdata_service_pb2.StreamKey(
                exchange="binance",
                market="futures",
                kind="kline",
                symbol="ETHUSDT",
                interval="1m",
            ),
        )
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    stream = proxy.marketdata_client().get_market_data_stream_status(
        exchange="binance",
        market="futures",
        symbol="ETHUSDT",
        interval="1m",
    )

    method, req = runtime.calls[-1]
    assert method == MARKETDATA_GET_STATUS
    assert req.key.symbol == "ETHUSDT"
    assert stream.stream_id == 11


def test_proxy_marketdata_client_fetches_klines_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    from google.protobuf.struct_pb2 import Struct

    resp = Struct()
    resp.update({
        "klines": [{
            "exchange": "binance",
            "market": "futures",
            "symbol": "ETHUSDT",
            "interval": "1m",
            "open_time": 1000,
            "close_time": 2000,
            "timestamp": 2000,
            "open": 1.0,
            "high": 2.0,
            "low": 0.5,
            "close": 1.5,
            "volume": 10.0,
        }]
    })
    runtime.responses[MARKETDATA_FETCH_KLINES] = resp
    proxy = RuntimeChannelPlatformProxy(runtime)

    rows = proxy.marketdata_client().fetch_klines(
        market="futures",
        symbol="ETHUSDT",
        interval="1m",
        start_time_ms=1000,
        end_time_ms=2000,
    )

    method, req = runtime.calls[-1]
    assert method == MARKETDATA_FETCH_KLINES
    assert req["symbol"] == "ETHUSDT"
    assert rows[0].open_time == 1000
    assert rows[0].close == 1.5


def test_proxy_log_client_emits_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    proxy = RuntimeChannelPlatformProxy(runtime)

    ok = proxy.log_client().emit(
        level="INFO",
        logger_name="strategy_service.test",
        message="hello",
        log_type="root",
        session_id="sess-1",
    )

    assert ok is True
    method, req = runtime.calls[-1]
    assert method == LOGS_EMIT
    assert req["logger"] == "strategy_service.test"
    assert req["message"] == "hello"
    assert req["session_id"] == "sess-1"


def test_runtime_channel_log_handler_forwards_non_internal_records():
    runtime = _FakeRuntimeChannel()
    proxy = RuntimeChannelPlatformProxy(runtime)
    handler = RuntimeChannelLogHandler(proxy)

    import logging

    record = logging.LogRecord(
        name="strategy_service.grpc_server",
        level=logging.WARNING,
        pathname=__file__,
        lineno=123,
        msg="session %s",
        args=("warned",),
        exc_info=None,
    )
    handler.emit(record)

    method, req = runtime.calls[-1]
    assert method == LOGS_EMIT
    assert req["level"] == "WARNING"
    assert req["message"] == "session warned"


def test_runtime_channel_log_handler_skips_proxy_internals():
    runtime = _FakeRuntimeChannel()
    proxy = RuntimeChannelPlatformProxy(runtime)
    handler = RuntimeChannelLogHandler(proxy)

    import logging

    record = logging.LogRecord(
        name="strategy_service.runtime_channel",
        level=logging.WARNING,
        pathname=__file__,
        lineno=123,
        msg="loop",
        args=(),
        exc_info=None,
    )
    handler.emit(record)

    assert runtime.calls == []


class _FakeRuntimeChannel:
    def __init__(self) -> None:
        self.calls = []
        self.errors = {}
        self.responses = {
            ACCOUNT_SAVE_SESSION: account_service_pb2.SaveSessionResponse(),
            ORDER_PLACE: order_service_pb2.PlaceOrderResponse(),
            MARKETDATA_GET_STATUS: marketdata_service_pb2.GetMarketDataStreamStatusResponse(),
        }

    def invoke_platform_unary(self, method, request, response_type, *, timeout_seconds=30.0):
        del timeout_seconds
        self.calls.append((method, request))
        if method in self.errors:
            raise self.errors[method]
        resp = self.responses.get(method)
        if resp is None:
            return response_type()
        return resp
