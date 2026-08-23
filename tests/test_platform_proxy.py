from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from strategy_service.gen import portfolio_service_pb2, marketdata_service_pb2, order_service_pb2
from strategy_service.platform_proxy import (
    PORTFOLIO_COMMIT_STRATEGY_SESSION_START,
    PORTFOLIO_PREFLIGHT_STRATEGY_SESSION,
    PORTFOLIO_GET_PORTFOLIO,
    PORTFOLIO_SAVE_SESSION,
    PORTFOLIO_SAVE_STRATEGY_INDICATORS,
    PORTFOLIO_UPDATE_WALLET_STATE,
    PORTFOLIO_UPDATE_SESSION,
    LOGS_EMIT,
    MARKETDATA_FETCH_BACKTEST_PAGE,
    MARKETDATA_FETCH_KLINES,
    MARKETDATA_GET_STATUS,
    ORDER_PLACE,
    ORDER_CLOSE_SPOT_TARGETS,
    ORDER_LIST_LIFECYCLE_EVENTS,
    RuntimeChannelLogHandler,
    RuntimeChannelPlatformProxy,
)
from strategy_service.indicators import IndicatorChunk, IndicatorDefinition
from strategy_service.inputs import StrategyOrderTarget
from strategy_service.types import OrderDecision


def test_proxy_portfolio_client_sends_save_session_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    proxy = RuntimeChannelPlatformProxy(runtime)

    ok = proxy.portfolio_client().save_session(
        session_id="sess-1",
        portfolio_id=7,
        strategy_id=9,
        environment=1,
        runtime_id="runtime-1",
        runtime_source="self_hosted",
        runtime_name="desk",
        leverage=4,
        initial_status="pending",
    )

    assert ok is True
    method, req = runtime.calls[-1]
    assert method == PORTFOLIO_SAVE_SESSION
    assert req.session_id == "sess-1"
    assert req.runtime_id == "runtime-1"
    assert req.leverage == 4
    assert req.initial_status == "pending"


def test_proxy_portfolio_client_sends_pending_status_cas():
    runtime = _FakeRuntimeChannel()
    proxy = RuntimeChannelPlatformProxy(runtime)

    ok = proxy.portfolio_client().update_session(
        session_id="sess-1",
        status="running",
        runtime_id="runtime-1",
        expected_status="pending",
    )

    assert ok is True
    method, req = runtime.calls[-1]
    assert method == PORTFOLIO_UPDATE_SESSION
    assert req.status == "running"
    assert req.expected_status == "pending"


def test_proxy_portfolio_client_sends_strategy_indicators_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[PORTFOLIO_SAVE_STRATEGY_INDICATORS] = portfolio_service_pb2.SaveStrategyIndicatorsResponse(
        definitions_saved=1,
        chunks_saved=1,
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    saved = proxy.portfolio_client().save_strategy_indicators(
        session_id="sess-1",
        user_id=6,
        definitions=[
            IndicatorDefinition(
                key="alpha_score",
                name="Alpha Score",
                type="line",
                pane="strategy",
                stream_key="binance:perpetual_futures:ETHUSDT:1m",
                color="#2563eb",
                config={"line_width": 2},
            )
        ],
        chunks=[
            IndicatorChunk(
                stream_key="binance:perpetual_futures:ETHUSDT:1m",
                indicator_key="alpha_score",
                chunk_index=0,
                start_time_ms=1_780_000_000_000,
                end_time_ms=1_780_000_060_000,
                interval_ms=60_000,
                count=2,
                values_json={"values": [0.12, 0.15], "times": None},
            )
        ],
    )

    method, req = runtime.calls[-1]
    assert saved == (1, 1)
    assert method == PORTFOLIO_SAVE_STRATEGY_INDICATORS
    assert req.session_id == "sess-1"
    assert req.user_id == 6
    assert req.definitions[0].stream_key == "binance:perpetual_futures:ETHUSDT:1m"
    assert req.definitions[0].indicator_key == "alpha_score"
    assert req.definitions[0].config_json == '{"line_width":2}'
    assert req.chunks[0].values_json == '{"values":[0.12,0.15],"times":null}'


def test_proxy_portfolio_client_fetches_portfolio_snapshot_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[PORTFOLIO_GET_PORTFOLIO] = portfolio_service_pb2.GetPortfolioSnapshotResponse(
        snapshot=portfolio_service_pb2.PortfolioSnapshot(portfolio_id=7, user_id=3)
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    snapshot = proxy.portfolio_client().get_portfolio_snapshot(
        portfolio_id=7,
        user_id=3,
        required_symbols={("binance", "perpetual_futures", "ethusdt")},
    )

    method, req = runtime.calls[-1]
    assert method == PORTFOLIO_GET_PORTFOLIO
    assert req.portfolio_id == 7
    assert req.user_id == 3
    assert len(req.required_symbols) == 1
    assert req.required_symbols[0].symbol == "ETHUSDT"
    assert snapshot.portfolio_id == 7


def test_proxy_portfolio_client_updates_portfolio_snapshot_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    proxy = RuntimeChannelPlatformProxy(runtime)

    with pytest.raises(RuntimeError, match="deprecated"):
        proxy.portfolio_client().update_portfolio_snapshot(
            portfolio_id=7,
            user_id=3,
            snapshot_reason=2,
            strategy_id=9,
            session_id="sess-1",
            snapshot_time=1780274580000,
        )

    assert runtime.calls == []


def test_proxy_portfolio_client_updates_backtest_wallet_state_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[PORTFOLIO_UPDATE_WALLET_STATE] = portfolio_service_pb2.UpdatePortfolioWalletStateResponse(
        wallet=portfolio_service_pb2.PortfolioWalletState(total_value=1200)
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

    wallet = proxy.portfolio_client().update_portfolio_wallet_state(
        portfolio_id=7,
        user_id=3,
        future_wallet=future_wallet,
        snapshot_reason=1,
        strategy_id=9,
        session_id="sess-1",
        snapshot_time=1780274580000,
    )

    method, req = runtime.calls[-1]
    assert method == PORTFOLIO_UPDATE_WALLET_STATE
    assert req.portfolio_id == 7
    assert req.user_id == 3
    assert req.futures.wallet_balance == 1100.0
    assert req.total_value == 1200.0
    assert req.snapshot_reason == 1
    assert req.session_id == "sess-1"
    assert wallet.total_value == 1200


def test_proxy_portfolio_client_surfaces_wallet_state_update_errors():
    runtime = _FakeRuntimeChannel()
    runtime.errors[PORTFOLIO_UPDATE_WALLET_STATE] = RuntimeError(
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
        proxy.portfolio_client().update_portfolio_wallet_state(
            portfolio_id=7,
            user_id=3,
            future_wallet=future_wallet,
            snapshot_reason=1,
            strategy_id=9,
            session_id="sess-1",
        )


def test_proxy_portfolio_client_preflight_sends_session_metadata_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[PORTFOLIO_PREFLIGHT_STRATEGY_SESSION] = (
        portfolio_service_pb2.PreflightStrategySessionResponse(ok=True)
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    resp = proxy.portfolio_client().preflight_strategy_session(
        portfolio_id=7,
        user_id=3,
        required_routes={("binance", "perpetual_futures")},
        required_symbols={
            ("binance", "perpetual_futures", "btcusdt"),
            ("binance", "spot", "ethusdt"),
        },
        order_targets=[
            StrategyOrderTarget(
                exchange="binance",
                market="perpetual_futures",
                symbol="BTCUSDT",
                effective_leverage=3,
                leverage_source="strategy_default",
            ),
            StrategyOrderTarget(
                exchange="binance",
                market="spot",
                symbol="ETHUSDT",
            ),
        ],
        session_id="preflight-session-1",
        strategy_id=9,
    )

    method, req = runtime.calls[-1]
    assert method == PORTFOLIO_PREFLIGHT_STRATEGY_SESSION
    assert resp.ok is True
    assert req.session_id == "preflight-session-1"
    assert req.strategy_id == 9
    assert req.leverage == 0
    symbols = {item.symbol: item for item in req.required_symbols}
    assert symbols["BTCUSDT"].effective_leverage == 3
    assert symbols["BTCUSDT"].leverage_source == "strategy_default"
    assert symbols["ETHUSDT"].effective_leverage == 0
    assert symbols["ETHUSDT"].leverage_source == ""


def test_proxy_portfolio_client_preflight_never_serializes_deprecated_global_leverage():
    runtime = _FakeRuntimeChannel()
    runtime.responses[PORTFOLIO_PREFLIGHT_STRATEGY_SESSION] = (
        portfolio_service_pb2.PreflightStrategySessionResponse(ok=True)
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    proxy.portfolio_client().preflight_strategy_session(
        portfolio_id=7,
        user_id=3,
        leverage=99,
    )

    _, request = runtime.calls[-1]
    assert request.leverage == 0


def test_proxy_portfolio_client_commit_strategy_session_start_preserves_typed_contract_and_deadline():
    runtime = _FakeRuntimeChannel()
    expected = portfolio_service_pb2.CommitStrategySessionStartResponse(
        ok=False,
        issues=[
            portfolio_service_pb2.PreflightIssue(
                code="LEVERAGE_SET_FAILED",
                message="exchange rejected leverage",
                exchange=1,
                market=2,
                symbol="BTCUSDT",
                venue_id=41,
                retryable=False,
                source="exchange",
            )
        ],
        confirmed_target_facts=[
            portfolio_service_pb2.SessionTargetLeverageFact(
                session_id="sess-proxy-commit-1",
                venue_id=41,
                exchange=1,
                environment=1,
                market=2,
                symbol="ETHUSDT",
                effective_leverage=3,
                leverage_source="order_target",
                previous_leverage=2,
                confirmed_leverage=3,
            )
        ],
        target_results=[
            portfolio_service_pb2.FuturesLeverageTargetResult(
                venue_id=41,
                exchange=1,
                market=2,
                symbol="BTCUSDT",
                effective_leverage=5,
                leverage_source="strategy_default",
                previous_leverage=2,
                current_leverage=2,
                confirmed_leverage=5,
                change_required=True,
                status="rolled_back",
                error_code="LEVERAGE_SET_FAILED",
                error_message="exchange rejected leverage",
                retryable=False,
            )
        ],
        rollback_failed=True,
        code="LEVERAGE_ROLLBACK_FAILED",
    )
    runtime.responses[PORTFOLIO_COMMIT_STRATEGY_SESSION_START] = expected
    request = portfolio_service_pb2.CommitStrategySessionStartRequest(
        launch_operation_id="launch-proxy-commit-1",
        session=portfolio_service_pb2.SaveSessionRequest(
            session_id="sess-proxy-commit-1",
            portfolio_id=7,
            strategy_id=9,
            environment=1,
            interval="5m",
            start_time_ms=1000,
            end_time_ms=2000,
            runtime_id="runtime-1",
            runtime_source="self_hosted",
            runtime_name="desk",
            session_type="live",
            runtime_version="v2",
            session_name="momentum",
            initial_status="pending",
            user_id=3,
        ),
        required_routes=[portfolio_service_pb2.RequiredRoute(exchange=1, market=2)],
        required_symbols=[
            portfolio_service_pb2.RequiredSymbol(
                exchange=1,
                market=2,
                symbol="BTCUSDT",
                order_target=True,
                required_order_types=["MARKET", "LIMIT"],
                effective_leverage=5,
                leverage_source="strategy_default",
            )
        ],
    )
    request_bytes = request.SerializeToString()
    proxy = RuntimeChannelPlatformProxy(runtime)

    response = proxy.portfolio_client().commit_strategy_session_start(
        request,
        timeout_seconds=19.25,
    )

    method, forwarded = runtime.calls[-1]
    assert method == "portfolio.CommitStrategySessionStart"
    assert forwarded.SerializeToString() == request_bytes
    assert runtime.timeouts[-1] == 19.25
    assert response.SerializeToString() == expected.SerializeToString()


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
            order_type="LIMIT",
            price="1999.5",
            time_in_force="GTD",
            post_only=True,
            good_till_date=datetime(2030, 1, 1, tzinfo=timezone.utc),
            reduce_only=True,
        ),
        2000,
        strategy_id=9,
        session_id="sess-1",
    )

    method, req = runtime.calls[-1]
    assert method == ORDER_PLACE
    assert req.portfolio_id == 7
    assert req.session_id == "sess-1"
    assert req.exchange == 1
    assert req.market == 2
    assert req.position_side == 0
    assert req.price == 1999.5
    assert req.order_type == "LIMIT"
    assert req.time_in_force == "GTD"
    assert req.post_only is True
    assert req.reduce_only is True
    assert req.HasField("good_till_date")
    assert req.good_till_date.ToDatetime(tzinfo=timezone.utc) == datetime(2030, 1, 1, tzinfo=timezone.utc)
    assert feedback.attempt_status == "ACCEPTED"
    assert feedback.order.order_id == "order-1"


def test_proxy_order_client_closes_spot_targets_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[ORDER_CLOSE_SPOT_TARGETS] = order_service_pb2.CloseSpotTargetsResponse(
        status="stopped", operation_id="stop-1"
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    response = proxy.order_client().close_spot_targets(
        user_id=3,
        portfolio_id=7,
        strategy_id=9,
        session_id="sess-1",
        operation_id="stop-1",
        targets=[SimpleNamespace(venue_id=10, exchange="binance", market="spot", symbol="btcusdt")],
    )

    method, req = runtime.calls[-1]
    assert method == ORDER_CLOSE_SPOT_TARGETS
    assert response.status == "stopped"
    assert req.user_id == 3
    assert req.operation_id == "stop-1"
    assert req.targets[0].symbol == "BTCUSDT"


def test_proxy_order_client_reads_lifecycle_events_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    runtime.responses[ORDER_LIST_LIFECYCLE_EVENTS] = (
        order_service_pb2.ListOrderLifecycleEventsResponse(events=[
            order_service_pb2.OrderLifecycleEventEntry(
                event_id=11,
                session_id="sess-1",
                portfolio_id=7,
                venue_id=10,
                exchange=1,
                market=1,
                side="BUY",
                event_type="fill",
                order_status="FILLED",
                order_id="order-1",
                fill_delta=order_service_pb2.FillDeltaEntry(
                    symbol="BTCUSDT",
                    qty=0.01,
                    fill_price=50_000,
                    fee=0.5,
                    fee_asset="USDT",
                ),
            ),
        ])
    )
    proxy = RuntimeChannelPlatformProxy(runtime)

    events = proxy.order_client().list_order_lifecycle_events(
        session_id="sess-1",
        after_event_id=10,
        limit=50,
        timeout_seconds=0.25,
    )

    method, req = runtime.calls[-1]
    assert method == ORDER_LIST_LIFECYCLE_EVENTS
    assert req.session_id == "sess-1"
    assert req.after_event_id == 10
    assert req.limit == 50
    assert len(events) == 1
    assert events[0].event_id == 11
    assert events[0].exchange == "binance"
    assert events[0].market == "spot"
    assert events[0].fill.qty == pytest.approx(0.01)
    assert runtime.timeouts[-1] == pytest.approx(0.25)


def test_proxy_order_lifecycle_read_fails_closed_when_runtime_channel_is_unavailable():
    runtime = _FakeRuntimeChannel()
    runtime.errors[ORDER_LIST_LIFECYCLE_EVENTS] = RuntimeError("unavailable")
    proxy = RuntimeChannelPlatformProxy(runtime)

    with pytest.raises(RuntimeError, match="unavailable"):
        proxy.order_client().list_order_lifecycle_events(session_id="sess-1")


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


def test_proxy_marketdata_client_fetches_backtest_page_over_runtime_channel():
    runtime = _FakeRuntimeChannel()
    from google.protobuf.struct_pb2 import Struct

    resp = Struct()
    resp.update({
        "stream_key": "binance/futures/kline/ETHUSDT/1s",
        "next_cursor_time_ms": 3000,
        "has_more": False,
        "limit": 8192,
        "klines": [{
            "exchange": "binance",
            "market": "futures",
            "symbol": "ETHUSDT",
            "interval": "1s",
            "open_time": 1000,
            "close_time": 1999,
            "timestamp": 1000,
            "open": 1.0,
            "high": 1.1,
            "low": 0.9,
            "close": 1.0,
            "volume": 2.0,
        }],
    })
    runtime.responses[MARKETDATA_FETCH_BACKTEST_PAGE] = resp
    proxy = RuntimeChannelPlatformProxy(runtime)

    page = proxy.marketdata_client().fetch_backtest_page(
        exchange="binance",
        market="futures",
        kind="kline",
        symbol="ETHUSDT",
        interval="1s",
        start_after_time_ms=0,
        end_time_ms=10_000,
    )

    method, req = runtime.calls[-1]
    assert method == MARKETDATA_FETCH_BACKTEST_PAGE
    assert int(req["limit"]) == 8192
    assert req["start_after_time_ms"] == 0
    assert page.stream_key == "binance/futures/kline/ETHUSDT/1s"
    assert page.next_cursor_time_ms == 3000
    assert page.has_more is False
    assert len(page.klines) == 1
    assert page.klines[0].open_time == 1000
    assert page.klines[0].market == "futures"


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
        name="strategy_service.worker_agent_client",
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
        self.timeouts = []
        self.errors = {}
        self.responses = {
            PORTFOLIO_SAVE_SESSION: portfolio_service_pb2.SaveSessionResponse(),
            ORDER_PLACE: order_service_pb2.PlaceOrderResponse(),
            MARKETDATA_GET_STATUS: marketdata_service_pb2.GetMarketDataStreamStatusResponse(),
        }

    def invoke_platform_unary(self, method, request, response_type, *, timeout_seconds=30.0):
        self.timeouts.append(timeout_seconds)
        self.calls.append((method, request))
        if method in self.errors:
            raise self.errors[method]
        resp = self.responses.get(method)
        if resp is None:
            return response_type()
        return resp
