from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import grpc
import pytest
from google.protobuf.timestamp_pb2 import Timestamp

from strategy_service.gen import order_service_pb2
from strategy_service.order_client import OrderClient, canonical_decimal_text
from strategy_service.types import OrderDecision, OrderUpdateEvent, OrderUpdateFill
from strategy_service.worker_agent_client import WorkerPlatformCallError


def test_canonical_decimal_text_normalizes_signed_zero() -> None:
    assert canonical_decimal_text("-0.000") == "0.000"


@pytest.mark.parametrize(
    "value",
    [
        "",
        "-1",
        "NaN",
        "Infinity",
        "123456789012345678901",
        "0.1234567890123456789",
    ],
)
def test_canonical_decimal_text_rejects_invalid_protocol_values(value) -> None:
    with pytest.raises(ValueError):
        canonical_decimal_text(value)


class _Stub:
    def __init__(self, response: order_service_pb2.PlaceOrderResponse) -> None:
        self.response = response
        self.last_request: order_service_pb2.PlaceOrderRequest | None = None
        self.place_calls = 0
        self.resolve_response: order_service_pb2.ResolveOrderAttemptResponse | None = None
        self.raise_on_place: Exception | None = None
        self.raise_on_resolve: Exception | None = None
        self.last_resolve_request: order_service_pb2.ResolveOrderAttemptRequest | None = None
        self.resolve_calls = 0
        self.lifecycle_response: order_service_pb2.ListOrderLifecycleEventsResponse | None = None
        self.last_lifecycle_request: order_service_pb2.ListOrderLifecycleEventsRequest | None = None
        self.last_lifecycle_timeout: float | None = None
        self.close_response: order_service_pb2.CloseSpotTargetsResponse | None = None
        self.last_close_request: order_service_pb2.CloseSpotTargetsRequest | None = None

    def PlaceOrder(self, request: order_service_pb2.PlaceOrderRequest):
        self.place_calls += 1
        self.last_request = request
        if self.raise_on_place is not None:
            raise self.raise_on_place
        return self.response

    def ResolveOrderAttempt(self, request: order_service_pb2.ResolveOrderAttemptRequest):
        self.resolve_calls += 1
        self.last_resolve_request = request
        if self.raise_on_resolve is not None:
            raise self.raise_on_resolve
        if self.resolve_response is None:
            raise RuntimeError("resolve response not configured")
        return self.resolve_response

    def ListOrderLifecycleEvents(
        self,
        request: order_service_pb2.ListOrderLifecycleEventsRequest,
        *,
        timeout: float | None = None,
    ):
        self.last_lifecycle_request = request
        self.last_lifecycle_timeout = timeout
        if self.lifecycle_response is None:
            return order_service_pb2.ListOrderLifecycleEventsResponse()
        return self.lifecycle_response

    def CloseSpotTargets(self, request: order_service_pb2.CloseSpotTargetsRequest):
        self.last_close_request = request
        if self.close_response is None:
            raise RuntimeError("close response not configured")
        return self.close_response


class _TransportError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode) -> None:
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code

    def details(self) -> str:
        return "order transport failed"


def _assert_order_error_detail(
    detail_json: str,
    *,
    intent_id: str,
    stage: str,
    cause: str,
    transport_code: str,
    transport_cause: str | None = None,
) -> None:
    detail = json.loads(detail_json)
    assert detail["intent_id"] == intent_id
    assert detail["symbol"] == "ETHUSDT"
    assert detail["venue"] == {"portfolio_id": 13, "exchange": 1, "market": 2}
    assert detail["stage"] == stage
    assert detail["cause"] == cause
    assert detail["transport_code"] == transport_code
    assert detail["transport_cause"] == (
        transport_cause if transport_cause is not None else f"_TransportError:{transport_code}"
    )


def test_close_spot_targets_preserves_operation_and_canonical_routes():
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.close_response = order_service_pb2.CloseSpotTargetsResponse(
        status="stopped", operation_id="stop-1"
    )
    client._stub = stub

    response = client.close_spot_targets(
        user_id=7,
        portfolio_id=8,
        strategy_id=9,
        session_id="session-1",
        operation_id="stop-1",
        targets=[
            {
                "venue_id": 10,
                "exchange": "binance",
                "market": "spot",
                "symbol": "btcusdt",
            }
        ],
    )

    assert response.status == "stopped"
    assert stub.last_close_request is not None
    assert stub.last_close_request.operation_id == "stop-1"
    assert stub.last_close_request.targets[0].symbol == "BTCUSDT"
    assert stub.last_close_request.targets[0].exchange == 1
    assert stub.last_close_request.targets[0].market == 1


def _decision(
    *,
    exchange: str = "binance",
    market: str = "perpetual_futures",
    symbol: str = "ethusdt",
    side: str = "BUY",
    qty: str = "0.05",
    order_type: str = "MARKET",
    price: str | None = None,
    position_side: str | None = None,
    time_in_force: str | None = None,
    post_only: bool = False,
    good_till_date: object | None = None,
    reduce_only: bool = False,
) -> OrderDecision:
    return OrderDecision(
        exchange=exchange,
        market=market,
        symbol=symbol,
        side=side,
        qty=qty,
        order_type=order_type,
        price=price,
        position_side=position_side,
        time_in_force=time_in_force,
        post_only=post_only,
        good_till_date=good_till_date,
        reduce_only=reduce_only,
    )


@pytest.mark.parametrize(
    "transport_code",
    [
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.NOT_FOUND,
    ],
)
def test_place_order_rejects_pre_persistence_request_without_resolving(transport_code):
    """A code-branch change to resolve deterministic rejects must fail this test."""
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.raise_on_place = _TransportError(transport_code)
    client._stub = stub

    with pytest.raises(WorkerPlatformCallError) as raised:
        client.place_order(
            13,
            _decision(),
            51000.0,
            portfolio_symbol="ETHUSDT",
            market="perpetual_futures",
            intent_id="intent-rejected",
        )

    assert raised.value.code == "ORDER_REQUEST_REJECTED"
    _assert_order_error_detail(
        raised.value.detail_json,
        intent_id="intent-rejected",
        stage="place_order",
        cause="place_order transport failure",
        transport_code=transport_code.name,
    )
    assert stub.place_calls == 1
    assert stub.resolve_calls == 0


@pytest.mark.parametrize(
    "transport_code",
    [
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.UNKNOWN,
    ],
)
def test_place_order_unknown_outcome_resolves_once_and_fails_when_no_attempt_exists(transport_code):
    """Removing resolve or accepting an absent attempt must fail this test."""
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.raise_on_place = _TransportError(transport_code)
    stub.resolve_response = order_service_pb2.ResolveOrderAttemptResponse(
        intent_id="intent-unknown",
        attempt_status="FAILED",
        error_message="attempt not found; no local execution record exists",
    )
    client._stub = stub

    with pytest.raises(WorkerPlatformCallError) as raised:
        client.place_order(
            13,
            _decision(),
            51000.0,
            portfolio_symbol="ETHUSDT",
            market="perpetual_futures",
            intent_id="intent-unknown",
        )

    assert raised.value.code == "ORDER_EXECUTION_UNKNOWN"
    _assert_order_error_detail(
        raised.value.detail_json,
        intent_id="intent-unknown",
        stage="resolve_order_attempt",
        cause="no persisted order attempt was found",
        transport_code=transport_code.name,
    )
    assert stub.place_calls == 1
    assert stub.resolve_calls == 1


def test_place_order_returns_persisted_failed_attempt_after_uncertain_transport():
    """Treating persisted FAILED as fatal must fail this test."""
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.raise_on_place = _TransportError(grpc.StatusCode.UNAVAILABLE)
    stub.resolve_response = order_service_pb2.ResolveOrderAttemptResponse(
        intent_id="intent-persisted-failed",
        attempt_id="attempt-persisted-failed",
        attempt_status="FAILED",
        error_message="venue rejected order",
    )
    client._stub = stub

    feedback = client.place_order(
        13,
        _decision(),
        51000.0,
        portfolio_symbol="ETHUSDT",
        market="perpetual_futures",
        intent_id="intent-persisted-failed",
    )

    assert feedback.attempt_id == "attempt-persisted-failed"
    assert feedback.attempt_status == "FAILED"
    assert stub.place_calls == 1
    assert stub.resolve_calls == 1


def test_place_order_fails_unknown_when_resolution_cannot_establish_attempt():
    """Swallowing resolution transport failures must fail this test."""
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.raise_on_place = _TransportError(grpc.StatusCode.UNAVAILABLE)
    stub.raise_on_resolve = _TransportError(grpc.StatusCode.UNAVAILABLE)
    client._stub = stub

    with pytest.raises(WorkerPlatformCallError) as raised:
        client.place_order(
            13,
            _decision(),
            51000.0,
            portfolio_symbol="ETHUSDT",
            market="perpetual_futures",
            intent_id="intent-resolution-failed",
        )

    assert raised.value.code == "ORDER_EXECUTION_UNKNOWN"
    _assert_order_error_detail(
        raised.value.detail_json,
        intent_id="intent-resolution-failed",
        stage="resolve_order_attempt",
        cause="resolve_order_attempt transport failure",
        transport_code="UNAVAILABLE",
    )
    detail = json.loads(raised.value.detail_json)
    assert detail["resolution_cause"] == "_TransportError:UNAVAILABLE"
    assert detail["resolution_code"] == "UNAVAILABLE"
    assert stub.place_calls == 1
    assert stub.resolve_calls == 1


@pytest.mark.parametrize("attempt_id", ["", "   "])
def test_place_order_success_without_persisted_attempt_identity_is_fatal(attempt_id):
    """Returning feedback for a response without an attempt identity must fail."""
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse(
        intent_id="intent-success-without-attempt",
        attempt_id=attempt_id,
        attempt_status="ACCEPTED",
    ))
    client._stub = stub

    with pytest.raises(WorkerPlatformCallError) as raised:
        client.place_order(
            13,
            _decision(),
            51000.0,
            portfolio_symbol="ETHUSDT",
            market="perpetual_futures",
            intent_id="intent-success-without-attempt",
        )

    assert raised.value.code == "ORDER_EXECUTION_UNKNOWN"
    _assert_order_error_detail(
        raised.value.detail_json,
        intent_id="intent-success-without-attempt",
        stage="place_order",
        cause="response missing persisted order attempt identity",
        transport_code="",
        transport_cause="successful_response",
    )
    assert stub.place_calls == 1
    assert stub.resolve_calls == 0


def test_place_order_uses_canonical_symbol_and_emits_fill_events():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-1",
        attempt_id="attempt-1",
        attempt_status="ACCEPTED",
        order=order_service_pb2.ExchangeOrderEntry(
            order_id="order-1",
            exchange_order_id="ex-1",
            symbol="ETHUSDT",
            side="BUY",
            orig_qty_decimal="0.05",
            executed_qty_decimal="0.05",
            remaining_qty_decimal="0",
            avg_price_decimal="51200",
            cumulative_quote_qty_decimal="2560",
            status="FILLED",
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                order_id="order-1",
                qty_decimal="0.02",
                fill_price_decimal="50000",
                fee_decimal="0.1",
                quote_qty_decimal="1000",
            ),
            order_service_pb2.OrderFillEntry(
                order_id="order-1",
                qty_decimal="0.03",
                fill_price_decimal="52000",
                fee_decimal="0.2",
                quote_qty_decimal="1560",
            ),
        ],
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub

    feedback = client.place_order(
        13,
        _decision(),
        51000.0,
        portfolio_symbol="ETHUSDT",
        market="perpetual_futures",
        intent_id="intent-1",
    )

    assert stub.last_request is not None
    assert stub.last_request.symbol == "ETHUSDT"
    assert stub.last_request.exchange == 1
    assert stub.last_request.market == 2
    assert stub.last_request.position_side == 0
    assert stub.last_request.order_type == "MARKET"
    assert feedback.attempt_status == "ACCEPTED"
    assert feedback.fill_count == 2
    assert feedback.delta_qty == 0.05
    assert len(feedback.fill_events) == 2
    assert feedback.fill_events[0].qty == 0.02
    assert feedback.fill_events[0].status == "PARTIALLY_FILLED"
    assert feedback.fill_events[0].executed_qty == 0.02
    assert feedback.fill_events[0].remaining_qty == pytest.approx(0.03)
    assert feedback.fill_events[1].qty == 0.03
    assert feedback.fill_events[1].status == "FILLED"
    assert feedback.fill_events[1].executed_qty == pytest.approx(0.05)
    assert feedback.fill_events[1].remaining_qty == pytest.approx(0.0)


def test_place_order_exact_fill_carries_funding_identity_and_market_time():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-funding",
        attempt_id="attempt-funding",
        attempt_status="ACCEPTED",
        order=order_service_pb2.ExchangeOrderEntry(
            order_id="order-funding",
            exchange_order_id="exchange-order-funding",
            symbol="ETHUSDT",
            side="BUY",
            orig_qty_decimal="0.001",
            executed_qty_decimal="0.001",
            remaining_qty_decimal="0",
            avg_price_decimal="2500",
            cumulative_quote_qty_decimal="2.5",
            status="FILLED",
            venue_id=10,
            exchange=1,
            market=2,
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                order_id="order-funding",
                exchange_order_id="exchange-order-funding",
                exchange_trade_id="trade-funding",
                qty_decimal="0.001",
                fill_price_decimal="2500",
                fee_decimal="0.001",
                quote_qty_decimal="2.5",
                venue_id=10,
                exchange=1,
                market=2,
                time=Timestamp(seconds=1_735_689_899, nanos=999_000_000),
            )
        ],
    )
    client = OrderClient("")
    client._stub = _Stub(response)

    feedback = client.place_order(
        13,
        _decision(),
        2500.0,
        portfolio_symbol="ETHUSDT",
        market="perpetual_futures",
        intent_id="intent-funding",
    )

    assert len(feedback.fill_events) == 1
    fill = feedback.fill_events[0]
    assert fill.event_type == "fill"
    assert fill.exchange_trade_id == "trade-funding"
    assert fill.trade_time == (1_735_689_899, 999_000_000)


def test_place_order_ioc_partial_expired_emits_terminal_fill_event():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-ioc",
        attempt_id="attempt-ioc",
        attempt_status="ACCEPTED",
        order=order_service_pb2.ExchangeOrderEntry(
            order_id="order-ioc",
            exchange_order_id="ex-ioc",
            symbol="ETHUSDT",
            side="BUY",
            orig_qty_decimal="0.02",
            executed_qty_decimal="0.004",
            remaining_qty_decimal="0.016",
            avg_price_decimal="2500",
            price_decimal="2500",
            cumulative_quote_qty_decimal="10",
            status="EXPIRED",
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                order_id="order-ioc",
                qty_decimal="0.004",
                fill_price_decimal="2500",
                fee_decimal="0.01",
                quote_qty_decimal="10",
            ),
        ],
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub

    feedback = client.place_order(
        13,
        _decision(order_type="LIMIT", price="2500", time_in_force="IOC"),
        2500.0,
        portfolio_symbol="ETHUSDT",
        market="perpetual_futures",
        intent_id="intent-ioc",
    )

    assert stub.last_request is not None
    assert stub.last_request.order_type == "LIMIT"
    assert stub.last_request.time_in_force == "IOC"
    assert feedback.attempt_status == "ACCEPTED"
    assert feedback.order is not None
    assert feedback.order.status == "EXPIRED"
    assert feedback.fill_count == 1
    assert feedback.delta_qty == pytest.approx(0.004)
    assert len(feedback.fill_events) == 1
    assert feedback.fill_events[0].status == "EXPIRED"
    assert feedback.fill_events[0].qty == pytest.approx(0.004)
    assert feedback.fill_events[0].executed_qty == pytest.approx(0.004)
    assert feedback.fill_events[0].remaining_qty == pytest.approx(0.016)


def test_place_order_passes_market_time_to_order_service():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-1",
        attempt_id="attempt-1",
        attempt_status="ACCEPTED",
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub
    market_time = datetime(2026, 6, 1, 0, 43, tzinfo=timezone.utc)

    client.place_order(
        13,
        _decision(),
        51000.0,
        portfolio_symbol="ETHUSDT",
        market="perpetual_futures",
        intent_id="intent-1",
        market_time=market_time,
    )

    assert stub.last_request is not None
    assert stub.last_request.HasField("market_time")
    assert stub.last_request.market_time.ToDatetime(tzinfo=timezone.utc) == market_time


def test_place_order_exception_triggers_resolve_unknown_attempt():
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.raise_on_place = RuntimeError("deadline exceeded")
    stub.resolve_response = order_service_pb2.ResolveOrderAttemptResponse(
        intent_id="intent-2",
        attempt_id="attempt-2",
        attempt_status="RECOVERED",
        order=order_service_pb2.ExchangeOrderEntry(
            order_id="order-2",
            exchange_order_id="ex-2",
            symbol="ETHUSDT",
            side="BUY",
            orig_qty_decimal="0.05",
            executed_qty_decimal="0.05",
            remaining_qty_decimal="0",
            avg_price_decimal="51000",
            cumulative_quote_qty_decimal="2550",
            status="FILLED",
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                order_id="order-2",
                qty_decimal="0.05",
                fill_price_decimal="51000",
                fee_decimal="0.2",
                quote_qty_decimal="2550",
            ),
        ],
        client_order_id="coid-2",
    )
    client._stub = stub

    feedback = client.place_order(
        13,
        _decision(),
        51000.0,
        portfolio_symbol="ETHUSDT",
        market="perpetual_futures",
        intent_id="intent-2",
    )

    assert stub.last_resolve_request is not None
    assert stub.last_resolve_request.intent_id == "intent-2"
    assert feedback.attempt_status == "RECOVERED"
    assert feedback.order is not None
    assert feedback.order.order_id == "order-2"


def test_fee_missing_fill_is_not_wallet_settleable():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-3",
        attempt_id="attempt-3",
        attempt_status="ACCEPTED",
        order=order_service_pb2.ExchangeOrderEntry(
            order_id="order-3",
            exchange_order_id="ex-3",
            symbol="ETHUSDT",
            side="BUY",
            orig_qty_decimal="0.05",
            executed_qty_decimal="0.05",
            remaining_qty_decimal="0",
            avg_price_decimal="51000",
            cumulative_quote_qty_decimal="2550",
            status="FILLED",
            error_message="binance trade fee data not available after confirmed execution",
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                order_id="order-3",
                qty_decimal="0.05",
                fill_price_decimal="51000",
                fee_decimal="0",
                quote_qty_decimal="2550",
                status="FEE_MISSING",
            ),
        ],
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub

    feedback = client.place_order(
        13,
        _decision(),
        51000.0,
        portfolio_symbol="ETHUSDT",
        market="perpetual_futures",
        intent_id="intent-3",
    )

    assert feedback.attempt_status == "ACCEPTED"
    assert feedback.order is not None
    assert feedback.fill_count == 0
    assert feedback.delta_qty == 0.0
    assert feedback.fill_events == []


def test_place_order_sends_explicit_venue_route_fields():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-4",
        attempt_id="attempt-4",
        attempt_status="ACCEPTED",
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub

    feedback = client.place_order(
        13,
        _decision(
            exchange="okx",
            market="delivery_futures",
            symbol="ETHUSDT",
            side="SELL",
            position_side="SHORT",
        ),
        51000.0,
        portfolio_symbol="ETHUSDT",
        market="delivery_futures",
        intent_id="intent-4",
    )

    assert stub.last_request is not None
    assert stub.last_request.exchange == 2
    assert stub.last_request.market == 3
    assert stub.last_request.position_side == 2
    assert feedback.attempt_status == "ACCEPTED"


def test_place_order_sends_advanced_order_contract_fields():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-limit",
        attempt_id="attempt-limit",
        attempt_status="ACCEPTED",
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub
    good_till_date = datetime(2030, 1, 1, tzinfo=timezone.utc)

    client.place_order(
        13,
        _decision(
            symbol="ETHUSDT",
            side="BUY",
            qty="0.05",
            price="50000",
            order_type="LIMIT",
            time_in_force="GTD",
            post_only=True,
            good_till_date=good_till_date,
            reduce_only=True,
        ),
        51000.0,
        portfolio_symbol="ETHUSDT",
        market="perpetual_futures",
        intent_id="intent-limit",
    )

    assert stub.last_request is not None
    assert "price" not in stub.last_request.DESCRIPTOR.fields_by_name
    assert stub.last_request.price_decimal == "50000"
    assert stub.last_request.order_type == "LIMIT"
    assert stub.last_request.time_in_force == "GTD"
    assert stub.last_request.post_only is True
    assert stub.last_request.reduce_only is True
    assert stub.last_request.HasField("good_till_date")
    assert stub.last_request.good_till_date.ToDatetime(tzinfo=timezone.utc) == good_till_date


def test_spot_place_order_preserves_exact_request_fill_fee_and_trade_identity():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-spot",
        attempt_id="attempt-spot",
        attempt_status="ACCEPTED",
        order=order_service_pb2.ExchangeOrderEntry(
            order_id="order-spot",
            exchange_order_id="42",
            venue_id=10,
            exchange=1,
            market=1,
            symbol="BTCUSDT",
            side="BUY",
            status="FILLED",
            orig_qty_decimal="0.01000000",
            executed_qty_decimal="0.01000000",
            remaining_qty_decimal="0.00000000",
            avg_price_decimal="50000.10",
            cumulative_quote_qty_decimal="500.00100000",
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                order_id="order-spot",
                exchange_order_id="42",
                exchange_trade_id="0",
                venue_id=10,
                exchange=1,
                market=1,
                symbol="BTCUSDT",
                side="BUY",
                fee_asset="BNB",
                qty_decimal="0.01000000",
                fill_price_decimal="50000.10",
                fee_decimal="0.00020000",
                quote_qty_decimal="500.00100000",
            )
        ],
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub

    feedback = client.place_order(
        13,
        _decision(
            market="spot",
            symbol="BTCUSDT",
            qty="0.01000000",
            order_type="LIMIT",
            price="50000.10",
        ),
        50000.125,
        portfolio_symbol="BTCUSDT",
        market="spot",
        intent_id="intent-spot",
    )

    assert stub.last_request is not None
    assert stub.last_request.qty_decimal == "0.01000000"
    assert stub.last_request.price_decimal == "50000.10"
    assert stub.last_request.mark_price_decimal == "50000.125"
    assert feedback.order is not None
    assert feedback.order.venue_id == 10
    assert feedback.order.exchange_order_id == "42"
    assert feedback.order.executed_qty_decimal == "0.01000000"
    assert feedback.order.cumulative_quote_qty_decimal == "500.00100000"
    assert len(feedback.fill_events) == 1
    fill = feedback.fill_events[0]
    assert fill.exchange_trade_id == "0"
    assert fill.fee_asset == "BNB"
    assert fill.qty_decimal == "0.01000000"
    assert fill.fill_price_decimal == "50000.10"
    assert fill.fee_decimal == "0.00020000"
    assert fill.quote_qty_decimal == "500.00100000"


def test_place_order_preserves_structured_error_without_message_inference():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-error",
        attempt_id="attempt-error",
        attempt_status="FAILED",
        error_message="opaque",
        error=order_service_pb2.OrderErrorDetail(
            code="SPOT_MIN_NOTIONAL",
            message="below minimum",
            venue_id=10,
            exchange=1,
            market=1,
            symbol="BTCUSDT",
            environment=1,
            retryable=False,
            source="risk_gate",
        ),
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub

    feedback = client.place_order(
        13,
        _decision(market="spot", symbol="BTCUSDT"),
        50000.0,
        portfolio_symbol="BTCUSDT",
        market="spot",
        intent_id="intent-error",
    )

    assert feedback.error_code == "SPOT_MIN_NOTIONAL"
    assert feedback.error_environment == 1
    assert feedback.error_retryable is False
    assert feedback.error_source == "risk_gate"
    assert feedback.error_venue_id == 10


def test_exchange_and_market_codes_do_not_default_missing_route() -> None:
    with pytest.raises(ValueError, match="unsupported exchange"):
        OrderClient._exchange_code(None)
    with pytest.raises(ValueError, match="unsupported market"):
        OrderClient._market_code(None)


def test_place_order_without_order_service_fails_fast() -> None:
    client = OrderClient("")

    with pytest.raises(RuntimeError, match="order.v1 gRPC client is not configured"):
        client.place_order(
            13,
            _decision(),
            51000.0,
            portfolio_symbol="ETHUSDT",
            market="perpetual_futures",
            intent_id="intent-missing-client",
        )


def test_order_response_from_update_rejects_missing_market_route() -> None:
    event = OrderUpdateEvent(
        event_id=1,
        session_id="session-1",
        portfolio_id=13,
        venue_id=20,
        exchange="binance",
        market="",
        position_side="BOTH",
        side="BUY",
        event_type="fill",
        order_status="FILLED",
        order_id="order-1",
        fill=OrderUpdateFill(symbol="ETHUSDT", qty=0.1, fill_price=3000.0),
    )

    with pytest.raises(ValueError, match="unsupported market"):
        OrderClient.order_response_from_update(event)


def test_public_order_update_event_from_proto_converts_lifecycle_entry() -> None:
    item = order_service_pb2.OrderLifecycleEventEntry(
        event_id=101,
        session_id="sess-1",
        portfolio_id=7,
        venue_id=20,
        exchange=1,
        market=2,
        position_side=0,
        side="BUY",
        event_type="fill",
        order_status="FILLED",
        event_source="websocket",
        order_id="order-1",
        exchange_order_id="1001",
        exchange_trade_id="0",
        occurred_at={"seconds": 100, "nanos": 7},
        fill_delta=order_service_pb2.FillDeltaEntry(
            symbol="ETHUSDT",
            fee_asset="USDT",
            exchange_trade_id="0",
            exchange_order_id="1001",
            qty_decimal="0.10000000",
            fill_price_decimal="2500.00",
            fee_decimal="0.02000000",
            quote_qty_decimal="250.00000000",
            trade_time={"seconds": 99, "nanos": 3},
        ),
        order_state=order_service_pb2.OrderStateEntry(
            symbol="ETHUSDT",
            status="FILLED",
            orig_qty_decimal="0.10000000",
            executed_qty_decimal="0.10000000",
            remaining_qty_decimal="0.00000000",
            avg_price_decimal="2500.00",
            cumulative_quote_qty_decimal="250.00000000",
        ),
    )

    event = OrderClient.order_update_event_from_proto(item)

    assert event.event_id == 101
    assert event.exchange == "binance"
    assert event.market == "perpetual_futures"
    assert event.position_side == "BOTH"
    assert event.symbol == "ETHUSDT"
    assert event.event_type == "fill"
    assert event.occurred_at == (100, 7)
    assert event.fill is not None
    assert event.fill.qty == pytest.approx(0.1)
    assert event.fill.fee == pytest.approx(0.02)
    assert event.fill.trade_time == (99, 3)
    order_response = OrderClient.order_response_from_update(event)
    assert order_response is not None
    assert order_response.exchange_trade_id == "0"
    assert order_response.exchange_order_id == "1001"
    assert order_response.qty_decimal == "0.10000000"
    assert order_response.fee_decimal == "0.02000000"
    assert order_response.quote_qty_decimal == "250.00000000"
    assert order_response.executed_qty_decimal == "0.10000000"
    assert order_response.cumulative_quote_qty_decimal == "250.00000000"


def test_order_lifecycle_conversion_rejects_boolean_position_side_without_coercion() -> None:
    item = SimpleNamespace(
        event_id=101,
        session_id="sess-1",
        portfolio_id=7,
        venue_id=20,
        exchange=1,
        market=2,
        position_side=True,
        side="BUY",
        event_type="fill",
        order_status="FILLED",
        HasField=lambda _field: False,
    )

    with pytest.raises(ValueError, match="invalid FuturesPositionSide"):
        OrderClient.order_update_event_from_proto(item)


def test_order_fill_conversion_rejects_boolean_position_side_without_coercion() -> None:
    order = SimpleNamespace(
        side="BUY",
        status="FILLED",
        order_id="order-1",
        price_decimal="",
        orig_qty_decimal="1",
    )
    fill = SimpleNamespace(
        status="",
        qty_decimal="1",
        fill_price_decimal="100",
        quote_qty_decimal="100",
        fee_decimal="0",
        position_side=True,
        HasField=lambda _field: False,
    )

    with pytest.raises(ValueError, match="invalid FuturesPositionSide"):
        OrderClient._build_fill_events(
            order,
            [fill],
            fallback_side="BUY",
            market="perpetual_futures",
            symbol="BTCUSDT",
        )


def test_lifecycle_mapping_rejects_missing_canonical_time() -> None:
    item = order_service_pb2.OrderLifecycleEventEntry(
        event_id=1,
        venue_id=11,
        exchange=1,
        market=2,
        side="BUY",
        position_side=0,
        event_type="fill",
        order_status="FILLED",
        order_id="order-1",
        fill_delta=order_service_pb2.FillDeltaEntry(
            symbol="BTCUSDT",
            exchange_trade_id="trade-1",
            qty_decimal="1",
            fill_price_decimal="1",
            fee_decimal="0",
            quote_qty_decimal="1",
            trade_time={"seconds": 10},
        ),
        order_state=order_service_pb2.OrderStateEntry(
            symbol="BTCUSDT",
            orig_qty_decimal="1",
            executed_qty_decimal="1",
            remaining_qty_decimal="0",
            avg_price_decimal="1",
            cumulative_quote_qty_decimal="1",
        ),
    )

    with pytest.raises(ValueError, match="occurred_at"):
        OrderClient.order_update_event_from_proto(item)


def test_list_order_lifecycle_events_maps_route_facts_and_fill():
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.lifecycle_response = order_service_pb2.ListOrderLifecycleEventsResponse(
        events=[
            order_service_pb2.OrderLifecycleEventEntry(
                event_id=11,
                session_id="session-1",
                portfolio_id=13,
                venue_id=20,
                exchange=1,
                market=2,
                position_side=0,
                side="BUY",
                event_type="fill",
                event_source="force_close",
                order_status="FILLED",
                order_id="order-1",
                occurred_at={"seconds": 101},
                fill_delta=order_service_pb2.FillDeltaEntry(
                    symbol="ETHUSDT",
                    fee_asset="USDT",
                    exchange_trade_id="trade-1",
                    qty_decimal="0.1",
                    fill_price_decimal="3000",
                    fee_decimal="0.2",
                    quote_qty_decimal="300",
                    trade_time={"seconds": 100},
                ),
                order_state=order_service_pb2.OrderStateEntry(
                    symbol="ETHUSDT",
                    status="FILLED",
                    orig_qty_decimal="0.1",
                    executed_qty_decimal="0.1",
                    remaining_qty_decimal="0",
                    avg_price_decimal="3000",
                    cumulative_quote_qty_decimal="300",
                ),
            )
        ]
    )
    client._stub = stub

    events = client.list_order_lifecycle_events(session_id="session-1", after_event_id=10, limit=50)

    assert stub.last_lifecycle_request is not None
    assert stub.last_lifecycle_request.after_event_id == 10
    assert len(events) == 1
    event = events[0]
    assert event.event_id == 11
    assert event.exchange == "binance"
    assert event.market == "perpetual_futures"
    assert event.position_side == "BOTH"
    assert event.side == "BUY"
    assert event.event_source == "force_close"
    assert event.symbol == "ETHUSDT"
    assert event.fill is not None
    assert event.fill.qty == pytest.approx(0.1)
    order_response = OrderClient.order_response_from_update(event)
    assert order_response is not None
    assert order_response.qty == pytest.approx(0.1)


def test_list_order_lifecycle_events_forwards_the_call_deadline():
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    client._stub = stub

    client.list_order_lifecycle_events(
        session_id="session-1",
        timeout_seconds=0.25,
    )

    assert stub.last_lifecycle_timeout == pytest.approx(0.25)


def test_list_order_lifecycle_events_fails_closed_without_a_configured_stub():
    client = OrderClient("")

    with pytest.raises(RuntimeError, match="not configured"):
        client.list_order_lifecycle_events(session_id="session-1")


def test_order_response_from_update_uses_lifecycle_order_state():
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.lifecycle_response = order_service_pb2.ListOrderLifecycleEventsResponse(
        events=[
            order_service_pb2.OrderLifecycleEventEntry(
                event_id=12,
                session_id="session-1",
                portfolio_id=13,
                venue_id=20,
                exchange=1,
                market=2,
                position_side=0,
                side="BUY",
                event_type="fill",
                order_status="PARTIALLY_FILLED",
                order_id="order-1",
                occurred_at={"seconds": 201},
                fill_delta=order_service_pb2.FillDeltaEntry(
                    symbol="ETHUSDT",
                    qty_decimal="0.02",
                    fill_price_decimal="3000",
                    fee_decimal="0.2",
                    quote_qty_decimal="60",
                    trade_time={"seconds": 200},
                ),
                order_state=order_service_pb2.OrderStateEntry(
                    symbol="ETHUSDT",
                    status="PARTIALLY_FILLED",
                    orig_qty_decimal="0.05",
                    executed_qty_decimal="0.02",
                    remaining_qty_decimal="0.03",
                    avg_price_decimal="3000",
                    cumulative_quote_qty_decimal="60",
                ),
            )
        ]
    )
    client._stub = stub

    event = client.list_order_lifecycle_events(session_id="session-1")[0]
    order_response = OrderClient.order_response_from_update(event)

    assert order_response is not None
    assert order_response.qty == pytest.approx(0.02)
    assert order_response.orig_qty == pytest.approx(0.05)
    assert order_response.executed_qty == pytest.approx(0.02)
    assert order_response.remaining_qty == pytest.approx(0.03)
    assert order_response.status == "PARTIALLY_FILLED"


def test_wallet_qty_uses_market_enum_aliases():
    assert OrderClient._wallet_qty(1, "SELL", "spot") == 1
    assert OrderClient._wallet_qty(1, "SELL", "futures") == -1
    assert OrderClient._wallet_qty(1, "SELL", "perpetual_futures") == -1
    assert OrderClient._wallet_qty(1, "SELL", "delivery_futures") == -1
