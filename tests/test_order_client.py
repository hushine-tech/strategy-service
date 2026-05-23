from __future__ import annotations

import pytest

from strategy_service.gen import order_service_pb2
from strategy_service.order_client import OrderClient
from strategy_service.types import OrderDecision


class _Stub:
    def __init__(self, response: order_service_pb2.PlaceOrderResponse) -> None:
        self.response = response
        self.last_request: order_service_pb2.PlaceOrderRequest | None = None
        self.resolve_response: order_service_pb2.ResolveOrderAttemptResponse | None = None
        self.raise_on_place: Exception | None = None
        self.last_resolve_request: order_service_pb2.ResolveOrderAttemptRequest | None = None

    def PlaceOrder(self, request: order_service_pb2.PlaceOrderRequest):
        if self.raise_on_place is not None:
            raise self.raise_on_place
        self.last_request = request
        return self.response

    def ResolveOrderAttempt(self, request: order_service_pb2.ResolveOrderAttemptRequest):
        self.last_resolve_request = request
        if self.resolve_response is None:
            raise RuntimeError("resolve response not configured")
        return self.resolve_response


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
            orig_qty=0.05,
            executed_qty=0.05,
            remaining_qty=0.0,
            avg_price=51200.0,
            status="FILLED",
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                order_id="order-1",
                qty=0.02,
                fill_price=50000.0,
                fee=0.1,
            ),
            order_service_pb2.OrderFillEntry(
                order_id="order-1",
                qty=0.03,
                fill_price=52000.0,
                fee=0.2,
            ),
        ],
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub

    feedback = client.place_order(
        13,
        OrderDecision(symbol="ethusdt", side="LONG", qty=0.05),
        51000.0,
        account_symbol="ETHUSDT",
        market="futures",
        intent_id="intent-1",
    )

    assert stub.last_request is not None
    assert stub.last_request.symbol == "ETHUSDT"
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
            orig_qty=0.05,
            executed_qty=0.05,
            remaining_qty=0.0,
            avg_price=51000.0,
            status="FILLED",
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                order_id="order-2",
                qty=0.05,
                fill_price=51000.0,
                fee=0.2,
            ),
        ],
        client_order_id="coid-2",
    )
    client._stub = stub

    feedback = client.place_order(
        13,
        OrderDecision(symbol="ethusdt", side="LONG", qty=0.05),
        51000.0,
        account_symbol="ETHUSDT",
        market="futures",
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
            orig_qty=0.05,
            executed_qty=0.05,
            remaining_qty=0.0,
            avg_price=51000.0,
            status="FILLED",
            error_message="binance trade fee data not available after confirmed execution",
        ),
        fill_deltas=[
            order_service_pb2.OrderFillEntry(
                order_id="order-3",
                qty=0.05,
                fill_price=51000.0,
                fee=0.0,
                status="FEE_MISSING",
            ),
        ],
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub

    feedback = client.place_order(
        13,
        OrderDecision(symbol="ethusdt", side="LONG", qty=0.05),
        51000.0,
        account_symbol="ETHUSDT",
        market="futures",
        intent_id="intent-3",
    )

    assert feedback.attempt_status == "ACCEPTED"
    assert feedback.order is not None
    assert feedback.fill_count == 0
    assert feedback.delta_qty == 0.0
    assert feedback.fill_events == []
