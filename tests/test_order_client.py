from __future__ import annotations

from datetime import datetime, timezone

import pytest

from strategy_service.gen import order_service_pb2
from strategy_service.order_client import OrderClient
from strategy_service.types import OrderDecision, OrderUpdateEvent, OrderUpdateFill


class _Stub:
    def __init__(self, response: order_service_pb2.PlaceOrderResponse) -> None:
        self.response = response
        self.last_request: order_service_pb2.PlaceOrderRequest | None = None
        self.resolve_response: order_service_pb2.ResolveOrderAttemptResponse | None = None
        self.raise_on_place: Exception | None = None
        self.last_resolve_request: order_service_pb2.ResolveOrderAttemptRequest | None = None
        self.lifecycle_response: order_service_pb2.ListOrderLifecycleEventsResponse | None = None
        self.last_lifecycle_request: order_service_pb2.ListOrderLifecycleEventsRequest | None = None

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

    def ListOrderLifecycleEvents(self, request: order_service_pb2.ListOrderLifecycleEventsRequest):
        self.last_lifecycle_request = request
        if self.lifecycle_response is None:
            return order_service_pb2.ListOrderLifecycleEventsResponse()
        return self.lifecycle_response


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
    )


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
        _decision(),
        51000.0,
        account_symbol="ETHUSDT",
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
        account_symbol="ETHUSDT",
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
        _decision(),
        51000.0,
        account_symbol="ETHUSDT",
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
        _decision(),
        51000.0,
        account_symbol="ETHUSDT",
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
        account_symbol="ETHUSDT",
        market="delivery_futures",
        intent_id="intent-4",
    )

    assert stub.last_request is not None
    assert stub.last_request.exchange == 2
    assert stub.last_request.market == 3
    assert stub.last_request.position_side == 2
    assert feedback.attempt_status == "ACCEPTED"


def test_place_order_sends_limit_gtc_when_price_is_set():
    response = order_service_pb2.PlaceOrderResponse(
        intent_id="intent-limit",
        attempt_id="attempt-limit",
        attempt_status="ACCEPTED",
    )
    client = OrderClient("")
    stub = _Stub(response)
    client._stub = stub

    client.place_order(
        13,
        _decision(symbol="ETHUSDT", side="BUY", qty="0.05", price="50000", order_type="LIMIT"),
        51000.0,
        account_symbol="ETHUSDT",
        market="perpetual_futures",
        intent_id="intent-limit",
    )

    assert stub.last_request is not None
    assert stub.last_request.price == 50000.0
    assert stub.last_request.order_type == "LIMIT"
    assert stub.last_request.time_in_force == "GTC"


def test_exchange_and_market_codes_do_not_default_missing_route() -> None:
    with pytest.raises(ValueError, match="unsupported exchange"):
        OrderClient._exchange_code(None)
    with pytest.raises(ValueError, match="unsupported market"):
        OrderClient._market_code(None)


def test_order_response_from_update_rejects_missing_market_route() -> None:
    event = OrderUpdateEvent(
        event_id=1,
        session_id="session-1",
        account_id=13,
        venue_id=20,
        exchange="binance",
        market="",
        position_side="both",
        side="BUY",
        event_type="fill",
        order_status="FILLED",
        order_id="order-1",
        fill=OrderUpdateFill(symbol="ETHUSDT", qty=0.1, fill_price=3000.0),
    )

    with pytest.raises(ValueError, match="unsupported market"):
        OrderClient.order_response_from_update(event)


def test_list_order_lifecycle_events_maps_route_facts_and_fill():
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.lifecycle_response = order_service_pb2.ListOrderLifecycleEventsResponse(
        events=[
            order_service_pb2.OrderLifecycleEventEntry(
                event_id=11,
                session_id="session-1",
                account_id=13,
                venue_id=20,
                exchange=1,
                market=2,
                position_side=0,
                side="BUY",
                event_type="fill",
                order_status="FILLED",
                order_id="order-1",
                fill_delta=order_service_pb2.FillDeltaEntry(
                    symbol="ETHUSDT",
                    qty=0.1,
                    fill_price=3000.0,
                    fee=0.2,
                    fee_asset="USDT",
                    exchange_trade_id="trade-1",
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
    assert event.position_side == "both"
    assert event.side == "BUY"
    assert event.fill is not None
    assert event.fill.qty == pytest.approx(0.1)
    order_response = OrderClient.order_response_from_update(event)
    assert order_response is not None
    assert order_response.qty == pytest.approx(0.1)


def test_order_response_from_update_uses_lifecycle_order_state():
    client = OrderClient("")
    stub = _Stub(order_service_pb2.PlaceOrderResponse())
    stub.lifecycle_response = order_service_pb2.ListOrderLifecycleEventsResponse(
        events=[
            order_service_pb2.OrderLifecycleEventEntry(
                event_id=12,
                session_id="session-1",
                account_id=13,
                venue_id=20,
                exchange=1,
                market=2,
                position_side=0,
                side="BUY",
                event_type="fill",
                order_status="PARTIALLY_FILLED",
                order_id="order-1",
                fill_delta=order_service_pb2.FillDeltaEntry(
                    symbol="ETHUSDT",
                    qty=0.02,
                    fill_price=3000.0,
                    fee=0.2,
                ),
                order_state=order_service_pb2.OrderStateEntry(
                    symbol="ETHUSDT",
                    status="PARTIALLY_FILLED",
                    orig_qty=0.05,
                    executed_qty=0.02,
                    remaining_qty=0.03,
                    avg_price=3000.0,
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
