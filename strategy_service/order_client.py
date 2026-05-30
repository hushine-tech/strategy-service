"""gRPC client for order.v1. Soft dependency: falls back to mock when address is empty."""

from __future__ import annotations

import logging
import uuid

from strategy_service.types import (
    ExecutionFeedback,
    OrderDecision,
    OrderResponse,
    OrderUpdateEvent,
    OrderUpdateFill,
)

logger = logging.getLogger(__name__)

EXCHANGE_CODES = {
    "binance": 1,
    "okx": 2,
}

MARKET_CODES = {
    "spot": 1,
    "futures": 2,
    "future": 2,
    "usdm_futures": 2,
    "perpetual_futures": 2,
    "perp": 2,
    "delivery_futures": 3,
}

POSITION_SIDE_CODES = {
    "": 0,
    "none": 0,
    "both": 0,
    "long": 1,
    "short": 2,
}

EXCHANGE_NAMES = {v: k for k, v in EXCHANGE_CODES.items()}
MARKET_NAMES = {
    1: "spot",
    2: "perpetual_futures",
    3: "delivery_futures",
}
POSITION_SIDE_NAMES = {
    0: "both",
    1: "long",
    2: "short",
}


class OrderClient:
    """Thin wrapper around order.v1 gRPC stubs.

    If *grpc_address* is empty, ``place_order`` falls back to an inline mock so
    backtest workflows continue to work without the order API running.
    """

    def __init__(self, grpc_address: str = "") -> None:
        self._address = grpc_address.strip()
        self._stub = None
        if self._address:
            self._connect()

    def _connect(self) -> None:
        try:
            import grpc
            from strategy_service.gen import order_service_pb2_grpc

            channel = grpc.insecure_channel(self._address)

            try:
                from utils.log import ClientExtInterceptor  # type: ignore

                channel = grpc.intercept_channel(
                    channel, ClientExtInterceptor(target_service=self._address)
                )
            except Exception:  # noqa: BLE001
                logger.debug("ClientExtInterceptor unavailable; no grpc_ext log for OrderClient")

            self._stub = order_service_pb2_grpc.OrderServiceStub(channel)
            logger.info("OrderClient connected to %s", self._address)
        except Exception:
            logger.warning("OrderClient: failed to connect to %s", self._address, exc_info=True)
            self._stub = None

    def place_order(
        self,
        account_id: int,
        decision: OrderDecision,
        mark_price: float,
        *,
        account_symbol: str | None = None,
        strategy_id: int = 0,
        market: str = "futures",
        session_id: str = "",
        intent_id: str = "",
    ) -> ExecutionFeedback:
        """Place an order via order.v1 (or mock if not configured)."""
        symbol = account_symbol or decision.symbol
        intent = intent_id.strip() or uuid.uuid4().hex
        effective_market = str(getattr(decision, "market", None) or market or "futures")
        exchange_code = self._exchange_code(getattr(decision, "exchange", None))
        market_code = self._market_code(effective_market)
        position_side_code = self._position_side_code(getattr(decision, "position_side", None))

        if not self._stub:
            return self._mock_fill(decision, mark_price, symbol, effective_market, intent)

        try:
            from strategy_service.gen import order_service_pb2

            kwargs: dict = dict(
                account_id=int(account_id),
                symbol=symbol,
                side=decision.side,
                qty=float(decision.qty),
                mark_price=float(mark_price),
                strategy_id=int(strategy_id),
                market=market_code,
                session_id=session_id,
                intent_id=intent,
                exchange=exchange_code,
                position_side=position_side_code,
            )
            if decision.price is not None:
                kwargs["price"] = float(decision.price)
            order_type = str(getattr(decision, "order_type", None) or "").strip().upper()
            if not order_type:
                order_type = "LIMIT" if decision.price is not None else "MARKET"
            kwargs["order_type"] = order_type
            time_in_force = str(getattr(decision, "time_in_force", None) or "").strip().upper()
            if order_type == "LIMIT":
                kwargs["time_in_force"] = time_in_force or "GTC"

            req = order_service_pb2.PlaceOrderRequest(**kwargs)
            resp = self._stub.PlaceOrder(req)
            return self._feedback_from_response(resp, decision=decision, market=effective_market, symbol=symbol)
        except Exception as exc:
            logger.warning("OrderClient.place_order failed for %d/%s", account_id, decision.symbol, exc_info=True)
            return self._resolve_unknown_attempt(
                account_id=account_id,
                intent_id=intent,
                error_message=str(exc),
                decision=decision,
                market=effective_market,
                symbol=symbol,
            )

    @staticmethod
    def _mock_fill(decision: OrderDecision, mark_price: float, symbol: str, market: str, intent_id: str) -> ExecutionFeedback:
        fill_price = float(decision.price) if decision.price is not None else float(mark_price)
        logger.debug("[Mock Order] %s %s %s @ %s", decision.side, decision.qty, decision.symbol, fill_price)
        order_id = uuid.uuid4().hex
        delta_qty = OrderClient._wallet_qty(decision.qty, decision.side, market)
        return ExecutionFeedback(
            intent_id=intent_id,
            attempt_id=uuid.uuid4().hex,
            attempt_status="ACCEPTED",
            order=OrderResponse(
                symbol=symbol,
                side=decision.side,
                qty=delta_qty,
                fill_price=fill_price,
                status="FILLED",
                order_id=order_id,
                orig_qty=abs(float(decision.qty)),
                executed_qty=abs(float(decision.qty)),
                remaining_qty=0.0,
            ),
            fill_count=1,
            delta_qty=delta_qty,
        )

    @staticmethod
    def _wallet_qty(qty: float, side: str, market: str) -> float:
        q = abs(float(qty))
        if not OrderClient._is_futures_market(market):
            return q
        side_upper = str(side).upper().strip()
        if side_upper in ("SHORT", "SELL"):
            return -q
        return q

    @staticmethod
    def _is_futures_market(market: str) -> bool:
        try:
            return OrderClient._market_code(market) in (2, 3)
        except ValueError:
            return str(market or "").strip().lower() == "futures"

    @staticmethod
    def _exchange_code(exchange: str | None) -> int:
        key = str(exchange or "binance").strip().lower()
        if key not in EXCHANGE_CODES:
            raise ValueError(f"unsupported exchange: {exchange!r}")
        return EXCHANGE_CODES[key]

    @staticmethod
    def _market_code(market: str | None) -> int:
        key = str(market or "futures").strip().lower()
        if key not in MARKET_CODES:
            raise ValueError(f"unsupported market: {market!r}")
        return MARKET_CODES[key]

    @staticmethod
    def _position_side_code(position_side: str | None) -> int:
        key = str(position_side or "").strip().lower()
        if key not in POSITION_SIDE_CODES:
            raise ValueError(f"unsupported position_side: {position_side!r}")
        return POSITION_SIDE_CODES[key]

    def list_order_lifecycle_events(
        self,
        *,
        session_id: str,
        after_event_id: int = 0,
        limit: int = 100,
    ) -> list[OrderUpdateEvent]:
        """Read normalized order lifecycle events after a session cursor."""
        if not self._stub or not str(session_id or "").strip():
            return []
        try:
            from strategy_service.gen import order_service_pb2

            resp = self._stub.ListOrderLifecycleEvents(order_service_pb2.ListOrderLifecycleEventsRequest(
                session_id=str(session_id).strip(),
                after_event_id=int(after_event_id),
                limit=int(limit),
            ))
        except Exception:
            logger.warning("OrderClient.list_order_lifecycle_events failed for session=%s", session_id, exc_info=True)
            return []
        return [self._order_update_event_from_proto(item) for item in resp.events]

    @classmethod
    def order_response_from_update(cls, event: OrderUpdateEvent) -> OrderResponse | None:
        """Convert a fill lifecycle event into the wallet-facing order delta."""
        if str(event.event_type or "").strip().lower() != "fill" or event.fill is None:
            return None
        if event.fill.fee_missing:
            return None
        raw_qty = abs(float(event.fill.qty or 0.0))
        if raw_qty <= 0.0:
            return None
        market = event.market or "perpetual_futures"
        side = event.side or ""
        wallet_qty = cls._wallet_qty(raw_qty, side, market)
        orig_qty = abs(float(getattr(event, "orig_qty", 0.0) or 0.0))
        executed_qty = abs(float(getattr(event, "executed_qty", 0.0) or 0.0))
        remaining_qty = abs(float(getattr(event, "remaining_qty", 0.0) or 0.0))
        if executed_qty <= 0.0:
            executed_qty = raw_qty
        if orig_qty <= 0.0:
            orig_qty = executed_qty + remaining_qty if remaining_qty > 0.0 else raw_qty
        if remaining_qty <= 0.0 and orig_qty > executed_qty:
            remaining_qty = max(0.0, orig_qty - executed_qty)
        return OrderResponse(
            symbol=event.fill.symbol,
            side=side,
            qty=wallet_qty,
            fill_price=float(event.fill.fill_price or 0.0),
            status=event.order_status or "FILLED",
            fee=float(event.fill.fee or 0.0),
            order_id=event.order_id,
            position_side=event.position_side,
            orig_qty=orig_qty,
            executed_qty=executed_qty,
            remaining_qty=remaining_qty,
            price=float(getattr(event, "avg_price", 0.0) or 0.0),
        )

    @staticmethod
    def _order_update_event_from_proto(item) -> OrderUpdateEvent:
        fill = None
        if item.HasField("fill_delta"):
            fill = OrderUpdateFill(
                symbol=str(item.fill_delta.symbol or item.order_state.symbol or "").upper(),
                qty=float(item.fill_delta.qty or 0.0),
                fill_price=float(item.fill_delta.fill_price or item.order_state.avg_price or 0.0),
                fee=float(item.fill_delta.fee or 0.0),
                fee_asset=str(item.fill_delta.fee_asset or ""),
                fee_missing=bool(item.fill_delta.fee_missing),
                exchange_trade_id=str(item.fill_delta.exchange_trade_id or ""),
                exchange_order_id=str(item.fill_delta.exchange_order_id or ""),
            )
        order_state = item.order_state if item.HasField("order_state") else None
        return OrderUpdateEvent(
            event_id=int(item.event_id),
            session_id=str(item.session_id or ""),
            account_id=int(item.account_id),
            venue_id=int(item.venue_id),
            exchange=EXCHANGE_NAMES.get(int(item.exchange), f"exchange:{int(item.exchange)}"),
            market=MARKET_NAMES.get(int(item.market), f"market:{int(item.market)}"),
            side=str(item.side or ""),
            position_side=POSITION_SIDE_NAMES.get(int(item.position_side), f"position_side:{int(item.position_side)}"),
            event_type=str(item.event_type or ""),
            order_status=str(item.order_status or ""),
            intent_id=str(item.intent_id or ""),
            attempt_id=str(item.attempt_id or ""),
            order_id=str(item.order_id or ""),
            exchange_order_id=str(item.exchange_order_id or ""),
            exchange_trade_id=str(item.exchange_trade_id or ""),
            fill=fill,
            orig_qty=float(getattr(order_state, "orig_qty", 0.0) or 0.0),
            executed_qty=float(getattr(order_state, "executed_qty", 0.0) or 0.0),
            remaining_qty=float(getattr(order_state, "remaining_qty", 0.0) or 0.0),
            avg_price=float(getattr(order_state, "avg_price", 0.0) or 0.0),
        )

    def _resolve_unknown_attempt(
        self,
        *,
        account_id: int,
        intent_id: str,
        error_message: str,
        decision: OrderDecision,
        market: str,
        symbol: str,
    ) -> ExecutionFeedback:
        if not self._stub:
            return ExecutionFeedback(
                intent_id=intent_id,
                attempt_status="UNKNOWN",
                error_message=error_message,
                order=None,
                fill_count=0,
                delta_qty=0.0,
            )
        try:
            from strategy_service.gen import order_service_pb2

            resp = self._stub.ResolveOrderAttempt(order_service_pb2.ResolveOrderAttemptRequest(
                account_id=int(account_id),
                intent_id=intent_id,
            ))
            feedback = self._feedback_from_response(resp, decision=decision, market=market, symbol=symbol)
            if not feedback.error_message:
                feedback.error_message = error_message
            return feedback
        except Exception:
            logger.warning("OrderClient.resolve_unknown_attempt failed for %d/%s", account_id, symbol, exc_info=True)
            return ExecutionFeedback(
                intent_id=intent_id,
                attempt_status="UNKNOWN",
                error_message=error_message,
                order=None,
                fill_count=0,
                delta_qty=0.0,
            )

    def _feedback_from_response(self, resp, *, decision: OrderDecision, market: str, symbol: str) -> ExecutionFeedback:
        order_event = None
        if resp.HasField("order"):
            fill_events = self._build_fill_events(
                resp.order,
                resp.fill_deltas,
                fallback_side=decision.side,
                market=market,
                symbol=symbol,
            )
            fill_count = len(fill_events)
            delta_qty = sum(float(event.qty or 0.0) for event in fill_events)
            total_fee = sum(float(event.fee or 0.0) for event in fill_events)
            last_fill_price = float(resp.order.avg_price or 0.0)
            if fill_events:
                last_fill_price = float(fill_events[-1].fill_price or last_fill_price or 0.0)
            wallet_qty = delta_qty
            order_event = OrderResponse(
                symbol=symbol,
                side=resp.order.side,
                qty=wallet_qty,
                fill_price=last_fill_price,
                status=resp.order.status,
                fee=total_fee,
                order_id=resp.order.order_id,
                orig_qty=float(resp.order.orig_qty or 0.0),
                executed_qty=float(resp.order.executed_qty or 0.0),
                remaining_qty=float(resp.order.remaining_qty or 0.0),
                price=float(resp.order.price or 0.0),
            )
            return ExecutionFeedback(
                intent_id=resp.intent_id,
                attempt_id=resp.attempt_id,
                attempt_status=resp.attempt_status,
                error_message=resp.error_message,
                order=order_event,
                fill_count=fill_count,
                delta_qty=wallet_qty,
                fill_events=fill_events,
            )

        return ExecutionFeedback(
            intent_id=resp.intent_id,
            attempt_id=resp.attempt_id,
            attempt_status=resp.attempt_status,
            error_message=resp.error_message,
            order=None,
            fill_count=0,
            delta_qty=0.0,
        )

    @classmethod
    def _build_fill_events(
        cls,
        order,
        fill_deltas,
        *,
        fallback_side: str,
        market: str,
        symbol: str,
    ) -> list[OrderResponse]:
        side = str(getattr(order, "side", "") or fallback_side or "").strip()
        orig_qty = abs(float(getattr(order, "orig_qty", 0.0) or 0.0))
        final_status = str(getattr(order, "status", "") or "").strip().upper()
        order_id = str(getattr(order, "order_id", "") or "")
        price = float(getattr(order, "price", 0.0) or 0.0)

        raw_fills = list(fill_deltas)
        if any(
            str(getattr(fill, "status", "") or "").strip().upper() in {"FEE_MISSING", "FILL_PENDING"}
            for fill in raw_fills
        ):
            return []

        events: list[OrderResponse] = []
        cumulative_executed = 0.0
        for index, fill in enumerate(raw_fills):
            raw_qty = abs(float(getattr(fill, "qty", 0.0) or 0.0))
            if raw_qty <= 0.0:
                continue
            cumulative_executed += raw_qty
            remaining_qty = max(0.0, orig_qty - cumulative_executed) if orig_qty > 0.0 else 0.0
            status = final_status
            if index < len(fill_deltas) - 1 and final_status == "FILLED":
                status = "PARTIALLY_FILLED"
            wallet_qty = cls._wallet_qty(raw_qty, side, market)
            events.append(OrderResponse(
                symbol=symbol,
                side=side,
                qty=wallet_qty,
                fill_price=float(getattr(fill, "fill_price", 0.0) or 0.0),
                status=status or "PARTIALLY_FILLED",
                fee=float(getattr(fill, "fee", 0.0) or 0.0),
                order_id=order_id,
                orig_qty=orig_qty,
                executed_qty=cumulative_executed,
                remaining_qty=remaining_qty,
                price=price,
            ))
        return events
