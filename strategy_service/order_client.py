"""gRPC client for order.v1."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
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


def _target_value(target, name: str, default=None):
    if isinstance(target, dict):
        return target.get(name, default)
    return getattr(target, name, default)


def _spot_close_target_proto(client, order_service_pb2, target):
    return order_service_pb2.SpotCloseTarget(
        venue_id=int(_target_value(target, "venue_id", 0) or 0),
        exchange=client._exchange_code(_target_value(target, "exchange", "")),
        market=client._market_code(_target_value(target, "market", "")),
        symbol=str(_target_value(target, "symbol", "") or "").strip().upper(),
    )


def _market_time_to_proto(value: object | None):
    if value is None:
        return None
    from google.protobuf.timestamp_pb2 import Timestamp

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, date):
        dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
    elif isinstance(value, (int, float)):
        raw = float(value)
        if raw <= 0.0:
            return None
        seconds = raw / 1000.0 if raw >= 100_000_000_000 else raw
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
    else:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    out = Timestamp()
    out.FromDatetime(dt)
    return out


class OrderClient:
    """Thin wrapper around order.v1 gRPC stubs.

    Order placement must go through core-service/order.v1. Tests may inject a
    fake stub explicitly, but production code must not synthesize local fills.
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
        portfolio_id: int,
        decision: OrderDecision,
        mark_price: float,
        *,
        portfolio_symbol: str | None = None,
        strategy_id: int = 0,
        market: str | None = None,
        session_id: str = "",
        intent_id: str = "",
        market_time: object | None = None,
        spot_risk_snapshot_id: str = "",
    ) -> ExecutionFeedback:
        """Place an order via order.v1."""
        symbol = portfolio_symbol or decision.symbol
        intent = intent_id.strip() or uuid.uuid4().hex
        effective_market = str(getattr(decision, "market", None) or "").strip()
        exchange_code = self._exchange_code(getattr(decision, "exchange", None))
        market_code = self._market_code(effective_market)
        if market is not None and str(market or "").strip():
            market_arg_code = self._market_code(market)
            if market_arg_code != market_code:
                raise ValueError(
                    f"market argument {market!r} does not match decision.market {effective_market!r}"
                )
        position_side_code = self._position_side_code(getattr(decision, "position_side", None))

        if not self._stub:
            raise RuntimeError("order.v1 gRPC client is not configured")

        try:
            from strategy_service.gen import order_service_pb2

            kwargs: dict = dict(
                portfolio_id=int(portfolio_id),
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
                qty_decimal=str(decision.qty).strip(),
                mark_price_decimal=str(mark_price).strip(),
            )
            if decision.price is not None:
                kwargs["price"] = float(decision.price)
                kwargs["price_decimal"] = str(decision.price).strip()
            order_type = str(getattr(decision, "order_type", None) or "").strip().upper()
            if not order_type:
                order_type = "LIMIT" if decision.price is not None else "MARKET"
            kwargs["order_type"] = order_type
            time_in_force = str(getattr(decision, "time_in_force", None) or "").strip().upper()
            if order_type == "LIMIT":
                kwargs["time_in_force"] = time_in_force or "GTC"
            kwargs["post_only"] = bool(getattr(decision, "post_only", False))
            kwargs["reduce_only"] = bool(getattr(decision, "reduce_only", False))
            good_till_date_pb = _market_time_to_proto(getattr(decision, "good_till_date", None))
            if good_till_date_pb is not None:
                kwargs["good_till_date"] = good_till_date_pb
            market_time_pb = _market_time_to_proto(market_time)
            if market_time_pb is not None:
                kwargs["market_time"] = market_time_pb
            effective_spot_snapshot_id = str(
                spot_risk_snapshot_id
                or getattr(decision, "spot_risk_snapshot_id", "")
                or ""
            ).strip()
            if effective_spot_snapshot_id:
                kwargs["spot_risk_snapshot_id"] = effective_spot_snapshot_id

            req = order_service_pb2.PlaceOrderRequest(**kwargs)
            resp = self._stub.PlaceOrder(req)
            return self._feedback_from_response(resp, decision=decision, market=effective_market, symbol=symbol)
        except Exception as exc:
            logger.warning("OrderClient.place_order failed for %d/%s", portfolio_id, decision.symbol, exc_info=True)
            return self._resolve_unknown_attempt(
                portfolio_id=portfolio_id,
                intent_id=intent,
                error_message=str(exc),
                decision=decision,
                market=effective_market,
                symbol=symbol,
            )

    def close_spot_targets(
        self,
        *,
        user_id: int,
        portfolio_id: int,
        strategy_id: int,
        session_id: str,
        operation_id: str,
        targets,
    ):
        """Ask core-service to atomically close declared Binance Spot targets."""
        if not self._stub:
            raise RuntimeError("order.v1 gRPC client is not configured")
        from strategy_service.gen import order_service_pb2

        request_targets = [_spot_close_target_proto(self, order_service_pb2, target) for target in targets]
        request = order_service_pb2.CloseSpotTargetsRequest(
            user_id=int(user_id),
            portfolio_id=int(portfolio_id),
            strategy_id=int(strategy_id),
            session_id=str(session_id or "").strip(),
            operation_id=str(operation_id or "").strip(),
            targets=request_targets,
        )
        return self._stub.CloseSpotTargets(request)

    @staticmethod
    def _wallet_qty(qty: float, side: str, market: str) -> float:
        q = abs(float(qty))
        if not OrderClient._is_futures_market(market):
            return q
        side_upper = str(side).upper().strip()
        if side_upper == "BUY":
            return q
        if side_upper == "SELL":
            return -q
        raise ValueError(f"unsupported order side: {side!r}")

    @staticmethod
    def _is_futures_market(market: str) -> bool:
        try:
            return OrderClient._market_code(market) in (2, 3)
        except ValueError:
            return str(market or "").strip().lower() == "futures"

    @staticmethod
    def _exchange_code(exchange: str | None) -> int:
        key = str(exchange or "").strip().lower()
        if key not in EXCHANGE_CODES:
            raise ValueError(f"unsupported exchange: {exchange!r}")
        return EXCHANGE_CODES[key]

    @staticmethod
    def _market_code(market: str | None) -> int:
        key = str(market or "").strip().lower()
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
        normalized_session_id = str(session_id or "").strip()
        if not normalized_session_id:
            raise ValueError("session_id is required")
        if not self._stub:
            raise RuntimeError("order lifecycle client is not configured")
        from strategy_service.gen import order_service_pb2

        resp = self._stub.ListOrderLifecycleEvents(order_service_pb2.ListOrderLifecycleEventsRequest(
            session_id=normalized_session_id,
            after_event_id=int(after_event_id),
            limit=int(limit),
        ))
        return [self._order_update_event_from_proto(item) for item in resp.events]

    @classmethod
    def order_response_from_update(cls, event: OrderUpdateEvent) -> OrderResponse | None:
        """Convert a fill lifecycle event into the wallet-facing order delta."""
        event_type = str(event.event_type or "").strip().lower()
        if event_type not in {"fill", "liquidation"} or event.fill is None:
            return None
        if event.fill.fee_missing:
            return None
        raw_qty = abs(float(event.fill.qty or 0.0))
        if raw_qty <= 0.0:
            return None
        cls._exchange_code(event.exchange)
        market = event.market
        cls._market_code(market)
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
            venue_id=int(getattr(event, "venue_id", 0) or 0),
            exchange=str(getattr(event, "exchange", "") or ""),
            market=str(getattr(event, "market", "") or ""),
            exchange_order_id=str(
                getattr(event.fill, "exchange_order_id", "")
                or getattr(event, "exchange_order_id", "")
                or ""
            ),
            exchange_trade_id=str(
                getattr(event.fill, "exchange_trade_id", "")
                or getattr(event, "exchange_trade_id", "")
                or ""
            ),
            fee_asset=str(getattr(event.fill, "fee_asset", "") or ""),
            qty_decimal=str(getattr(event.fill, "qty_decimal", "") or event.fill.qty or "0"),
            fill_price_decimal=str(
                getattr(event.fill, "fill_price_decimal", "")
                or event.fill.fill_price
                or "0"
            ),
            fee_decimal=str(getattr(event.fill, "fee_decimal", "") or event.fill.fee or "0"),
            quote_qty_decimal=str(getattr(event.fill, "quote_qty_decimal", "") or ""),
            orig_qty_decimal=str(getattr(event, "orig_qty_decimal", "") or orig_qty or "0"),
            executed_qty_decimal=str(getattr(event, "executed_qty_decimal", "") or executed_qty or "0"),
            remaining_qty_decimal=str(getattr(event, "remaining_qty_decimal", "") or remaining_qty or "0"),
            price_decimal=str(getattr(event, "price_decimal", "") or ""),
            cumulative_quote_qty_decimal=str(getattr(event, "cumulative_quote_qty_decimal", "") or ""),
        )

    @staticmethod
    def order_update_event_from_proto(item) -> OrderUpdateEvent:
        return OrderClient._order_update_event_from_proto(item)

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
            object.__setattr__(fill, "qty_decimal", str(getattr(item.fill_delta, "qty_decimal", "") or ""))
            object.__setattr__(fill, "fill_price_decimal", str(getattr(item.fill_delta, "fill_price_decimal", "") or ""))
            object.__setattr__(fill, "fee_decimal", str(getattr(item.fill_delta, "fee_decimal", "") or ""))
            object.__setattr__(fill, "quote_qty_decimal", str(getattr(item.fill_delta, "quote_qty_decimal", "") or ""))
        order_state = item.order_state if item.HasField("order_state") else None
        event = OrderUpdateEvent(
            event_id=int(item.event_id),
            session_id=str(item.session_id or ""),
            portfolio_id=int(item.portfolio_id),
            venue_id=int(item.venue_id),
            exchange=EXCHANGE_NAMES.get(int(item.exchange), f"exchange:{int(item.exchange)}"),
            market=MARKET_NAMES.get(int(item.market), f"market:{int(item.market)}"),
            side=str(item.side or ""),
            position_side=POSITION_SIDE_NAMES.get(int(item.position_side), f"position_side:{int(item.position_side)}"),
            event_type=str(item.event_type or ""),
            order_status=str(item.order_status or ""),
            event_source=str(getattr(item, "event_source", "") or ""),
            symbol=str((getattr(order_state, "symbol", "") if order_state is not None else "") or (fill.symbol if fill is not None else "") or "").upper(),
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
        object.__setattr__(event, "orig_qty_decimal", str(getattr(order_state, "orig_qty_decimal", "") or ""))
        object.__setattr__(event, "executed_qty_decimal", str(getattr(order_state, "executed_qty_decimal", "") or ""))
        object.__setattr__(event, "remaining_qty_decimal", str(getattr(order_state, "remaining_qty_decimal", "") or ""))
        object.__setattr__(event, "price_decimal", str(getattr(order_state, "price_decimal", "") or ""))
        object.__setattr__(event, "cumulative_quote_qty_decimal", str(getattr(order_state, "cumulative_quote_qty_decimal", "") or ""))
        return event

    def _resolve_unknown_attempt(
        self,
        *,
        portfolio_id: int,
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
                portfolio_id=int(portfolio_id),
                intent_id=intent_id,
            ))
            feedback = self._feedback_from_response(resp, decision=decision, market=market, symbol=symbol)
            if not feedback.error_message:
                feedback.error_message = error_message
            return feedback
        except Exception:
            logger.warning("OrderClient.resolve_unknown_attempt failed for %d/%s", portfolio_id, symbol, exc_info=True)
            return ExecutionFeedback(
                intent_id=intent_id,
                attempt_status="UNKNOWN",
                error_message=error_message,
                order=None,
                fill_count=0,
                delta_qty=0.0,
            )

    def _feedback_from_response(self, resp, *, decision: OrderDecision, market: str, symbol: str) -> ExecutionFeedback:
        error_fields = self._structured_error_fields(resp)
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
                venue_id=int(getattr(resp.order, "venue_id", 0) or 0),
                exchange=EXCHANGE_NAMES.get(int(getattr(resp.order, "exchange", 0) or 0), ""),
                market=MARKET_NAMES.get(int(getattr(resp.order, "market", 0) or 0), market),
                exchange_order_id=str(getattr(resp.order, "exchange_order_id", "") or ""),
                fee_asset=(
                    fill_events[0].fee_asset
                    if fill_events and len({item.fee_asset for item in fill_events}) == 1
                    else ""
                ),
                qty_decimal=str(sum((Decimal(item.qty_decimal or "0") for item in fill_events), Decimal("0"))),
                fill_price_decimal=(fill_events[-1].fill_price_decimal if fill_events else str(getattr(resp.order, "avg_price_decimal", "") or "")),
                fee_decimal=str(sum((Decimal(item.fee_decimal or "0") for item in fill_events), Decimal("0"))),
                quote_qty_decimal=str(sum((Decimal(item.quote_qty_decimal or "0") for item in fill_events), Decimal("0"))),
                orig_qty_decimal=str(getattr(resp.order, "orig_qty_decimal", "") or resp.order.orig_qty or "0"),
                executed_qty_decimal=str(getattr(resp.order, "executed_qty_decimal", "") or resp.order.executed_qty or "0"),
                remaining_qty_decimal=str(getattr(resp.order, "remaining_qty_decimal", "") or resp.order.remaining_qty or "0"),
                price_decimal=str(getattr(resp.order, "price_decimal", "") or resp.order.price or ""),
                cumulative_quote_qty_decimal=str(getattr(resp.order, "cumulative_quote_qty_decimal", "") or ""),
                environment=int(getattr(resp.order, "environment", 0) or 0),
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
                **error_fields,
            )

        return ExecutionFeedback(
            intent_id=resp.intent_id,
            attempt_id=resp.attempt_id,
            attempt_status=resp.attempt_status,
            error_message=resp.error_message,
            order=None,
            fill_count=0,
            delta_qty=0.0,
            **error_fields,
        )

    @staticmethod
    def _structured_error_fields(resp) -> dict[str, object]:
        has_field = getattr(resp, "HasField", None)
        if not callable(has_field):
            return {}
        try:
            if not resp.HasField("error"):
                return {}
        except ValueError:
            return {}
        detail = resp.error
        return {
            "error_code": str(getattr(detail, "code", "") or ""),
            "error_environment": int(getattr(detail, "environment", 0) or 0),
            "error_retryable": bool(getattr(detail, "retryable", False)),
            "error_source": str(getattr(detail, "source", "") or ""),
            "error_venue_id": int(getattr(detail, "venue_id", 0) or 0),
            "error_exchange": int(getattr(detail, "exchange", 0) or 0),
            "error_market": int(getattr(detail, "market", 0) or 0),
            "error_symbol": str(getattr(detail, "symbol", "") or ""),
        }

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
        orig_qty_decimal = str(getattr(order, "orig_qty_decimal", "") or orig_qty or "0")
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
        cumulative_executed = Decimal("0")
        cumulative_quote = Decimal("0")
        for index, fill in enumerate(raw_fills):
            qty_decimal = str(getattr(fill, "qty_decimal", "") or getattr(fill, "qty", 0.0) or "0")
            raw_qty_decimal = abs(Decimal(qty_decimal))
            raw_qty = float(raw_qty_decimal)
            if raw_qty_decimal <= 0:
                continue
            fill_price_decimal = str(
                getattr(fill, "fill_price_decimal", "")
                or getattr(fill, "fill_price", 0.0)
                or "0"
            )
            quote_qty_decimal = str(getattr(fill, "quote_qty_decimal", "") or "")
            if quote_qty_decimal:
                raw_quote = Decimal(quote_qty_decimal)
            else:
                raw_quote = raw_qty_decimal * Decimal(fill_price_decimal)
                quote_qty_decimal = str(raw_quote)
            cumulative_executed += raw_qty_decimal
            cumulative_quote += raw_quote
            remaining_exact = max(Decimal("0"), Decimal(orig_qty_decimal) - cumulative_executed)
            remaining_qty = float(remaining_exact)
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
                executed_qty=float(cumulative_executed),
                remaining_qty=remaining_qty,
                price=price,
                venue_id=int(getattr(fill, "venue_id", 0) or getattr(order, "venue_id", 0) or 0),
                exchange=EXCHANGE_NAMES.get(int(getattr(fill, "exchange", 0) or getattr(order, "exchange", 0) or 0), ""),
                market=MARKET_NAMES.get(int(getattr(fill, "market", 0) or getattr(order, "market", 0) or 0), market),
                exchange_order_id=str(getattr(fill, "exchange_order_id", "") or getattr(order, "exchange_order_id", "") or ""),
                exchange_trade_id=str(getattr(fill, "exchange_trade_id", "") or ""),
                fee_asset=str(getattr(fill, "fee_asset", "") or ""),
                qty_decimal=str(raw_qty_decimal),
                fill_price_decimal=fill_price_decimal,
                fee_decimal=str(getattr(fill, "fee_decimal", "") or getattr(fill, "fee", 0.0) or "0"),
                quote_qty_decimal=quote_qty_decimal,
                orig_qty_decimal=orig_qty_decimal,
                executed_qty_decimal=str(cumulative_executed),
                remaining_qty_decimal=str(remaining_exact),
                price_decimal=str(getattr(order, "price_decimal", "") or getattr(order, "price", 0.0) or ""),
                cumulative_quote_qty_decimal=str(cumulative_quote),
                environment=int(getattr(fill, "environment", 0) or getattr(order, "environment", 0) or 0),
            ))
        return events
