from __future__ import annotations

from decimal import Decimal

from strategy_service.types import ExecutionFeedback, OrderResponse


class FilledOrderClient:
    """Test double for order.v1 that returns a confirmed market fill."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def place_order(
        self,
        portfolio_id: int,
        decision,
        mark_price: float,
        *,
        portfolio_symbol: str | None = None,
        strategy_id: int = 0,
        market: str | None = None,
        session_id: str = "",
        intent_id: str = "",
        market_time=None,
        spot_risk_snapshot_id: str = "",
    ) -> ExecutionFeedback:
        self.calls.append({
            "portfolio_id": portfolio_id,
            "decision": decision,
            "mark_price": mark_price,
            "portfolio_symbol": portfolio_symbol,
            "strategy_id": strategy_id,
            "market": market,
            "session_id": session_id,
            "intent_id": intent_id,
            "market_time": market_time,
            "spot_risk_snapshot_id": spot_risk_snapshot_id,
        })
        sequence = len(self.calls)
        symbol = str(portfolio_symbol or decision.symbol).strip().upper()
        route_market = str(market or getattr(decision, "market", "") or "").strip().lower()
        raw_qty = abs(float(decision.qty))
        side = str(decision.side or "").strip().upper()
        wallet_qty = raw_qty
        if route_market != "spot" and side == "SELL":
            wallet_qty = -raw_qty
        fill_price = float(decision.price) if decision.price is not None else float(mark_price)
        qty_decimal = format(Decimal(str(raw_qty)), "f")
        fill_price_decimal = format(Decimal(str(fill_price)), "f")
        quote_qty_decimal = format(
            Decimal(qty_decimal) * Decimal(fill_price_decimal),
            "f",
        )
        order = OrderResponse(
            symbol=symbol,
            side=decision.side,
            qty=wallet_qty,
            fill_price=fill_price,
            status="FILLED",
            order_id=f"test-order-{sequence}",
            position_side=getattr(decision, "position_side", None),
            orig_qty=raw_qty,
            executed_qty=raw_qty,
            remaining_qty=0.0,
            exchange="binance",
            market=route_market,
            exchange_order_id=f"test-exchange-order-{sequence}",
            exchange_trade_id=f"test-trade-{sequence}",
            qty_decimal=qty_decimal,
            fill_price_decimal=fill_price_decimal,
            fee_decimal="0",
            quote_qty_decimal=quote_qty_decimal,
            orig_qty_decimal=qty_decimal,
            executed_qty_decimal=qty_decimal,
            remaining_qty_decimal="0",
            price_decimal=(
                format(Decimal(str(decision.price)), "f")
                if decision.price is not None
                else ""
            ),
            cumulative_quote_qty_decimal=quote_qty_decimal,
        )
        return ExecutionFeedback(
            intent_id=intent_id,
            attempt_id=f"test-attempt-{sequence}",
            attempt_status="ACCEPTED",
            order=order,
            fill_count=1,
            delta_qty=wallet_qty,
        )
