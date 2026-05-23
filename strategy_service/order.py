"""Mock execution: construct wallet-compatible OrderResponse (replace with real broker later)."""

from __future__ import annotations

import logging

from strategy_service.types import OrderDecision, OrderResponse

logger = logging.getLogger(__name__)


def place_order(
    decision: OrderDecision, mark_price: float, *, account_symbol: str
) -> OrderResponse:
    """
    Log a mock fill and return an OrderResponse suitable for Wallet.on_order.

    fill_price uses decision.price when set, otherwise mark_price.
    account_symbol is the slot key passed to Wallet.on_order (registered symbol);
    it must match that argument so the wallet's symbol consistency check passes.
    """
    fill_price = float(decision.price) if decision.price is not None else float(mark_price)
    logger.info(
        "[Mock Order] %s %s %s @ %s",
        decision.side, decision.qty, decision.symbol, fill_price,
    )
    return OrderResponse(
        symbol=account_symbol,
        side=decision.side,
        qty=float(decision.qty),
        fill_price=fill_price,
        status="FILLED",
    )
