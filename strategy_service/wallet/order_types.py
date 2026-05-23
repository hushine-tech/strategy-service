"""Wallet-facing order and ledger records for unified runtime routing."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class OrderResponse:
    """Wallet-facing order event consumed by runtime wallets.

    Existing call sites only populate the FILLED path fields. Phase B3 extends the
    shape so runtime state machines can also consume NEW / PARTIALLY_FILLED /
    CANCELED / EXPIRED events without introducing a second event type.
    """

    symbol: str
    side: str
    qty: float
    fill_price: float
    status: str
    fee: float = 0.0
    order_id: str = ""
    position_side: str = ""
    reduce_only: bool = False
    orig_qty: float = 0.0
    executed_qty: float = 0.0
    remaining_qty: float = 0.0
    price: float = 0.0


@dataclass
class ExecutionFeedback:
    """Execution-layer feedback for one place-order attempt.

    Separates:
    - attempt outcome (did we create a real exchange order?)
    - order lifecycle snapshot (if accepted)
    - fill delta summary (qty/fee count returned synchronously)
    """

    intent_id: str = ""
    attempt_id: str = ""
    attempt_status: str = ""
    error_message: str = ""
    order: OrderResponse | None = None
    fill_count: int = 0
    delta_qty: float = 0.0
    fill_events: list[OrderResponse] = field(default_factory=list)

    @property
    def status(self) -> str:
        if self.order is not None:
            return str(self.order.status or "")
        return str(self.attempt_status or "")

    @property
    def order_accepted(self) -> bool:
        return self.order is not None

    @property
    def qty(self) -> float:
        return float(self.order.qty if self.order is not None else 0.0)

    @property
    def side(self) -> str:
        return str(self.order.side if self.order is not None else "")

    @property
    def fill_price(self) -> float:
        return float(self.order.fill_price if self.order is not None else 0.0)

    @property
    def order_id(self) -> str:
        return str(self.order.order_id if self.order is not None else "")


@dataclass
class LedgerEvent:
    """Non-fill wallet event routed directly into runtime state."""

    event_type: str
    amount: float
    symbol: str = ""
    position_side: str = ""
