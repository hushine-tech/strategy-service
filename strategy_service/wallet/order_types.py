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
    venue_id: int = 0
    exchange: str = ""
    market: str = ""
    exchange_order_id: str = ""
    exchange_trade_id: str = ""
    fee_asset: str = ""
    qty_decimal: str = ""
    fill_price_decimal: str = ""
    fee_decimal: str = ""
    quote_qty_decimal: str = ""
    orig_qty_decimal: str = ""
    executed_qty_decimal: str = ""
    remaining_qty_decimal: str = ""
    price_decimal: str = ""
    cumulative_quote_qty_decimal: str = ""
    environment: int = 0
    retryable: bool = False
    source: str = ""
    error_code: str = ""
    event_type: str = ""


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
    error_code: str = ""
    error_environment: int = 0
    error_retryable: bool = False
    error_source: str = ""
    error_venue_id: int = 0
    error_exchange: int = 0
    error_market: int = 0
    error_symbol: str = ""

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
    income_entry_id: int = 0
    venue_id: int = 0
    asset: str = ""
    amount_decimal: str = ""
    margin_mode: str = ""
