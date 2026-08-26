"""Exact Futures leg facts used when settling Funding income."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable


@dataclass(frozen=True, slots=True)
class FundingPositionLegFact:
    symbol: str
    position_side: str
    margin_mode: str
    signed_qty_decimal: str


@dataclass(frozen=True, slots=True)
class _LifecycleDelta:
    key: tuple[int, str, str, str]
    signed_qty: Decimal
    time_key: tuple[int, int]
    identity: tuple[int, str, str]


class FundingPositionTracker:
    """Track exact Futures legs independently of float wallet presentation."""

    def __init__(self) -> None:
        self._restored_legs: dict[tuple[int, str, str, str], Decimal] = {}
        self._deltas: dict[tuple[int, str, str], _LifecycleDelta] = {}

    def on_lifecycle_fill(
        self,
        event: object,
        *,
        position_mode: str,
        margin_mode: str,
    ) -> None:
        event_type = str(getattr(event, "event_type", "") or "").strip().lower()
        if event_type not in {"fill", "liquidation"}:
            raise ValueError("event_type must be fill or liquidation")
        venue_id = int(getattr(event, "venue_id", 0) or 0)
        if venue_id <= 0:
            raise ValueError("venue_id is required")
        symbol = str(getattr(event, "symbol", "") or "").strip().upper()
        if not symbol:
            raise ValueError("symbol is required")
        trade_id = str(getattr(event, "exchange_trade_id", "") or "").strip()
        if trade_id in {"", "0"}:
            raise ValueError("exchange_trade_id is required")
        identity = (venue_id, symbol, trade_id)
        if identity in self._deltas:
            return
        time_key = _canonical_time_key(
            getattr(event, "trade_time", None) or getattr(event, "occurred_at", None)
        )

        mode = str(position_mode or "").strip().lower()
        if mode not in {"one_way", "hedge"}:
            raise ValueError("position_mode is required and must be one_way or hedge")
        margin = str(margin_mode or "").strip().lower()
        if margin not in {"cross", "isolated"}:
            raise ValueError("margin_mode is required and must be cross or isolated")
        side = str(getattr(event, "side", "") or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError("side is required and must be BUY or SELL")
        raw_qty = getattr(event, "qty_decimal", "")
        if not isinstance(raw_qty, str) or not raw_qty:
            raise ValueError("qty_decimal is required")
        try:
            qty = Decimal(raw_qty)
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("qty_decimal is invalid") from exc
        if not qty.is_finite() or qty <= 0:
            raise ValueError("qty_decimal must be a positive finite decimal")

        raw_position_side = str(getattr(event, "position_side", "") or "").strip().upper()
        if mode == "one_way":
            position_side = "BOTH"
        else:
            if raw_position_side not in {"LONG", "SHORT"}:
                raise ValueError("position_side is required for hedge mode")
            position_side = raw_position_side

        signed_qty = qty if side == "BUY" else -qty
        key = (venue_id, symbol, position_side, margin)
        self._deltas[identity] = _LifecycleDelta(key, signed_qty, time_key, identity)

    def restore(self, venue_id: int, facts: Iterable[FundingPositionLegFact]) -> None:
        if int(venue_id) <= 0:
            raise ValueError("venue_id is required")
        restored: dict[tuple[int, str, str, str], Decimal] = {}
        for fact in facts:
            symbol = str(fact.symbol or "").strip().upper()
            side = str(fact.position_side or "").strip().upper()
            margin = str(fact.margin_mode or "").strip().lower()
            if not symbol or side not in {"BOTH", "LONG", "SHORT"} or margin not in {"cross", "isolated"}:
                raise ValueError("FundingPositionLegFact is invalid")
            try:
                qty = Decimal(fact.signed_qty_decimal)
            except (InvalidOperation, ValueError) as exc:
                raise ValueError("FundingPositionLegFact.signed_qty_decimal is invalid") from exc
            if not qty.is_finite():
                raise ValueError("FundingPositionLegFact.signed_qty_decimal is invalid")
            restored[(int(venue_id), symbol, side, margin)] = qty
        self._restored_legs.update(restored)

    def legs_for(
        self,
        venue_id: int,
        symbol: str,
        funding_time: object | None = None,
    ) -> list[FundingPositionLegFact]:
        target_venue = int(venue_id)
        target_symbol = str(symbol or "").strip().upper()
        cutoff = _canonical_time_key(funding_time) if funding_time is not None else None
        legs = dict(self._restored_legs)
        for delta in sorted(self._deltas.values(), key=lambda item: (item.time_key, item.identity)):
            if cutoff is not None and delta.time_key > cutoff:
                continue
            legs[delta.key] = legs.get(delta.key, Decimal()) + delta.signed_qty
        return [
            FundingPositionLegFact(
                symbol=leg_symbol,
                position_side=position_side,
                margin_mode=margin_mode,
                signed_qty_decimal=format(qty, "f"),
            )
            for (_venue, leg_symbol, position_side, margin_mode), qty in sorted(legs.items())
            if _venue == target_venue and leg_symbol == target_symbol and qty != 0
        ]


def _canonical_time_key(value: object) -> tuple[int, int]:
    if isinstance(value, tuple) and len(value) == 2:
        seconds, nanos = value
        if (
            type(seconds) is int
            and type(nanos) is int
            and seconds >= 0
            and 0 <= nanos < 1_000_000_000
        ):
            return seconds, nanos
    if type(value) is int and value >= 0:
        return value, 0
    raise ValueError("canonical lifecycle time is required")
