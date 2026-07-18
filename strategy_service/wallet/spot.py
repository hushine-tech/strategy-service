"""Exact Binance Spot wallet ledger.

Balances are keyed by account asset code (``BTC``, ``USDT``, ``BNB``).
Trading symbols (``BTCUSDT``) exist only in immutable metadata, price indexes,
and order identities.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
from itertools import count
from typing import Any, Mapping

from .canonical import SpotSymbolMetadata, norm_symbol
from .order_types import OrderResponse
from .spot_filters import evaluate_spot_filter_vector


ZERO = Decimal("0")
_ACTIVE_ORDER_STATUSES = {"NEW", "PARTIALLY_FILLED"}
_TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "EXPIRED", "REJECTED"}
_SUPPORTED_ORDER_STATUSES = _ACTIVE_ORDER_STATUSES | _TERMINAL_ORDER_STATUSES
_ORDER_STATUS_RANK = {
    "": 0,
    "NEW": 1,
    "PARTIALLY_FILLED": 2,
    "FILLED": 3,
    "CANCELED": 3,
    "EXPIRED": 3,
    "REJECTED": 3,
}
_EXCHANGE_NAMES = {1: "binance", 2: "okx"}
_MARKET_NAMES = {1: "spot", 2: "perpetual_futures", 3: "delivery_futures"}


def _decimal(value: Any, *, default: Decimal = ZERO) -> Decimal:
    if value is None or value == "":
        return default
    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc
    if not parsed.is_finite():
        raise ValueError(f"decimal value must be finite: {value!r}")
    return parsed


def _nonnegative(value: Any, field_name: str) -> Decimal:
    parsed = _decimal(value)
    if parsed < ZERO:
        raise ValueError(f"{field_name} must be non-negative")
    return parsed


def _norm_exchange(value: Any) -> str:
    if isinstance(value, int):
        return _EXCHANGE_NAMES.get(value, f"exchange:{value}")
    raw = str(value or "").strip().lower()
    if raw.isdigit():
        return _EXCHANGE_NAMES.get(int(raw), f"exchange:{raw}")
    return raw


def _norm_market(value: Any) -> str:
    if isinstance(value, int):
        return _MARKET_NAMES.get(value, f"market:{value}")
    raw = str(value or "").strip().lower()
    if raw.isdigit():
        return _MARKET_NAMES.get(int(raw), f"market:{raw}")
    if raw in {"spot"}:
        return "spot"
    if raw in {"futures", "perpetual", "perpetual_futures", "usdm_futures"}:
        return "perpetual_futures"
    return raw


def _exact_field(source: Any, exact_name: str, legacy_name: str = "") -> Decimal:
    exact = getattr(source, exact_name, "")
    if exact not in (None, ""):
        return _nonnegative(exact, exact_name)
    if legacy_name:
        return _nonnegative(getattr(source, legacy_name, 0) or 0, legacy_name)
    return ZERO


@dataclass(init=False)
class SpotAsset:
    free: Decimal
    locked: Decimal
    avg_entry_price: Decimal
    price: Decimal | None

    def __init__(
        self,
        qty: Any = ZERO,
        locked: Any = ZERO,
        avg_entry_price: Any = ZERO,
        price: Any | None = None,
        *,
        free: Any | None = None,
    ) -> None:
        locked_value = _nonnegative(locked, "locked")
        if free is None:
            total = _nonnegative(qty, "qty")
            free_value = total - locked_value
            if free_value < ZERO:
                raise ValueError("spot asset locked balance exceeds total quantity")
        else:
            free_value = _nonnegative(free, "free")
        self.free = free_value
        self.locked = locked_value
        self.avg_entry_price = _nonnegative(avg_entry_price, "avg_entry_price")
        self.price = None if price is None else _nonnegative(price, "price")

    @property
    def qty(self) -> Decimal:
        """Compatibility total: Binance ``free + locked``."""
        return self.free + self.locked

    @qty.setter
    def qty(self, value: Any) -> None:
        total = _nonnegative(value, "qty")
        if total < self.locked:
            raise ValueError("spot asset quantity cannot be below locked balance")
        self.free = total - self.locked

    def get_unrealized_pnl(self, current_price: Any) -> Decimal:
        price = _nonnegative(current_price, "current_price")
        return self.qty * (price - self.avg_entry_price)

    def get_estimated_value(self, current_price: Any) -> Decimal:
        return self.qty * _nonnegative(current_price, "current_price")


@dataclass
class SpotOpenOrder:
    route_key: tuple[int, str, str, str]
    order_identity: str
    side: str
    status: str
    orig_qty: Decimal = ZERO
    executed_qty: Decimal = ZERO
    remaining_qty: Decimal = ZERO
    cumulative_quote_qty: Decimal = ZERO
    price: Decimal = ZERO
    locked_quote: Decimal = ZERO
    locked_base: Decimal = ZERO

    @property
    def order_id(self) -> str:
        return self.order_identity

    @property
    def symbol(self) -> str:
        return self.route_key[3]


@dataclass(frozen=True)
class _CumulativeOrderState:
    executed_qty: Decimal
    cumulative_quote_qty: Decimal
    status: str


class SpotFilterViolation(ValueError):
    def __init__(self, code: str) -> None:
        self.code = str(code)
        super().__init__(f"{self.code}: Hosted Spot order rejected by immutable risk facts")


@dataclass
class SpotWallet:
    assets: dict[str, SpotAsset] = field(default_factory=dict)
    open_orders: dict[tuple[int, str, str, str, str], SpotOpenOrder] = field(default_factory=dict)
    symbol_metadata: dict[tuple[int, str, str, str], SpotSymbolMetadata] = field(default_factory=dict)
    symbol_prices: dict[tuple[int, str, str, str], Decimal] = field(default_factory=dict)
    order_states: dict[tuple[int, str, str, str, str], _CumulativeOrderState] = field(default_factory=dict)
    applied_trade_ids: set[tuple[int, str, str, str, str, str]] = field(default_factory=set)
    recovery_pending_orders: set[tuple[int, str, str, str, str]] = field(default_factory=set)
    risk_facts: dict[tuple[int, str, str, str], dict[str, Any]] = field(default_factory=dict)
    _synthetic_trade_ids: Any = field(default_factory=lambda: count(1), repr=False)

    def __init__(
        self,
        free: Any = ZERO,
        locked: Any = ZERO,
        assets: Mapping[str, SpotAsset] | None = None,
        open_orders: Mapping[Any, SpotOpenOrder] | None = None,
    ) -> None:
        self.assets = {}
        for raw_asset, value in (assets or {}).items():
            asset = norm_symbol(raw_asset)
            if not asset:
                raise ValueError("spot asset code is required")
            if isinstance(value, SpotAsset):
                self.assets[asset] = value
            else:
                raise TypeError(f"unsupported SpotAsset value for {asset}: {type(value).__name__}")
        quote = self.assets.get("USDT")
        if quote is None:
            self.assets["USDT"] = SpotAsset(free=free, locked=locked)
        elif _decimal(free) != ZERO or _decimal(locked) != ZERO:
            raise ValueError("USDT balance must be supplied either through assets or free/locked, not both")
        self.open_orders = dict(open_orders or {})
        self.symbol_metadata = {}
        self.symbol_prices = {}
        self.order_states = {}
        self.applied_trade_ids = set()
        self.recovery_pending_orders = set()
        self.risk_facts = {}
        self._synthetic_trade_ids = count(1)

    @property
    def free(self) -> Decimal:
        return self.assets["USDT"].free

    @free.setter
    def free(self, value: Any) -> None:
        self.assets.setdefault("USDT", SpotAsset()).free = _nonnegative(value, "free")

    @property
    def locked(self) -> Decimal:
        return self.assets["USDT"].locked

    @locked.setter
    def locked(self, value: Any) -> None:
        self.assets.setdefault("USDT", SpotAsset()).locked = _nonnegative(value, "locked")

    @classmethod
    def from_assets(cls, assets: Mapping[str, tuple[Any, Any] | SpotAsset]) -> "SpotWallet":
        normalized: dict[str, SpotAsset] = {}
        for raw_asset, value in assets.items():
            asset = norm_symbol(raw_asset)
            if isinstance(value, SpotAsset):
                normalized[asset] = value
            else:
                free, locked = value
                normalized[asset] = SpotAsset(free=free, locked=locked)
        return cls(assets=normalized)

    @classmethod
    def from_canonical(cls, state: Any) -> "SpotWallet":
        normalized: dict[str, SpotAsset] = {}
        for item in getattr(state, "assets", []) or []:
            asset_code = norm_symbol(getattr(item, "asset", "") or getattr(item, "symbol", ""))
            if not asset_code:
                raise ValueError("canonical Spot asset is missing asset code")
            # Compatibility for the old pseudo-asset snapshots. New snapshots
            # always set ``asset`` and therefore never use this suffix rule.
            if not getattr(item, "asset", "") and asset_code.endswith("USDT") and asset_code != "USDT":
                asset_code = asset_code[:-4]
            free_decimal = str(getattr(item, "free_decimal", "") or "")
            locked_decimal = str(getattr(item, "locked_decimal", "") or "")
            if free_decimal:
                free = free_decimal
            elif getattr(item, "free", None) is not None:
                free = getattr(item, "free")
            else:
                total = _nonnegative(getattr(item, "qty", 0) or 0, "qty")
                legacy_locked = _nonnegative(getattr(item, "locked", 0) or 0, "locked")
                free = total - legacy_locked
            locked = locked_decimal or getattr(item, "locked", 0) or 0
            normalized[asset_code] = SpotAsset(
                free=free,
                locked=locked,
                avg_entry_price=getattr(item, "avg_entry_price", 0) or 0,
                price=getattr(item, "price", None),
            )
        if "USDT" not in normalized:
            normalized["USDT"] = SpotAsset(
                free=getattr(state, "free", 0) or 0,
                locked=getattr(state, "locked", 0) or 0,
            )
        return cls(assets=normalized)

    def register_metadata(self, metadata: SpotSymbolMetadata) -> SpotSymbolMetadata:
        if not isinstance(metadata, SpotSymbolMetadata):
            raise TypeError("SpotSymbolMetadata is required")
        if metadata.route_key[2] != "spot":
            raise ValueError("Spot metadata route must use market=spot")
        if norm_symbol(metadata.quote_asset) != "USDT":
            raise ValueError("only Binance USDT Spot symbols are supported")
        if not norm_symbol(metadata.base_asset) or metadata.route_key[3] == "":
            raise ValueError("Spot metadata requires symbol/base_asset")
        self.symbol_metadata[metadata.route_key] = metadata
        return metadata

    def register_risk_facts(
        self,
        *,
        snapshot_id: str,
        metadata: SpotSymbolMetadata,
        exchange_filters: list[dict[str, Any]] | None = None,
        symbol_filters: list[dict[str, Any]] | None = None,
        asset_filters: list[dict[str, Any]] | None = None,
        reference_price_decimal: str,
    ) -> None:
        facts = self.register_metadata(metadata)
        snapshot_id = str(snapshot_id or "").strip()
        if not snapshot_id:
            raise ValueError("Spot risk snapshot_id is required")
        self.risk_facts[facts.route_key] = {
            "snapshot_id": snapshot_id,
            "metadata": {
                "symbol": facts.symbol,
                "status": facts.status,
                "base_asset": facts.base_asset,
                "quote_asset": facts.quote_asset,
                "base_asset_precision": facts.base_asset_precision,
                "quote_asset_precision": facts.quote_asset_precision,
                "spot_trading_allowed": facts.spot_trading_allowed,
                "permission_sets": [list(group) for group in facts.permission_sets],
                "order_types": list(facts.order_types),
                "filters": [asdict(item) for item in facts.filters],
            },
            "exchange_filters": list(exchange_filters or []),
            "symbol_filters": list(symbol_filters or []),
            "asset_filters": list(asset_filters or []),
            "reference_price_decimal": str(reference_price_decimal or ""),
        }

    def review_order(
        self,
        *,
        symbol: str,
        side: str,
        order_type: str,
        qty_decimal: str,
        price_decimal: str | None,
        reduce_only: bool = False,
    ) -> str:
        metadata = self._resolve_metadata(symbol, None)
        stored = self.risk_facts.get(metadata.route_key)
        if stored is None:
            raise SpotFilterViolation("SPOT_RISK_FACTS_UNAVAILABLE")
        facts = dict(stored)
        facts.pop("snapshot_id", None)
        facts["open_orders"] = [
            {
                "symbol": item.symbol,
                "side": item.side,
                "orig_qty_decimal": str(item.orig_qty),
                "executed_qty_decimal": str(item.executed_qty),
            }
            for item in self.open_orders.values()
        ]
        vector = {
            "request": {
                "symbol": norm_symbol(symbol),
                "side": str(side).strip().upper(),
                "order_type": str(order_type).strip().upper(),
                "qty_decimal": str(qty_decimal).strip(),
                "price_decimal": None if price_decimal is None else str(price_decimal).strip(),
                "reduce_only": bool(reduce_only),
            },
            "balances": [
                {
                    "asset": asset,
                    "available_decimal": str(entry.free),
                    "locked_decimal": str(entry.locked),
                }
                for asset, entry in self.assets.items()
            ],
            "facts": facts,
        }
        code = evaluate_spot_filter_vector(vector)
        if code:
            raise SpotFilterViolation(code)
        return str(stored["snapshot_id"])

    def _resolve_metadata(
        self,
        symbol: str,
        metadata: SpotSymbolMetadata | None,
    ) -> SpotSymbolMetadata:
        normalized_symbol = norm_symbol(symbol)
        if metadata is not None:
            metadata = self.register_metadata(metadata)
            if metadata.route_key[3] != normalized_symbol:
                raise ValueError("Spot metadata route does not match symbol")
            return metadata
        matches = [
            item for key, item in self.symbol_metadata.items() if key[3] == normalized_symbol
        ]
        if len(matches) != 1:
            reason = "missing" if not matches else "ambiguous"
            raise ValueError(f"{reason} Spot metadata route for {normalized_symbol}")
        return matches[0]

    def on_market_data(
        self,
        symbol: str,
        price: Any,
        metadata: SpotSymbolMetadata | None = None,
    ) -> None:
        facts = self._resolve_metadata(symbol, metadata)
        exact_price = _nonnegative(price, "price")
        if exact_price <= ZERO:
            raise ValueError("Spot price must be positive")
        self.symbol_prices[facts.route_key] = exact_price
        base = norm_symbol(facts.base_asset)
        if base in self.assets:
            self.assets[base].price = exact_price

    def get_unrealized_pnl(self) -> Decimal:
        total = ZERO
        for asset_code, asset in self.assets.items():
            if asset_code == "USDT" or asset.qty == ZERO:
                continue
            if asset.price is None:
                raise ValueError(f"spot mark price not set for asset {asset_code!r}")
            total += asset.get_unrealized_pnl(asset.price)
        return total

    def get_estimated_value(self) -> Decimal:
        total = self.assets["USDT"].qty
        for asset_code, asset in self.assets.items():
            if asset_code == "USDT" or asset.qty == ZERO:
                continue
            if asset.price is None:
                raise ValueError(f"spot mark price not set for asset {asset_code!r}")
            total += asset.get_estimated_value(asset.price)
        return total

    def asset_for_symbol(
        self,
        symbol: str,
        metadata: SpotSymbolMetadata | None = None,
    ) -> SpotAsset | None:
        facts = self._resolve_metadata(symbol, metadata)
        return self.assets.get(norm_symbol(facts.base_asset))

    @staticmethod
    def _event_route(update: Any, metadata: SpotSymbolMetadata) -> tuple[int, str, str, str]:
        venue_id = int(getattr(update, "venue_id", 0) or metadata.venue_id)
        exchange = _norm_exchange(getattr(update, "exchange", "")) or metadata.route_key[1]
        market = _norm_market(getattr(update, "market", "")) or metadata.route_key[2]
        symbol = norm_symbol(getattr(update, "symbol", "") or metadata.symbol)
        route = (venue_id, exchange, market, symbol)
        if route != metadata.route_key:
            raise ValueError(f"Spot order route {route!r} does not match metadata route {metadata.route_key!r}")
        return route

    @staticmethod
    def _order_identity(update: Any) -> str:
        return str(
            getattr(update, "exchange_order_id", "")
            or getattr(update, "order_id", "")
            or ""
        ).strip()

    @staticmethod
    def _status(update: Any) -> str:
        return str(getattr(update, "status", "") or "").strip().upper()

    @staticmethod
    def _side(update: Any) -> str:
        side = str(getattr(update, "side", "") or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            raise ValueError(f"unknown Spot order side: {side!r}")
        return side

    @staticmethod
    def _debit(planned: dict[str, list[Decimal]], asset: str, amount: Decimal, *, prefer_locked: bool) -> None:
        if amount == ZERO:
            return
        if asset not in planned:
            raise ValueError(f"missing Spot asset balance for debit: {asset}")
        free, locked = planned[asset]
        if prefer_locked:
            from_locked = min(locked, amount)
            locked -= from_locked
            amount -= from_locked
        if free < amount:
            raise ValueError(f"insufficient Spot {asset} balance")
        free -= amount
        planned[asset] = [free, locked]

    def _apply_fill(
        self,
        *,
        metadata: SpotSymbolMetadata,
        side: str,
        qty: Decimal,
        quote_qty: Decimal,
        fee: Decimal,
        fee_asset: str,
        order: SpotOpenOrder | None,
    ) -> None:
        base = norm_symbol(metadata.base_asset)
        quote = norm_symbol(metadata.quote_asset)
        planned = {
            asset: [entry.free, entry.locked]
            for asset, entry in self.assets.items()
        }
        planned.setdefault(base, [ZERO, ZERO])
        planned.setdefault(quote, [ZERO, ZERO])
        if side == "BUY":
            self._debit(planned, quote, quote_qty, prefer_locked=order is not None)
            planned[base][0] += qty
        else:
            self._debit(planned, base, qty, prefer_locked=order is not None)
            planned[quote][0] += quote_qty
        if fee > ZERO:
            self._debit(planned, fee_asset, fee, prefer_locked=False)

        previous_base_qty = self.assets.get(base, SpotAsset()).qty
        for asset, (free, locked) in planned.items():
            entry = self.assets.setdefault(asset, SpotAsset())
            entry.free = free
            entry.locked = locked
        base_entry = self.assets[base]
        if side == "BUY" and qty > ZERO:
            fill_price = quote_qty / qty
            gross_after = previous_base_qty + qty
            if previous_base_qty == ZERO:
                base_entry.avg_entry_price = fill_price
            elif gross_after > ZERO:
                base_entry.avg_entry_price = (
                    base_entry.avg_entry_price * previous_base_qty + quote_qty
                ) / gross_after
        elif side == "SELL" and base_entry.qty == ZERO:
            base_entry.avg_entry_price = ZERO

    def _release_order_locks(self, order: SpotOpenOrder, metadata: SpotSymbolMetadata) -> None:
        quote = self.assets[norm_symbol(metadata.quote_asset)]
        base = self.assets.setdefault(norm_symbol(metadata.base_asset), SpotAsset())
        if order.locked_quote > ZERO:
            release = min(order.locked_quote, quote.locked)
            quote.locked -= release
            quote.free += release
            order.locked_quote -= release
        if order.locked_base > ZERO:
            release = min(order.locked_base, base.locked)
            base.locked -= release
            base.free += release
            order.locked_base -= release

    def _sync_order_locks(self, order: SpotOpenOrder, metadata: SpotSymbolMetadata) -> None:
        if order.status not in _ACTIVE_ORDER_STATUSES:
            self._release_order_locks(order, metadata)
            return
        if order.side == "BUY":
            desired = order.remaining_qty * order.price
            quote = self.assets[norm_symbol(metadata.quote_asset)]
            delta = desired - order.locked_quote
            if delta > ZERO:
                if quote.free < delta:
                    raise ValueError("insufficient Spot quote balance to lock order")
                quote.free -= delta
                quote.locked += delta
            elif delta < ZERO:
                release = min(-delta, quote.locked)
                quote.locked -= release
                quote.free += release
            order.locked_quote = desired
            return
        base = self.assets.setdefault(norm_symbol(metadata.base_asset), SpotAsset())
        desired = order.remaining_qty
        delta = desired - order.locked_base
        if delta > ZERO:
            if base.free < delta:
                raise ValueError("insufficient Spot base balance to lock order")
            base.free -= delta
            base.locked += delta
        elif delta < ZERO:
            release = min(-delta, base.locked)
            base.locked -= release
            base.free += release
        order.locked_base = desired

    def apply_order_update(
        self,
        update: Any,
        metadata: SpotSymbolMetadata | None = None,
    ) -> bool:
        facts = self._resolve_metadata(getattr(update, "symbol", ""), metadata)
        route = self._event_route(update, facts)
        status = self._status(update)
        if status not in _SUPPORTED_ORDER_STATUSES:
            raise ValueError(f"unsupported Spot order status: {status!r}")
        side = self._side(update)
        order_identity = self._order_identity(update)
        if not order_identity:
            raise ValueError("Spot lifecycle order_id/exchange_order_id is required")
        order_key = (*route, order_identity)
        previous_state = self.order_states.get(order_key, _CumulativeOrderState(ZERO, ZERO, ""))

        fill_qty = _exact_field(update, "qty_decimal", "qty")
        fill_price = _exact_field(update, "fill_price_decimal", "fill_price")
        fill_quote = _exact_field(update, "quote_qty_decimal")
        if fill_qty > ZERO and fill_quote == ZERO:
            if fill_price <= ZERO:
                raise ValueError("Spot fill requires exact quote quantity or fill price")
            fill_quote = fill_qty * fill_price
        fee = _exact_field(update, "fee_decimal", "fee")
        fee_asset = norm_symbol(getattr(update, "fee_asset", "") or facts.quote_asset)

        cumulative_qty_raw = getattr(update, "executed_qty_decimal", "")
        cumulative_qty = (
            _nonnegative(cumulative_qty_raw, "executed_qty_decimal")
            if cumulative_qty_raw not in (None, "")
            else _nonnegative(getattr(update, "executed_qty", 0) or 0, "executed_qty")
        )
        if cumulative_qty == ZERO and fill_qty > ZERO:
            cumulative_qty = previous_state.executed_qty + fill_qty
        cumulative_quote_raw = getattr(update, "cumulative_quote_qty_decimal", "")
        cumulative_quote = (
            _nonnegative(cumulative_quote_raw, "cumulative_quote_qty_decimal")
            if cumulative_quote_raw not in (None, "")
            else previous_state.cumulative_quote_qty + fill_quote
        )

        if (
            cumulative_qty < previous_state.executed_qty
            or cumulative_quote < previous_state.cumulative_quote_qty
        ):
            return False

        cumulative_advanced = (
            cumulative_qty > previous_state.executed_qty
            or cumulative_quote > previous_state.cumulative_quote_qty
        )
        if fill_qty == ZERO and cumulative_advanced:
            # Cumulative state proves that execution advanced, but it cannot
            # prove the individual trade identity, fee asset, or fee amount.
            # Keep the prior durable wallet state until recovery supplies the
            # missing fill delta; otherwise that later delta would look like a
            # replay and its asset/fee mutation would be lost permanently.
            self.recovery_pending_orders.add(order_key)
            return False

        if _ORDER_STATUS_RANK[status] < _ORDER_STATUS_RANK[previous_state.status]:
            if fill_qty == ZERO:
                return False
            # A delayed fill may legitimately arrive after a terminal state.
            # Apply its unseen trade delta, but never reopen/regress the order.
            status = previous_state.status

        trade_id = str(getattr(update, "exchange_trade_id", "") or "").strip()
        trade_key = (*route, order_identity, trade_id)
        fill_applied = False
        if fill_qty > ZERO:
            if trade_id == "":
                self.recovery_pending_orders.add(order_key)
                return False
            if trade_key in self.applied_trade_ids:
                if (
                    cumulative_qty > previous_state.executed_qty
                    or cumulative_quote > previous_state.cumulative_quote_qty
                ):
                    self.recovery_pending_orders.add(order_key)
                    return False
            else:
                qty_delta = cumulative_qty - previous_state.executed_qty
                quote_delta = cumulative_quote - previous_state.cumulative_quote_qty
                if qty_delta != fill_qty or quote_delta != fill_quote:
                    self.recovery_pending_orders.add(order_key)
                    return False
                fill_applied = True

        orig_qty = _exact_field(update, "orig_qty_decimal", "orig_qty")
        remaining_qty = _exact_field(update, "remaining_qty_decimal", "remaining_qty")
        if orig_qty == ZERO:
            orig_qty = cumulative_qty + remaining_qty
        if remaining_qty == ZERO and status in _ACTIVE_ORDER_STATUSES and orig_qty > cumulative_qty:
            remaining_qty = orig_qty - cumulative_qty
        price = _exact_field(update, "price_decimal", "price")
        if price == ZERO:
            price = fill_price
        existing = self.open_orders.get(order_key)
        order = SpotOpenOrder(
            route_key=route,
            order_identity=order_identity,
            side=side,
            status=status,
            orig_qty=orig_qty,
            executed_qty=cumulative_qty,
            remaining_qty=remaining_qty,
            cumulative_quote_qty=cumulative_quote,
            price=price,
            locked_quote=existing.locked_quote if existing is not None else ZERO,
            locked_base=existing.locked_base if existing is not None else ZERO,
        )

        if fill_applied:
            self._apply_fill(
                metadata=facts,
                side=side,
                qty=fill_qty,
                quote_qty=fill_quote,
                fee=fee,
                fee_asset=fee_asset,
                order=existing,
            )
            if existing is not None and side == "BUY":
                order.locked_quote = max(ZERO, order.locked_quote - fill_quote)
            elif existing is not None:
                order.locked_base = max(ZERO, order.locked_base - fill_qty)
            self.applied_trade_ids.add(trade_key)
            self.recovery_pending_orders.discard(order_key)

        self.order_states[order_key] = _CumulativeOrderState(cumulative_qty, cumulative_quote, status)
        self._sync_order_locks(order, facts)
        if status in _TERMINAL_ORDER_STATUSES or remaining_qty == ZERO:
            self._release_order_locks(order, facts)
            self.open_orders.pop(order_key, None)
        else:
            self.open_orders[order_key] = order
        return fill_applied

    def _legacy_metadata(self, symbol: str, update: Any) -> SpotSymbolMetadata:
        normalized = norm_symbol(symbol)
        if not normalized.endswith("USDT") or normalized == "USDT":
            raise ValueError(f"missing Spot metadata route for {normalized}")
        return SpotSymbolMetadata(
            venue_id=int(getattr(update, "venue_id", 0) or 1),
            exchange=_norm_exchange(getattr(update, "exchange", "")) or "binance",
            market=_norm_market(getattr(update, "market", "")) or "spot",
            symbol=normalized,
            status="TRADING",
            base_asset=normalized[:-4],
            quote_asset="USDT",
            base_asset_precision=8,
            quote_asset_precision=8,
            spot_trading_allowed=True,
            permission_sets=(("SPOT",),),
            order_types=("LIMIT", "MARKET"),
        )

    def on_order(self, symbol: str, order_resp: Any) -> None:
        status = self._status(order_resp)
        if status not in _SUPPORTED_ORDER_STATUSES:
            return
        try:
            metadata = self._resolve_metadata(symbol, None)
        except ValueError:
            # Read compatibility for old in-memory backtest fixtures. Production
            # snapshots register authoritative metadata before this path.
            metadata = self.register_metadata(self._legacy_metadata(symbol, order_resp))
        identity = str(
            getattr(order_resp, "exchange_order_id", "")
            or getattr(order_resp, "order_id", "")
            or getattr(order_resp, "client_order_id", "")
            or ""
        ).strip()
        if not identity and status == "FILLED":
            identity = f"legacy-direct-{next(self._synthetic_trade_ids)}"
        if not identity:
            raise ValueError("Spot lifecycle order_id/exchange_order_id is required")
        trade_id = str(getattr(order_resp, "exchange_trade_id", "") or "").strip()
        qty = _exact_field(order_resp, "qty_decimal", "qty")
        if qty > ZERO and trade_id == "":
            trade_id = f"legacy-{next(self._synthetic_trade_ids)}"
        normalized = OrderResponse(
            symbol=norm_symbol(symbol),
            side=str(getattr(order_resp, "side", "") or ""),
            qty=float(qty),
            fill_price=float(getattr(order_resp, "fill_price", 0) or getattr(order_resp, "price", 0) or 0),
            status=status,
            fee=float(getattr(order_resp, "fee", 0) or 0),
            order_id=str(getattr(order_resp, "order_id", "") or identity),
            orig_qty=float(getattr(order_resp, "orig_qty", 0) or 0),
            executed_qty=float(getattr(order_resp, "executed_qty", 0) or 0),
            remaining_qty=float(getattr(order_resp, "remaining_qty", 0) or 0),
            price=float(getattr(order_resp, "price", 0) or getattr(order_resp, "fill_price", 0) or 0),
            venue_id=metadata.venue_id,
            exchange=metadata.exchange,
            market=metadata.market,
            exchange_order_id=identity,
            exchange_trade_id=trade_id,
            fee_asset=str(getattr(order_resp, "fee_asset", "") or metadata.quote_asset),
            qty_decimal=str(getattr(order_resp, "qty_decimal", "") or qty),
            fill_price_decimal=str(
                getattr(order_resp, "fill_price_decimal", "")
                or getattr(order_resp, "fill_price", 0)
                or getattr(order_resp, "price", 0)
                or ""
            ),
            fee_decimal=str(getattr(order_resp, "fee_decimal", "") or getattr(order_resp, "fee", 0) or "0"),
            quote_qty_decimal=str(getattr(order_resp, "quote_qty_decimal", "") or ""),
            orig_qty_decimal=str(getattr(order_resp, "orig_qty_decimal", "") or getattr(order_resp, "orig_qty", 0) or ""),
            executed_qty_decimal=str(getattr(order_resp, "executed_qty_decimal", "") or getattr(order_resp, "executed_qty", 0) or ""),
            remaining_qty_decimal=str(getattr(order_resp, "remaining_qty_decimal", "") or getattr(order_resp, "remaining_qty", 0) or ""),
            cumulative_quote_qty_decimal=str(getattr(order_resp, "cumulative_quote_qty_decimal", "") or ""),
        )
        self.apply_order_update(normalized, metadata)

    def on_fill(
        self,
        *,
        symbol: str,
        side: str,
        qty: Any,
        fill_price: Any,
        fee: Any = ZERO,
        fee_asset: str = "USDT",
        metadata: SpotSymbolMetadata | None = None,
    ) -> None:
        if metadata is None:
            try:
                metadata = self._resolve_metadata(symbol, None)
            except ValueError:
                metadata = self.register_metadata(self._legacy_metadata(symbol, object()))
        sequence = next(self._synthetic_trade_ids)
        qty_exact = _nonnegative(qty, "qty")
        price_exact = _nonnegative(fill_price, "fill_price")
        self.apply_order_update(
            OrderResponse(
                symbol=norm_symbol(symbol),
                side=str(side).strip().upper(),
                qty=float(qty_exact),
                fill_price=float(price_exact),
                status="FILLED",
                fee=float(_nonnegative(fee, "fee")),
                order_id=f"local-fill-{sequence}",
                venue_id=metadata.venue_id,
                exchange=metadata.exchange,
                market=metadata.market,
                exchange_order_id=f"local-fill-{sequence}",
                exchange_trade_id=str(sequence),
                fee_asset=fee_asset,
                qty_decimal=str(qty_exact),
                fill_price_decimal=str(price_exact),
                quote_qty_decimal=str(qty_exact * price_exact),
                fee_decimal=str(_nonnegative(fee, "fee")),
                orig_qty_decimal=str(qty_exact),
                executed_qty_decimal=str(qty_exact),
                remaining_qty_decimal="0",
                cumulative_quote_qty_decimal=str(qty_exact * price_exact),
            ),
            metadata,
        )
