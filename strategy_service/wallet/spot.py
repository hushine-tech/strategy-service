"""Exact Binance Spot wallet ledger.

Balances are keyed by account asset code (``BTC``, ``USDT``, ``BNB``).
Trading symbols (``BTCUSDT``) exist only in immutable metadata, price indexes,
and order identities.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from decimal import Decimal, InvalidOperation
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


def _exact_field(source: Any, exact_name: str) -> Decimal:
    exact = getattr(source, exact_name, "")
    if exact in (None, ""):
        raise ValueError(f"{exact_name} is required")
    return _nonnegative(exact, exact_name)


def _optional_exact_field(source: Any, exact_name: str) -> Decimal:
    exact = getattr(source, exact_name, "")
    if exact in (None, ""):
        return ZERO
    return _nonnegative(exact, exact_name)


@dataclass
class SpotAsset:
    free: Decimal = ZERO
    locked: Decimal = ZERO
    avg_entry_price: Decimal = ZERO
    price: Decimal | None = None

    def __post_init__(self) -> None:
        self.free = _nonnegative(self.free, "free")
        self.locked = _nonnegative(self.locked, "locked")
        self.avg_entry_price = _nonnegative(self.avg_entry_price, "avg_entry_price")
        self.price = None if self.price is None else _nonnegative(self.price, "price")

    @property
    def total(self) -> Decimal:
        return self.free + self.locked

    def get_unrealized_pnl(self, current_price: Any) -> Decimal:
        price = _nonnegative(current_price, "current_price")
        return self.total * (price - self.avg_entry_price)

    def get_estimated_value(self, current_price: Any) -> Decimal:
        return self.total * _nonnegative(current_price, "current_price")


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
    def __init__(
        self,
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
        self.open_orders = dict(open_orders or {})
        self.symbol_metadata = {}
        self.symbol_prices = {}
        self.order_states = {}
        self.applied_trade_ids = set()
        self.recovery_pending_orders = set()
        self.risk_facts = {}

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
            asset_code = norm_symbol(getattr(item, "asset", ""))
            if not asset_code:
                raise ValueError("canonical Spot asset is missing asset code")
            if asset_code in normalized:
                raise ValueError(f"duplicate canonical Spot asset: {asset_code}")
            free = _exact_field(item, "free_decimal")
            locked = _exact_field(item, "locked_decimal")
            avg_entry = getattr(item, "avg_entry_price_decimal", "") or "0"
            price = getattr(item, "price_decimal", None)
            normalized[asset_code] = SpotAsset(
                free=free,
                locked=locked,
                avg_entry_price=avg_entry,
                price=price,
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
        environment: int,
        metadata: SpotSymbolMetadata,
        exchange_filters: list[dict[str, Any]] | None = None,
        symbol_filters: list[dict[str, Any]] | None = None,
        asset_filters: list[dict[str, Any]] | None = None,
        open_orders: list[dict[str, Any]] | None = None,
        reference_price_decimal: str,
    ) -> None:
        facts = self.register_metadata(metadata)
        snapshot_id = str(snapshot_id or "").strip()
        if not snapshot_id:
            raise ValueError("Spot risk snapshot_id is required")
        environment = int(environment)
        if environment not in {0, 1, 2}:
            raise ValueError("Spot risk environment is invalid")
        self.risk_facts[facts.route_key] = {
            "snapshot_id": snapshot_id,
            "environment": environment,
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
            "open_orders": list(open_orders or []),
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
        if (
            int(facts.get("environment", -1)) == 0
            and not str(facts.get("reference_price_decimal", "") or "").strip()
        ):
            replay_price = self.symbol_prices.get(metadata.route_key)
            if replay_price is not None:
                facts["reference_price_decimal"] = str(replay_price)
        facts["open_orders"] = list(facts.get("open_orders") or []) + [
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
            if asset_code == "USDT" or asset.total == ZERO:
                continue
            if asset.price is None:
                raise ValueError(f"spot mark price not set for asset {asset_code!r}")
            total += asset.get_unrealized_pnl(asset.price)
        return total

    def get_estimated_value(self) -> Decimal:
        quote = self.assets.get("USDT")
        total = quote.total if quote is not None else ZERO
        for asset_code, asset in self.assets.items():
            if asset_code == "USDT" or asset.total == ZERO:
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

        previous_base_qty = self.assets.get(base, SpotAsset()).total
        for asset, (free, locked) in planned.items():
            entry = self.assets.setdefault(asset, SpotAsset())
            entry.free = free
            entry.locked = locked
        base_entry = self.assets[base]
        if base_entry.price is None:
            base_entry.price = self.symbol_prices.get(metadata.route_key)
        if side == "BUY" and qty > ZERO:
            fill_price = quote_qty / qty
            gross_after = previous_base_qty + qty
            if previous_base_qty == ZERO:
                base_entry.avg_entry_price = fill_price
            elif gross_after > ZERO:
                base_entry.avg_entry_price = (
                    base_entry.avg_entry_price * previous_base_qty + quote_qty
                ) / gross_after
        elif side == "SELL" and base_entry.total == ZERO:
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

        fill_qty = _exact_field(update, "qty_decimal")
        fill_price = _exact_field(update, "fill_price_decimal")
        fill_quote = _exact_field(update, "quote_qty_decimal")
        if fill_qty > ZERO and fill_quote == ZERO:
            raise ValueError("Spot fill requires exact quote quantity")
        fee = _exact_field(update, "fee_decimal")
        fee_asset = norm_symbol(getattr(update, "fee_asset", "") or facts.quote_asset)

        cumulative_qty = _exact_field(update, "executed_qty_decimal")
        if cumulative_qty == ZERO and fill_qty > ZERO:
            cumulative_qty = previous_state.executed_qty + fill_qty
        cumulative_quote = _exact_field(update, "cumulative_quote_qty_decimal")

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

        orig_qty = _exact_field(update, "orig_qty_decimal")
        remaining_qty = _exact_field(update, "remaining_qty_decimal")
        if orig_qty == ZERO:
            orig_qty = cumulative_qty + remaining_qty
        if remaining_qty == ZERO and status in _ACTIVE_ORDER_STATUSES and orig_qty > cumulative_qty:
            remaining_qty = orig_qty - cumulative_qty
        price = _optional_exact_field(update, "price_decimal")
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

    def on_order(self, symbol: str, order_resp: Any) -> None:
        status = self._status(order_resp)
        if status not in _SUPPORTED_ORDER_STATUSES:
            return
        metadata = self._resolve_metadata(symbol, None)
        identity = str(
            getattr(order_resp, "exchange_order_id", "")
            or getattr(order_resp, "order_id", "")
            or getattr(order_resp, "client_order_id", "")
            or ""
        ).strip()
        if not identity:
            raise ValueError("Spot lifecycle order_id/exchange_order_id is required")
        trade_id = str(getattr(order_resp, "exchange_trade_id", "") or "").strip()
        qty = _exact_field(order_resp, "qty_decimal")
        if qty > ZERO and trade_id == "":
            raise ValueError("Spot fill exchange_trade_id is required")
        fill_price = _exact_field(order_resp, "fill_price_decimal")
        fee = _exact_field(order_resp, "fee_decimal")
        orig_qty = _exact_field(order_resp, "orig_qty_decimal")
        executed_qty = _exact_field(order_resp, "executed_qty_decimal")
        remaining_qty = _exact_field(order_resp, "remaining_qty_decimal")
        price = _optional_exact_field(order_resp, "price_decimal")
        normalized = OrderResponse(
            symbol=norm_symbol(symbol),
            side=str(getattr(order_resp, "side", "") or ""),
            qty=float(qty),
            fill_price=float(fill_price),
            status=status,
            fee=float(fee),
            order_id=str(getattr(order_resp, "order_id", "") or identity),
            orig_qty=float(orig_qty),
            executed_qty=float(executed_qty),
            remaining_qty=float(remaining_qty),
            price=float(price),
            venue_id=metadata.venue_id,
            exchange=metadata.exchange,
            market=metadata.market,
            exchange_order_id=identity,
            exchange_trade_id=trade_id,
            fee_asset=str(getattr(order_resp, "fee_asset", "") or metadata.quote_asset),
            qty_decimal=str(qty),
            fill_price_decimal=str(fill_price),
            fee_decimal=str(fee),
            quote_qty_decimal=str(_exact_field(order_resp, "quote_qty_decimal")),
            orig_qty_decimal=str(orig_qty),
            executed_qty_decimal=str(executed_qty),
            remaining_qty_decimal=str(remaining_qty),
            price_decimal=(str(price) if getattr(order_resp, "price_decimal", "") else ""),
            cumulative_quote_qty_decimal=str(
                _exact_field(order_resp, "cumulative_quote_qty_decimal")
            ),
        )
        self.apply_order_update(normalized, metadata)
