"""Spot ledger: quote cash plus per-symbol base holdings."""

from __future__ import annotations

from dataclasses import dataclass, field


_QTY_EPS = 1e-12
_ACTIVE_ORDER_STATUSES = {"NEW", "PARTIALLY_FILLED"}
_TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "EXPIRED"}
_SUPPORTED_ORDER_STATUSES = _ACTIVE_ORDER_STATUSES | _TERMINAL_ORDER_STATUSES


def _norm_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


@dataclass
class SpotAsset:
    qty: float = 0.0
    locked: float = 0.0
    avg_entry_price: float = 0.0
    price: float | None = None

    def get_unrealized_pnl(self, current_price: float) -> float:
        return self.qty * (float(current_price) - self.avg_entry_price)

    def get_estimated_value(self, current_price: float) -> float:
        return self.qty * float(current_price)


@dataclass
class SpotOpenOrder:
    order_id: str
    symbol: str
    side: str
    status: str
    orig_qty: float = 0.0
    executed_qty: float = 0.0
    remaining_qty: float = 0.0
    price: float = 0.0
    locked_quote: float = 0.0
    locked_base: float = 0.0


@dataclass
class SpotWallet:
    """Quote balances (e.g. USDT) plus base assets; prices from on_market_data."""

    free: float = 0.0
    locked: float = 0.0
    assets: dict[str, SpotAsset] = field(default_factory=dict)
    open_orders: dict[str, SpotOpenOrder] = field(default_factory=dict)

    @classmethod
    def from_canonical(cls, state) -> "SpotWallet":
        wallet = cls(free=float(state.free), locked=float(state.locked))
        for asset in state.assets:
            wallet.assets[_norm_symbol(asset.symbol)] = SpotAsset(
                qty=float(asset.qty),
                locked=float(asset.locked),
                avg_entry_price=float(asset.avg_entry_price),
                price=float(asset.price) if asset.price is not None else None,
            )
        return wallet

    def _get_or_create_asset(self, symbol: str) -> SpotAsset:
        sym = _norm_symbol(symbol)
        asset = self.assets.get(sym)
        if asset is not None:
            return asset
        for key, value in self.assets.items():
            if _norm_symbol(key) == sym:
                if key != sym:
                    self.assets[sym] = value
                    del self.assets[key]
                return value
        asset = SpotAsset()
        self.assets[sym] = asset
        return asset

    def get_unrealized_pnl(self) -> float:
        total = 0.0
        for sym, asset in self.assets.items():
            if abs(asset.qty) <= _QTY_EPS:
                continue
            if asset.price is None:
                raise ValueError(f"spot mark price not set for {sym!r}; call on_market_data first")
            total += asset.get_unrealized_pnl(asset.price)
        return total

    def get_estimated_value(self) -> float:
        ev = float(self.free) + float(self.locked)
        for sym, asset in self.assets.items():
            if abs(asset.qty) <= _QTY_EPS:
                continue
            if asset.price is None:
                raise ValueError(f"spot mark price not set for {sym!r}; call on_market_data first")
            ev += asset.get_estimated_value(asset.price)
        return ev

    def on_market_data(self, symbol: str, price: float) -> None:
        self._get_or_create_asset(symbol).price = float(price)

    def _normalize_order_id(
        self,
        *,
        status: str,
        raw_orig_qty: float,
        raw_executed_qty: float,
        raw_remaining_qty: float,
        order_resp: object,
    ) -> str:
        raw = str(
            getattr(order_resp, "order_id", "")
            or getattr(order_resp, "client_order_id", "")
            or ""
        ).strip()
        if raw:
            return raw
        if (
            status != "FILLED"
            or raw_orig_qty > _QTY_EPS
            or raw_executed_qty > _QTY_EPS
            or raw_remaining_qty > _QTY_EPS
        ):
            raise ValueError("spot lifecycle order events require explicit order_id")
        return ""

    def _extract_order_state(self, symbol: str, order_resp: object) -> tuple[SpotOpenOrder, float, float, float, bool]:
        status = str(getattr(order_resp, "status", "") or "").strip().upper()
        if status not in _SUPPORTED_ORDER_STATUSES:
            raise ValueError(f"unsupported spot order status: {status!r}")
        side = str(getattr(order_resp, "side", "") or "").strip().upper()
        if side not in ("BUY", "SELL", "LONG", "SHORT"):
            raise ValueError(f"unknown spot order side: {side!r}")
        event_qty = abs(float(getattr(order_resp, "qty", 0.0) or 0.0))
        raw_orig_qty = abs(float(getattr(order_resp, "orig_qty", 0.0) or 0.0))
        raw_executed_qty = abs(float(getattr(order_resp, "executed_qty", 0.0) or 0.0))
        raw_remaining_qty = abs(float(getattr(order_resp, "remaining_qty", 0.0) or 0.0))
        orig_qty = raw_orig_qty
        executed_qty = raw_executed_qty
        remaining_qty = raw_remaining_qty
        price = float(getattr(order_resp, "price", 0.0) or getattr(order_resp, "fill_price", 0.0) or 0.0)

        if orig_qty <= _QTY_EPS:
            if remaining_qty > _QTY_EPS and executed_qty > _QTY_EPS:
                orig_qty = remaining_qty + executed_qty
            elif remaining_qty > _QTY_EPS and event_qty > _QTY_EPS:
                orig_qty = remaining_qty + event_qty
            elif event_qty > _QTY_EPS:
                orig_qty = event_qty

        order_id = self._normalize_order_id(
            status=status,
            raw_orig_qty=raw_orig_qty,
            raw_executed_qty=raw_executed_qty,
            raw_remaining_qty=raw_remaining_qty,
            order_resp=order_resp,
        )
        existing = self.open_orders.get(order_id)
        if existing is not None:
            if orig_qty <= _QTY_EPS:
                orig_qty = float(existing.orig_qty or 0.0)
            if price <= 0.0:
                price = float(existing.price or 0.0)

        previous_executed = float(existing.executed_qty or 0.0) if existing is not None else 0.0
        if executed_qty > 0.0:
            fill_delta = max(0.0, executed_qty - previous_executed)
            executed_total = executed_qty
        elif status in {"PARTIALLY_FILLED", "FILLED"} and event_qty > 0.0:
            fill_delta = event_qty
            executed_total = previous_executed + event_qty
        else:
            fill_delta = 0.0
            executed_total = previous_executed

        if remaining_qty <= _QTY_EPS:
            if status in _TERMINAL_ORDER_STATUSES:
                remaining_qty = 0.0
            elif orig_qty > _QTY_EPS:
                remaining_qty = max(0.0, orig_qty - executed_total)

        order = SpotOpenOrder(
            order_id=order_id,
            symbol=_norm_symbol(symbol),
            side=side,
            status=status,
            orig_qty=orig_qty,
            executed_qty=executed_total,
            remaining_qty=remaining_qty,
            price=price,
            locked_quote=float(existing.locked_quote or 0.0) if existing is not None else 0.0,
            locked_base=float(existing.locked_base or 0.0) if existing is not None else 0.0,
        )
        return order, fill_delta, float(getattr(order_resp, "fill_price", 0.0) or price or 0.0), float(
            getattr(order_resp, "fee", 0.0) or 0.0
        ), status in _TERMINAL_ORDER_STATUSES

    def _sync_quote_lock(self, order: SpotOpenOrder, desired_quote: float) -> None:
        desired = max(0.0, float(desired_quote or 0.0))
        delta = desired - float(order.locked_quote or 0.0)
        if delta > _QTY_EPS:
            if self.free + _QTY_EPS < delta:
                raise ValueError("insufficient spot free balance to lock quote funds")
            self.free -= delta
            self.locked += delta
        elif delta < -_QTY_EPS:
            release = -delta
            self.free += release
            self.locked = max(0.0, self.locked - release)
        order.locked_quote = desired

    def _sync_base_lock(self, order: SpotOpenOrder, desired_base: float) -> None:
        desired = max(0.0, float(desired_base or 0.0))
        asset = self._get_or_create_asset(order.symbol)
        delta = desired - float(order.locked_base or 0.0)
        if delta > _QTY_EPS:
            available = float(asset.qty) - float(asset.locked)
            if available + _QTY_EPS < delta:
                raise ValueError("insufficient spot base available to lock")
            asset.locked += delta
        elif delta < -_QTY_EPS:
            asset.locked = max(0.0, asset.locked + delta)
        order.locked_base = desired

    def _desired_order_lock(self, order: SpotOpenOrder) -> tuple[float, float]:
        if order.side in ("BUY", "LONG"):
            return max(0.0, float(order.remaining_qty or 0.0) * float(order.price or 0.0)), 0.0
        return 0.0, max(0.0, float(order.remaining_qty or 0.0))

    def _apply_buy_fill(self, asset: SpotAsset, qty: float, fill_price: float, fee: float) -> None:
        total_cost = float(qty) * float(fill_price) + float(fee)
        if total_cost > self.locked + _QTY_EPS:
            shortfall = total_cost - self.locked
            if self.free + _QTY_EPS < shortfall:
                raise ValueError("insufficient spot quote balance for buy")
            self.free -= shortfall
            self.locked += shortfall
        self.locked = max(0.0, self.locked - total_cost)
        if asset.qty <= _QTY_EPS:
            asset.avg_entry_price = float(fill_price)
            asset.qty = float(qty)
        else:
            total_val = asset.avg_entry_price * asset.qty + float(fill_price) * float(qty)
            asset.qty += float(qty)
            asset.avg_entry_price = total_val / asset.qty

    def _apply_sell_fill(self, asset: SpotAsset, qty: float, fill_price: float, fee: float) -> None:
        q = float(qty)
        if q > float(asset.locked) + _QTY_EPS:
            shortfall = q - float(asset.locked)
            available = float(asset.qty) - float(asset.locked)
            if available + _QTY_EPS < shortfall:
                raise ValueError("insufficient spot base available to sell")
            asset.locked += shortfall
        asset.locked = max(0.0, asset.locked - q)
        asset.qty -= q
        proceeds = q * float(fill_price) - float(fee)
        new_free = float(self.free) + proceeds
        if new_free < -_QTY_EPS:
            raise ValueError("spot quote balance would go negative after sell fee")
        self.free = new_free
        if asset.qty <= _QTY_EPS:
            asset.qty = 0.0
            asset.avg_entry_price = 0.0

    def _apply_direct_fill(self, *, symbol: str, side: str, qty: float, fill_price: float, fee: float) -> None:
        sym = _norm_symbol(symbol)
        asset = self._get_or_create_asset(sym)
        if qty <= 0:
            raise ValueError("fill qty must be > 0")
        if side in ("BUY", "LONG"):
            cost = qty * fill_price + fee
            if self.free + _QTY_EPS < cost:
                raise ValueError("insufficient spot free balance for buy")
            self.free -= cost
            if asset.qty <= _QTY_EPS:
                asset.avg_entry_price = fill_price
                asset.qty = qty
            else:
                total_val = asset.avg_entry_price * asset.qty + fill_price * qty
                asset.qty += qty
                asset.avg_entry_price = total_val / asset.qty
            return
        if side in ("SELL", "SHORT"):
            available = asset.qty - asset.locked
            if qty > available + _QTY_EPS:
                raise ValueError("insufficient spot base available to sell")
            proceeds = qty * fill_price - fee
            asset.qty -= qty
            self.free += proceeds
            if asset.qty <= _QTY_EPS:
                asset.qty = 0.0
                asset.avg_entry_price = 0.0
            return
        raise ValueError("unknown spot order side")

    def on_order(self, symbol: str, order_resp: object) -> None:
        status = str(getattr(order_resp, "status", "") or "").strip().upper()
        if status not in _SUPPORTED_ORDER_STATUSES:
            return

        order, fill_delta, fill_price, fee, terminal = self._extract_order_state(symbol, order_resp)
        existing = self.open_orders.get(order.order_id)

        if fill_delta > _QTY_EPS:
            if existing is None and status == "FILLED":
                self._apply_direct_fill(
                    symbol=order.symbol,
                    side=order.side,
                    qty=fill_delta,
                    fill_price=fill_price,
                    fee=fee,
                )
            else:
                asset = self._get_or_create_asset(order.symbol)
                if order.side in ("BUY", "LONG"):
                    self._apply_buy_fill(asset, fill_delta, fill_price, fee)
                    order.locked_quote = float(existing.locked_quote or 0.0) if existing is not None else 0.0
                    order.locked_quote = max(0.0, order.locked_quote - (fill_delta * fill_price + fee))
                else:
                    self._apply_sell_fill(asset, fill_delta, fill_price, fee)
                    order.locked_base = float(existing.locked_base or 0.0) if existing is not None else 0.0
                    order.locked_base = max(0.0, order.locked_base - fill_delta)

        if existing is not None:
            order.locked_quote = float(order.locked_quote or 0.0)
            order.locked_base = float(order.locked_base or 0.0)

        desired_quote, desired_base = self._desired_order_lock(order)
        if order.side in ("BUY", "LONG"):
            self._sync_quote_lock(order, desired_quote)
        else:
            self._sync_base_lock(order, desired_base)

        if terminal or order.remaining_qty <= _QTY_EPS:
            self._sync_quote_lock(order, 0.0)
            self._sync_base_lock(order, 0.0)
            self.open_orders.pop(order.order_id, None)
        else:
            self.open_orders[order.order_id] = order

    def on_fill(
        self,
        *,
        symbol: str,
        side: str,
        qty: float,
        fill_price: float,
        fee: float = 0.0,
    ) -> None:
        self._apply_direct_fill(
            symbol=symbol,
            side=str(side).upper().strip(),
            qty=float(qty),
            fill_price=float(fill_price),
            fee=float(fee),
        )
