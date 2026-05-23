"""Binance testnet parity wallet runtime."""

from __future__ import annotations

from dataclasses import dataclass
import math

from .canonical import (
    CanonicalAccountState,
    CanonicalFuturesPositionState,
    CanonicalFuturesRiskMetadata,
    CanonicalFuturesState,
    CanonicalSpotAssetState,
    CanonicalSpotState,
    norm_symbol,
)
from .spot import SpotWallet

__all__ = [
    "BinanceWalletRuntime",
    "BinanceFuturesBook",
    "BinancePosition",
    "BinanceOpenOrder",
]


_QTY_EPS = 1e-12
_ACTIVE_ORDER_STATUSES = {"NEW", "PARTIALLY_FILLED"}
_TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "EXPIRED"}
_SUPPORTED_ORDER_STATUSES = _ACTIVE_ORDER_STATUSES | _TERMINAL_ORDER_STATUSES


def _sign(value: float) -> int:
    if value > _QTY_EPS:
        return 1
    if value < -_QTY_EPS:
        return -1
    return 0


def _side_sign(side: str) -> int:
    side_upper = str(side or "").strip().upper()
    if side_upper in ("BUY", "LONG"):
        return 1
    if side_upper in ("SELL", "SHORT"):
        return -1
    raise ValueError(f"unsupported order side: {side!r}")


def _normalize_position_side(position_side: str, direction_key: int = 0) -> str:
    side = str(position_side or "").strip().upper()
    if side:
        return side
    if direction_key > 0:
        return "LONG"
    if direction_key < 0:
        return "SHORT"
    return "BOTH"


def _should_bootstrap_wallet_balance(state: CanonicalFuturesState) -> bool:
    return (
        float(state.wallet_balance or 0.0) == 0.0
        and float(state.available_balance or 0.0) == 0.0
        and float(state.margin_balance or 0.0) == 0.0
        and (
            float(state.initial_balance or 0.0) != 0.0
            or float(state.deposit_sum or 0.0) != 0.0
            or float(state.withdrawal_sum or 0.0) != 0.0
            or any(float(pos.initial_balance or 0.0) != 0.0 for pos in state.positions)
        )
    )


def _bootstrap_wallet_balance(state: CanonicalFuturesState) -> float:
    deposit_sum = float(state.deposit_sum or 0.0)
    withdrawal_sum = float(state.withdrawal_sum or 0.0)
    if str(state.margin_mode or "cross").strip().lower() == "cross":
        return float(state.initial_balance or 0.0) + deposit_sum - withdrawal_sum
    isolated_seed = sum(float(pos.initial_balance or 0.0) for pos in state.positions)
    return isolated_seed + deposit_sum - withdrawal_sum


@dataclass
class BinanceOpenOrder:
    order_id: str
    symbol: str
    direction_key: int
    side: str
    position_side: str
    reduce_only: bool = False
    orig_qty: float = 0.0
    executed_qty: float = 0.0
    remaining_qty: float = 0.0
    price: float = 0.0
    status: str = "NEW"


@dataclass
class BinancePosition:
    symbol: str
    direction_key: int
    leverage: float
    fee_rate: float
    margin_mode: str
    position_side: str
    initial_balance: float = 0.0
    position_qty: float = 0.0
    entry_price: float = 0.0
    mark_price: float | None = None
    initial_margin: float = 0.0
    position_initial_margin: float = 0.0
    open_order_initial_margin: float = 0.0
    maint_margin: float = 0.0
    isolated_wallet: float = 0.0
    liquidation_price: float = 0.0
    break_even_price: float = 0.0
    notional: float = 0.0
    carry_cost: float = 0.0
    oracle_unrealized_pnl: float = 0.0
    oracle_initial_margin: float = 0.0
    oracle_position_initial_margin: float = 0.0
    oracle_open_order_initial_margin: float = 0.0
    oracle_maint_margin: float = 0.0
    oracle_isolated_wallet: float = 0.0
    oracle_liquidation_price: float = 0.0
    oracle_break_even_price: float = 0.0
    oracle_notional: float = 0.0

    @classmethod
    def from_canonical(cls, state: CanonicalFuturesPositionState) -> "BinancePosition":
        qty = float(state.position_qty or 0.0)
        margin_mode = str(state.margin_mode or "cross").strip().lower()
        isolated_wallet = float(state.isolated_wallet or 0.0)
        if margin_mode == "isolated" and isolated_wallet == 0.0:
            isolated_wallet = float(
                state.initial_balance or state.position_initial_margin or state.initial_margin or 0.0
            )
        carry_cost = 0.0
        if abs(qty) > _QTY_EPS and float(state.break_even_price or 0.0) != 0.0:
            carry_cost = (float(state.break_even_price) - float(state.entry_price or 0.0)) * abs(qty) * _sign(qty)

        pos = cls(
            symbol=norm_symbol(state.symbol),
            direction_key=int(state.direction_key),
            leverage=float(state.leverage or 1.0),
            fee_rate=float(state.fee_rate or 0.0004),
            margin_mode=margin_mode,
            position_side=(
                "BOTH"
                if int(state.direction_key) == 0
                else _normalize_position_side(state.position_side, int(state.direction_key))
            ),
            initial_balance=float(state.initial_balance or 0.0),
            position_qty=qty,
            entry_price=float(state.entry_price or 0.0),
            mark_price=float(state.mark_price) if state.mark_price is not None else None,
            initial_margin=float(state.initial_margin or 0.0),
            position_initial_margin=float(state.position_initial_margin or 0.0),
            open_order_initial_margin=float(state.open_order_initial_margin or 0.0),
            maint_margin=float(state.maint_margin or 0.0),
            isolated_wallet=isolated_wallet,
            liquidation_price=float(state.liquidation_price or 0.0),
            break_even_price=float(state.break_even_price or 0.0),
            notional=float(state.notional or 0.0),
            carry_cost=carry_cost,
            oracle_unrealized_pnl=float(state.unrealized_pnl or 0.0),
            oracle_initial_margin=float(state.initial_margin or 0.0),
            oracle_position_initial_margin=float(state.position_initial_margin or 0.0),
            oracle_open_order_initial_margin=float(state.open_order_initial_margin or 0.0),
            oracle_maint_margin=float(state.maint_margin or 0.0),
            oracle_isolated_wallet=float(state.isolated_wallet or 0.0),
            oracle_liquidation_price=float(state.liquidation_price or 0.0),
            oracle_break_even_price=float(state.break_even_price or 0.0),
            oracle_notional=float(state.notional or 0.0),
        )
        pos._refresh_derived_fields()
        return pos

    @property
    def net_qty(self) -> float:
        return float(self.position_qty)

    @property
    def net_direction(self) -> int:
        return _sign(self.position_qty)

    @property
    def avg_entry_price(self) -> float:
        return float(self.entry_price)

    def update_mark_price(self, mark_price: float) -> None:
        self.mark_price = float(mark_price)
        self._refresh_derived_fields()

    def get_unrealized_pnl(self) -> float:
        if abs(self.position_qty) <= _QTY_EPS or self.mark_price is None:
            return 0.0
        return float(self.position_qty) * (float(self.mark_price) - float(self.entry_price))

    def _compute_break_even_price(self) -> float:
        qty = float(self.position_qty or 0.0)
        if abs(qty) <= _QTY_EPS:
            return 0.0
        # Binance does not publish the complete breakEvenPrice formula. This
        # runtime keeps the observable form inferred from mode=2 testnet
        # samples: carry_cost is the cumulative cost allocated to the remaining
        # open position, so break_even_price is the price where that remaining
        # position earns back carry_cost. Partial-close allocation is handled in
        # BinanceFuturesBook._apply_fill.
        return float(self.entry_price or 0.0) + _sign(qty) * float(self.carry_cost or 0.0) / abs(qty)

    def _refresh_derived_fields(self) -> None:
        if abs(self.position_qty) <= _QTY_EPS:
            self.position_qty = 0.0
            self.entry_price = 0.0
            self.notional = 0.0
            self.position_initial_margin = 0.0
            self.break_even_price = 0.0
            if abs(self.open_order_initial_margin) <= _QTY_EPS:
                self.initial_margin = 0.0
            else:
                self.initial_margin = float(self.open_order_initial_margin)
            self.carry_cost = 0.0
            return
        if self.mark_price is not None:
            self.notional = abs(self.position_qty) * float(self.mark_price)
            self.position_initial_margin = self.notional / float(self.leverage or 1.0)
        self.initial_margin = float(self.position_initial_margin or 0.0) + float(self.open_order_initial_margin or 0.0)
        self.break_even_price = self._compute_break_even_price()

    def apply_fill(self, *, fill_qty: float, fill_price: float) -> float:
        """Apply a signed fill and return realized pnl before fee."""
        current_qty = float(self.position_qty)
        incoming_qty = float(fill_qty)
        price = float(fill_price)

        if abs(incoming_qty) <= _QTY_EPS:
            return 0.0

        if abs(current_qty) <= _QTY_EPS or _sign(current_qty) == _sign(incoming_qty):
            total_qty = abs(current_qty) + abs(incoming_qty)
            if total_qty <= _QTY_EPS:
                self.position_qty = 0.0
                self.entry_price = 0.0
            else:
                weighted_cost = abs(current_qty) * float(self.entry_price) + abs(incoming_qty) * price
                self.position_qty = current_qty + incoming_qty
                self.entry_price = weighted_cost / total_qty
            self.position_side = _normalize_position_side(self.position_side, _sign(self.position_qty))
            self._refresh_derived_fields()
            return 0.0

        close_qty = min(abs(current_qty), abs(incoming_qty))
        realized = close_qty * _sign(current_qty) * (price - float(self.entry_price))
        remaining = abs(incoming_qty) - close_qty
        if remaining <= _QTY_EPS:
            self.position_qty = current_qty + incoming_qty
            if abs(self.position_qty) <= _QTY_EPS:
                self.position_qty = 0.0
                self.entry_price = 0.0
        else:
            self.position_qty = _sign(incoming_qty) * remaining
            self.entry_price = price
        self.position_side = _normalize_position_side(self.position_side, _sign(self.position_qty))
        self._refresh_derived_fields()
        return realized

    def to_canonical(self) -> CanonicalFuturesPositionState:
        canonical_side = "BOTH" if int(self.direction_key) == 0 else (
            self.position_side or _normalize_position_side("", self.net_direction)
        )
        return CanonicalFuturesPositionState(
            symbol=self.symbol,
            direction_key=self.direction_key,
            initial_balance=self.initial_balance,
            leverage=self.leverage,
            fee_rate=self.fee_rate,
            mark_price=self.mark_price,
            position_qty=self.position_qty,
            entry_price=self.entry_price,
            unrealized_pnl=self.get_unrealized_pnl(),
            position_side=canonical_side,
            margin_mode=self.margin_mode,
            notional=self.notional if self.notional else self.oracle_notional,
            initial_margin=self.initial_margin if self.initial_margin else self.oracle_initial_margin,
            position_initial_margin=(
                self.position_initial_margin if self.position_initial_margin else self.oracle_position_initial_margin
            ),
            open_order_initial_margin=self.open_order_initial_margin,
            maint_margin=self.maint_margin if self.maint_margin else self.oracle_maint_margin,
            isolated_wallet=self.isolated_wallet if self.margin_mode == "isolated" else self.oracle_isolated_wallet,
            liquidation_price=self.liquidation_price if self.liquidation_price else self.oracle_liquidation_price,
            break_even_price=self.break_even_price if self.break_even_price else self.oracle_break_even_price,
        )


class BinanceFuturesBook:
    """Exchange-aware futures runtime with explicit local/oracle boundaries."""

    def __init__(self, state: CanonicalFuturesState) -> None:
        self.margin_mode = str(state.margin_mode or "cross").strip().lower()
        self.position_mode = str(state.position_mode or "one_way").strip().lower()
        self.multi_assets_mode = bool(state.multi_assets_mode)
        self.portfolio_margin = bool(state.portfolio_margin)
        self.initial_balance = float(state.initial_balance or 0.0)
        self.deposit_sum = float(state.deposit_sum or 0.0)
        self.withdrawal_sum = float(state.withdrawal_sum or 0.0)
        self.wallet_balance = float(state.wallet_balance or 0.0)
        if _should_bootstrap_wallet_balance(state):
            self.wallet_balance = _bootstrap_wallet_balance(state)
        self.oracle_available_balance = float(state.available_balance or 0.0)
        self.oracle_margin_balance = float(state.margin_balance or 0.0)
        self.oracle_unrealized_pnl = float(state.unrealized_pnl or 0.0)
        self.oracle_total_open_order_initial_margin = float(state.total_open_order_initial_margin or 0.0)
        self.total_open_order_initial_margin = float(state.total_open_order_initial_margin or 0.0)
        self.total_cross_wallet_balance = float(state.total_cross_wallet_balance or 0.0)
        self.total_cross_un_pnl = float(state.total_cross_un_pnl or 0.0)
        self.total_maint_margin = float(state.total_maint_margin or 0.0)
        self.risk_metadata: dict[str, CanonicalFuturesRiskMetadata] = {
            item.normalized_symbol(): item
            for item in state.risk_metadata
        }
        self.positions: dict[tuple[str, int], BinancePosition] = {}
        for pos_state in state.positions:
            pos = BinancePosition.from_canonical(pos_state)
            pos.leverage = self._resolve_position_leverage(pos.symbol, pos.leverage)
            pos._refresh_derived_fields()
            self.positions[(norm_symbol(pos.symbol), int(pos.direction_key))] = pos
        self.open_orders: dict[str, BinanceOpenOrder] = {}
        self._refresh_account_fields()

    def _get_positions_for_symbol(self, symbol: str) -> list[tuple[tuple[str, int], BinancePosition]]:
        sym = norm_symbol(symbol)
        return [(k, p) for k, p in self.positions.items() if k[0] == sym]

    def _has_symbol(self, symbol: str) -> bool:
        return any(k[0] == norm_symbol(symbol) for k in self.positions)

    def _resolve_metadata(self, symbol: str) -> CanonicalFuturesRiskMetadata | None:
        return self.risk_metadata.get(norm_symbol(symbol))

    def _position_key_from_order(self, symbol: str, position_side: str, side: str) -> int:
        if self.position_mode != "hedge":
            return 0
        normalized_side = str(position_side or "").strip().upper()
        if normalized_side == "LONG":
            return +1
        if normalized_side == "SHORT":
            return -1
        raise ValueError("hedge-mode parity orders require explicit position_side")

    def _normalize_order_id(
        self,
        *,
        status: str,
        raw_orig_qty: float,
        raw_executed_qty: float,
        raw_remaining_qty: float,
        raw_order_id: str,
    ) -> str:
        if raw_order_id:
            return raw_order_id
        if (
            status != "FILLED"
            or raw_orig_qty > _QTY_EPS
            or raw_executed_qty > _QTY_EPS
            or raw_remaining_qty > _QTY_EPS
        ):
            raise ValueError("futures lifecycle order events require explicit order_id")
        return ""

    def _resolve_position_side(self, direction_key: int, side: str, position_side: str) -> str:
        if self.position_mode == "hedge":
            return _normalize_position_side(position_side, direction_key)
        return "BOTH"

    def _default_leverage(self, symbol: str) -> float:
        return self._resolve_position_leverage(symbol, 0.0)

    def _resolve_position_leverage(self, symbol: str, current: float) -> float:
        current_lev = float(current or 0.0)
        metadata = self._resolve_metadata(symbol)
        metadata_lev = float(metadata.configured_leverage or 0.0) if metadata is not None else 0.0
        if metadata_lev > 0.0 and (current_lev <= 0.0 or math.isclose(current_lev, 1.0, rel_tol=0.0, abs_tol=1e-12)):
            return metadata_lev
        if current_lev > 0.0:
            return current_lev
        if metadata_lev > 0.0:
            return metadata_lev
        return 1.0

    def _default_margin_mode(self, symbol: str) -> str:
        metadata = self._resolve_metadata(symbol)
        if metadata is not None and str(metadata.configured_margin_mode or "").strip():
            return str(metadata.configured_margin_mode).strip().lower()
        return self.margin_mode

    def _ensure_position(self, symbol: str, direction_key: int, position_side: str) -> BinancePosition:
        key = (norm_symbol(symbol), int(direction_key))
        pos = self.positions.get(key)
        if pos is not None:
            return pos
        pos = BinancePosition(
            symbol=key[0],
            direction_key=key[1],
            leverage=self._default_leverage(key[0]),
            fee_rate=0.0004,
            margin_mode=self._default_margin_mode(key[0]),
            position_side=self._resolve_position_side(direction_key, "", position_side),
            initial_balance=0.0,
            isolated_wallet=0.0,
        )
        if pos.margin_mode == "isolated":
            pos.isolated_wallet = float(pos.initial_balance or 0.0)
        pos._refresh_derived_fields()
        self.positions[key] = pos
        return pos

    def _select_bracket(self, pos: BinancePosition) -> object | None:
        metadata = self._resolve_metadata(pos.symbol)
        if metadata is None or not metadata.brackets:
            return None
        notional = abs(float(pos.notional or 0.0))
        ordered = sorted(
            metadata.brackets,
            key=lambda item: (float(item.notional_floor or 0.0), float(item.notional_cap or 0.0)),
        )
        for bracket in ordered:
            floor = float(bracket.notional_floor or 0.0)
            cap = float(bracket.notional_cap or 0.0)
            if notional < floor:
                continue
            if cap == 0.0 or notional <= cap or math.isclose(notional, cap, rel_tol=1e-12, abs_tol=1e-12):
                return bracket
        return ordered[-1]

    def _compute_maint_margin(self, pos: BinancePosition) -> float:
        if abs(pos.position_qty) <= _QTY_EPS:
            return 0.0
        bracket = self._select_bracket(pos)
        if bracket is None:
            return float(pos.oracle_maint_margin or 0.0)
        mmr = float(getattr(bracket, "maint_margin_ratio", 0.0) or 0.0)
        cumulative = float(getattr(bracket, "cumulative", 0.0) or 0.0)
        return max(0.0, abs(float(pos.notional or 0.0)) * mmr - cumulative)

    def _compute_liquidation_price(self, key: tuple[str, int], pos: BinancePosition) -> float:
        if abs(pos.position_qty) <= _QTY_EPS:
            return 0.0
        bracket = self._select_bracket(pos)
        if bracket is None:
            return float(pos.oracle_liquidation_price or 0.0)

        mmr = float(getattr(bracket, "maint_margin_ratio", 0.0) or 0.0)
        cumulative = float(getattr(bracket, "cumulative", 0.0) or 0.0)
        qty = abs(float(pos.position_qty or 0.0))
        if qty <= _QTY_EPS:
            return 0.0

        if pos.margin_mode == "isolated":
            balance = float(pos.isolated_wallet or pos.initial_balance or pos.position_initial_margin or 0.0)
            if pos.position_qty > 0:
                denom = qty * (1.0 - mmr)
                if denom <= _QTY_EPS:
                    return float(pos.oracle_liquidation_price or 0.0)
                price = (qty * float(pos.entry_price or 0.0) - balance - cumulative) / denom
            else:
                denom = qty * (1.0 + mmr)
                if denom <= _QTY_EPS:
                    return float(pos.oracle_liquidation_price or 0.0)
                price = (balance + qty * float(pos.entry_price or 0.0) + cumulative) / denom
        else:
            upnl_others = 0.0
            maint_margin_others = 0.0
            for other_key, other_pos in self.positions.items():
                if other_key == key:
                    continue
                upnl_others += other_pos.get_unrealized_pnl()
                maint_margin_others += float(other_pos.maint_margin or 0.0)

            if pos.position_qty > 0:
                denom = qty * (1.0 - mmr)
                if denom <= _QTY_EPS:
                    return float(pos.oracle_liquidation_price or 0.0)
                price = (
                    qty * float(pos.entry_price or 0.0)
                    - float(self.wallet_balance)
                    - upnl_others
                    - cumulative
                    + maint_margin_others
                ) / denom
            else:
                denom = qty * (1.0 + mmr)
                if denom <= _QTY_EPS:
                    return float(pos.oracle_liquidation_price or 0.0)
                price = (
                    qty * float(pos.entry_price or 0.0)
                    + float(self.wallet_balance)
                    + upnl_others
                    + cumulative
                    - maint_margin_others
                ) / denom

        metadata = self._resolve_metadata(pos.symbol)
        if metadata is not None and metadata.price_precision > 0:
            price = round(price, int(metadata.price_precision))
        return max(0.0, float(price))

    def _resolve_effective_leverage(self, order: BinanceOpenOrder, pos: BinancePosition | None) -> float:
        if pos is not None:
            return self._resolve_position_leverage(order.symbol, pos.leverage)
        metadata = self._resolve_metadata(order.symbol)
        if metadata is not None and float(metadata.configured_leverage or 0.0) > 0.0:
            return float(metadata.configured_leverage)
        return 1.0

    def _opening_remaining_qty(self, order: BinanceOpenOrder, pos: BinancePosition | None) -> float:
        remaining = max(0.0, float(order.remaining_qty or 0.0))
        if remaining <= _QTY_EPS or order.reduce_only:
            return 0.0
        if self.position_mode == "hedge":
            direction_key = int(order.direction_key)
            side_sign = _side_sign(order.side)
            if side_sign == direction_key:
                return remaining
            return 0.0

        current_qty = float(pos.position_qty) if pos is not None else 0.0
        side_sign = _side_sign(order.side)
        if side_sign > 0:
            closing_capacity = max(0.0, -current_qty)
        else:
            closing_capacity = max(0.0, current_qty)
        return max(0.0, remaining - closing_capacity)

    def _compute_order_open_margin(self, order: BinanceOpenOrder, pos: BinancePosition | None) -> float:
        opening_qty = self._opening_remaining_qty(order, pos)
        if opening_qty <= _QTY_EPS:
            return 0.0
        leverage = self._resolve_effective_leverage(order, pos)
        if leverage <= _QTY_EPS:
            leverage = 1.0
        mark_price = None
        if pos is not None and pos.mark_price is not None:
            mark_price = float(pos.mark_price)
        elif float(order.price or 0.0) > 0.0:
            mark_price = float(order.price)
        if mark_price is None or mark_price <= 0.0:
            return 0.0
        return opening_qty * mark_price / leverage

    def _refresh_order_margins(self) -> None:
        keyed_orders: dict[tuple[str, int], list[BinanceOpenOrder]] = {}
        for order in self.open_orders.values():
            keyed_orders.setdefault((order.symbol, order.direction_key), []).append(order)

        local_total = 0.0
        for key, pos in self.positions.items():
            orders = keyed_orders.get(key, [])
            if orders:
                pos.open_order_initial_margin = sum(self._compute_order_open_margin(order, pos) for order in orders)
            else:
                pos.open_order_initial_margin = float(pos.oracle_open_order_initial_margin or 0.0)
            pos.initial_margin = float(pos.position_initial_margin or 0.0) + float(pos.open_order_initial_margin or 0.0)
            local_total += float(pos.open_order_initial_margin or 0.0)

        if self.open_orders:
            self.total_open_order_initial_margin = local_total
        else:
            self.total_open_order_initial_margin = float(self.oracle_total_open_order_initial_margin or 0.0)

    def _refresh_account_fields(self) -> None:
        for pos in self.positions.values():
            pos._refresh_derived_fields()
        for pos in self.positions.values():
            pos.maint_margin = self._compute_maint_margin(pos)
        for key, pos in self.positions.items():
            pos.liquidation_price = self._compute_liquidation_price(key, pos)
        self._refresh_order_margins()
        self.total_position_initial_margin = sum(pos.position_initial_margin for pos in self.positions.values())
        self.total_maint_margin = sum(float(pos.maint_margin or 0.0) for pos in self.positions.values())
        if self.margin_mode == "cross":
            self.total_cross_wallet_balance = self.wallet_balance
            self.total_cross_un_pnl = self.get_unrealized_pnl()
        self.unrealized_pnl = self.get_unrealized_pnl()
        self.margin_balance = self.wallet_balance + self.unrealized_pnl
        self.available_balance = self._compute_available_balance()

    def get_unrealized_pnl(self) -> float:
        return sum(pos.get_unrealized_pnl() for pos in self.positions.values())

    def get_margin_balance(self) -> float:
        return float(self.margin_balance)

    def get_total_equity(self) -> float:
        return self.get_margin_balance()

    def get_total_position_equity(self) -> float:
        return self.get_total_equity()

    def get_wallet_balance(self) -> float:
        return float(self.wallet_balance)

    def get_WB(self) -> float:
        return self.get_wallet_balance()

    def _compute_available_balance(self) -> float:
        if self.margin_mode == "cross":
            cross_upnl = self.get_unrealized_pnl()
            candidate = self.total_cross_wallet_balance + cross_upnl - (
                self.total_position_initial_margin + self.total_open_order_initial_margin
            )
            return max(0.0, candidate)
        candidate = self.get_margin_balance() - (
            self.total_position_initial_margin + self.total_open_order_initial_margin
        )
        return max(0.0, candidate)

    def get_available_balance(self) -> float:
        return float(self.available_balance)

    def on_market_data(self, symbol: str, symbol_type: str, price: float) -> None:
        if str(symbol_type).strip().lower() != "futures":
            return
        affected = self._get_positions_for_symbol(symbol)
        for _key, pos in affected:
            pos.update_mark_price(float(price))
        if affected or any(order.symbol == norm_symbol(symbol) for order in self.open_orders.values()):
            self._refresh_account_fields()

    def _extract_fill_delta(
        self,
        *,
        existing: BinanceOpenOrder | None,
        status: str,
        event_qty: float,
        executed_qty: float,
    ) -> tuple[float, float]:
        previous_executed = float(existing.executed_qty or 0.0) if existing is not None else 0.0
        if executed_qty > 0.0:
            return max(0.0, executed_qty - previous_executed), executed_qty
        if status in {"PARTIALLY_FILLED", "FILLED"} and event_qty > 0.0:
            total = previous_executed + event_qty
            return event_qty, total
        return 0.0, previous_executed

    def _event_order_payload(self, symbol: str, order_resp: object) -> tuple[BinanceOpenOrder, float, float, float, bool]:
        side = str(getattr(order_resp, "side", "") or "").strip().upper()
        status = str(getattr(order_resp, "status", "") or "").strip().upper()
        if status not in _SUPPORTED_ORDER_STATUSES:
            raise ValueError(f"unsupported futures order status: {status!r}")
        direction_key = self._position_key_from_order(
            norm_symbol(symbol),
            str(getattr(order_resp, "position_side", "") or ""),
            side,
        )
        raw_price = float(
            getattr(order_resp, "price", 0.0)
            or getattr(order_resp, "fill_price", 0.0)
            or 0.0
        )
        event_qty = abs(float(getattr(order_resp, "qty", 0.0) or 0.0))
        raw_orig_qty = abs(float(getattr(order_resp, "orig_qty", 0.0) or 0.0))
        raw_executed_qty = abs(float(getattr(order_resp, "executed_qty", 0.0) or 0.0))
        raw_remaining_qty = abs(float(getattr(order_resp, "remaining_qty", 0.0) or 0.0))
        orig_qty = raw_orig_qty
        executed_qty = raw_executed_qty
        remaining_qty = raw_remaining_qty
        raw_order_id = str(
            getattr(order_resp, "order_id", "")
            or getattr(order_resp, "client_order_id", "")
            or ""
        ).strip()

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
            raw_order_id=raw_order_id,
        )
        existing = self.open_orders.get(order_id)
        if existing is not None:
            if orig_qty <= _QTY_EPS:
                orig_qty = float(existing.orig_qty or 0.0)
            if raw_price <= 0.0:
                raw_price = float(existing.price or 0.0)

        fill_delta, executed_total = self._extract_fill_delta(
            existing=existing,
            status=status,
            event_qty=event_qty,
            executed_qty=executed_qty,
        )
        if remaining_qty <= _QTY_EPS:
            if status in _TERMINAL_ORDER_STATUSES:
                remaining_qty = 0.0
            elif orig_qty > _QTY_EPS:
                remaining_qty = max(0.0, orig_qty - executed_total)
        reduce_only = bool(getattr(order_resp, "reduce_only", False))
        order = BinanceOpenOrder(
            order_id=order_id,
            symbol=norm_symbol(symbol),
            direction_key=direction_key,
            side=side,
            position_side=self._resolve_position_side(
                direction_key, side, str(getattr(order_resp, "position_side", "") or "")
            ),
            reduce_only=reduce_only,
            orig_qty=orig_qty,
            executed_qty=executed_total,
            remaining_qty=remaining_qty,
            price=raw_price,
            status=status,
        )
        fee = float(getattr(order_resp, "fee", 0.0) or 0.0)
        return order, fill_delta, float(getattr(order_resp, "fill_price", 0.0) or raw_price or 0.0), fee, status in _TERMINAL_ORDER_STATUSES

    def _signed_fill_qty(self, order: BinanceOpenOrder, fill_qty: float) -> float:
        delta = abs(float(fill_qty or 0.0))
        if delta <= _QTY_EPS:
            return 0.0
        side_sign = _side_sign(order.side)
        if self.position_mode == "hedge":
            if side_sign == order.direction_key:
                return delta * float(order.direction_key)
            return -delta * float(order.direction_key)
        return delta if side_sign > 0 else -delta

    def _apply_fill(
        self,
        symbol: str,
        *,
        direction_key: int,
        position_side: str,
        fill_qty: float,
        fill_price: float,
        fee: float,
    ) -> None:
        key = (symbol, int(direction_key))
        pos = self._ensure_position(symbol, direction_key, position_side)
        previous_qty = float(pos.position_qty or 0.0)
        previous_abs = abs(previous_qty)
        previous_sign = _sign(previous_qty)
        previous_carry_cost = float(pos.carry_cost or 0.0)
        fill_abs = abs(float(fill_qty or 0.0))
        realized = pos.apply_fill(fill_qty=fill_qty, fill_price=fill_price)
        new_abs = abs(float(pos.position_qty or 0.0))
        new_sign = _sign(pos.position_qty)

        # Event-driven snapshots can happen before the next market-data tick
        # arrives. In that window, keep a conservative local mark aligned to
        # the latest fill so risk-derived fields (notional / initial_margin /
        # maint_margin / unrealized_pnl) do not collapse to zero.
        if new_abs > _QTY_EPS and (pos.mark_price is None or float(pos.mark_price or 0.0) <= 0.0):
            pos.mark_price = float(fill_price)

        if pos.margin_mode == "isolated":
            pos.isolated_wallet = float(pos.isolated_wallet or 0.0) + realized - float(fee)

        # Cost-basis model for Binance-style break-even:
        # break_even_price = entry_price + sign(qty) * carry_cost / abs(qty).
        # The exact Binance formula is not published. The partial-close rule
        # below is inferred from mode=2 testnet samples where exchange
        # breakEvenPrice retained realized PnL + commission impact in the
        # remaining position instead of proportionally shrinking old costs.
        fill_sign = _sign(fill_qty)
        fee_value = float(fee)
        if previous_abs <= _QTY_EPS or previous_sign == fill_sign:
            if new_abs > _QTY_EPS:
                pos.carry_cost = previous_carry_cost + fee_value
        elif new_abs <= _QTY_EPS:
            pos.carry_cost = 0.0
        elif previous_sign == new_sign and previous_abs > _QTY_EPS:
            pos.carry_cost = previous_carry_cost - realized + fee_value
        else:
            open_qty = max(0.0, fill_abs - previous_abs)
            open_fee = fee_value * (open_qty / fill_abs) if fill_abs > _QTY_EPS else 0.0
            pos.carry_cost = open_fee

        pos._refresh_derived_fields()
        self.positions[key] = pos
        self.wallet_balance = float(self.wallet_balance) + realized - float(fee)

    def on_order(self, symbol: str, symbol_type: str, order_resp: object) -> None:
        if str(symbol_type).strip().lower() != "futures":
            return
        status = str(getattr(order_resp, "status", "") or "").strip().upper()
        if status not in _SUPPORTED_ORDER_STATUSES:
            return

        order, fill_delta, fill_price, fee, terminal = self._event_order_payload(symbol, order_resp)
        if fill_delta > _QTY_EPS:
            self._apply_fill(
                order.symbol,
                direction_key=order.direction_key,
                position_side=order.position_side,
                fill_qty=self._signed_fill_qty(order, fill_delta),
                fill_price=fill_price,
                fee=fee,
            )

        if terminal or order.remaining_qty <= _QTY_EPS:
            self.open_orders.pop(order.order_id, None)
        else:
            self.open_orders[order.order_id] = order
            self._ensure_position(order.symbol, order.direction_key, order.position_side)
        self._refresh_account_fields()

    def on_ledger_event(self, event: object) -> None:
        event_type = str(
            getattr(event, "event_type", "")
            or getattr(event, "type", "")
            or ""
        ).strip().lower()
        if not event_type:
            raise ValueError("ledger event_type is required")

        amount = float(getattr(event, "amount", 0.0) or 0.0)
        if event_type in {"transfer_out", "withdrawal"} and amount > 0.0:
            amount = -amount
        self.wallet_balance = float(self.wallet_balance) + amount

        symbol = str(getattr(event, "symbol", "") or "").strip()
        position_side = str(getattr(event, "position_side", "") or "").strip().upper()
        if symbol:
            # In hedge mode the per-position update requires an explicit
            # position_side. Account-level ledger events (e.g. a funding_fee
            # or transfer that aren't bound to LONG vs SHORT) legitimately
            # arrive without it. Apply only the wallet-level delta (already
            # added to wallet_balance above) and skip per-position work,
            # rather than raising inside _position_key_from_order.
            if self.position_mode == "hedge" and position_side not in {"LONG", "SHORT"}:
                self._refresh_account_fields()
                return
            direction_key = self._position_key_from_order(
                norm_symbol(symbol), position_side,
                "BUY" if amount >= 0.0 else "SELL",
            ) if self.position_mode == "hedge" else 0
            key = (norm_symbol(symbol), int(direction_key))
            pos = self.positions.get(key)
            if pos is None and self.position_mode != "hedge":
                pos = self.positions.get((norm_symbol(symbol), 0))
            if pos is not None and pos.margin_mode == "isolated" and event_type in {
                "funding_fee",
                "transfer_in",
                "transfer_out",
                "deposit",
                "withdrawal",
            }:
                pos.isolated_wallet = float(pos.isolated_wallet or 0.0) + amount
            if pos is not None and event_type == "funding_fee" and pos.margin_mode == "isolated":
                pos.carry_cost = float(pos.carry_cost or 0.0) - amount
                pos._refresh_derived_fields()

        self._refresh_account_fields()

    def to_canonical(self) -> CanonicalFuturesState:
        return CanonicalFuturesState(
            margin_mode=self.margin_mode,
            position_mode=self.position_mode,
            multi_assets_mode=self.multi_assets_mode,
            portfolio_margin=self.portfolio_margin,
            initial_balance=self.initial_balance,
            deposit_sum=self.deposit_sum,
            withdrawal_sum=self.withdrawal_sum,
            positions=[pos.to_canonical() for pos in self.positions.values()],
            wallet_balance=self.get_wallet_balance(),
            available_balance=self.get_available_balance(),
            margin_balance=self.get_margin_balance(),
            unrealized_pnl=self.get_unrealized_pnl(),
            total_position_initial_margin=self.total_position_initial_margin,
            total_open_order_initial_margin=self.total_open_order_initial_margin,
            total_maint_margin=self.total_maint_margin,
            total_cross_wallet_balance=self.total_cross_wallet_balance,
            total_cross_un_pnl=self.get_unrealized_pnl() if self.margin_mode == "cross" else self.total_cross_un_pnl,
            risk_metadata=list(self.risk_metadata.values()),
        )


def _build_spot_wallet(state: CanonicalSpotState) -> SpotWallet:
    return SpotWallet.from_canonical(state)


class BinanceWalletRuntime:
    """Binance-parity wallet runtime.

    Implements the :class:`ExchangeWalletRuntime` protocol. Used by:
      - mode=2 Binance testnet (authoritative exchange hydration)
      - mode=0 backtest (after C2a cutover — treats canonical state as
        its own source of truth, no exchange snapshot available)

    ``provider`` and ``environment`` are class attributes describing the
    runtime class itself (a Binance implementation); they are the lookup
    key that ``wallet_factory.RUNTIME_REGISTRY`` uses to route by
    ``(provider, environment)``.

    ``mode`` is per-instance because it reflects the **session** mode, not
    the runtime class. A mode=0 session routed through this runtime still
    serializes ``mode=0`` back out of ``to_canonical_state()``.
    """

    provider = "binance"
    environment = "testnet"

    def __init__(
        self,
        *,
        futures: BinanceFuturesBook,
        spot: SpotWallet,
        source_state: CanonicalAccountState,
        mode: int = 2,
    ) -> None:
        self.futures = futures
        self.spot = spot
        self.mode = int(mode)
        self._source_state = source_state

    @classmethod
    def from_canonical(cls, state: CanonicalAccountState) -> "BinanceWalletRuntime":
        return cls(
            futures=BinanceFuturesBook(state.futures),
            spot=_build_spot_wallet(state.spot),
            source_state=state,
            mode=int(state.mode),
        )

    def get_total_value(self) -> float:
        """Return a display-oriented ``total_value`` (spot+futures equity).

        This is purely a display-projection — runtime risk / precheck /
        reconciliation code MUST NOT read it. The authoritative futures
        risk value is ``get_available_balance``; the authoritative spot
        balance is ``spot.free`` + ``spot.locked`` + priced assets.
        """
        spot_value = self._spot_estimated_value()
        return self.futures.get_margin_balance() + float(spot_value)

    def _spot_estimated_value(self) -> float:
        """Spot leg estimate for the display projection.

        Canonical-only: computes from priced assets when prices are set,
        otherwise falls back to the cash position (``free + locked``).
        Deliberately does NOT read ``_source_state.spot_estimated_value`` —
        per ``canonical-wallet-display-boundary``, provider display fields
        MUST NOT be required to hydrate runtime-facing projections. If the
        caller wants a provider-supplied USD total, they should read the
        display wallet directly, not re-enter it through the runtime.
        """
        try:
            return float(self.spot.get_estimated_value())
        except ValueError:
            return float(self.spot.free + self.spot.locked)

    def get_wallet_balance(self) -> float:
        return self.futures.get_wallet_balance()

    def get_WB(self) -> float:
        return self.get_wallet_balance()

    def get_available_balance(self) -> float:
        return self.futures.get_available_balance()

    def on_market_data(self, symbol: str, symbol_type: str, price: float) -> None:
        st = str(symbol_type).strip().lower()
        if st == "futures":
            self.futures.on_market_data(symbol, st, price)
        elif st == "spot":
            self.spot.on_market_data(symbol, float(price))

    def on_order(self, symbol: str, symbol_type: str, order_resp: object) -> None:
        st = str(symbol_type).strip().lower()
        if st == "futures":
            self.futures.on_order(symbol, st, order_resp)
        elif st == "spot":
            self.spot.on_order(symbol, order_resp)

    def on_ledger_event(self, event: object) -> None:
        self.futures.on_ledger_event(event)

    def to_canonical_state(self) -> CanonicalAccountState:
        return CanonicalAccountState(
            mode=self.mode,
            futures=self.futures.to_canonical(),
            spot=CanonicalSpotState(
                free=float(self.spot.free),
                locked=float(self.spot.locked),
                assets=[
                    CanonicalSpotAssetState(
                        symbol=symbol,
                        qty=float(asset.qty),
                        locked=float(asset.locked),
                        avg_entry_price=float(asset.avg_entry_price),
                        price=float(asset.price) if asset.price is not None else None,
                    )
                    for symbol, asset in self.spot.assets.items()
                ],
            ),
            total_value=self.get_total_value(),
            updated_at=self._source_state.updated_at,
            spot_estimated_value=self._spot_estimated_value(),
            futures_position_equity=self.futures.get_margin_balance(),
            metrics_authoritative=False,
        )
