"""Canonical wallet state shared by strategy-service wallet runtimes."""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any


def norm_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def derive_position_key(
    *,
    position_mode: str,
    position_side: str,
    direction: int = 0,
    position_qty: float = 0.0,
) -> int:
    """Return the runtime position key direction.

    - one_way -> always 0
    - hedge LONG -> +1
    - hedge SHORT -> -1
    """
    pm = str(position_mode or "one_way").strip().lower()
    if pm != "hedge":
        return 0

    side = str(position_side or "").strip().upper()
    if side == "LONG":
        return +1
    if side == "SHORT":
        return -1
    if direction in (+1, -1):
        return direction
    qty = float(position_qty)
    if qty > 0:
        return +1
    if qty < 0:
        return -1
    return 0


@dataclass(slots=True)
class CanonicalFuturesPositionState:
    symbol: str
    direction_key: int = 0
    initial_balance: float = 0.0
    leverage: float = 1.0
    fee_rate: float = 0.0004
    mark_price: float | None = None
    position_qty: float = 0.0
    entry_price: float = 0.0
    unrealized_pnl: float = 0.0
    position_side: str = ""
    margin_mode: str = ""
    notional: float = 0.0
    initial_margin: float = 0.0
    position_initial_margin: float = 0.0
    open_order_initial_margin: float = 0.0
    maint_margin: float = 0.0
    isolated_wallet: float = 0.0
    liquidation_price: float = 0.0
    break_even_price: float = 0.0

    def normalized_symbol(self) -> str:
        return norm_symbol(self.symbol)


@dataclass(slots=True)
class CanonicalFuturesRiskBracket:
    bracket: int = 0
    notional_floor: float = 0.0
    notional_cap: float = 0.0
    initial_leverage: float = 0.0
    maint_margin_ratio: float = 0.0
    cumulative: float = 0.0


@dataclass(slots=True)
class CanonicalFuturesRiskMetadata:
    symbol: str
    configured_leverage: float = 0.0
    configured_margin_mode: str = ""
    price_precision: int = 0
    quantity_precision: int = 0
    tick_size: float = 0.0
    step_size: float = 0.0
    brackets: list[CanonicalFuturesRiskBracket] = field(default_factory=list)

    def normalized_symbol(self) -> str:
        return norm_symbol(self.symbol)


@dataclass(slots=True)
class CanonicalFuturesState:
    margin_mode: str = "cross"
    position_mode: str = "one_way"
    multi_assets_mode: bool = False
    portfolio_margin: bool = False
    initial_balance: float = 0.0
    deposit_sum: float = 0.0
    withdrawal_sum: float = 0.0
    positions: list[CanonicalFuturesPositionState] = field(default_factory=list)
    wallet_balance: float = 0.0
    available_balance: float = 0.0
    margin_balance: float = 0.0
    unrealized_pnl: float = 0.0
    total_position_initial_margin: float = 0.0
    total_open_order_initial_margin: float = 0.0
    total_maint_margin: float = 0.0
    total_cross_wallet_balance: float = 0.0
    total_cross_un_pnl: float = 0.0
    risk_metadata: list[CanonicalFuturesRiskMetadata] = field(default_factory=list)


@dataclass(slots=True)
class CanonicalSpotAssetState:
    symbol: str
    qty: float = 0.0
    locked: float = 0.0
    avg_entry_price: float = 0.0
    price: float | None = None
    # Binance account semantics. ``symbol``/``qty`` remain additive-read
    # compatibility for snapshots written before asset-code fields existed.
    asset: str = ""
    free: Decimal | str | float | int | None = None
    free_decimal: str = ""
    locked_decimal: str = ""

    def normalized_symbol(self) -> str:
        return norm_symbol(self.symbol)

    def normalized_asset(self) -> str:
        return norm_symbol(self.asset or self.symbol)


@dataclass(slots=True)
class CanonicalSpotState:
    free: float = 0.0
    locked: float = 0.0
    assets: list[CanonicalSpotAssetState] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SpotSymbolFilter:
    filter_type: str
    min_price: str = ""
    max_price: str = ""
    tick_size: str = ""
    min_qty: str = ""
    max_qty: str = ""
    step_size: str = ""
    min_notional: str = ""
    max_notional: str = ""
    apply_to_market: bool = False
    apply_min_to_market: bool = False
    apply_max_to_market: bool = False
    avg_price_mins: int = 0
    limit: int = 0
    multiplier_up: str = ""
    multiplier_down: str = ""
    bid_multiplier_up: str = ""
    bid_multiplier_down: str = ""
    ask_multiplier_up: str = ""
    ask_multiplier_down: str = ""
    raw_json: str = ""
    max_position: str = ""
    max_num_orders: int = 0
    max_num_algo_orders: int = 0
    max_num_iceberg_orders: int = 0
    max_num_order_amends: int = 0
    max_num_order_lists: int = 0


@dataclass(frozen=True, slots=True)
class SpotSymbolMetadata:
    """Immutable Binance Spot symbol facts for one exact Venue route."""

    venue_id: int
    exchange: str
    market: str
    symbol: str
    status: str
    base_asset: str
    quote_asset: str
    base_asset_precision: int
    quote_asset_precision: int
    spot_trading_allowed: bool
    permission_sets: tuple[tuple[str, ...], ...] = ()
    order_types: tuple[str, ...] = ()
    filters: tuple[SpotSymbolFilter, ...] = ()
    snapshot_time_ms: int = 0

    @property
    def route_key(self) -> tuple[int, str, str, str]:
        return (
            int(self.venue_id),
            str(self.exchange).strip().lower(),
            str(self.market).strip().lower(),
            norm_symbol(self.symbol),
        )


@dataclass(slots=True)
class CanonicalPortfolioState:
    """Canonical wallet state — the sole runtime contract.

    Per ``canonical-wallet-display-boundary``, this dataclass is the ONLY
    surface that strategy-service runtime / risk / reconciliation paths may
    consume. Provider-specific display totals (e.g. Binance's multi-asset
    USD sums) MUST NOT extend this shape and MUST NOT drive runtime
    decisions — they belong on a separate display projection.

    Fields are split into two groups:

    **Runtime-authoritative** (strategy engine / precheck / reconciliation
    read these directly):

    - ``environment`` — portfolio environment (backtest / demo / live)
    - ``futures`` — canonical futures state (single-asset USDT@-M)
    - ``spot``   — canonical spot state (USDT-mediated)
    - ``updated_at`` — snapshot time

    **Display-derived** (projection outputs; produced by the runtime but
    NEVER read back into runtime / risk / persistence-as-source-of-truth).
    These exist on the canonical state only because the same struct
    round-trips through proto for snapshot storage; callers that need them
    should treat the values as advisory UI numbers, not as authoritative
    balances:

    - ``total_value`` — convenience sum `futures.margin_balance + spot_estimated_value`
    - ``spot_estimated_value`` — computed from priced spot assets (display)
    - ``futures_position_equity`` — mirror of `futures.margin_balance` for UI
    - ``metrics_authoritative`` — provider flag for gateway recompute-or-trust
    """

    environment: int = 0
    futures: CanonicalFuturesState = field(default_factory=CanonicalFuturesState)
    spot: CanonicalSpotState = field(default_factory=CanonicalSpotState)
    # Runtime-authoritative.
    updated_at: Any = None
    # Display-derived (see class docstring — do NOT read from these in
    # runtime/risk/reconciliation code).
    total_value: float = 0.0
    spot_estimated_value: float = 0.0
    futures_position_equity: float = 0.0
    metrics_authoritative: bool = False
