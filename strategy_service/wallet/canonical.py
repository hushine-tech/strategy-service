"""Canonical wallet state shared by strategy-service wallet runtimes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from strategy_service.position_side import BOTH, position_direction_key


def norm_symbol(symbol: str) -> str:
    return str(symbol).strip().upper()


def derive_position_key(
    *,
    position_mode: str,
    position_side: int,
) -> int:
    """Return the private wallet key from the shared enum contract only."""
    return position_direction_key(position_mode=position_mode, position_side=position_side)


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
    position_side: int = BOTH
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


@dataclass(frozen=True, slots=True)
class CanonicalFuturesOrderCheckpoint:
    order_id: str
    executed_qty_decimal: str
    terminal: bool


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
    last_applied_income_entry_id: int = 0
    risk_metadata: list[CanonicalFuturesRiskMetadata] = field(default_factory=list)
    order_checkpoints: list[CanonicalFuturesOrderCheckpoint] = field(default_factory=list)


def validate_canonical_futures_positions(state: CanonicalFuturesState) -> None:
    """Enforce the shared position-side invariant before runtime installation."""
    position_mode = str(state.position_mode or "").strip().lower()
    if position_mode not in {"one_way", "hedge"}:
        raise ValueError("canonical Futures position_mode is missing or invalid")
    for position in state.positions:
        expected_direction_key = derive_position_key(
            position_mode=position_mode,
            position_side=position.position_side,
        )
        if type(position.direction_key) is not int or position.direction_key != expected_direction_key:
            raise ValueError(
                "canonical FuturesPosition.direction_key does not match position_side"
            )


@dataclass(slots=True)
class CanonicalSpotAssetState:
    asset: str
    free_decimal: str
    locked_decimal: str
    avg_entry_price_decimal: str = ""
    price_decimal: str | None = None

    def normalized_asset(self) -> str:
        return norm_symbol(self.asset)


@dataclass(slots=True)
class CanonicalSpotState:
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
