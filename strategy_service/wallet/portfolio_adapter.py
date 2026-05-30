"""Build portfolio wallet runtimes from core-service portfolio snapshots."""

from __future__ import annotations

from typing import Any

from strategy_service.inputs import _normalize_exchange, _normalize_market
from strategy_service.wallet_adapter import proto_to_account_spec
from strategy_service.wallet.binance import BinanceWalletRuntime
from strategy_service.wallet.canonical import (
    CanonicalAccountState,
    CanonicalFuturesPositionState,
    CanonicalFuturesState,
    CanonicalSpotAssetState,
    CanonicalSpotState,
    derive_position_key,
    norm_symbol,
)
from strategy_service.wallet.portfolio import PortfolioWalletRuntime


_EXCHANGE_LABELS = {
    1: "binance",
    2: "okx",
}

_MARKET_LABELS = {
    1: "spot",
    2: "perpetual_futures",
    3: "delivery_futures",
}


def _enum_label(value: Any, labels: dict[int, str], field_name: str) -> str:
    if isinstance(value, str):
        raw = value.strip().lower()
        if not raw:
            raise ValueError(f"missing VenueSnapshot.{field_name}")
        return raw
    try:
        return labels[int(value)]
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(
            f"unsupported enum value for VenueSnapshot.{field_name}: {value!r}"
        ) from exc


def _float_field(source: Any, field_name: str, default: float = 0.0) -> float:
    return float(getattr(source, field_name, default) or default)


def _validate_route(exchange: str, market: str) -> tuple[str, str]:
    return (_normalize_exchange(exchange), _normalize_market(market))


def _venue_route(venue: Any) -> tuple[str, str]:
    exchange = _enum_label(getattr(venue, "exchange", 0), _EXCHANGE_LABELS, "exchange")
    market = _enum_label(getattr(venue, "market", 0), _MARKET_LABELS, "market")
    return _validate_route(exchange, market)


def _venue_id(venue: Any) -> int:
    venue_id = int(getattr(venue, "venue_id", 0) or 0)
    if venue_id <= 0:
        raise ValueError("missing VenueSnapshot.venue_id")
    return venue_id


def _build_spot_state(venue: Any) -> CanonicalSpotState:
    balances = list(getattr(venue, "balances", []) or [])
    free = 0.0
    locked = 0.0
    assets: list[CanonicalSpotAssetState] = []
    for item in balances:
        asset = str(getattr(item, "asset", "") or "").strip().upper()
        if not asset:
            raise ValueError("missing BalanceEntry.asset")
        available_balance = _float_field(item, "available_balance")
        item_locked = _float_field(item, "locked")
        wallet_balance = _float_field(item, "wallet_balance")
        if asset == "USDT":
            free += available_balance
            locked += item_locked
            continue
        assets.append(
            CanonicalSpotAssetState(
                symbol=asset,
                qty=wallet_balance,
                locked=item_locked,
            )
        )
    return CanonicalSpotState(free=free, locked=locked, assets=assets)


def _futures_margin_balance(venue: Any, positions: list[Any]) -> float:
    explicit = sum(_float_field(pos, "margin_balance") for pos in positions)
    if explicit != 0.0:
        return explicit
    wallet_balance = _float_field(venue, "wallet_balance")
    unrealized_pnl = sum(_float_field(pos, "unrealized_pnl") for pos in positions)
    return wallet_balance + unrealized_pnl


def _build_futures_state(venue: Any) -> CanonicalFuturesState:
    positions = list(getattr(venue, "positions", []) or [])
    position_mode = "one_way"
    margin_mode = "cross"
    canonical_positions: list[CanonicalFuturesPositionState] = []
    for pos in positions:
        symbol = norm_symbol(getattr(pos, "symbol", ""))
        if not symbol:
            raise ValueError("missing PositionEntry.symbol")
        position_side = str(getattr(pos, "position_side", "") or "BOTH").strip().upper()
        position_qty = _float_field(pos, "qty")
        canonical_positions.append(
            CanonicalFuturesPositionState(
                symbol=symbol,
                direction_key=derive_position_key(
                    position_mode=position_mode,
                    position_side=position_side,
                    position_qty=position_qty,
                ),
                mark_price=_float_field(pos, "mark_price") or None,
                position_qty=position_qty,
                entry_price=_float_field(pos, "entry_price"),
                unrealized_pnl=_float_field(pos, "unrealized_pnl"),
                position_side=position_side,
                margin_mode=margin_mode,
                liquidation_price=_float_field(pos, "liquidation_price"),
            )
        )

    return CanonicalFuturesState(
        margin_mode=margin_mode,
        position_mode=position_mode,
        positions=canonical_positions,
        wallet_balance=_float_field(venue, "wallet_balance"),
        available_balance=_float_field(venue, "available_balance"),
        margin_balance=_futures_margin_balance(venue, positions),
        unrealized_pnl=sum(_float_field(pos, "unrealized_pnl") for pos in positions),
        total_cross_wallet_balance=_float_field(venue, "wallet_balance"),
        total_cross_un_pnl=sum(_float_field(pos, "unrealized_pnl") for pos in positions),
    )


def _build_binance_wallet_from_venue_snapshot(
    venue: Any,
    *,
    market: str,
    updated_at: Any,
) -> BinanceWalletRuntime:
    wallet = getattr(venue, "wallet", None)
    if _has_full_wallet(venue):
        return BinanceWalletRuntime.from_canonical(proto_to_account_spec(wallet))
    if market == "spot":
        state = CanonicalAccountState(
            mode=2,
            spot=_build_spot_state(venue),
            total_value=_float_field(venue, "total_value"),
            updated_at=updated_at,
        )
        return BinanceWalletRuntime.from_canonical(state)
    if market == "perpetual_futures":
        if list(getattr(venue, "positions", []) or []):
            raise ValueError(
                "futures VenueSnapshot with positions requires full canonical wallet"
            )
        futures_state = _build_futures_state(venue)
        state = CanonicalAccountState(
            mode=2,
            futures=futures_state,
            total_value=_float_field(venue, "total_value"),
            futures_position_equity=futures_state.margin_balance,
            updated_at=updated_at,
        )
        return BinanceWalletRuntime.from_canonical(state)
    raise ValueError(f"unsupported portfolio wallet market for binance: {market}")


def _has_full_wallet(venue: Any) -> bool:
    has_field = getattr(venue, "HasField", None)
    if callable(has_field):
        try:
            return bool(has_field("wallet"))
        except ValueError:
            return False
    return getattr(venue, "wallet", None) is not None


def build_portfolio_wallet_from_snapshot(
    snapshot: Any,
    allowed_routes: set[tuple[str, str]],
) -> PortfolioWalletRuntime:
    """Convert a core ``PortfolioSnapshot`` into a routed portfolio runtime."""
    wallets: dict[tuple[str, str, int], Any] = {}
    for venue in getattr(snapshot, "venues", []) or []:
        exchange, market = _venue_route(venue)
        venue_id = _venue_id(venue)
        if exchange != "binance":
            raise ValueError(f"unsupported portfolio wallet exchange: {exchange}")
        if market not in {"spot", "perpetual_futures"}:
            raise ValueError(f"unsupported portfolio wallet market: {market}")
        wallets[(exchange, market, venue_id)] = _build_binance_wallet_from_venue_snapshot(
            venue,
            market=market,
            updated_at=getattr(venue, "updated_at", None) or getattr(snapshot, "updated_at", None),
        )

    return PortfolioWalletRuntime(
        account_id=int(getattr(snapshot, "account_id", 0) or 0),
        allowed_routes=allowed_routes,
        wallets=wallets,
    )
