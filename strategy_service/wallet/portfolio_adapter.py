"""Build portfolio wallet runtimes from core-service portfolio snapshots."""

from __future__ import annotations

from typing import Any

from strategy_service.inputs import _normalize_exchange, _normalize_market
from strategy_service.wallet_adapter import proto_to_account_spec
from strategy_service.wallet.binance import BinanceWalletRuntime
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


def _build_binance_wallet_from_venue_snapshot(
    venue: Any,
    *,
    market: str,
) -> BinanceWalletRuntime:
    wallet = _validated_full_wallet_for_market(venue, market)
    if market == "spot":
        if wallet is None:
            raise ValueError("spot VenueSnapshot requires full canonical wallet")
        return BinanceWalletRuntime.from_canonical(proto_to_account_spec(wallet))
    if market == "perpetual_futures":
        if wallet is None:
            raise ValueError("futures VenueSnapshot requires full canonical wallet")
        return BinanceWalletRuntime.from_canonical(proto_to_account_spec(wallet))
    raise ValueError(f"unsupported portfolio wallet market for binance: {market}")


def _has_message_field(message: Any, field_name: str) -> bool:
    has_field = getattr(message, "HasField", None)
    if callable(has_field):
        try:
            return bool(has_field(field_name))
        except ValueError:
            return False
    return getattr(message, field_name, None) is not None


def _has_full_wallet(venue: Any) -> bool:
    return _has_message_field(venue, "wallet")


def _validated_full_wallet_for_market(venue: Any, market: str) -> Any | None:
    if not _has_full_wallet(venue):
        return None

    wallet = getattr(venue, "wallet", None)
    if market == "spot":
        if not _spot_wallet_has_content(getattr(wallet, "spot", None)):
            raise ValueError("spot VenueSnapshot requires non-empty full canonical wallet")
        return wallet
    if market == "perpetual_futures":
        if not _futures_wallet_has_content(getattr(wallet, "futures", None)):
            raise ValueError("futures VenueSnapshot requires non-empty full canonical wallet")
        return wallet
    return wallet


def _spot_wallet_has_content(spot: Any) -> bool:
    if spot is None:
        return False
    return (
        _float_field(spot, "free") != 0.0
        or _float_field(spot, "locked") != 0.0
        or bool(list(getattr(spot, "assets", []) or []))
    )


def _futures_wallet_has_content(futures: Any) -> bool:
    if futures is None:
        return False
    balance_fields = (
        "initial_balance",
        "deposit_sum",
        "withdrawal_sum",
        "wallet_balance",
        "available_balance",
        "total_unrealized_pnl",
        "total_margin_balance",
        "total_position_initial_margin",
        "total_open_order_initial_margin",
        "total_maint_margin",
        "total_cross_wallet_balance",
        "total_cross_un_pnl",
        "margin_balance",
        "unrealized_pnl",
    )
    return (
        any(_float_field(futures, field_name) != 0.0 for field_name in balance_fields)
        or bool(list(getattr(futures, "positions", []) or []))
        or bool(list(getattr(futures, "risk_metadata", []) or []))
    )


def build_portfolio_wallet_from_snapshot(
    snapshot: Any,
    allowed_routes: set[tuple[str, str]],
) -> PortfolioWalletRuntime:
    """Convert a core ``PortfolioSnapshot`` into a routed portfolio runtime."""
    normalized_allowed_routes = {
        _validate_route(exchange, market)
        for exchange, market in allowed_routes
    }
    wallets: dict[tuple[str, str, int], Any] = {}
    for venue in getattr(snapshot, "venues", []) or []:
        exchange, market = _venue_route(venue)
        if (exchange, market) not in normalized_allowed_routes:
            continue
        venue_id = _venue_id(venue)
        if exchange != "binance":
            raise ValueError(f"unsupported portfolio wallet exchange: {exchange}")
        if market not in {"spot", "perpetual_futures"}:
            raise ValueError(f"unsupported portfolio wallet market: {market}")
        wallets[(exchange, market, venue_id)] = _build_binance_wallet_from_venue_snapshot(
            venue,
            market=market,
        )

    return PortfolioWalletRuntime(
        account_id=int(getattr(snapshot, "account_id", 0) or 0),
        allowed_routes=normalized_allowed_routes,
        wallets=wallets,
    )
