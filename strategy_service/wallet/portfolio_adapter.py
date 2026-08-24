"""Build portfolio wallet runtimes from core-service portfolio snapshots."""

from __future__ import annotations

from dataclasses import asdict
from typing import Any, Iterable

from strategy_service.inputs import _normalize_exchange, _normalize_market
from strategy_service.wallet_adapter import proto_to_portfolio_spec
from strategy_service.wallet.binance import BinanceWalletRuntime
from strategy_service.wallet.canonical import SpotSymbolFilter, SpotSymbolMetadata
from strategy_service.wallet.portfolio import PortfolioWalletRuntime
from strategy_service.wallet_factory import install_simulated_target_leverages


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
        runtime = BinanceWalletRuntime.from_canonical(proto_to_portfolio_spec(wallet))
        venue_id = _venue_id(venue)
        for raw in getattr(venue, "spot_symbols", []) or []:
            runtime.spot.register_metadata(_spot_metadata_from_proto(raw, venue_id=venue_id))
        return runtime
    if market == "perpetual_futures":
        if wallet is None:
            raise ValueError("futures VenueSnapshot requires full canonical wallet")
        return BinanceWalletRuntime.from_canonical(proto_to_portfolio_spec(wallet))
    raise ValueError(f"unsupported portfolio wallet market for binance: {market}")


def _spot_metadata_from_proto(raw: Any, *, venue_id: int) -> SpotSymbolMetadata:
    return SpotSymbolMetadata(
        venue_id=int(venue_id),
        exchange="binance",
        market="spot",
        symbol=str(getattr(raw, "symbol", "") or "").strip().upper(),
        status=str(getattr(raw, "status", "") or "").strip().upper(),
        base_asset=str(getattr(raw, "base_asset", "") or "").strip().upper(),
        quote_asset=str(getattr(raw, "quote_asset", "") or "").strip().upper(),
        base_asset_precision=int(getattr(raw, "base_asset_precision", 0) or 0),
        quote_asset_precision=int(getattr(raw, "quote_asset_precision", 0) or 0),
        spot_trading_allowed=bool(getattr(raw, "spot_trading_allowed", False)),
        permission_sets=tuple(
            tuple(str(value).strip().upper() for value in getattr(group, "alternatives", []) or [])
            for group in getattr(raw, "permission_sets", []) or []
        ),
        order_types=tuple(
            str(value).strip().upper() for value in getattr(raw, "order_types", []) or []
        ),
        filters=tuple(_spot_filter_from_proto(value) for value in getattr(raw, "filters", []) or []),
        snapshot_time_ms=int(getattr(raw, "snapshot_time_ms", 0) or 0),
    )


def _spot_filter_from_proto(raw: Any) -> SpotSymbolFilter:
    return SpotSymbolFilter(
        filter_type=str(getattr(raw, "filter_type", "") or ""),
        min_price=str(getattr(raw, "min_price", "") or ""),
        max_price=str(getattr(raw, "max_price", "") or ""),
        tick_size=str(getattr(raw, "tick_size", "") or ""),
        min_qty=str(getattr(raw, "min_qty", "") or ""),
        max_qty=str(getattr(raw, "max_qty", "") or ""),
        step_size=str(getattr(raw, "step_size", "") or ""),
        min_notional=str(getattr(raw, "min_notional", "") or ""),
        max_notional=str(getattr(raw, "max_notional", "") or ""),
        apply_to_market=bool(getattr(raw, "apply_to_market", False)),
        apply_min_to_market=bool(getattr(raw, "apply_min_to_market", False)),
        apply_max_to_market=bool(getattr(raw, "apply_max_to_market", False)),
        avg_price_mins=int(getattr(raw, "avg_price_mins", 0) or 0),
        limit=int(getattr(raw, "limit", 0) or 0),
        multiplier_up=str(getattr(raw, "multiplier_up", "") or ""),
        multiplier_down=str(getattr(raw, "multiplier_down", "") or ""),
        bid_multiplier_up=str(getattr(raw, "bid_multiplier_up", "") or ""),
        bid_multiplier_down=str(getattr(raw, "bid_multiplier_down", "") or ""),
        ask_multiplier_up=str(getattr(raw, "ask_multiplier_up", "") or ""),
        ask_multiplier_down=str(getattr(raw, "ask_multiplier_down", "") or ""),
        raw_json=str(getattr(raw, "raw_json", "") or ""),
        max_position=str(getattr(raw, "max_position", "") or ""),
        max_num_orders=int(getattr(raw, "max_num_orders", 0) or 0),
        max_num_algo_orders=int(getattr(raw, "max_num_algo_orders", 0) or 0),
        max_num_iceberg_orders=int(getattr(raw, "max_num_iceberg_orders", 0) or 0),
        max_num_order_amends=int(getattr(raw, "max_num_order_amends", 0) or 0),
        max_num_order_lists=int(getattr(raw, "max_num_order_lists", 0) or 0),
    )


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
        if not _has_message_field(wallet, "spot"):
            raise ValueError(
                "spot VenueSnapshot requires full canonical wallet with SpotWallet presence"
            )
        return wallet
    if market == "perpetual_futures":
        if not _futures_wallet_has_content(getattr(wallet, "futures", None)):
            raise ValueError("futures VenueSnapshot requires non-empty full canonical wallet")
        return wallet
    return wallet


def _futures_wallet_has_content(futures: Any) -> bool:
    if futures is None:
        return False

    if str(getattr(futures, "margin_mode", "") or "").strip():
        return True
    if str(getattr(futures, "position_mode", "") or "").strip():
        return True

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
    *,
    simulated_order_targets: Iterable[Any] = (),
) -> PortfolioWalletRuntime:
    """Convert a core ``PortfolioSnapshot`` into a routed portfolio runtime."""
    normalized_allowed_routes = {
        _validate_route(exchange, market)
        for exchange, market in allowed_routes
    }
    simulated_futures_targets: dict[tuple[str, str], list[Any]] = {}
    for target in simulated_order_targets:
        route = _validate_route(
            getattr(target, "exchange", ""),
            getattr(target, "market", ""),
        )
        if route[1] == "spot":
            continue
        if route[1] != "perpetual_futures":
            raise ValueError(f"unsupported simulated target market: {route[1]}")
        simulated_futures_targets.setdefault(route, []).append(target)
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
        runtime = _build_binance_wallet_from_venue_snapshot(
            venue,
            market=market,
        )
        install_simulated_target_leverages(
            runtime,
            simulated_futures_targets.get((exchange, market), ()),
            exchange=exchange,
            market=market,
        )
        wallets[(exchange, market, venue_id)] = runtime

    return PortfolioWalletRuntime(
        portfolio_id=int(getattr(snapshot, "portfolio_id", 0) or 0),
        allowed_routes=normalized_allowed_routes,
        wallets=wallets,
    )


def apply_venue_wallet_snapshot(
    runtime: PortfolioWalletRuntime,
    venue: Any,
    *,
    expected_environment: int,
) -> tuple[str, str, int]:
    """Replace one existing route with a core-authoritative Venue snapshot."""
    if not isinstance(runtime, PortfolioWalletRuntime):
        raise ValueError("portfolio wallet runtime is required")
    exchange, market = _venue_route(venue)
    venue_id = _venue_id(venue)
    key = (exchange, market, venue_id)
    if key not in runtime.wallets:
        raise ValueError(
            f"authoritative snapshot route is not in the Session wallet: "
            f"{exchange}/{market} venue {venue_id}"
        )
    environment = int(getattr(venue, "environment", 0) or 0)
    if environment != int(expected_environment):
        raise ValueError(
            f"authoritative snapshot environment mismatch: got {environment}, "
            f"want {int(expected_environment)}"
        )
    runtime.wallets[key] = _build_binance_wallet_from_venue_snapshot(venue, market=market)
    return key


def attach_spot_risk_snapshots(
    runtime: PortfolioWalletRuntime,
    snapshots: Any,
) -> None:
    """Attach immutable preflight facts to their exact routed Spot wallet."""
    for raw in snapshots or []:
        venue_id = int(getattr(raw, "venue_id", 0) or 0)
        exchange = _enum_label(getattr(raw, "exchange", 0), _EXCHANGE_LABELS, "exchange")
        market = _enum_label(getattr(raw, "market", 0), _MARKET_LABELS, "market")
        key = (*_validate_route(exchange, market), venue_id)
        venue_wallet = runtime.wallets.get(key)
        if venue_wallet is None:
            raise ValueError(
                f"Spot risk snapshot route has no wallet: {exchange}/{market} venue {venue_id}"
            )
        if market != "spot" or not hasattr(venue_wallet, "spot"):
            raise ValueError("Spot risk snapshot must resolve to a Spot wallet")
        metadata = _spot_metadata_from_proto(getattr(raw, "metadata", None), venue_id=venue_id)
        venue_wallet.spot.register_risk_facts(
            snapshot_id=str(getattr(raw, "snapshot_id", "") or ""),
            environment=int(getattr(raw, "environment", -1)),
            metadata=metadata,
            exchange_filters=[
                asdict(_spot_filter_from_proto(item))
                for item in getattr(raw, "exchange_filters", []) or []
            ],
            symbol_filters=[
                asdict(_spot_filter_from_proto(item))
                for item in getattr(raw, "symbol_filters", []) or []
            ],
            asset_filters=[
                {
                    "filter_type": str(getattr(item, "filter_type", "") or ""),
                    "asset": str(getattr(item, "asset", "") or ""),
                    "limit": str(getattr(item, "limit", "") or ""),
                }
                for item in getattr(raw, "asset_filters", []) or []
            ],
            open_orders=[
                {
                    "symbol": str(getattr(item, "symbol", "") or ""),
                    "side": str(getattr(item, "side", "") or ""),
                    "orig_qty_decimal": str(
                        getattr(item, "orig_qty_decimal", "") or ""
                    ),
                    "executed_qty_decimal": str(
                        getattr(item, "executed_qty_decimal", "") or ""
                    ),
                }
                for item in getattr(raw, "open_orders", []) or []
            ],
            reference_price_decimal=str(
                getattr(raw, "reference_price_decimal", "") or ""
            ),
        )
