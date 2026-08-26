"""Portfolio wallet runtime for declaration-routed strategy sessions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from strategy_service.inputs import _normalize_exchange, _normalize_market
from strategy_service.funding_position_tracker import FundingPositionTracker

RouteKey = tuple[str, str]
VenueWalletKey = tuple[str, str, int]


@dataclass
class PortfolioWalletRuntime:
    portfolio_id: int
    allowed_routes: set[RouteKey]
    wallets: dict[VenueWalletKey, Any] = field(default_factory=dict)
    funding_position_tracker: FundingPositionTracker = field(default_factory=FundingPositionTracker)

    def __post_init__(self) -> None:
        self.allowed_routes = {
            (_normalize_exchange(exchange), _normalize_market(market))
            for exchange, market in self.allowed_routes
        }
        self.wallets = {
            (
                _normalize_exchange(exchange),
                _normalize_market(market),
                int(venue_id),
            ): wallet
            for (exchange, market, venue_id), wallet in self.wallets.items()
        }

    def get(self, exchange: str, market: str) -> Any:
        route = self._normalize_route(exchange, market)
        self._require_declared(route)
        matches = [
            (venue_id, wallet)
            for (wallet_exchange, wallet_market, venue_id), wallet in self.wallets.items()
            if (wallet_exchange, wallet_market) == route
        ]
        if not matches:
            raise ValueError(
                f"missing wallet for route {route[0]}/{route[1]}"
            )
        if len(matches) > 1:
            venue_ids = ", ".join(str(venue_id) for venue_id, _wallet in matches)
            raise ValueError(
                f"ambiguous wallet route {route[0]}/{route[1]} "
                f"matched venue ids: {venue_ids}"
            )
        return matches[0][1]

    def on_market_data(
        self,
        exchange: str,
        market: str,
        symbol: str,
        symbol_type: str,
        price: float,
    ) -> None:
        wallet = self.get(exchange, market)
        wallet.on_market_data(symbol, symbol_type, price)

    def on_order(
        self,
        exchange: str,
        market: str,
        venue_id: int,
        symbol: str,
        symbol_type: str,
        order_resp: object,
    ) -> None:
        route = self._normalize_route(exchange, market)
        self._require_declared(route)
        key = (route[0], route[1], int(venue_id))
        wallet = self.wallets.get(key)
        if wallet is None:
            raise ValueError(
                f"missing wallet for route {route[0]}/{route[1]} venue {venue_id}"
            )
        is_lifecycle_event = bool(str(getattr(order_resp, "event_type", "") or "").strip())
        event_venue_id = int(getattr(order_resp, "venue_id", 0) or 0)
        if is_lifecycle_event and event_venue_id <= 0:
            raise ValueError("order lifecycle event venue_id is required")
        if event_venue_id and event_venue_id != int(venue_id):
            raise ValueError("order event venue_id does not match routed wallet")
        event_symbol = str(getattr(order_resp, "symbol", "") or "").strip().upper()
        if is_lifecycle_event and not event_symbol:
            raise ValueError("order lifecycle event symbol is required")
        if event_symbol and event_symbol != str(symbol or "").strip().upper():
            raise ValueError("order event symbol does not match routed wallet")
        self._track_futures_fill_before_wallet_mutation(
            wallet, symbol, symbol_type, order_resp
        )
        wallet.on_order(symbol, symbol_type, order_resp)

    def _track_futures_fill_before_wallet_mutation(
        self, wallet: Any, symbol: str, symbol_type: str, order_resp: object
    ) -> None:
        if str(symbol_type or "").strip().lower() != "futures":
            return
        raw_qty = getattr(order_resp, "qty_decimal", "")
        trade_id = getattr(order_resp, "exchange_trade_id", "")
        if (raw_qty in (None, "") and trade_id in (None, "")) or getattr(
            order_resp, "event_type", ""
        ) in (None, ""):
            return
        futures = getattr(wallet, "futures", None)
        if futures is None:
            return
        position_mode = str(getattr(futures, "position_mode", "") or "").strip().lower()
        if position_mode not in {"one_way", "hedge"}:
            raise ValueError("canonical Futures position_mode is missing or invalid")
        normalized_symbol = str(symbol or "").strip().upper()
        metadata = getattr(futures, "risk_metadata", {}).get(normalized_symbol)
        configured_margin_mode = str(
            getattr(metadata, "configured_margin_mode", "") or ""
        ).strip().lower()
        if configured_margin_mode in {"cross", "isolated"}:
            margin_mode = configured_margin_mode
        else:
            wallet_margin_mode = str(getattr(futures, "margin_mode", "") or "").strip().lower()
            configured_modes = {
                str(getattr(item, "configured_margin_mode", "") or "").strip().lower()
                for item in getattr(futures, "risk_metadata", {}).values()
                if str(getattr(item, "configured_margin_mode", "") or "").strip().lower()
            }
            position_modes = {
                str(getattr(position, "margin_mode", "") or "").strip().lower()
                for position in getattr(futures, "positions", {}).values()
                if str(getattr(position, "margin_mode", "") or "").strip().lower()
            }
            if (
                wallet_margin_mode not in {"cross", "isolated"}
                or configured_modes not in (set(), {wallet_margin_mode})
                or position_modes not in (set(), {wallet_margin_mode})
            ):
                raise ValueError("canonical Futures margin_mode is missing or ambiguous")
            margin_mode = wallet_margin_mode
        expected_side = (
            "BOTH"
            if position_mode == "one_way"
            else str(getattr(order_resp, "position_side", "") or "").strip().upper()
        )
        for leg in self.funding_position_tracker.legs_for(int(getattr(order_resp, "venue_id", 0) or 0), normalized_symbol):
            if leg.position_side == expected_side and leg.margin_mode != margin_mode:
                raise ValueError("canonical Futures margin_mode conflicts with restored leg")
        self.funding_position_tracker.on_lifecycle_fill(
            order_resp, position_mode=position_mode, margin_mode=margin_mode
        )

    def on_ledger_event(
        self,
        exchange: str,
        market: str,
        venue_id: int,
        event: object,
    ) -> None:
        route = self._normalize_route(exchange, market)
        self._require_declared(route)
        if route[1] != "perpetual_futures":
            raise ValueError("Funding ledger events require a Futures route")
        wallet = self.wallets.get((route[0], route[1], int(venue_id)))
        if wallet is None:
            raise ValueError(
                f"missing wallet for route {route[0]}/{route[1]} venue {venue_id}"
            )
        wallet.on_ledger_event(event)

    def _normalize_route(self, exchange: str, market: str) -> RouteKey:
        return (_normalize_exchange(exchange), _normalize_market(market))

    def _require_declared(self, route: RouteKey) -> None:
        if route not in self.allowed_routes:
            raise ValueError(f"wallet route {route[0]}/{route[1]} is not declared")
