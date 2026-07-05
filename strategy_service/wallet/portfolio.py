"""Portfolio wallet runtime for declaration-routed strategy sessions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from strategy_service.inputs import _normalize_exchange, _normalize_market

RouteKey = tuple[str, str]
VenueWalletKey = tuple[str, str, int]


@dataclass
class PortfolioWalletRuntime:
    portfolio_id: int
    allowed_routes: set[RouteKey]
    wallets: dict[VenueWalletKey, Any] = field(default_factory=dict)

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
        wallet.on_order(symbol, symbol_type, order_resp)

    def _normalize_route(self, exchange: str, market: str) -> RouteKey:
        return (_normalize_exchange(exchange), _normalize_market(market))

    def _require_declared(self, route: RouteKey) -> None:
        if route not in self.allowed_routes:
            raise ValueError(f"wallet route {route[0]}/{route[1]} is not declared")
