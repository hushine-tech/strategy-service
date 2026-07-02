from __future__ import annotations

from typing import Callable

from strategy_service.wallet.runtime import WalletRuntime

from strategy_service.notification import StrategyNotifier
from strategy_service.order_client import OrderClient
from strategy_service.strategy.base import BaseStrategy
from strategy_service.types import MarketData, OrderUpdateEvent
from strategy_service.inputs import _normalize_exchange, _normalize_market


class StrategyEngine:
    """Route market ticks to strategies by declared route key.

    The router is bound only to each strategy's declared ``INPUTS`` universe:
    ``(exchange, market, symbol, interval)``. Wallet positions / assets do not
    contribute to the routing key set.
    """

    def __init__(self) -> None:
        self.strategies: dict[str, BaseStrategy] = {}
        # Key: (exchange, market, symbol, interval) -> strategy instance.
        self.strategy_router: dict[tuple[str, str, str, str], BaseStrategy] = {}

    def create_strategy(
        self,
        user_id: str,
        strategy_path: str,
        wallet: WalletRuntime,
        order_client: OrderClient | None = None,
        account_id: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        strategy_code: str | None = None,
        notifier: StrategyNotifier | None = None,
        hot_reload: bool = False,
        on_user_code_error: Callable[[str], None] | None = None,
        on_user_code_recovered: Callable[[], None] | None = None,
    ) -> BaseStrategy:
        user_strategy = BaseStrategy(
            strategy_path,
            wallet,
            order_client=order_client,
            account_id=account_id,
            strategy_id=strategy_id,
            session_id=session_id,
            strategy_code=strategy_code,
            notifier=notifier,
            hot_reload=hot_reload,
            on_user_code_error=on_user_code_error,
            on_user_code_recovered=on_user_code_recovered,
        )
        self.strategies[user_id] = user_strategy
        # Register every declared (market, symbol, interval) → this strategy.
        for inp in user_strategy.declared_inputs:
            self.strategy_router[inp.key] = user_strategy
        return user_strategy

    def running_strategy(self, market_data: MarketData) -> bool:
        exchange = _normalize_exchange(getattr(market_data, "exchange", "binance"))
        market = _normalize_market(market_data.market)
        symbol = str(market_data.symbol).strip().upper()
        interval = str(getattr(market_data, "interval", "")).strip()
        key = (exchange, market, symbol, interval)
        strategy = self.strategy_router.get(key)
        if strategy is None:
            return False
        strategy.running_strategy(market_data)
        return True

    def handle_order_update(self, event: OrderUpdateEvent) -> bool:
        session_id = str(getattr(event, "session_id", "") or "").strip()
        if not session_id:
            return False
        delivered = False
        for strategy in self.strategies.values():
            if str(getattr(strategy, "_session_id", "") or "").strip() != session_id:
                continue
            strategy.handle_order_update(event)
            delivered = True
        return delivered

__all__ = ["StrategyEngine"]
