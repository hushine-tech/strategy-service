from __future__ import annotations

from typing import Callable

from strategy_service.wallet.runtime import WalletRuntime

from strategy_service.notification import StrategyNotifier
from strategy_service.order_client import OrderClient
from strategy_service.strategy.base import BaseStrategy, StrategyUserCodeFatalError
from strategy_service.strategy_imports import PreparedStrategy, _is_sealed_prepared_strategy
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
        prepared_strategy: PreparedStrategy,
        wallet: WalletRuntime,
        order_client: OrderClient | None = None,
        portfolio_id: int = 0,
        strategy_id: int = 0,
        session_id: str = "",
        notifier: StrategyNotifier | None = None,
        on_user_code_error: Callable[[str], None] | None = None,
        on_user_code_recovered: Callable[[], None] | None = None,
        on_user_code_fatal: Callable[[StrategyUserCodeFatalError], None] | None = None,
    ) -> BaseStrategy:
        if not _is_sealed_prepared_strategy(prepared_strategy):
            raise TypeError("StrategyEngine.create_strategy requires PreparedStrategy")
        user_strategy = BaseStrategy(
            prepared_strategy,
            wallet,
            order_client=order_client,
            portfolio_id=portfolio_id,
            strategy_id=strategy_id,
            session_id=session_id,
            notifier=notifier,
            on_user_code_error=on_user_code_error,
            on_user_code_recovered=on_user_code_recovered,
            on_user_code_fatal=on_user_code_fatal,
        )
        self.strategies[user_id] = user_strategy
        # Register every declared (market, symbol, interval) → this strategy.
        for inp in user_strategy.declared_inputs:
            self.strategy_router[inp.key] = user_strategy
        return user_strategy

    def running_strategy(self, market_data: MarketData) -> bool:
        self.raise_if_user_code_fatal()
        exchange = _normalize_exchange(getattr(market_data, "exchange", "binance"))
        market = _normalize_market(market_data.market)
        symbol = str(market_data.symbol).strip().upper()
        interval = str(getattr(market_data, "interval", "")).strip()
        key = (exchange, market, symbol, interval)
        strategy = self.strategy_router.get(key)
        if strategy is None:
            return False
        strategy.running_strategy(market_data)
        self.raise_if_user_code_fatal()
        return True

    def raise_if_user_code_fatal(self) -> None:
        for strategy in self.strategies.values():
            strategy.raise_if_user_code_fatal()

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
