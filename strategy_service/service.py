from __future__ import annotations

from strategy_service.wallet.runtime import WalletRuntime

from strategy_service.notification import StrategyNotifier
from strategy_service.order_client import OrderClient
from strategy_service.strategy.base import BaseStrategy
from strategy_service.strategy.user import UserStrategy
from strategy_service.types import MarketData


class StrategyEngine:
    """Route market ticks to strategies by declared (market, symbol, interval).

    Pre-C3: the router is bound only to each strategy's declared ``INPUTS``
    universe. Wallet positions / assets no longer contribute to the routing
    key set — a strategy with an empty wallet still receives the ticks it
    declared.
    """

    def __init__(self) -> None:
        self.strategies: dict[str, BaseStrategy] = {}
        # Key: (market, symbol, interval)  →  strategy instance.
        self.strategy_router: dict[tuple[str, str, str], BaseStrategy] = {}

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
    ) -> UserStrategy:
        user_strategy = UserStrategy(
            strategy_path,
            wallet,
            order_client=order_client,
            account_id=account_id,
            strategy_id=strategy_id,
            session_id=session_id,
            strategy_code=strategy_code,
            notifier=notifier,
        )
        self.strategies[user_id] = user_strategy
        # Register every declared (market, symbol, interval) → this strategy.
        for inp in user_strategy.declared_inputs:
            self.strategy_router[inp.key] = user_strategy
        return user_strategy

    def running_strategy(self, market_data: MarketData) -> bool:
        market = str(market_data.market).strip().lower()
        symbol = str(market_data.symbol).strip().upper()
        interval = str(getattr(market_data, "interval", "")).strip()
        key = (market, symbol, interval)
        strategy = self.strategy_router.get(key)
        if strategy is None:
            return False
        strategy.running_strategy(market_data)
        return True


# Backward-compatible alias
StrategyService = StrategyEngine

__all__ = ["StrategyEngine", "StrategyService"]
