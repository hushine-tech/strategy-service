from strategy_service.service import StrategyEngine
from strategy_service.strategy.base import BaseStrategy
from strategy_service.types import (
    ExecutionFeedback,
    MarketData,
    OrderDecision,
    OrderResponse,
    OrderUpdateEvent,
    OrderUpdateFill,
)

__all__ = [
    "BacktestDataLoop",
    "BaseStrategy",
    "ExecutionFeedback",
    "KafkaConfig",
    "LiveDataLoop",
    "MarketData",
    "OrderDecision",
    "OrderResponse",
    "OrderUpdateEvent",
    "OrderUpdateFill",
    "StrategyEngine",
    "TimescaleConfig",
]


def __getattr__(name: str):
    if name == "BacktestDataLoop":
        from strategy_service.data_loop import BacktestDataLoop

        return BacktestDataLoop
    if name == "LiveDataLoop":
        from strategy_service.data_loop import LiveDataLoop

        return LiveDataLoop
    if name == "KafkaConfig":
        from market_data.config import KafkaConfig

        return KafkaConfig
    if name == "TimescaleConfig":
        from market_data.config import TimescaleConfig

        return TimescaleConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
