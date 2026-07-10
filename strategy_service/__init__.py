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
    "BaseStrategy",
    "ExecutionFeedback",
    "KafkaConfig",
    "MarketData",
    "OrderDecision",
    "OrderResponse",
    "OrderUpdateEvent",
    "OrderUpdateFill",
    "StrategyEngine",
    "TimescaleConfig",
]


def __getattr__(name: str):
    if name == "KafkaConfig":
        from market_data.config import KafkaConfig

        return KafkaConfig
    if name == "TimescaleConfig":
        from market_data.config import TimescaleConfig

        return TimescaleConfig
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
