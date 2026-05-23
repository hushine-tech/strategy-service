"""Framework data types."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from strategy_service.wallet.order_types import ExecutionFeedback, OrderResponse

__all__ = ["MarketData", "OrderDecision", "OrderResponse", "ExecutionFeedback"]


@dataclass
class OrderDecision:
    """User strategy output: intent to trade (mock-filled in v1)."""

    symbol: str
    side: str
    qty: float
    price: float | None = None
    market: str | None = None  # "futures" / "spot"; None = 继承触发 tick 的 market


@dataclass
class MarketData:
    """Inbound market snapshot; optional fields use Any to avoid hard dependency on market_data.models."""

    symbol: str
    price: float
    timestamp: Any
    market: str = "futures"  # "futures" / "spot"
    # Bar interval ("1m", "5m", "1h", ...). Part of the routing key for the
    # declaration-bound strategy router. Kept mandatory-with-default so existing
    # call sites that build MarketData directly keep working; canonical flow is
    # via data_loop._adapt_kline which always sets this from MarketKline.interval.
    interval: str = "1m"
    klines: Any = None
    orderbook: Any = None
    oi: float | None = None
    funding_rate: float | None = None
