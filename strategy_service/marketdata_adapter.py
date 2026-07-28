"""Adapt platform market-data messages to the public strategy data model."""

from __future__ import annotations

from market_data.models import MarketKline

from strategy_service.types import MarketData


def _adapt_kline(kline: MarketKline, market: str | None = None) -> MarketData:
    """Map one platform K-line to the strategy framework representation."""
    resolved_market = market
    if resolved_market is None:
        raw_market = getattr(kline, "market", None)
        if isinstance(raw_market, str) and raw_market.strip():
            resolved_market = raw_market.strip().lower()
        else:
            resolved_market = "futures"
    return MarketData(
        symbol=kline.symbol,
        price=float(kline.close),
        timestamp=kline.timestamp,
        market=resolved_market,
        interval=str(getattr(kline, "interval", "") or "1m").strip(),
        klines={
            "open_time": kline.open_time,
            "close_time": kline.close_time,
            "timestamp": kline.timestamp,
            "open": kline.open,
            "high": kline.high,
            "low": kline.low,
            "close": kline.close,
            "volume": kline.volume,
        },
        orderbook=None,
        oi=None,
        funding_rate=None,
    )
