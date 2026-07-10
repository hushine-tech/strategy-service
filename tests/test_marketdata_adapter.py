"""Tests for the platform market-data adapter."""

from market_data.models import MarketKline

from strategy_service.marketdata_adapter import _adapt_kline
from strategy_service.types import MarketData


def test_adapt_kline_maps_fields():
    kline = MarketKline(
        symbol="BTCUSDT",
        interval="1m",
        open_time=1000,
        close_time=60000,
        open=49000.0,
        high=51000.0,
        low=48000.0,
        close=50000.0,
        volume=100.5,
        timestamp=60000,
    )

    md = _adapt_kline(kline)

    assert isinstance(md, MarketData)
    assert md.symbol == "BTCUSDT"
    assert md.price == 50000.0
    assert md.timestamp == 60000
    assert md.market == "futures"
    assert md.interval == "1m"
    assert md.klines == {
        "open": 49000.0,
        "high": 51000.0,
        "low": 48000.0,
        "close": 50000.0,
        "volume": 100.5,
    }
    assert md.orderbook is None
    assert md.oi is None
    assert md.funding_rate is None


def test_adapt_kline_uses_explicit_market():
    kline = MarketKline(
        symbol="ETHUSDT",
        interval="5m",
        open_time=2000,
        close_time=300000,
        open=3000.0,
        high=3100.0,
        low=2950.0,
        close=3050.0,
        volume=50.0,
        timestamp=300000,
    )

    md = _adapt_kline(kline, "spot")

    assert md.market == "spot"
