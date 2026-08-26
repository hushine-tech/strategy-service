from __future__ import annotations

from types import SimpleNamespace

import pytest

from market_data.models import MarketKline

from strategy_service.backtest_pages import PagedBacktestDataSource
from strategy_service.platform_proxy import MarketFundingFact


class FakeMarketDataClient:
    def __init__(self, pages):
        self.pages = {key: list(value) for key, value in pages.items()}
        self.calls = []

    def fetch_backtest_page(self, **kwargs):
        self.calls.append(kwargs)
        stream_key = (
            f"{kwargs['exchange']}/{kwargs['market']}/"
            f"{kwargs['kind']}/{kwargs['symbol']}/{kwargs['interval']}"
        )
        page = self.pages[stream_key].pop(0)
        rows = list(page.get("klines", ()))
        return SimpleNamespace(
            stream_key=stream_key,
            klines=rows,
            funding_facts=list(page.get("funding_facts", ())),
            funding_coverage_complete=page.get("funding_coverage_complete"),
            next_cursor_time_ms=(
                int(page["next_cursor_time_ms"])
                if "next_cursor_time_ms" in page
                else (rows[-1].open_time if rows else kwargs["start_after_time_ms"])
            ),
            has_more=bool(self.pages[stream_key]),
        )


def kline(symbol: str, open_time: int, market: str = "futures", interval: str = "1s") -> MarketKline:
    return MarketKline(
        symbol=symbol,
        interval=interval,
        open_time=open_time,
        close_time=open_time + 999,
        open=1.0,
        high=1.1,
        low=0.9,
        close=1.0,
        volume=1.0,
        timestamp=open_time,
        market=market,
    )


def funding(symbol: str, funding_time_ms: int, *, exchange: str = "binance") -> MarketFundingFact:
    return MarketFundingFact(
        exchange=exchange,
        market="futures",
        symbol=symbol,
        funding_time_ms=funding_time_ms,
        funding_rate_decimal="0.000100000000000001",
        mark_price_decimal="50000.123456789012345678",
        settlement_asset="USDT",
    )


def binding(symbol: str, interval: str = "1s", *, market: str = "futures", exchange: str = "binance"):
    return SimpleNamespace(
        exchange=exchange,
        market=market,
        kind="kline",
        symbol=symbol,
        interval=interval,
    )


def test_timeline_reads_page_boundary_funding_once_before_same_time_kline():
    client = FakeMarketDataClient({
        "binance/futures/kline/BTCUSDT/1s": [
            {
                "klines": [kline("BTCUSDT", 1_000), kline("BTCUSDT", 2_000)],
                "funding_facts": [funding("BTCUSDT", 2_000)],
                "funding_coverage_complete": True,
            },
            {
                "klines": [kline("BTCUSDT", 3_000)],
                "funding_facts": [funding("BTCUSDT", 2_000)],
                "funding_coverage_complete": True,
            },
        ],
    })
    source = PagedBacktestDataSource(
        client,
        start_time_ms=1_000,
        end_time_ms=4_000,
        streams=[binding("BTCUSDT")],
    )

    events = list(source.iter_timeline())

    assert [
        (event.kind, event.market_time_ms)
        for event in events
        if event.kind != "coverage"
    ] == [
        ("kline", 1_000),
        ("funding", 2_000),
        ("kline", 2_000),
        ("kline", 3_000),
    ]
    assert len(client.calls) == 2
    assert client.calls[0]["start_after_time_ms"] == 0
    assert client.calls[1]["start_after_time_ms"] == 2_000


def test_same_time_symbols_use_stream_order_with_funding_before_klines():
    client = FakeMarketDataClient({
        "okx/futures/kline/ETHUSDT/1s": [{
            "klines": [kline("ETHUSDT", 9_000)],
            "funding_facts": [funding("ETHUSDT", 9_000, exchange="okx")],
            "funding_coverage_complete": True,
        }],
        "binance/futures/kline/ZECUSDT/1s": [{
            "klines": [kline("ZECUSDT", 9_000)],
            "funding_facts": [funding("ZECUSDT", 9_000)],
            "funding_coverage_complete": True,
        }],
    })
    source = PagedBacktestDataSource(
        client,
        start_time_ms=9_000,
        end_time_ms=10_000,
        streams=[
            binding("ETHUSDT", exchange="okx"),
            binding("ZECUSDT"),
        ],
    )

    events = list(source.iter_timeline())

    assert [
        (event.kind, event.stream_index, event.payload.symbol)
        for event in events
        if event.kind != "coverage"
    ] == [
        ("funding", 0, "ETHUSDT"),
        ("funding", 1, "ZECUSDT"),
        ("kline", 0, "ETHUSDT"),
        ("kline", 1, "ZECUSDT"),
    ]
    assert [call["exchange"] for call in client.calls] == ["okx", "binance"]


def test_spot_timeline_contains_only_klines_and_no_funding_coverage_requirement():
    client = FakeMarketDataClient({
        "binance/spot/kline/ZECUSDT/1s": [{
            "klines": [kline("ZECUSDT", 1_000, market="spot")],
        }],
    })
    source = PagedBacktestDataSource(
        client,
        start_time_ms=1_000,
        end_time_ms=2_000,
        streams=[binding("ZECUSDT", market="spot")],
    )

    events = list(source.iter_timeline())

    assert [(event.kind, event.funding_coverage_complete) for event in events] == [
        ("kline", None),
    ]
    assert not hasattr(source, "iter_klines")


def test_same_funding_fact_across_1m_and_5m_inputs_is_emitted_once_globally():
    repeated = funding("BTCUSDT", 9_000)
    client = FakeMarketDataClient({
        "binance/futures/kline/BTCUSDT/1m": [{
            "klines": [kline("BTCUSDT", 9_000, interval="1m")],
            "funding_facts": [repeated],
            "funding_coverage_complete": True,
        }],
        "binance/futures/kline/BTCUSDT/5m": [{
            "klines": [kline("BTCUSDT", 9_000, interval="5m")],
            "funding_facts": [repeated],
            "funding_coverage_complete": True,
        }],
    })
    source = PagedBacktestDataSource(
        client,
        start_time_ms=9_000,
        end_time_ms=10_000,
        streams=[binding("BTCUSDT", "1m"), binding("BTCUSDT", "5m")],
    )

    funding_events = [
        event for event in source.iter_timeline() if event.kind == "funding"
    ]

    assert [(event.stream_index, event.market_time_ms) for event in funding_events] == [
        (0, 9_000),
    ]


def test_conflicting_funding_fact_across_inputs_fails_closed_as_ambiguous():
    first = funding("BTCUSDT", 9_000)
    conflict = MarketFundingFact(
        exchange=first.exchange,
        market=first.market,
        symbol=first.symbol,
        funding_time_ms=first.funding_time_ms,
        funding_rate_decimal="0.0002",
        mark_price_decimal=first.mark_price_decimal,
        settlement_asset=first.settlement_asset,
    )
    client = FakeMarketDataClient({
        "binance/futures/kline/BTCUSDT/1m": [{
            "klines": [],
            "funding_facts": [first],
            "funding_coverage_complete": True,
        }],
        "binance/futures/kline/BTCUSDT/5m": [{
            "klines": [],
            "funding_facts": [conflict],
            "funding_coverage_complete": True,
        }],
    })
    source = PagedBacktestDataSource(
        client,
        start_time_ms=9_000,
        end_time_ms=10_000,
        streams=[binding("BTCUSDT", "1m"), binding("BTCUSDT", "5m")],
    )

    with pytest.raises(ValueError, match="ambiguous"):
        list(source.iter_timeline())
