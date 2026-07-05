from __future__ import annotations

from types import SimpleNamespace

from market_data.models import MarketKline

from strategy_service.backtest_pages import PagedBacktestDataSource


class FakeMarketDataClient:
    def __init__(self, pages):
        self.pages = {k: [list(page) for page in v] for k, v in pages.items()}
        self.calls = []

    def fetch_backtest_page(self, **kwargs):
        self.calls.append(kwargs)
        stream_key = (
            f"{kwargs['exchange']}/{kwargs['market']}/"
            f"{kwargs['kind']}/{kwargs['symbol']}/{kwargs['interval']}"
        )
        rows = self.pages[stream_key].pop(0)
        return SimpleNamespace(
            stream_key=stream_key,
            klines=rows,
            next_cursor_time_ms=rows[-1].open_time if rows else kwargs["start_after_time_ms"],
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


def binding(symbol: str, interval: str = "1s"):
    return SimpleNamespace(
        exchange="binance",
        market="futures",
        kind="kline",
        symbol=symbol,
        interval=interval,
    )


def test_single_stream_reads_multiple_pages_in_order():
    client = FakeMarketDataClient({
        "binance/futures/kline/ETHUSDT/1s": [
            [kline("ETHUSDT", 1000), kline("ETHUSDT", 2000)],
            [kline("ETHUSDT", 3000)],
        ],
    })
    source = PagedBacktestDataSource(
        client,
        start_time_ms=1000,
        end_time_ms=4000,
        streams=[binding("ETHUSDT")],
    )

    rows = list(source.iter_klines())

    assert [r.open_time for r in rows] == [1000, 2000, 3000]
    assert len(client.calls) == 2
    assert client.calls[0]["start_after_time_ms"] == 0
    assert client.calls[1]["start_after_time_ms"] == 2000


def test_multi_stream_merges_by_open_time_with_independent_cursors():
    client = FakeMarketDataClient({
        "binance/futures/kline/ETHUSDT/1s": [[kline("ETHUSDT", 1000), kline("ETHUSDT", 3000)]],
        "binance/futures/kline/BTCUSDT/1s": [[kline("BTCUSDT", 2000), kline("BTCUSDT", 4000)]],
    })
    source = PagedBacktestDataSource(
        client,
        start_time_ms=1000,
        end_time_ms=5000,
        streams=[binding("ETHUSDT"), binding("BTCUSDT")],
    )

    rows = list(source.iter_klines())

    assert [(r.symbol, r.open_time) for r in rows] == [
        ("ETHUSDT", 1000),
        ("BTCUSDT", 2000),
        ("ETHUSDT", 3000),
        ("BTCUSDT", 4000),
    ]
    assert [c["symbol"] for c in client.calls] == ["ETHUSDT", "BTCUSDT"]
