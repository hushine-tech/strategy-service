"""Tests for strategy_service/data_loop.py."""

from unittest.mock import MagicMock, patch

import pytest

from strategy_service import StrategyService
from strategy_service.data_loop import (
    BacktestDataLoop,
    LiveDataLoop,
    _adapt_kline,
)
from strategy_service.types import Exchange, Market, MarketData
from strategy_service.wallet.portfolio import PortfolioWalletRuntime
from tests.helpers.wallet_fixtures import make_backtest_wallet


# ---------------------------------------------------------------------------
# _adapt_kline
# ---------------------------------------------------------------------------

def test_adapt_kline_maps_fields():
    """MarketKline fields map to the correct MarketData fields."""
    from market_data.models import MarketKline

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
    assert md.klines["open"] == 49000.0
    assert md.klines["high"] == 51000.0
    assert md.klines["low"] == 48000.0
    assert md.klines["close"] == 50000.0
    assert md.klines["volume"] == 100.5
    assert md.orderbook is None
    assert md.oi is None
    assert md.funding_rate is None


def test_adapt_kline_optional_fields_are_none():
    """Fields not present in MarketKline are None in MarketData."""
    from market_data.models import MarketKline

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

    md = _adapt_kline(kline)

    assert md.orderbook is None
    assert md.oi is None
    assert md.funding_rate is None


# ---------------------------------------------------------------------------
# BacktestDataLoop
# ---------------------------------------------------------------------------

def test_backtest_loop_calls_running_strategy_per_kline():
    """BacktestDataLoop calls running_strategy once per kline from the source."""
    from market_data.models import MarketKline

    service = StrategyService()
    dispatched_mds: list[MarketData] = []

    def counting_run(md: MarketData):
        dispatched_mds.append(md)

    service.running_strategy = counting_run

    klines = [
        MarketKline(
            symbol="BTCUSDT", interval="1m",
            open_time=1000, close_time=60000,
            open=49000.0, high=51000.0, low=48000.0,
            close=50000.0, volume=100.0, timestamp=60000,
        ),
        MarketKline(
            symbol="BTCUSDT", interval="1m",
            open_time=60000, close_time=120000,
            open=50000.0, high=52000.0, low=49000.0,
            close=51000.0, volume=110.0, timestamp=120000,
        ),
    ]

    class FakeDataSource:
        def get_klines(self, symbol, interval, st, et, market="futures"):
            return iter(klines)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    loop = BacktestDataLoop(service=service, config=None)
    with patch("strategy_service.data_loop.BacktestDataSource", return_value=FakeDataSource()):
        loop.run("BTCUSDT", start_time=0, end_time=200000, interval="1m")

    assert len(dispatched_mds) == 2
    assert dispatched_mds[0].price == 50000.0
    assert dispatched_mds[1].price == 51000.0


def test_backtest_loop_uses_interval_and_market_params():
    """BacktestDataLoop passes interval and market through to BacktestDataSource."""
    from market_data.models import MarketKline

    service = StrategyService()
    received_args: dict = {}

    def capture_run(md: MarketData):
        pass

    service.running_strategy = capture_run

    class FakeDataSource:
        def get_klines(self, symbol, interval, st, et, market="futures"):
            received_args.update(symbol=symbol, interval=interval, st=st, et=et, market=market)
            return iter([])

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    loop = BacktestDataLoop(service=service, config=None)
    with patch("strategy_service.data_loop.BacktestDataSource", return_value=FakeDataSource()):
        loop.run("ETHUSDT", start_time=0, end_time=100000, interval="5m", market="spot")

    assert received_args["symbol"] == "ETHUSDT"
    assert received_args["interval"] == "5m"
    assert received_args["market"] == "spot"


def test_run_declared_iterates_per_declared_interval():
    """pre_C3 gate 2: a strategy declaring BTCUSDT 1m + BTCUSDT 5m must
    replay BOTH iterators through running_strategy — the legacy
    single-interval ``run_account`` would have silently dropped one."""
    from market_data.models import MarketKline

    service = StrategyService()
    seen: list[tuple[str, int, float]] = []

    def capture(md):
        seen.append((md.interval, md.timestamp, md.price))

    service.running_strategy = capture

    # Two iterators, each yielding three klines at different open_times.
    klines_1m = [
        MarketKline(symbol="BTCUSDT", interval="1m",
                    open_time=1000 + i * 60_000, close_time=1000 + (i + 1) * 60_000,
                    open=100.0, high=110.0, low=90.0, close=100.0 + i,
                    volume=1.0, timestamp=1000 + (i + 1) * 60_000)
        for i in range(3)
    ]
    klines_5m = [
        MarketKline(symbol="BTCUSDT", interval="5m",
                    open_time=30_000 + i * 300_000, close_time=30_000 + (i + 1) * 300_000,
                    open=200.0, high=210.0, low=190.0, close=200.0 + i,
                    volume=5.0, timestamp=30_000 + (i + 1) * 300_000)
        for i in range(3)
    ]

    class FakeDataSource:
        def get_klines(self, symbol, interval, _st, _et, market="futures"):
            if interval == "1m":
                return iter(klines_1m)
            if interval == "5m":
                return iter(klines_5m)
            return iter(())

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    loop = BacktestDataLoop(service=service, config=None)
    with patch("strategy_service.data_loop.BacktestDataSource", return_value=FakeDataSource()):
        n = loop.run_declared(
            [("futures", "BTCUSDT", "1m"), ("futures", "BTCUSDT", "5m")],
            start_time=0, end_time=10_000_000,
        )

    assert n == 6
    # Both intervals reached the strategy — neither was silently dropped.
    intervals_seen = {i for i, _, _ in seen}
    assert intervals_seen == {"1m", "5m"}


def test_run_declared_accepts_strategyinput_like_objects():
    """``run_declared`` duck-types ``StrategyInput`` so grpc_server can
    pass declared_inputs straight through without re-serialising."""
    from market_data.models import MarketKline

    service = StrategyService()
    service.running_strategy = MagicMock()

    class DuckInput:
        def __init__(self, market, symbol, interval):
            self.market = market
            self.symbol = symbol
            self.interval = interval

    class FakeDataSource:
        def get_klines(self, symbol, interval, _st, _et, market="futures"):
            return iter([MarketKline(
                symbol=symbol, interval=interval,
                open_time=1000, close_time=60000, open=1.0, high=2.0, low=0.5,
                close=1.5, volume=1.0, timestamp=60000,
            )])

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

    loop = BacktestDataLoop(service=service, config=None)
    with patch("strategy_service.data_loop.BacktestDataSource", return_value=FakeDataSource()):
        n = loop.run_declared(
            [DuckInput("futures", "BTCUSDT", "1m"), DuckInput("spot", "ETHUSDT", "5m")],
            start_time=0, end_time=1_000_000,
        )
    assert n == 2


def _wallet_with_spot_slot(symbol: str = "BTCUSDT", *, spot_free: float = 0.0):
    """Build a backtest wallet with one spot asset slot preconfigured.

    Post-Phase-C2b this goes through the canonical proto path:
    ``make_backtest_wallet`` → ``build_wallet_from_account`` →
    ``BinanceWalletRuntime``. The returned runtime exposes the same
    ``wallet.spot.assets[SYMBOL]`` dict interface the previous legacy
    ``Wallet`` aggregate did, so downstream test assertions are unchanged.
    """
    return make_backtest_wallet(
        margin_mode="isolated",
        spot_free=spot_free,
        spot_assets=[{"symbol": symbol.strip().upper()}],
    )


def _portfolio_wallet(default_wallet, *routes: tuple[str, str]) -> PortfolioWalletRuntime:
    route_set = set(routes) or {(Exchange.BINANCE, Market.SPOT)}
    wallets = {
        (exchange, market, idx): default_wallet
        for idx, (exchange, market) in enumerate(sorted(route_set), start=1001)
    }
    return PortfolioWalletRuntime(1, route_set, wallets)


# ---------------------------------------------------------------------------
# LiveDataLoop
# ---------------------------------------------------------------------------

def test_live_loop_start_calls_live_start():
    """LiveDataLoop.start() calls LiveDataSource.start()."""
    service = StrategyService()
    service.running_strategy = MagicMock()

    loop = LiveDataLoop(service=service, config=None, now_ms_fn=lambda: 60_000)

    with patch.object(loop._live, "start") as mock_start:
        loop.start()
        mock_start.assert_called_once()


def test_live_loop_stop_calls_live_stop():
    """LiveDataLoop.stop() calls LiveDataSource.stop()."""
    service = StrategyService()
    service.running_strategy = MagicMock()

    loop = LiveDataLoop(service=service, config=None, now_ms_fn=lambda: 60_000)
    loop._live.start = MagicMock()
    loop._live.stop = MagicMock()

    loop.stop()
    loop._live.stop.assert_called_once()


def test_live_loop_callback_adapts_and_dispatches():
    """LiveDataLoop's internal callback converts MarketKline and calls running_strategy."""
    from market_data.models import MarketKline

    service = StrategyService()
    dispatched: list[MarketData] = []

    def collect(md: MarketData):
        dispatched.append(md)

    service.running_strategy = collect

    loop = LiveDataLoop(service=service, config=None, now_ms_fn=lambda: 60_000)

    kline = MarketKline(
        symbol="BTCUSDT", interval="1m",
        open_time=1000, close_time=60000,
        open=49000.0, high=51000.0, low=48000.0,
        close=50000.0, volume=100.0, timestamp=60000,
    )

    loop._on_kline(kline)

    assert len(dispatched) == 1
    assert dispatched[0].symbol == "BTCUSDT"
    assert dispatched[0].price == 50000.0
    assert dispatched[0].market == "futures"
    assert dispatched[0].klines["close"] == 50000.0


def test_live_loop_callback_replays_spot_kline_into_spot_strategy():
    """Spot live replay must route by kline.market instead of hard-coding futures.

    Strategy explicitly declares spot BTCUSDT 1m via INPUTS (pre_C3 contract).
    """
    from market_data.models import MarketKline

    wallet = _wallet_with_spot_slot(symbol="BTCUSDT", spot_free=1_000.0)
    service = StrategyService()
    strategy_code = (
        "from strategy_service.types import Exchange, Market, OrderDecision, OrderSide, OrderType\n"
        "\n"
        "class MyStrategy:\n"
        '    INPUTS = [{"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "BTCUSDT", "interval": "1m"}]\n'
        '    ORDER_TARGETS = [{"exchange": Exchange.BINANCE, "market": Market.SPOT, "symbol": "BTCUSDT"}]\n'
        "\n"
        "    def on_market_data(self, data, wallet):\n"
        "        return OrderDecision(exchange=Exchange.BINANCE, market=Market.SPOT, symbol='BTCUSDT', side=OrderSide.BUY, qty='0.1', order_type=OrderType.MARKET)\n"
    )
    service.create_strategy(
        "u1",
        "<db:spot_live_replay>",
        _portfolio_wallet(wallet, (Exchange.BINANCE, Market.SPOT)),
        strategy_code=strategy_code,
    )

    loop = LiveDataLoop(service=service, config=None, now_ms_fn=lambda: 60_000)
    kline = MarketKline(
        symbol="BTCUSDT", interval="1m",
        open_time=1000, close_time=60000,
        open=99.0, high=101.0, low=98.0,
        close=100.0, volume=100.0, timestamp=60000,
        market="spot",
    )

    loop._on_kline(kline)

    assert wallet.spot.assets["BTCUSDT"].qty == pytest.approx(0.1)
    assert wallet.spot.assets["BTCUSDT"].avg_entry_price == pytest.approx(100.0)
    assert wallet.spot.free == pytest.approx(990.0)
    assert wallet.futures.positions == {}


def test_live_loop_calls_unroutable_callback_when_route_misses():
    """LiveDataLoop should surface unroutable live traffic for diagnostics."""
    from market_data.models import MarketKline

    service = StrategyService()
    seen: list[MarketKline] = []
    service.running_strategy = lambda _md: False

    loop = LiveDataLoop(service=service, config=None, on_unroutable=seen.append, now_ms_fn=lambda: 60_000)
    kline = MarketKline(
        symbol="BTCUSDT", interval="1m",
        open_time=1000, close_time=60000,
        open=49000.0, high=51000.0, low=48000.0,
        close=50000.0, volume=100.0, timestamp=60000,
        market="futures",
    )

    loop._on_kline(kline)

    assert seen == [kline]


def test_live_loop_drops_stale_kline_older_than_60_seconds():
    """Live stream guard: pushed klines older than 60s are discarded."""
    from market_data.models import MarketKline

    service = StrategyService()
    service.running_strategy = MagicMock()
    seen: list[MarketKline] = []

    loop = LiveDataLoop(
        service=service,
        config=None,
        on_unroutable=seen.append,
        max_kline_age_ms=60_000,
        now_ms_fn=lambda: 200_000,
    )
    stale = MarketKline(
        symbol="BTCUSDT", interval="1m",
        open_time=1000, close_time=60000,
        open=49000.0, high=51000.0, low=48000.0,
        close=50000.0, volume=100.0, timestamp=139_999,
        market="futures",
    )

    loop._on_kline(stale)

    service.running_strategy.assert_not_called()
    assert seen == []


def test_live_loop_accepts_kline_within_60_seconds():
    from market_data.models import MarketKline

    service = StrategyService()
    service.running_strategy = MagicMock(return_value=True)

    loop = LiveDataLoop(
        service=service,
        config=None,
        max_kline_age_ms=60_000,
        now_ms_fn=lambda: 200_000,
    )
    fresh = MarketKline(
        symbol="BTCUSDT", interval="1m",
        open_time=1000, close_time=60000,
        open=49000.0, high=51000.0, low=48000.0,
        close=50000.0, volume=100.0, timestamp=140_000,
        market="futures",
    )

    loop._on_kline(fresh)

    service.running_strategy.assert_called_once()
