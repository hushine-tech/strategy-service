"""Drive StrategyEngine from strategy-library market_data (TimescaleDB / Kafka)."""

from __future__ import annotations

import heapq
import logging
import time
from typing import TYPE_CHECKING, Callable, Optional

from market_data.backtest import BacktestDataSource
from market_data.config import KafkaConfig, TimescaleConfig
from market_data.live import LiveDataSource
from market_data.models import MarketKline

from strategy_service.types import MarketData

if TYPE_CHECKING:
    from strategy_service.service import StrategyEngine

__all__ = ["BacktestDataLoop", "LiveDataLoop"]

logger = logging.getLogger(__name__)
DEFAULT_LIVE_KLINE_MAX_AGE_MS = 60_000


def _marketdata_market(market: str) -> str:
    market_key = str(market or "").strip().lower()
    if market_key == "perpetual_futures":
        return "futures"
    return market_key


def _stream_key(market: object, symbol: object, interval: object) -> tuple[str, str, str]:
    return (
        str(market or "").strip().lower(),
        str(symbol or "").strip().upper(),
        str(interval or "").strip() or "1m",
    )


def _adapt_kline(kline: MarketKline, market: str | None = None) -> MarketData:
    """Map MarketKline from market_data to framework MarketData."""
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


class BacktestDataLoop:
    """Replay historical klines from BacktestDataSource into StrategyEngine."""

    def __init__(
        self,
        service: "StrategyEngine",
        config: Optional[TimescaleConfig] = None,
    ) -> None:
        self._service = service
        self._config = config

    @property
    def service(self) -> "StrategyEngine":
        return self._service

    @property
    def config(self) -> Optional[TimescaleConfig]:
        return self._config

    def run(
        self,
        symbol: str,
        start_time: int,
        end_time: int,
        interval: str,
        market: str = "futures",
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> None:
        """Single-symbol replay (backward compatible)."""
        with BacktestDataSource(self._config) as ds:
            for kline in ds.get_klines(symbol, interval, start_time, end_time, market=market):
                if should_stop is not None and should_stop():
                    break
                self._service.running_strategy(_adapt_kline(kline, market))

    def run_account(
        self,
        symbols_with_market: list[tuple[str, str]],
        start_time: int,
        end_time: int,
        interval: str,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> int:
        """Legacy single-interval multi-symbol replay (kept for callers that
        still pass a uniform interval across all symbols).

        For pre_C3 multi-interval strategies, use ``run_declared`` instead —
        that path honours each declared input's own interval, which a single
        shared ``interval`` argument cannot represent.
        """
        declared = [(market, symbol, interval) for symbol, market in symbols_with_market]
        return self.run_declared(declared, start_time, end_time, should_stop=should_stop)

    def run_declared(
        self,
        declared: list[tuple[str, str, str]] | list[object],
        start_time: int,
        end_time: int,
        should_stop: Optional[Callable[[], bool]] = None,
    ) -> int:
        """Multi-input replay: one heap iterator per declared
        ``(market, symbol, interval)`` 3-tuple, merged by open_time.

        Pre_C3 gate 2 requires that every declared input — including two
        inputs that differ only in interval — reaches the strategy. Collapsing
        to ``(symbol, market)`` + a single ``interval`` silently dropped
        fine-grained intervals here; this path keeps each declared tuple as
        its own source iterator.

        Accepted item shapes:
          - ``(market, symbol, interval)`` 3-tuple
          - any object with ``.market`` / ``.symbol`` / ``.interval``
            attributes (e.g. ``strategy_service.inputs.StrategyInput``)

        Returns the total number of bars processed across all inputs.
        """
        triples: list[tuple[str, str, str]] = []
        for item in declared:
            if hasattr(item, "market") and hasattr(item, "symbol") and hasattr(item, "interval"):
                triples.append((item.market, item.symbol, item.interval))  # type: ignore[attr-defined]
            else:
                triples.append(tuple(item))  # type: ignore[arg-type]

        n = 0
        with BacktestDataSource(self._config) as ds:
            # heap entry: (open_time, idx, kline, canonical_market, iterator)
            heap: list[tuple[int, int, MarketKline, str, object]] = []
            iters: list[tuple] = []
            for idx, (market, symbol, interval) in enumerate(triples):
                canonical_market = str(market or "").strip().lower()
                storage_market = _marketdata_market(canonical_market)
                try:
                    it = ds.get_klines(symbol, interval, start_time, end_time, market=storage_market)
                    iters.append((it, idx, canonical_market))
                except Exception:
                    # 表不存在等情况，跳过该条声明输入
                    continue

            for it, idx, market in iters:
                kline = next(it, None)
                if kline is not None:
                    heapq.heappush(heap, (kline.open_time, idx, kline, market, it))

            while heap:
                if should_stop is not None and should_stop():
                    break
                _, idx, kline, market, it = heapq.heappop(heap)
                self._service.running_strategy(_adapt_kline(kline, market))
                n += 1
                nxt = next(it, None)
                if nxt is not None:
                    heapq.heappush(heap, (nxt.open_time, idx, nxt, market, it))

        return n


class LiveDataLoop:
    """Subscribe to Kafka via LiveDataSource and forward session-relevant klines.

    Topic selection and K-line filtering are library-owned by `market_data`.
    The service loop only adapts delivered K-lines into StrategyEngine's
    existing execution path.
    """

    def __init__(
        self,
        service: "StrategyEngine",
        config: Optional[KafkaConfig] = None,
        logger: Optional[object] = None,
        on_unroutable: Optional[Callable[[MarketKline], None]] = None,
        max_kline_age_ms: int = DEFAULT_LIVE_KLINE_MAX_AGE_MS,
        now_ms_fn: Optional[Callable[[], int]] = None,
        canonical_markets: Optional[dict[tuple[str, str, str], str]] = None,
    ) -> None:
        self._service = service
        self._on_unroutable = on_unroutable
        self._live = LiveDataSource(config=config, logger=logger)
        self._max_kline_age_ms = int(max_kline_age_ms)
        self._now_ms = now_ms_fn or (lambda: int(time.time() * 1000))
        self._canonical_markets = canonical_markets or {}
        self._live.on_kline(self._on_kline)

    def _is_stale_kline(self, kline: MarketKline) -> bool:
        if self._max_kline_age_ms <= 0:
            return False
        age_ms = self._now_ms() - int(kline.timestamp)
        return age_ms > self._max_kline_age_ms

    def _on_kline(self, kline: MarketKline) -> None:
        if self._is_stale_kline(kline):
            logger.warning(
                "dropping stale live kline: symbol=%s market=%s interval=%s age_ms=%s max_age_ms=%s",
                kline.symbol,
                getattr(kline, "market", None) or "futures",
                kline.interval,
                self._now_ms() - int(kline.timestamp),
                self._max_kline_age_ms,
            )
            return
        canonical_market = self._canonical_markets.get(
            _stream_key(
                getattr(kline, "market", None),
                getattr(kline, "symbol", None),
                getattr(kline, "interval", None),
            )
        )
        routed = self._service.running_strategy(_adapt_kline(kline, canonical_market))
        if routed is False and self._on_unroutable is not None:
            self._on_unroutable(kline)

    def start(self) -> None:
        self._live.start()

    def stop(self) -> None:
        self._live.stop()
