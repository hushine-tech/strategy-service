from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any, Iterator

from strategy_service.platform_proxy import MarketFundingFact

BACKTEST_PAGE_SIZE = 8192


class BacktestFundingDataGapError(RuntimeError):
    code = "BACKTEST_FUNDING_DATA_GAP"

    def __init__(self, *, exchange: str, market: str, symbol: str, venue_id: int, market_time_ms: int):
        self.exchange = exchange
        self.market = market
        self.symbol = symbol
        self.venue_id = int(venue_id)
        self.market_time_ms = int(market_time_ms)
        super().__init__(
            f"{self.code}: missing Funding storage for open Futures leg "
            f"{exchange}/{market}/{symbol} venue {venue_id} at {market_time_ms}"
        )


class BacktestFundingSettlementError(RuntimeError):
    code = "BACKTEST_FUNDING_SETTLEMENT_FAILED"

    def __init__(self, *, symbol: str, venue_id: int, funding_time_ms: int, cause: Exception):
        self.symbol = symbol
        self.venue_id = int(venue_id)
        self.funding_time_ms = int(funding_time_ms)
        super().__init__(
            f"{self.code}: {symbol} venue {venue_id} at {funding_time_ms}: {cause}"
        )


class BacktestFundingFactConflictError(ValueError):
    code = "BACKTEST_FUNDING_FACT_AMBIGUOUS"

    def __init__(self, identity: tuple[str, str, str, int]):
        self.identity = identity
        super().__init__(
            f"{self.code}: ambiguous exact Funding facts for "
            f"{identity[0]}/{identity[1]}/{identity[2]} at {identity[3]}"
        )


def stream_key_for_binding(binding: Any) -> str:
    return "/".join([
        str(getattr(binding, "exchange", "")),
        str(getattr(binding, "market", "")),
        str(getattr(binding, "kind", "") or "kline"),
        str(getattr(binding, "symbol", "")),
        str(getattr(binding, "interval", "")),
    ])


@dataclass(frozen=True, slots=True)
class BacktestTimelineEvent:
    kind: str
    market_time_ms: int
    stream_index: int
    payload: Any
    funding_coverage_complete: bool | None = None


@dataclass(frozen=True, slots=True)
class BacktestFundingCoverageCheckpoint:
    symbol: str


@dataclass
class _Cursor:
    index: int
    binding: Any
    cursor_time_ms: int
    events: list[BacktestTimelineEvent] = field(default_factory=list)
    event_index: int = 0
    exhausted: bool = False


class PagedBacktestDataSource:
    def __init__(
        self,
        marketdata_client: Any,
        *,
        start_time_ms: int,
        end_time_ms: int,
        streams: list[Any],
    ) -> None:
        self._client = marketdata_client
        self._start_time_ms = int(start_time_ms)
        self._end_time_ms = int(end_time_ms)
        self._funding_facts: dict[
            tuple[str, str, str, int], tuple[str, str, str]
        ] = {}
        self._cursors = [
            _Cursor(
                index=idx,
                binding=stream,
                cursor_time_ms=self._start_time_ms - _interval_step_ms(getattr(stream, "interval", "1m")),
            )
            for idx, stream in enumerate(streams)
        ]

    def iter_timeline(self) -> Iterator[BacktestTimelineEvent]:
        heap: list[tuple[int, int, int, int, BacktestTimelineEvent, _Cursor]] = []
        sequence = 0
        for cursor in self._cursors:
            self._fill(cursor)
            event = self._pop(cursor)
            if event is not None:
                heapq.heappush(heap, self._heap_item(event, sequence, cursor))
                sequence += 1

        while heap:
            _, _, _, _, event, cursor = heapq.heappop(heap)
            yield event

            next_event = self._pop(cursor)
            if next_event is None and not cursor.exhausted:
                self._fill(cursor)
                next_event = self._pop(cursor)
            if next_event is not None:
                heapq.heappush(heap, self._heap_item(next_event, sequence, cursor))
                sequence += 1

    @staticmethod
    def _heap_item(event: BacktestTimelineEvent, sequence: int, cursor: _Cursor):
        return (
            int(event.market_time_ms),
            _event_priority(event.kind),
            int(event.stream_index),
            int(sequence),
            event,
            cursor,
        )

    def _fill(self, cursor: _Cursor) -> None:
        if cursor.exhausted:
            return
        binding = cursor.binding
        page = self._client.fetch_backtest_page(
            exchange=str(getattr(binding, "exchange", "")),
            market=str(getattr(binding, "market", "")),
            kind=str(getattr(binding, "kind", "") or "kline"),
            symbol=str(getattr(binding, "symbol", "")),
            interval=str(getattr(binding, "interval", "")),
            start_after_time_ms=int(cursor.cursor_time_ms),
            end_time_ms=self._end_time_ms,
        )
        klines = list(page.klines or [])
        funding_facts = list(getattr(page, "funding_facts", ()) or ())
        coverage = getattr(page, "funding_coverage_complete", None)
        if not klines and not funding_facts and bool(page.has_more):
            raise RuntimeError(f"backtest page returned no rows before end: {stream_key_for_binding(binding)}")

        local: list[tuple[int, int, int, BacktestTimelineEvent]] = []
        local_sequence = 0
        page_time_ms = max(self._start_time_ms, int(cursor.cursor_time_ms))
        if str(getattr(binding, "market", "")).strip().lower() in {
            "futures",
            "perpetual_futures",
        }:
            checkpoint = BacktestTimelineEvent(
                kind="coverage",
                market_time_ms=page_time_ms,
                stream_index=cursor.index,
                payload=BacktestFundingCoverageCheckpoint(
                    symbol=str(getattr(binding, "symbol", "")).strip().upper()
                ),
                funding_coverage_complete=coverage,
            )
            local.append(
                (
                    checkpoint.market_time_ms,
                    _event_priority(checkpoint.kind),
                    local_sequence,
                    checkpoint,
                )
            )
            local_sequence += 1
        for fact in funding_facts:
            self._validate_funding_fact(binding, fact)
            if not self._register_funding_fact(fact):
                continue
            event = BacktestTimelineEvent(
                kind="funding",
                market_time_ms=int(fact.funding_time_ms),
                stream_index=cursor.index,
                payload=fact,
                funding_coverage_complete=coverage,
            )
            local.append(
                (
                    event.market_time_ms,
                    _event_priority(event.kind),
                    local_sequence,
                    event,
                )
            )
            local_sequence += 1
        for row in klines:
            event = BacktestTimelineEvent(
                kind="kline",
                market_time_ms=int(row.open_time),
                stream_index=cursor.index,
                payload=row,
                funding_coverage_complete=coverage,
            )
            local.append(
                (
                    event.market_time_ms,
                    _event_priority(event.kind),
                    local_sequence,
                    event,
                )
            )
            local_sequence += 1
        local.sort(key=lambda item: item[:3])
        cursor.events = [item[3] for item in local]
        cursor.event_index = 0
        cursor.cursor_time_ms = int(page.next_cursor_time_ms or cursor.cursor_time_ms)
        cursor.exhausted = not bool(page.has_more)

    def _register_funding_fact(self, fact: MarketFundingFact) -> bool:
        identity = (
            str(fact.exchange or "").strip().lower(),
            str(fact.market or "").strip().lower(),
            str(fact.symbol or "").strip().upper(),
            int(fact.funding_time_ms),
        )
        exact_fact = (
            fact.funding_rate_decimal,
            fact.mark_price_decimal,
            str(fact.settlement_asset or "").strip().upper(),
        )
        previous = self._funding_facts.get(identity)
        if previous is None:
            self._funding_facts[identity] = exact_fact
            return True
        if previous != exact_fact:
            raise BacktestFundingFactConflictError(identity)
        return False

    @staticmethod
    def _validate_funding_fact(binding: Any, fact: MarketFundingFact) -> None:
        expected = (
            str(getattr(binding, "exchange", "")).strip().lower(),
            str(getattr(binding, "market", "")).strip().lower(),
            str(getattr(binding, "symbol", "")).strip().upper(),
        )
        actual = (
            str(fact.exchange or "").strip().lower(),
            str(fact.market or "").strip().lower(),
            str(fact.symbol or "").strip().upper(),
        )
        if actual != expected:
            raise ValueError("Backtest Funding fact does not match its declared input stream")

    @staticmethod
    def _pop(cursor: _Cursor) -> BacktestTimelineEvent | None:
        if cursor.event_index >= len(cursor.events):
            return None
        event = cursor.events[cursor.event_index]
        cursor.event_index += 1
        return event


def _interval_step_ms(interval: str) -> int:
    text = str(interval or "").strip().lower()
    if len(text) < 2:
        raise ValueError(f"invalid interval: {interval!r}")
    value = int(text[:-1])
    unit = text[-1]
    if value <= 0:
        raise ValueError(f"invalid interval: {interval!r}")
    if unit == "s":
        return value * 1000
    if unit == "m":
        return value * 60_000
    if unit == "h":
        return value * 3_600_000
    if unit == "d":
        return value * 86_400_000
    raise ValueError(f"unsupported interval unit: {interval!r}")


def _event_priority(kind: str) -> int:
    if kind == "coverage":
        return -1
    if kind == "funding":
        return 0
    return 1
