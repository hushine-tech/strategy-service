from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any, Iterator

from market_data.models import MarketKline

BACKTEST_PAGE_SIZE = 8192


def stream_key_for_binding(binding: Any) -> str:
    return "/".join([
        str(getattr(binding, "exchange", "") or "binance"),
        str(getattr(binding, "market", "")),
        str(getattr(binding, "kind", "") or "kline"),
        str(getattr(binding, "symbol", "")),
        str(getattr(binding, "interval", "")),
    ])


@dataclass
class _Cursor:
    index: int
    binding: Any
    cursor_time_ms: int
    rows: list[MarketKline] = field(default_factory=list)
    row_index: int = 0
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
        self._cursors = [
            _Cursor(
                index=idx,
                binding=stream,
                cursor_time_ms=self._start_time_ms - _interval_step_ms(getattr(stream, "interval", "1m")),
            )
            for idx, stream in enumerate(streams)
        ]

    def iter_klines(self) -> Iterator[MarketKline]:
        heap: list[tuple[int, int, int, MarketKline, _Cursor]] = []
        sequence = 0
        for cursor in self._cursors:
            self._fill(cursor)
            item = self._pop(cursor)
            if item is not None:
                heapq.heappush(heap, (int(item.open_time), cursor.index, sequence, item, cursor))
                sequence += 1

        while heap:
            _, _, _, item, cursor = heapq.heappop(heap)
            yield item

            next_item = self._pop(cursor)
            if next_item is None and not cursor.exhausted:
                self._fill(cursor)
                next_item = self._pop(cursor)
            if next_item is not None:
                heapq.heappush(heap, (int(next_item.open_time), cursor.index, sequence, next_item, cursor))
                sequence += 1

    def _fill(self, cursor: _Cursor) -> None:
        if cursor.exhausted:
            return
        binding = cursor.binding
        page = self._client.fetch_backtest_page(
            exchange=str(getattr(binding, "exchange", "") or "binance"),
            market=str(getattr(binding, "market", "")),
            kind=str(getattr(binding, "kind", "") or "kline"),
            symbol=str(getattr(binding, "symbol", "")),
            interval=str(getattr(binding, "interval", "")),
            start_after_time_ms=int(cursor.cursor_time_ms),
            end_time_ms=self._end_time_ms,
        )
        rows = list(page.klines or [])
        if not rows and bool(page.has_more):
            raise RuntimeError(f"backtest page returned no rows before end: {stream_key_for_binding(binding)}")
        cursor.rows = rows
        cursor.row_index = 0
        cursor.cursor_time_ms = int(page.next_cursor_time_ms or cursor.cursor_time_ms)
        cursor.exhausted = not bool(page.has_more)

    def _pop(self, cursor: _Cursor) -> MarketKline | None:
        if cursor.row_index >= len(cursor.rows):
            return None
        item = cursor.rows[cursor.row_index]
        cursor.row_index += 1
        return item


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
