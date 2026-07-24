from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hushine_strategy.indicator_output import (
    IndicatorDefinition,
    IndicatorFrame,
    IndicatorWriter,
    SUPPORTED_PANES,
    SUPPORTED_TYPES,
    parse_indicator_definitions,
)


DEFAULT_CHUNK_SIZE = 1024

__all__ = [
    "DEFAULT_CHUNK_SIZE",
    "IndicatorChunk",
    "IndicatorChunkBuffer",
    "IndicatorDefinition",
    "IndicatorFrame",
    "IndicatorWriter",
    "SUPPORTED_PANES",
    "SUPPORTED_TYPES",
    "parse_indicator_definitions",
]


@dataclass
class IndicatorChunk:
    stream_key: str
    indicator_key: str
    chunk_index: int
    start_time_ms: int
    end_time_ms: int
    interval_ms: int
    count: int
    values_json: dict[str, Any]


@dataclass
class _OpenChunk:
    start_time_ms: int
    interval_ms: int
    chunk_index: int
    count: int = 0
    values: list[float | None] = field(default_factory=list)
    markers: list[dict[str, Any]] = field(default_factory=list)


class IndicatorChunkBuffer:
    def __init__(self, definitions: list[IndicatorDefinition], chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self._definitions = list(definitions)
        self._chunk_size = int(chunk_size)
        self._open: dict[tuple[str, str], _OpenChunk] = {}
        self._next_chunk_index: dict[tuple[str, str], int] = {}

    def record_bar(
        self,
        stream_key: str,
        bar_time_ms: int,
        interval_ms: int,
        values: IndicatorFrame,
    ) -> list[IndicatorChunk]:
        if not self._definitions:
            return []
        stream_key = str(stream_key or "").strip()
        if not stream_key:
            raise ValueError("stream_key is required")
        bar_time_ms = int(bar_time_ms)
        interval_ms = int(interval_ms)
        if interval_ms <= 0:
            raise ValueError("interval_ms must be positive")

        emitted: list[IndicatorChunk] = []
        for definition in self._definitions:
            state_key = (stream_key, definition.key)
            chunk = self._open.get(state_key)
            if chunk is None:
                chunk_index = self._next_chunk_index.get(state_key, 0)
                chunk = _OpenChunk(
                    start_time_ms=bar_time_ms,
                    interval_ms=interval_ms,
                    chunk_index=chunk_index,
                )
                self._open[state_key] = chunk

            offset = chunk.count
            if definition.type == "marker":
                for marker in values.markers.get(definition.key, []):
                    item = dict(marker)
                    item["offset"] = offset
                    chunk.markers.append(item)
            else:
                chunk.values.append(values.values.get(definition.key))

            chunk.count += 1
            if chunk.count >= self._chunk_size:
                emitted.append(self._emit_chunk(stream_key, definition, chunk))
                self._next_chunk_index[state_key] = chunk.chunk_index + 1
                del self._open[state_key]
        return emitted

    def flush_open(self) -> list[IndicatorChunk]:
        emitted: list[IndicatorChunk] = []
        for stream_key, indicator_key in sorted(self._open):
            chunk = self._open[(stream_key, indicator_key)]
            definition = next(item for item in self._definitions if item.key == indicator_key)
            emitted.append(self._emit_chunk(stream_key, definition, chunk))
            self._next_chunk_index[(stream_key, indicator_key)] = chunk.chunk_index + 1
        self._open.clear()
        return emitted

    def _emit_chunk(
        self,
        stream_key: str,
        definition: IndicatorDefinition,
        chunk: _OpenChunk,
    ) -> IndicatorChunk:
        end_time_ms = chunk.start_time_ms + (chunk.count - 1) * chunk.interval_ms
        if definition.type == "marker":
            values_json = {"markers": list(chunk.markers)}
        else:
            values_json = {"values": list(chunk.values), "times": None}
        return IndicatorChunk(
            stream_key=stream_key,
            indicator_key=definition.key,
            chunk_index=chunk.chunk_index,
            start_time_ms=chunk.start_time_ms,
            end_time_ms=end_time_ms,
            interval_ms=chunk.interval_ms,
            count=chunk.count,
            values_json=values_json,
        )
