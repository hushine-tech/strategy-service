from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

SUPPORTED_TYPES = {"line", "histogram", "marker"}
SUPPORTED_PANES = {"price", "strategy"}
DEFAULT_CHUNK_SIZE = 1024


@dataclass(frozen=True)
class IndicatorDefinition:
    key: str
    name: str
    type: str
    pane: str
    stream_key: str = ""
    color: str = ""
    unit: str = ""
    description: str = ""
    config: dict[str, Any] = field(default_factory=dict)


@dataclass
class IndicatorFrame:
    values: dict[str, float | None] = field(default_factory=dict)
    markers: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


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


def parse_indicator_definitions(raw: object) -> list[IndicatorDefinition]:
    if raw in (None, {}, []):
        return []
    if not isinstance(raw, dict):
        raise ValueError("INDICATORS must be a dict keyed by indicator key")

    out: list[IndicatorDefinition] = []
    seen: set[str] = set()
    for raw_key, raw_cfg in raw.items():
        if not isinstance(raw_key, str) or not raw_key.strip():
            raise ValueError("indicator key must be a non-empty string")
        key = raw_key.strip()
        if key in seen:
            raise ValueError(f"indicator {key} is duplicated")
        seen.add(key)
        if not isinstance(raw_cfg, dict):
            raise ValueError(f"indicator {key} config must be a dict")

        typ = str(raw_cfg.get("type", "")).strip().lower()
        pane = str(raw_cfg.get("pane", "")).strip().lower()
        if typ not in SUPPORTED_TYPES:
            raise ValueError(f"indicator {key} type must be one of: histogram, line, marker")
        if pane not in SUPPORTED_PANES and not pane.startswith("custom:"):
            raise ValueError(f"indicator {key} pane must be price, strategy, or custom:<name>")

        config = raw_cfg.get("config") or {}
        if not isinstance(config, dict):
            raise ValueError(f"indicator {key} config.config must be a dict")
        out.append(IndicatorDefinition(
            key=key,
            name=str(raw_cfg.get("name") or key).strip(),
            type=typ,
            pane=pane,
            color=str(raw_cfg.get("color") or "").strip(),
            unit=str(raw_cfg.get("unit") or "").strip(),
            description=str(raw_cfg.get("description") or "").strip(),
            config=dict(config),
        ))
    return out


class IndicatorWriter:
    def __init__(self, definitions: list[IndicatorDefinition]) -> None:
        self._definitions = {definition.key: definition for definition in definitions}
        self._frame = IndicatorFrame()

    def reset_bar(self) -> None:
        self._frame = IndicatorFrame()

    def set(self, key: str, value: float | int | None) -> None:
        key = str(key or "").strip()
        definition = self._definitions.get(key)
        if definition is None:
            self._frame.warnings.append(f"undeclared indicator key ignored: {key}")
            return
        if definition.type == "marker":
            self._frame.warnings.append(f"marker indicator key ignored by set(): {key}")
            return
        if value is None:
            self._frame.values[key] = None
            return
        try:
            self._frame.values[key] = float(value)
        except (TypeError, ValueError):
            self._frame.warnings.append(f"indicator value must be numeric or None: {key}")

    def mark(self, key: str, text: str = "", price: float | None = None, color: str = "") -> None:
        key = str(key or "").strip()
        definition = self._definitions.get(key)
        if definition is None:
            self._frame.warnings.append(f"undeclared indicator key ignored: {key}")
            return
        if definition.type != "marker":
            self._frame.warnings.append(f"non-marker indicator key ignored by mark(): {key}")
            return
        marker: dict[str, Any] = {
            "text": str(text or ""),
        }
        if price is not None:
            try:
                marker["price"] = float(price)
            except (TypeError, ValueError):
                self._frame.warnings.append(f"marker price must be numeric: {key}")
                return
        color = str(color or "").strip()
        if color:
            marker["color"] = color
        self._frame.markers.setdefault(key, []).append(marker)

    def drain(self) -> IndicatorFrame:
        frame = self._frame
        self.reset_bar()
        return frame


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
