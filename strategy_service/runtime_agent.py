"""Self-hosted runtime agent/worker process helpers.

The agent owns the RuntimeChannel and heartbeat. The worker process is the
only component that should execute user strategy code or sit at debugger
breakpoints. Keeping these responsibilities separate lets control-plane
shutdown, status patches, and heartbeat continue while user code is paused.
"""

from __future__ import annotations

import multiprocessing
import json
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable

from google.protobuf.json_format import MessageToDict
from google.protobuf.struct_pb2 import Struct

from strategy_service.gen import control_panel_service_pb2 as cp_pb2
from strategy_service.debug_control_server import DebugControlServer, DebugReplayRequest
from strategy_service.debug_workspace import prepare_debug_workspace
from strategy_service.runtime_channel import RuntimeChannelClient

_DATASET_END = object()


@dataclass(frozen=True)
class RuntimeWorkerHealth:
    status: str
    reason: str = ""
    pid: int = 0
    checked_at_unix_ms: int = 0


class RuntimeBusyError(RuntimeError):
    pass


@dataclass(frozen=True)
class DebugDataset:
    dataset_id: str
    user_id: int
    account_id: int
    runtime_id: str
    market: str
    symbol: str
    interval: str
    start_time_ms: int
    end_time_ms: int
    loaded_at_ms: int
    klines: list[Any]


class RuntimeWorkerProcess:
    """Small process manager for a self-hosted runtime worker."""

    def __init__(self, *, name: str = "runtime-worker") -> None:
        self._name = name
        self._process: multiprocessing.Process | None = None
        self._last_reason = ""

    def start(self, target: Callable[..., Any], args: tuple[Any, ...] = ()) -> None:
        if self._process is not None and self._process.is_alive():
            return
        self._last_reason = ""
        self._process = multiprocessing.Process(
            target=target,
            args=args,
            name=self._name,
            daemon=True,
        )
        self._process.start()

    def health(self) -> RuntimeWorkerHealth:
        now_ms = int(time.time() * 1000)
        proc = self._process
        if proc is None:
            return RuntimeWorkerHealth(status="worker_not_started", reason=self._last_reason, checked_at_unix_ms=now_ms)
        if proc.is_alive():
            return RuntimeWorkerHealth(
                status="worker_active",
                reason=self._last_reason,
                pid=int(proc.pid or 0),
                checked_at_unix_ms=now_ms,
            )
        code = proc.exitcode
        reason = self._last_reason or (f"worker exited with code {code}" if code is not None else "worker exited")
        return RuntimeWorkerHealth(
            status="worker_exited",
            reason=reason,
            pid=int(proc.pid or 0),
            checked_at_unix_ms=now_ms,
        )

    def stop(self, timeout_seconds: float = 5.0) -> None:
        proc = self._process
        if proc is None:
            return
        proc.join(timeout=timeout_seconds)
        if proc.is_alive():
            self._last_reason = "worker terminated by runtime agent"
            proc.terminate()
            proc.join(timeout=timeout_seconds)
        if proc.is_alive():
            self._last_reason = "worker killed by runtime agent"
            proc.kill()
            proc.join(timeout=timeout_seconds)


class RuntimeAgent:
    """Coordinates the RuntimeChannel agent and isolated worker process."""

    def __init__(
        self,
        channel: RuntimeChannelClient,
        worker: RuntimeWorkerProcess | None = None,
        *,
        runtime_id: str = "",
    ) -> None:
        self._channel = channel
        self._worker = worker
        self._runtime_id = runtime_id
        self._data_cache: dict[tuple[str, str, int], bytes] = {}
        self._live_queues: dict[str, queue.Queue[Any]] = {}
        self._dataset_queues: dict[str, queue.Queue[Any]] = {}
        self._debug_dataset: DebugDataset | None = None
        self._debug_lock = threading.Lock()
        self._debug_replay_running = False
        self._debug_control_server: DebugControlServer | None = None

    def start_channel(self) -> None:
        self._channel.start()

    def stop(self) -> None:
        if self._debug_control_server is not None:
            self._debug_control_server.stop()
        if self._worker is not None:
            self._worker.stop()
        self._channel.stop()

    def start_debug_control_server(
        self,
        *,
        socket_path: str = "/tmp/hushine-debug.sock",
        replay_handler: Callable[[DebugReplayRequest], dict] | None = None,
    ) -> None:
        if self._debug_control_server is not None:
            return
        self._debug_control_server = DebugControlServer(
            socket_path,
            replay_handler or self._debug_replay_not_configured,
        )
        self._debug_control_server.start()

    def _debug_replay_not_configured(self, _request: DebugReplayRequest) -> dict:
        dataset = self.active_debug_dataset()
        if dataset is None:
            raise RuntimeError("no active debug dataset loaded")
        raise RuntimeError("debug replay runner is not configured")

    def handle_runtime_command(self, command_type: str, payload: bytes) -> bytes:
        if command_type == "prepare_debug_workspace":
            body = _json_object(payload)
            result = prepare_debug_workspace(
                container_path=str(body.get("container_path") or "/workspace"),
                host_path=str(body.get("host_path") or ""),
            )
            return json.dumps(result.to_dict(), separators=(",", ":")).encode("utf-8")
        if command_type == "load_debug_dataset":
            if self.is_debug_replay_running():
                raise RuntimeBusyError("runtime is replaying")
            dataset = _debug_dataset_from_payload(payload)
            with self._debug_lock:
                if self._debug_replay_running:
                    raise RuntimeBusyError("runtime is replaying")
                self._debug_dataset = dataset
            return json.dumps(
                {
                    "dataset_id": dataset.dataset_id,
                    "bar_count": len(dataset.klines),
                    "state": "active",
                },
                separators=(",", ":"),
            ).encode("utf-8")
        raise RuntimeError(f"unsupported runtime command: {command_type}")

    def active_debug_dataset(self) -> DebugDataset | None:
        with self._debug_lock:
            return self._debug_dataset

    def is_debug_replay_running(self) -> bool:
        with self._debug_lock:
            return self._debug_replay_running

    def try_acquire_debug_replay(self) -> bool:
        with self._debug_lock:
            if self._debug_replay_running:
                return False
            self._debug_replay_running = True
            return True

    def release_debug_replay(self) -> None:
        with self._debug_lock:
            self._debug_replay_running = False

    def worker_health(self) -> RuntimeWorkerHealth:
        if self._worker is None:
            return RuntimeWorkerHealth(
                status="worker_not_configured",
                checked_at_unix_ms=int(time.time() * 1000),
            )
        return self._worker.health()

    def report_worker_health(self, *, session_id: str = "") -> RuntimeWorkerHealth:
        health = self.worker_health()
        self._channel.send_status_patch(
            runtime_id=self._runtime_id,
            session_id=session_id,
            status=health.status,
            reason=health.reason,
        )
        return health

    def handle_data_frame(self, frame: cp_pb2.RuntimeFrame) -> None:
        if frame.frame_type == cp_pb2.FRAME_TYPE_DATASET_CHUNK and frame.HasField("dataset_chunk"):
            chunk = frame.dataset_chunk
            self._data_cache[(chunk.session_id, chunk.dataset_id, int(chunk.sequence))] = bytes(chunk.payload)
            q = self._dataset_queues.setdefault(chunk.session_id, queue.Queue())
            for kline in _decode_dataset_chunk_payload(chunk.payload):
                q.put(kline)
            if chunk.end:
                q.put(_DATASET_END)
            return
        if frame.frame_type == cp_pb2.FRAME_TYPE_LIVE_KLINE_BATCH and frame.HasField("live_kline_batch"):
            batch = frame.live_kline_batch
            self._data_cache[
                (batch.session_id, batch.stream_key, int(batch.sequence))
            ] = frame.SerializeToString()
            q = self._live_queues.setdefault(batch.session_id, queue.Queue())
            for kline in _decode_live_kline_batch(batch):
                q.put(kline)

    def cached_data(self, session_id: str, stream_key: str, sequence: int) -> bytes | None:
        return self._data_cache.get((session_id, stream_key, int(sequence)))

    def iter_live_klines(
        self,
        *,
        session_id: str,
        required_streams: list[Any],
        stop_event: Any,
        idle_timeout_seconds: float = 1.0,
        stop_when_idle: bool = False,
    ):
        allowed = {
            runtime_stream_key(
                getattr(stream, "exchange", "binance"),
                getattr(stream, "market", ""),
                getattr(stream, "kind", "kline"),
                getattr(stream, "symbol", ""),
                getattr(stream, "interval", ""),
            )
            for stream in required_streams
        }
        q = self._live_queues.setdefault(session_id, queue.Queue())
        while not stop_event.is_set():
            try:
                kline = q.get(timeout=max(0.01, float(idle_timeout_seconds)))
            except queue.Empty:
                if stop_when_idle:
                    return
                continue
            if kline is _DATASET_END:
                return
            if allowed and runtime_stream_key(
                "binance",
                getattr(kline, "market", ""),
                "kline",
                getattr(kline, "symbol", ""),
                getattr(kline, "interval", ""),
            ) not in allowed:
                continue
            yield kline

    def iter_dataset_klines(
        self,
        *,
        session_id: str,
        required_streams: list[Any],
        stop_event: Any,
        idle_timeout_seconds: float = 1.0,
        stop_when_idle: bool = False,
    ):
        allowed = {
            runtime_stream_key(
                getattr(stream, "exchange", "binance"),
                getattr(stream, "market", ""),
                getattr(stream, "kind", "kline"),
                getattr(stream, "symbol", ""),
                getattr(stream, "interval", ""),
            )
            for stream in required_streams
        }
        q = self._dataset_queues.setdefault(session_id, queue.Queue())
        while not stop_event.is_set():
            try:
                kline = q.get(timeout=max(0.01, float(idle_timeout_seconds)))
            except queue.Empty:
                if stop_when_idle:
                    return
                continue
            if kline is _DATASET_END:
                return
            if allowed and runtime_stream_key(
                "binance",
                getattr(kline, "market", ""),
                "kline",
                getattr(kline, "symbol", ""),
                getattr(kline, "interval", ""),
            ) not in allowed:
                continue
            yield kline


def runtime_stream_key(exchange: str, market: str, kind: str, symbol: str, interval: str) -> str:
    return "/".join([
        str(exchange or "binance").strip().lower(),
        str(market or "").strip().lower(),
        str(kind or "kline").strip().lower(),
        str(symbol or "").strip().upper(),
        str(interval or "").strip(),
    ])


def _decode_live_kline_batch(batch: cp_pb2.RuntimeLiveKlineBatch) -> list[Any]:
    from market_data.models import MarketKline

    out = []
    for packed in batch.klines:
        st = Struct()
        if not packed.Unpack(st):
            continue
        data = MessageToDict(st)
        symbol = str(data.get("symbol") or "").strip().upper()
        interval = str(data.get("interval") or "").strip()
        if not symbol or not interval:
            continue
        out.append(MarketKline(
            symbol=symbol,
            interval=interval,
            open_time=int(data.get("open_time") or 0),
            close_time=int(data.get("close_time") or 0),
            open=float(data.get("open") or 0.0),
            high=float(data.get("high") or 0.0),
            low=float(data.get("low") or 0.0),
            close=float(data.get("close") or 0.0),
            volume=float(data.get("volume") or 0.0),
            timestamp=int(data.get("timestamp") or data.get("close_time") or 0),
            market=str(data.get("market") or "futures").strip().lower(),
        ))
    return out


def _decode_dataset_chunk_payload(payload: bytes) -> list[Any]:
    try:
        raw = json.loads(bytes(payload).decode("utf-8"))
    except Exception:
        return []
    rows = raw.get("klines", raw) if isinstance(raw, dict) else raw
    if not isinstance(rows, list):
        return []
    return [_kline_from_mapping(item) for item in rows if isinstance(item, dict)]


def _json_object(payload: bytes) -> dict[str, Any]:
    try:
        raw = json.loads(bytes(payload or b"{}").decode("utf-8"))
    except Exception as exc:
        raise RuntimeError("runtime command payload must be valid JSON") from exc
    if not isinstance(raw, dict):
        raise RuntimeError("runtime command payload must be a JSON object")
    return raw


def _debug_dataset_from_payload(payload: bytes) -> DebugDataset:
    raw = _json_object(payload)
    rows = raw.get("klines")
    if not isinstance(rows, list):
        raise RuntimeError("debug dataset payload requires klines")
    klines = [_kline_from_mapping(item) for item in rows if isinstance(item, dict)]
    dataset_id = str(raw.get("dataset_id") or "").strip()
    if not dataset_id:
        raise RuntimeError("debug dataset payload requires dataset_id")
    if not klines:
        raise RuntimeError("debug dataset payload has no klines")
    return DebugDataset(
        dataset_id=dataset_id,
        user_id=int(raw.get("user_id") or 0),
        account_id=int(raw.get("account_id") or 0),
        runtime_id=str(raw.get("runtime_id") or "").strip(),
        market=str(raw.get("market") or "").strip().lower(),
        symbol=str(raw.get("symbol") or "").strip().upper(),
        interval=str(raw.get("interval") or "").strip(),
        start_time_ms=int(raw.get("start_time_ms") or 0),
        end_time_ms=int(raw.get("end_time_ms") or 0),
        loaded_at_ms=int(raw.get("loaded_at_ms") or 0),
        klines=klines,
    )


def _kline_from_mapping(data: dict[str, Any]) -> Any:
    from market_data.models import MarketKline

    return MarketKline(
        symbol=str(data.get("symbol") or "").strip().upper(),
        interval=str(data.get("interval") or "").strip(),
        open_time=int(data.get("open_time_ms") or data.get("open_time") or 0),
        close_time=int(data.get("close_time_ms") or data.get("close_time") or 0),
        open=float(data.get("open") or 0.0),
        high=float(data.get("high") or 0.0),
        low=float(data.get("low") or 0.0),
        close=float(data.get("close") or 0.0),
        volume=float(data.get("volume") or 0.0),
        timestamp=int(data.get("timestamp") or data.get("close_time_ms") or data.get("close_time") or 0),
        market=str(data.get("market") or "futures").strip().lower(),
    )
