from __future__ import annotations

import os
import queue
import threading
import uuid
import json
from dataclasses import dataclass
from typing import Callable

import grpc
from google.protobuf.any_pb2 import Any
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
from google.protobuf.struct_pb2 import Struct
from strategy_service.gen import runtime_worker_pb2 as worker_pb2
from strategy_service.gen import runtime_worker_pb2_grpc as worker_grpc

WORKER_VERSION = "0.1.0"


class FinalStatusRejected(RuntimeError):
    pass


def _platform_timeout_seconds(timeout_seconds: float | None) -> float:
    return max(0.1, float(timeout_seconds or 30.0))


def _platform_reply_wait_seconds(timeout_seconds: float | None) -> float:
    platform_timeout = _platform_timeout_seconds(timeout_seconds)
    return platform_timeout + min(5.0, max(1.0, platform_timeout * 0.1))


@dataclass(frozen=True)
class WorkerEnv:
    agent_addr: str
    token: str
    session_id: str
    debugpy_port: int = 0


def load_worker_env() -> WorkerEnv:
    agent_addr = os.environ.get("HUSHINE_AGENT_ADDR", "").strip()
    token = os.environ.get("HUSHINE_WORKER_TOKEN", "").strip()
    session_id = os.environ.get("HUSHINE_SESSION_ID", "").strip()
    if not agent_addr:
        raise RuntimeError("HUSHINE_AGENT_ADDR is required")
    if not token:
        raise RuntimeError("HUSHINE_WORKER_TOKEN is required")
    if not session_id:
        raise RuntimeError("HUSHINE_SESSION_ID is required")
    debugpy_port_raw = os.environ.get("HUSHINE_DEBUGPY_PORT", "").strip()
    debugpy_port = int(debugpy_port_raw) if debugpy_port_raw else 0
    return WorkerEnv(
        agent_addr=agent_addr,
        token=token,
        session_id=session_id,
        debugpy_port=debugpy_port,
    )


def build_worker_hello_frame(env: WorkerEnv, *, pid: int | None = None) -> worker_pb2.WorkerFrame:
    return worker_pb2.WorkerFrame(
        hello=worker_pb2.WorkerHello(
            session_id=env.session_id,
            token=env.token,
            worker_version=WORKER_VERSION,
            pid=int(pid if pid is not None else os.getpid()),
        )
    )


class WorkerAgentClient:
    def __init__(
        self,
        env: WorkerEnv,
        *,
        stub=None,
        channel_factory: Callable[[str], grpc.Channel] | None = None,
        call_id_factory: Callable[[], str] | None = None,
    ) -> None:
        self.env = env
        self._stub = stub
        self._channel_factory = channel_factory or grpc.insecure_channel
        self._call_id_factory = call_id_factory or (lambda: uuid.uuid4().hex)
        self._outbound: queue.Queue[worker_pb2.WorkerFrame | None] = queue.Queue()
        self._incoming: queue.Queue[worker_pb2.AgentFrame] = queue.Queue()
        self._pending: dict[str, queue.Queue[worker_pb2.PlatformCallResult]] = {}
        self._pending_lock = threading.Lock()
        self._pending_replies: dict[str, queue.Queue[worker_pb2.AgentFrame]] = {}
        self._pending_reply_lock = threading.Lock()
        self._agent_platform_call_handler: Callable[[worker_pb2.PlatformCall], Any] | None = None
        self._agent_platform_call_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._closed = threading.Event()
        self._error: BaseException | None = None

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._outbound.put(build_worker_hello_frame(self.env))
        self._thread = threading.Thread(target=self._run, name="worker-agent-client", daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._outbound.put(None)
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        self._closed.set()

    def wait_for_start_session(self, *, timeout_seconds: float = 30.0) -> worker_pb2.StartSession:
        deadline = max(0.1, float(timeout_seconds or 30.0))
        while True:
            self._raise_if_failed()
            try:
                frame = self._incoming.get(timeout=deadline)
            except queue.Empty as exc:
                raise TimeoutError("timed out waiting for StartSession") from exc
            if frame.WhichOneof("payload") == "start_session":
                return frame.start_session

    def set_agent_platform_call_handler(
        self,
        handler: Callable[[worker_pb2.PlatformCall], Any] | None,
    ) -> None:
        with self._agent_platform_call_lock:
            self._agent_platform_call_handler = handler

    def invoke_platform_unary(
        self,
        method: str,
        request: Message,
        response_type: type[Message],
        *,
        timeout_seconds: float = 30.0,
    ) -> Message:
        method = str(method or "").strip()
        if not method:
            raise RuntimeError("platform method is required")
        packed = Any()
        packed.Pack(request)
        call_id = self._call_id_factory()
        reply: queue.Queue[worker_pb2.PlatformCallResult] = queue.Queue(maxsize=1)
        with self._pending_lock:
            self._pending[call_id] = reply
        try:
            platform_timeout = _platform_timeout_seconds(timeout_seconds)
            self._outbound.put(
                worker_pb2.WorkerFrame(
                    platform_call=worker_pb2.PlatformCall(
                        call_id=call_id,
                        method=method,
                        request=packed,
                        timeout_ms=int(platform_timeout * 1000),
                    )
                )
            )
            try:
                result = reply.get(timeout=_platform_reply_wait_seconds(timeout_seconds))
            except queue.Empty as exc:
                raise TimeoutError(f"platform call timed out: {method}") from exc
            if not result.ok:
                raise RuntimeError(result.error or f"platform call failed: {method}")
            response = response_type()
            if not result.response.Unpack(response):
                raise RuntimeError(f"platform response type mismatch for {method}")
            return response
        finally:
            with self._pending_lock:
                self._pending.pop(call_id, None)

    def send_progress(self, *, session_id: str, status: str, bars_processed: int = 0, error: str = "") -> None:
        self._outbound.put(
            worker_pb2.WorkerFrame(
                progress=worker_pb2.SessionProgress(
                    session_id=session_id,
                    status=status,
                    bars_processed=int(bars_processed),
                    error=error,
                )
            )
        )

    def send_final_status(
        self,
        *,
        session_id: str,
        status: str,
        bars_processed: int = 0,
        error: str = "",
        timeout_seconds: float = 35.0,
    ) -> None:
        frame_id = self._call_id_factory()
        reply: queue.Queue[worker_pb2.AgentFrame] = queue.Queue(maxsize=1)
        with self._pending_reply_lock:
            self._pending_replies[frame_id] = reply
        try:
            self._outbound.put(
                worker_pb2.WorkerFrame(
                    frame_id=frame_id,
                    final_status=worker_pb2.FinalStatus(
                        session_id=session_id,
                        status=status,
                        bars_processed=int(bars_processed),
                        error=error,
                    ),
                )
            )
            try:
                ack = reply.get(timeout=max(0.01, float(timeout_seconds)))
            except queue.Empty as exc:
                raise TimeoutError(f"timed out waiting for final status ack: {session_id}") from exc
            if ack.WhichOneof("payload") == "error":
                raise FinalStatusRejected(ack.error.message or ack.error.code)
        finally:
            with self._pending_reply_lock:
                self._pending_replies.pop(frame_id, None)

    def send_platform_call_result(
        self,
        *,
        call_id: str,
        ok: bool,
        response: Any | None = None,
        error: str = "",
    ) -> None:
        packed = response if response is not None else Any()
        self._outbound.put(
            worker_pb2.WorkerFrame(
                platform_call_result=worker_pb2.PlatformCallResult(
                    call_id=str(call_id or ""),
                    ok=bool(ok),
                    response=packed,
                    error=str(error or ""),
                )
            )
        )

    def send_indicator_frame(
        self,
        *,
        session_id: str,
        user_id: int,
        strategy_id: int,
        stream_key: str,
        market_time_ms: int,
        interval_ms: int,
        definitions: list[object],
        frame: object,
    ) -> None:
        msg = worker_pb2.IndicatorFrame(
            session_id=str(session_id or ""),
            user_id=int(user_id or 0),
            strategy_id=int(strategy_id or 0),
            stream_key=str(stream_key or ""),
            market_time_ms=int(market_time_ms or 0),
            interval_ms=int(interval_ms or 0),
        )
        values = getattr(frame, "values", {}) or {}
        markers = getattr(frame, "markers", {}) or {}
        for definition in definitions or []:
            key = str(getattr(definition, "key", "") or getattr(definition, "indicator_key", "") or "").strip()
            if not key:
                continue
            cfg = getattr(definition, "config", {}) or {}
            msg.definitions.add(
                indicator_key=key,
                name=str(getattr(definition, "name", "") or key),
                type=str(getattr(definition, "type", "") or ""),
                pane=str(getattr(definition, "pane", "") or ""),
                color=str(getattr(definition, "color", "") or ""),
                unit=str(getattr(definition, "unit", "") or ""),
                description=str(getattr(definition, "description", "") or ""),
                config_json=json.dumps(cfg, separators=(",", ":")),
            )
        known_keys = {
            str(getattr(definition, "key", "") or getattr(definition, "indicator_key", "") or "").strip()
            for definition in definitions or []
        }
        known_keys.update(str(key or "").strip() for key in values.keys())
        known_keys.update(str(key or "").strip() for key in markers.keys())
        for key in sorted(item for item in known_keys if item):
            marker_items = markers.get(key) or []
            if marker_items:
                msg.values.add(
                    indicator_key=key,
                    has_value=False,
                    marker_json=json.dumps(list(marker_items), separators=(",", ":")),
                )
                continue
            raw_value = values.get(key)
            if raw_value is None:
                msg.values.add(indicator_key=key, has_value=False)
                continue
            msg.values.add(indicator_key=key, value=float(raw_value), has_value=True)
        self._outbound.put(worker_pb2.WorkerFrame(indicator_frame=msg))

    def next_agent_frame(self, *, timeout_seconds: float = 1.0) -> worker_pb2.AgentFrame | None:
        self._raise_if_failed()
        try:
            return self._incoming.get(timeout=max(0.01, float(timeout_seconds)))
        except queue.Empty:
            return None

    def _run(self) -> None:
        channel = None
        try:
            stub = self._stub
            if stub is None:
                channel = self._channel_factory(self.env.agent_addr)
                stub = worker_grpc.RuntimeWorkerAgentStub(channel)
            for frame in stub.Connect(self._outbound_frames()):
                self._handle_agent_frame(frame)
        except BaseException as exc:  # noqa: BLE001
            self._error = exc
        finally:
            close = getattr(channel, "close", None)
            if callable(close):
                close()

    def _outbound_frames(self):
        while True:
            frame = self._outbound.get()
            if frame is None:
                return
            yield frame

    def _handle_agent_frame(self, frame: worker_pb2.AgentFrame) -> None:
        if frame.reply_to:
            with self._pending_reply_lock:
                reply = self._pending_replies.get(frame.reply_to)
            if reply is not None:
                reply.put(frame)
                return
        if frame.WhichOneof("payload") == "platform_call_result":
            call_id = frame.platform_call_result.call_id
            with self._pending_lock:
                reply = self._pending.get(call_id)
            if reply is not None:
                reply.put(frame.platform_call_result)
            return
        if frame.WhichOneof("payload") == "platform_call":
            with self._agent_platform_call_lock:
                handler = self._agent_platform_call_handler
            if handler is not None:
                threading.Thread(
                    target=self._dispatch_agent_platform_call,
                    args=(frame.platform_call, handler),
                    name=f"agent-platform-call-{frame.platform_call.call_id}",
                    daemon=True,
                ).start()
                return
        self._incoming.put(frame)

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise RuntimeError(f"worker agent stream failed: {self._error}") from self._error

    def _dispatch_agent_platform_call(
        self,
        call: worker_pb2.PlatformCall,
        handler: Callable[[worker_pb2.PlatformCall], Any],
    ) -> None:
        try:
            response = handler(call)
            if isinstance(response, Any):
                packed = response
            elif isinstance(response, Message):
                packed = Any()
                packed.Pack(response)
            elif response is None:
                packed = Any()
            else:
                raise TypeError(f"unsupported platform call response type: {type(response)!r}")
            self.send_platform_call_result(call_id=call.call_id, ok=True, response=packed)
        except Exception as exc:  # noqa: BLE001
            self.send_platform_call_result(call_id=call.call_id, ok=False, error=str(exc))


class WorkerRuntimeChannelAdapter:
    def __init__(self, client: WorkerAgentClient) -> None:
        self._client = client

    def invoke_platform_unary(
        self,
        method: str,
        request: Message,
        response_type: type[Message],
        *,
        timeout_seconds: float = 30.0,
    ) -> Message:
        return self._client.invoke_platform_unary(
            method,
            request,
            response_type,
            timeout_seconds=timeout_seconds,
        )

    def send_status_patch(
        self,
        *,
        runtime_id: str = "",
        session_id: str = "",
        status: str,
        reason: str = "",
        payload: Any | None = None,
    ) -> None:
        del runtime_id, payload
        self._client.send_progress(session_id=session_id, status=status, error=reason)


@dataclass(frozen=True)
class RuntimeSessionEvent:
    kind: str
    payload: object
    stream_key: str = ""


class WorkerAgentDataSource:
    def __init__(self, client: WorkerAgentClient) -> None:
        self._client = client

    def iter_session_events(
        self,
        *,
        session_id: str,
        required_streams: list[object],
        stop_event: object,
        idle_timeout_seconds: float = 1.0,
        stop_when_idle: bool = False,
    ):
        del required_streams
        is_set = getattr(stop_event, "is_set", lambda: False)
        while not is_set():
            frame = self._client.next_agent_frame(timeout_seconds=idle_timeout_seconds)
            if frame is None:
                if stop_when_idle:
                    return
                continue
            payload = frame.WhichOneof("payload")
            if payload == "market_data_batch":
                batch = frame.market_data_batch
                if batch.session_id != session_id:
                    continue
                for kline in _decode_market_data_batch(batch):
                    yield RuntimeSessionEvent(kind="kline", payload=kline, stream_key=batch.stream_key)
            elif payload == "order_update_batch":
                batch = frame.order_update_batch
                if batch.session_id != session_id:
                    continue
                for event in _decode_order_update_batch(batch):
                    yield RuntimeSessionEvent(kind="order_update", payload=event, stream_key=batch.stream_key)
            elif payload == "stop_session":
                if frame.stop_session.session_id == session_id:
                    return

    def iter_live_klines(
        self,
        *,
        session_id: str,
        required_streams: list[object],
        stop_event: object,
        idle_timeout_seconds: float = 1.0,
    ):
        for event in self.iter_session_events(
            session_id=session_id,
            required_streams=required_streams,
            stop_event=stop_event,
            idle_timeout_seconds=idle_timeout_seconds,
        ):
            if event.kind == "kline":
                yield event.payload


def _decode_market_data_batch(batch: worker_pb2.MarketDataBatch) -> list[object]:
    from market_data.models import MarketKline

    out: list[object] = []
    for packed in batch.klines:
        st = Struct()
        if not packed.Unpack(st):
            continue
        data = MessageToDict(st)
        symbol = str(data.get("symbol") or "").strip().upper()
        interval = str(data.get("interval") or "").strip()
        if not symbol or not interval:
            continue
        out.append(
            MarketKline(
                symbol=symbol,
                interval=interval,
                open_time=int(data.get("open_time") or data.get("open_time_ms") or 0),
                close_time=int(data.get("close_time") or data.get("close_time_ms") or 0),
                open=float(data.get("open") or 0.0),
                high=float(data.get("high") or 0.0),
                low=float(data.get("low") or 0.0),
                close=float(data.get("close") or 0.0),
                volume=float(data.get("volume") or 0.0),
                timestamp=int(data.get("timestamp") or data.get("close_time") or data.get("close_time_ms") or 0),
                market=str(data.get("market") or "futures").strip().lower(),
            )
        )
    return out


def _decode_order_update_batch(batch: worker_pb2.OrderUpdateBatch) -> list[object]:
    from strategy_service.gen import order_service_pb2
    from strategy_service.order_client import OrderClient

    out: list[object] = []
    for packed in batch.events:
        item = order_service_pb2.OrderLifecycleEventEntry()
        if packed.Unpack(item):
            out.append(OrderClient.order_update_event_from_proto(item))
    return out
