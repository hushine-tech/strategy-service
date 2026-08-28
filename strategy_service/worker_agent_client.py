from __future__ import annotations

import json
import math
import os
import queue
import threading
import time
import uuid
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Callable

import grpc
from google.protobuf.any_pb2 import Any
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message
from google.protobuf.struct_pb2 import Struct
from strategy_service.gen import runtime_worker_pb2 as worker_pb2
from strategy_service.gen import runtime_worker_pb2_grpc as worker_grpc
from strategy_service.gen import portfolio_service_pb2 as portfolio_pb2

WORKER_VERSION = "0.1.0"
WORKER_PROTOCOL_VERSION = 6


class FinalStatusRejected(RuntimeError):
    pass


class WorkerPlatformCallError(RuntimeError):
    def __init__(
        self,
        message: str,
        dependency_error=None,
        *,
        code: str = "",
        detail_json: str = "{}",
    ) -> None:
        super().__init__(message)
        self.code = str(code or "")
        self.message = str(message or "")
        self.detail_json = str(detail_json or "{}").strip() or "{}"
        self.dependency_error = dependency_error


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
            protocol_version=WORKER_PROTOCOL_VERSION,
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
        self._outbound_lock = threading.Lock()
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
        self._enqueue_outbound(build_worker_hello_frame(self.env))
        self._thread = threading.Thread(target=self._run, name="worker-agent-client", daemon=True)
        self._thread.start()

    def close(self) -> None:
        with self._outbound_lock:
            if not self._closed.is_set():
                self._closed.set()
                self._outbound.put(None)
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def _enqueue_outbound(self, frame: worker_pb2.WorkerFrame) -> None:
        with self._outbound_lock:
            if self._closed.is_set():
                raise RuntimeError("worker agent client is closed")
            if self._error is not None:
                raise RuntimeError("worker agent stream failed") from self._error
            self._outbound.put(frame)

    def wait_for_start_session(self, *, timeout_seconds: float = 30.0) -> worker_pb2.StartSession:
        deadline = time.monotonic() + max(0.1, float(timeout_seconds or 30.0))
        while True:
            try:
                frame = self._incoming.get_nowait()
            except queue.Empty:
                self._raise_if_failed()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out waiting for StartSession")
                try:
                    frame = self._incoming.get(timeout=min(0.05, remaining))
                except queue.Empty:
                    continue
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
            self._enqueue_outbound(
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
                message = result.error_message or result.error or f"platform call failed: {method}"
                dependency_error = (
                    result.dependency_error if result.HasField("dependency_error") else None
                )
                raise WorkerPlatformCallError(
                    message,
                    dependency_error,
                    code=result.error_code,
                    detail_json=result.error_detail_json or "{}",
                )
            response = response_type()
            if not result.response.Unpack(response):
                raise RuntimeError(f"platform response type mismatch for {method}")
            return response
        finally:
            with self._pending_lock:
                self._pending.pop(call_id, None)

    def send_progress(
        self,
        *,
        session_id: str,
        status: str,
        bars_processed: int = 0,
        error: str = "",
        error_code: str = "",
        error_message: str = "",
        error_detail_json: str = "{}",
        dependency_error=None,
    ) -> None:
        self._enqueue_outbound(
            worker_pb2.WorkerFrame(
                progress=worker_pb2.SessionProgress(
                    session_id=session_id,
                    status=status,
                    bars_processed=int(bars_processed),
                    error=error,
                    error_code=str(error_code or ""),
                    error_message=str(error_message or ""),
                    error_detail_json=str(error_detail_json or "{}").strip() or "{}",
                    dependency_error=dependency_error,
                )
            )
        )

    def send_data_ack(self, *, session_id: str, stream_key: str, sequence: int) -> None:
        session_id = str(session_id or "").strip()
        stream_key = str(stream_key or "").strip()
        sequence = int(sequence)
        if not session_id:
            raise ValueError("Income data ACK session_id is required")
        if stream_key != f"income/{session_id}":
            raise ValueError("Income data ACK stream_key is invalid")
        if sequence <= 0:
            raise ValueError("Income data ACK sequence must be positive")
        self._enqueue_outbound(
            worker_pb2.WorkerFrame(
                data_ack=worker_pb2.WorkerDataAck(
                    session_id=session_id,
                    stream_key=stream_key,
                    sequence=sequence,
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
        error_code: str = "",
        error_message: str = "",
        error_detail_json: str = "{}",
        reconciliation_run_id: str = "",
        dependency_error=None,
        timeout_seconds: float = 35.0,
    ) -> None:
        frame_id = self._call_id_factory()
        reply: queue.Queue[worker_pb2.AgentFrame] = queue.Queue(maxsize=1)
        with self._pending_reply_lock:
            self._pending_replies[frame_id] = reply
        try:
            self._enqueue_outbound(
                worker_pb2.WorkerFrame(
                    frame_id=frame_id,
                    final_status=worker_pb2.FinalStatus(
                        session_id=session_id,
                        status=status,
                        bars_processed=int(bars_processed),
                        error=error,
                        error_code=str(error_code or ""),
                        error_message=str(error_message or ""),
                        error_detail_json=str(error_detail_json or "{}").strip() or "{}",
                        dependency_error=dependency_error,
                        reconciliation_run_id=str(reconciliation_run_id or "").strip(),
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
        error_code: str = "",
        error_message: str = "",
        error_detail_json: str = "{}",
        dependency_error=None,
    ) -> None:
        packed = response if response is not None else Any()
        self._enqueue_outbound(
            worker_pb2.WorkerFrame(
                platform_call_result=worker_pb2.PlatformCallResult(
                    call_id=str(call_id or ""),
                    ok=bool(ok),
                    response=packed,
                    error=str(error or ""),
                    error_code=str(error_code or ""),
                    error_message=str(error_message or ""),
                    error_detail_json=str(error_detail_json or "{}").strip() or "{}",
                    dependency_error=dependency_error,
                )
            )
        )

    def send_worker_error(
        self,
        *,
        session_id: str,
        error_type: str,
        message: str,
        stack: str = "",
        dependency_error=None,
    ) -> None:
        self._enqueue_outbound(
            worker_pb2.WorkerFrame(
                worker_error=worker_pb2.WorkerError(
                    session_id=str(session_id or ""),
                    error_type=str(error_type or ""),
                    message=str(message or ""),
                    stack=str(stack or ""),
                    dependency_error=dependency_error,
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
        stream_sequence: int,
        market_time_ms: int,
        interval_ms: int,
        definitions: list[object],
        frame: object,
    ) -> None:
        msg = worker_pb2.IndicatorFrameV2(
            session_id=str(session_id or ""),
            user_id=int(user_id or 0),
            strategy_id=int(strategy_id or 0),
            stream_key=str(stream_key or ""),
            stream_sequence=int(stream_sequence),
            market_time_ms=int(market_time_ms or 0),
            interval_ms=int(interval_ms or 0),
        )
        values = getattr(frame, "values", {}) or {}
        markers = getattr(frame, "markers", {}) or {}
        for definition in definitions or []:
            key = str(getattr(definition, "key", "") or getattr(definition, "indicator_key", "") or "").strip()
            if not key:
                continue
            raw_config = getattr(definition, "config", None)
            cfg = {} if raw_config is None else raw_config
            if type(cfg) is not dict:
                raise ValueError(
                    f"indicator config must be a plain mapping: {key}"
                )
            msg.definitions.add(
                indicator_key=key,
                name=str(getattr(definition, "name", "") or key),
                type=str(getattr(definition, "type", "") or ""),
                pane=str(getattr(definition, "pane", "") or ""),
                color=str(getattr(definition, "color", "") or ""),
                unit=str(getattr(definition, "unit", "") or ""),
                description=str(getattr(definition, "description", "") or ""),
                config_json=json.dumps(
                    cfg,
                    separators=(",", ":"),
                    allow_nan=False,
                ),
            )
        value_keys = {str(key or "").strip() for key in values}
        marker_keys = {str(key or "").strip() for key in markers}
        overlap = sorted((value_keys & marker_keys) - {""})
        if overlap:
            raise ValueError(
                "indicator key cannot contain both scalar and marker samples: "
                + ", ".join(overlap)
            )
        for raw_key, raw_value in sorted(values.items(), key=lambda item: str(item[0])):
            key = str(raw_key or "").strip()
            if not key:
                continue
            sample = msg.samples.add(indicator_key=key)
            if raw_value is not None:
                scalar_value = float(raw_value)
                if not math.isfinite(scalar_value):
                    raise ValueError(f"indicator scalar value must be finite: {key}")
                sample.scalar_value = scalar_value
        for raw_key, raw_markers in sorted(markers.items(), key=lambda item: str(item[0])):
            key = str(raw_key or "").strip()
            if not key:
                continue
            sample = msg.samples.add(indicator_key=key)
            for raw in raw_markers or []:
                marker = sample.markers.add(
                    text=str(raw.get("text", "") or ""),
                    color=str(raw.get("color", "") or ""),
                    position=str(raw.get("position", "") or ""),
                    shape=str(raw.get("shape", "") or ""),
                )
                if raw.get("price") is not None:
                    marker_price = float(raw["price"])
                    if not math.isfinite(marker_price):
                        raise ValueError(f"indicator marker price must be finite: {key}")
                    marker.price = marker_price
        self._enqueue_outbound(worker_pb2.WorkerFrame(indicator_frame_v2=msg))

    def next_agent_frame(self, *, timeout_seconds: float = 1.0) -> worker_pb2.AgentFrame | None:
        try:
            return self._incoming.get_nowait()
        except queue.Empty:
            self._raise_if_failed()
            try:
                return self._incoming.get(timeout=max(0.01, float(timeout_seconds)))
            except queue.Empty:
                self._raise_if_failed()
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
            with self._outbound_lock:
                self._error = exc
        finally:
            with self._outbound_lock:
                self._closed.set()
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
        with self._outbound_lock:
            error = self._error
            closed = self._closed.is_set()
        if error is not None:
            raise RuntimeError("worker agent stream failed") from error
        if closed:
            raise RuntimeError("worker agent client is closed")

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
        except WorkerPlatformCallError as exc:
            self.send_platform_call_result(
                call_id=call.call_id,
                ok=False,
                error=str(exc),
                error_code=exc.code,
                error_message=exc.message,
                error_detail_json=exc.detail_json,
                dependency_error=exc.dependency_error,
            )
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

@dataclass(frozen=True)
class RuntimeSessionEvent:
    kind: str
    payload: object
    stream_key: str = ""
    session_id: str = ""
    sequence: int = 0
    batch_end: bool = False


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
            elif payload == "income_batch":
                batch = frame.income_batch
                entries = _decode_income_batch(batch, expected_session_id=session_id)
                for index, entry in enumerate(entries):
                    yield RuntimeSessionEvent(
                        kind="income",
                        payload=entry,
                        stream_key=batch.stream_key,
                        session_id=batch.session_id,
                        sequence=batch.sequence,
                        batch_end=index == len(entries) - 1,
                    )
            elif payload == "stop_session":
                if frame.stop_session.session_id == session_id:
                    return

    def acknowledge_income_applied(self, event: RuntimeSessionEvent) -> None:
        if event.kind != "income" or not isinstance(
            event.payload, portfolio_pb2.VenueIncomeEntry
        ):
            raise ValueError("acknowledgment requires a typed Income event")
        if not event.batch_end:
            raise ValueError("only the final Income event may acknowledge its batch")
        if event.payload.income_entry_id != event.sequence:
            raise ValueError("final Income event does not match batch sequence")
        self._client.send_data_ack(
            session_id=event.session_id,
            stream_key=event.stream_key,
            sequence=event.sequence,
        )

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


def _decode_income_batch(
    batch: worker_pb2.IncomeBatch,
    *,
    expected_session_id: str,
) -> list[portfolio_pb2.VenueIncomeEntry]:
    session_id = str(batch.session_id or "").strip()
    if session_id != str(expected_session_id or "").strip():
        raise ValueError("Income batch session_id does not match Worker Session")
    if batch.stream_key != f"income/{session_id}":
        raise ValueError("Income batch stream_key is invalid")
    if batch.sequence <= 0:
        raise ValueError("Income batch sequence must be positive")
    if not batch.entries:
        raise ValueError("Income batch entries are required")

    entries: list[portfolio_pb2.VenueIncomeEntry] = []
    last_id = 0
    canonical_type_url = "type.googleapis.com/portfolio.v1.VenueIncomeEntry"
    for packed in batch.entries:
        if packed.type_url != canonical_type_url:
            raise ValueError("Income entry type_url must be canonical VenueIncomeEntry")
        entry = portfolio_pb2.VenueIncomeEntry()
        try:
            unpacked = packed.Unpack(entry)
        except Exception as exc:  # noqa: BLE001
            raise ValueError("Income entry payload is malformed") from exc
        if not unpacked:
            raise ValueError("Income entry payload is malformed")
        if entry.session_id != session_id:
            raise ValueError("Income entry session_id does not match its batch")
        if entry.income_entry_id <= last_id:
            raise ValueError("Income entry IDs must be positive and strictly ascending")
        if entry.venue_id <= 0:
            raise ValueError("Income entry venue_id must be positive")
        if not entry.income_type or not entry.source or not entry.settlement_key:
            raise ValueError("Income entry identity fields are required")
        if not entry.symbol or not entry.asset:
            raise ValueError("Income entry route fields are required")
        if not entry.HasField("occurred_at"):
            raise ValueError("Income entry occurred_at is required")
        try:
            entry.occurred_at.ToDatetime()
        except (OverflowError, ValueError) as exc:
            raise ValueError("Income entry occurred_at is invalid") from exc
        for name in (
            "calculated_amount_decimal",
            "applied_amount_decimal",
            "reconciliation_delta_decimal",
        ):
            _validate_exact_decimal(getattr(entry, name), name=name, required=True)
        for name in ("exchange_amount_decimal",):
            _validate_exact_decimal(getattr(entry, name), name=name, required=False)
        try:
            details = json.loads(
                entry.calculation_details_json,
                parse_float=str,
                parse_int=str,
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("Income entry calculation_details_json is invalid") from exc
        if not isinstance(details, list):
            raise ValueError("Income entry calculation_details_json must be an array")
        if entry.status not in {"confirmed", "calculated"}:
            raise ValueError("Income entry status is not deliverable")
        entries.append(entry)
        last_id = entry.income_entry_id
    if batch.sequence != last_id:
        raise ValueError("Income batch sequence must equal final income_entry_id")
    return entries


def _validate_exact_decimal(value: str, *, name: str, required: bool) -> None:
    if value == "" and not required:
        return
    if not value or value != value.strip():
        raise ValueError(f"Income entry {name} is invalid")
    try:
        parsed = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"Income entry {name} is invalid") from exc
    if not parsed.is_finite():
        raise ValueError(f"Income entry {name} is invalid")


def _decode_order_update_batch(batch: worker_pb2.OrderUpdateBatch) -> list[object]:
    from strategy_service.gen import order_service_pb2
    from strategy_service.order_client import OrderClient

    out: list[object] = []
    for packed in batch.events:
        item = order_service_pb2.OrderLifecycleEventEntry()
        if packed.Unpack(item):
            out.append(OrderClient.order_update_event_from_proto(item))
    return out
