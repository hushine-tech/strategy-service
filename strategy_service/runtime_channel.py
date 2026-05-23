"""Phase D3 RuntimeChannel helpers for self-hosted strategy-runtime."""

from __future__ import annotations

import base64
import json
import logging
import os
import queue
import secrets
import stat
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Callable, Iterable, TypeVar

import grpc
from google.protobuf.any_pb2 import Any
from google.protobuf.message import Message
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from strategy_service.gen import control_panel_service_pb2 as cp_pb2
from strategy_service.gen import control_panel_service_pb2_grpc as cp_grpc
from strategy_service.gen import strategy_service_pb2 as strategy_pb2

logger = logging.getLogger(__name__)

DEFAULT_RUNTIME_CREDENTIAL_PATH = "/etc/hushine/runtime.cred"
DEFAULT_HEARTBEAT_SECONDS = 30
COMMAND_PAYLOAD_JSON_TYPE_URL = "type.googleapis.com/controlpanel.v1.RuntimeCommandPayloadJSON"
T = TypeVar("T", bound=Message)


class RuntimeCredentialError(RuntimeError):
    pass


def _b64url_no_pad(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def canonical_hello_payload(hello: cp_pb2.RuntimeHello) -> bytes:
    """Return the exact bytes signed by runtime and verified by control-panel.

    Keep this in sync with control-panel-service/internal/runtimechannel.
    """

    payload = {
        "capabilities": list(hello.capabilities),
        "debug_port": int(hello.debug_port),
        "endpoint_host": str(hello.endpoint_host or ""),
        "grpc_port": int(hello.grpc_port),
        "issued_at_unix_ms": int(hello.issued_at_unix_ms),
        "key_id": str(hello.key_id or ""),
        "nonce": str(hello.nonce or ""),
        "resource_profile": str(hello.resource_profile or ""),
        "runtime_id": str(hello.runtime_id or ""),
        "name": str(hello.name or ""),
        "version": str(hello.version or ""),
    }
    return json.dumps(payload, separators=(",", ":")).encode("utf-8")


def load_ed25519_private_key(private_key_pem: str) -> Ed25519PrivateKey:
    try:
        key = load_pem_private_key(private_key_pem.encode("utf-8"), password=None)
    except ValueError as exc:
        raise ValueError("private_key_pem must contain an Ed25519 private key") from exc
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("private_key_pem must contain an Ed25519 private key")
    return key


@dataclass(frozen=True)
class RuntimeCredential:
    key_id: str
    private_key_pem: str
    private_key: Ed25519PrivateKey
    path: str


def load_runtime_credential(path: str | None = None) -> RuntimeCredential:
    """Load the D3 runtime credential file and fail closed on malformed input."""

    if path is None:
        inline = os.environ.get("RUNTIME_CREDENTIAL_JSON")
        if inline:
            try:
                raw = json.loads(inline)
            except json.JSONDecodeError as exc:
                raise RuntimeCredentialError("runtime credential env RUNTIME_CREDENTIAL_JSON is malformed JSON") from exc
            return _runtime_credential_from_raw(raw, "env:RUNTIME_CREDENTIAL_JSON")

    resolved = path or os.environ.get("RUNTIME_CREDENTIAL_PATH") or DEFAULT_RUNTIME_CREDENTIAL_PATH
    try:
        st = os.stat(resolved)
    except FileNotFoundError as exc:
        raise RuntimeCredentialError(f"runtime credential file not found: {resolved}") from exc
    mode = stat.S_IMODE(st.st_mode)
    if mode & 0o077:
        logger.warning(
            "runtime credential file %s permissions are %o; expected 0600 or stricter",
            resolved,
            mode,
        )
    try:
        with open(resolved, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except json.JSONDecodeError as exc:
        raise RuntimeCredentialError(f"runtime credential file is malformed JSON: {resolved}") from exc
    except OSError as exc:
        raise RuntimeCredentialError(f"runtime credential file cannot be read: {resolved}") from exc
    return _runtime_credential_from_raw(raw, resolved)


def _runtime_credential_from_raw(raw: object, source: str) -> RuntimeCredential:
    if not isinstance(raw, dict):
        raise RuntimeCredentialError("runtime credential file must be a JSON object")
    if raw.get("version") != 1:
        raise RuntimeCredentialError("runtime credential version must be 1")
    key_id = str(raw.get("key_id") or "").strip()
    if not key_id:
        raise RuntimeCredentialError("runtime credential key_id is required")
    private_key_pem = str(raw.get("private_key_pem") or "")
    if not private_key_pem:
        raise RuntimeCredentialError("runtime credential private_key_pem is required")
    try:
        private_key = load_ed25519_private_key(private_key_pem)
    except ValueError as exc:
        raise RuntimeCredentialError(str(exc)) from exc
    return RuntimeCredential(
        key_id=key_id,
        private_key_pem=private_key_pem,
        private_key=private_key,
        path=source,
    )


@dataclass(frozen=True)
class RuntimeHelloArgs:
    key_id: str
    private_key_pem: str
    runtime_id: str = ""
    name: str = ""
    endpoint_host: str = ""
    grpc_port: int = 0
    debug_port: int = 0
    capabilities: tuple[str, ...] = ("strategy", "spot", "futures")
    resource_profile: str = "small"
    version: str = "0.1.0"


def build_signed_hello(args: RuntimeHelloArgs, *, now_ms: int | None = None) -> cp_pb2.RuntimeHello:
    if not args.key_id:
        raise ValueError("key_id is required")
    key = load_ed25519_private_key(args.private_key_pem)
    hello = cp_pb2.RuntimeHello(
        key_id=args.key_id,
        runtime_id=args.runtime_id,
        name=(args.name or "").strip(),
        endpoint_host=args.endpoint_host,
        grpc_port=int(args.grpc_port or 0),
        debug_port=int(args.debug_port or 0),
        capabilities=list(_normalize_capabilities(args.capabilities)),
        resource_profile=args.resource_profile or "small",
        version=args.version or "0.1.0",
        issued_at_unix_ms=int(now_ms if now_ms is not None else time.time() * 1000),
        nonce=_b64url_no_pad(secrets.token_bytes(16)),
    )
    signature = key.sign(canonical_hello_payload(hello))
    hello.signature = _b64url_no_pad(signature)
    return hello


def build_signed_hello_from_credential(
    credential: RuntimeCredential,
    args: RuntimeHelloArgs,
    *,
    now_ms: int | None = None,
) -> cp_pb2.RuntimeHello:
    hello_args = RuntimeHelloArgs(
        key_id=credential.key_id,
        private_key_pem=credential.private_key_pem,
        runtime_id=args.runtime_id,
        name=args.name,
        endpoint_host=args.endpoint_host,
        grpc_port=args.grpc_port,
        debug_port=args.debug_port,
        capabilities=args.capabilities,
        resource_profile=args.resource_profile,
        version=args.version,
    )
    return build_signed_hello(hello_args, now_ms=now_ms)


def hello_frame(hello: cp_pb2.RuntimeHello) -> cp_pb2.RuntimeFrame:
    return cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_HELLO,
        hello=hello,
    )


def heartbeat_frame(fingerprint: str = "") -> cp_pb2.RuntimeFrame:
    return cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_HEARTBEAT,
        heartbeat=cp_pb2.Heartbeat(
            sent_at_unix_ms=int(time.time() * 1000),
            fingerprint=str(fingerprint or ""),
        ),
    )


def _normalize_capabilities(values: Iterable[str]) -> tuple[str, ...]:
    out = tuple(str(v) for v in values if str(v))
    return out or ("strategy", "spot", "futures")


class RuntimeChannelClient:
    """Outbound RuntimeChannel client for D3 self-hosted runtimes.

    The class owns stream lifecycle only. Section 4.4 wires REQUEST frames to
    StrategyServiceServicer; until then REQUEST receives a terminal ERROR.
    """

    def __init__(
        self,
        address: str,
        credential: RuntimeCredential,
        hello_args: RuntimeHelloArgs,
        *,
        heartbeat_seconds: int = DEFAULT_HEARTBEAT_SECONDS,
        request_handler: Callable[[cp_pb2.RuntimeFrame], cp_pb2.RuntimeFrame] | None = None,
        data_handler: Callable[[cp_pb2.RuntimeFrame], None] | None = None,
        command_handler: Callable[[str, bytes], bytes | None] | None = None,
        grpc_channel_factory: Callable[[str], grpc.Channel] | None = None,
        stub_factory: Callable[[grpc.Channel], cp_grpc.ControlPanelServiceStub] | None = None,
    ) -> None:
        if not address:
            raise ValueError("control-panel-service address is empty")
        self._address = address
        self._credential = credential
        self._hello_args = hello_args
        self._heartbeat_seconds = max(1, int(heartbeat_seconds or DEFAULT_HEARTBEAT_SECONDS))
        self._request_handler = request_handler
        self._data_handler = data_handler
        self._command_handler = command_handler
        self._grpc_channel_factory = grpc_channel_factory or grpc.insecure_channel
        self._stub_factory = stub_factory or cp_grpc.ControlPanelServiceStub
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._channel_lock = threading.Lock()
        self._current_channel = None
        self._inflight_lock = threading.Lock()
        self._inflight_abort: dict[str, Callable[[str], None]] = {}
        self._outbound_lock = threading.Lock()
        self._outbound: queue.Queue[cp_pb2.RuntimeFrame | None] | None = None
        self._connected = threading.Event()
        self._pending_lock = threading.Lock()
        self._pending: dict[str, queue.Queue[cp_pb2.RuntimeFrame]] = {}
        self._resume_lock = threading.Lock()
        self._resume_runtime_id = ""
        self._resume_token = ""
        self._resume_expires_unix_ms = 0
        self._fingerprint = ""
        self._fingerprint_expires_unix_ms = 0

    def set_data_handler(self, handler: Callable[[cp_pb2.RuntimeFrame], None] | None) -> None:
        self._data_handler = handler

    def set_command_handler(self, handler: Callable[[str, bytes], bytes | None] | None) -> None:
        self._command_handler = handler

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="runtime-channel",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout_seconds: float = 5.0) -> None:
        self._stop_event.set()
        with self._channel_lock:
            channel = self._current_channel
        close = getattr(channel, "close", None)
        if callable(close):
            close()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout_seconds)
        self.abort_all("runtime channel stopped")
        self._fail_pending("runtime channel stopped")

    def is_alive(self) -> bool:
        thread = self._thread
        return thread is not None and thread.is_alive()

    def register_inflight(self, correlation_id: str, abort: Callable[[str], None]) -> None:
        if not correlation_id:
            return
        with self._inflight_lock:
            self._inflight_abort[correlation_id] = abort

    def clear_inflight(self, correlation_id: str) -> None:
        if not correlation_id:
            return
        with self._inflight_lock:
            self._inflight_abort.pop(correlation_id, None)

    def abort_all(self, reason: str) -> None:
        with self._inflight_lock:
            callbacks = list(self._inflight_abort.values())
            self._inflight_abort.clear()
        for abort in callbacks:
            try:
                abort(reason)
            except Exception:  # noqa: BLE001
                logger.warning("runtime channel abort callback failed", exc_info=True)

    def invoke_platform_unary(
        self,
        method: str,
        request: Message,
        response_type: type[T],
        *,
        timeout_seconds: float = 30.0,
    ) -> T:
        method = str(method or "").strip()
        if not method:
            raise RuntimeError("runtime platform method is required")
        packed = Any()
        packed.Pack(request)
        correlation_id = uuid.uuid4().hex
        deadline = time.monotonic() + max(0.1, float(timeout_seconds or 30.0))
        remaining = max(0.0, deadline - time.monotonic())
        if not self._connected.wait(timeout=remaining):
            raise RuntimeError("runtime channel is not connected")
        with self._outbound_lock:
            outbound = self._outbound
        if outbound is None:
            raise RuntimeError("runtime channel is not connected")

        reply: queue.Queue[cp_pb2.RuntimeFrame] = queue.Queue()
        with self._pending_lock:
            self._pending[correlation_id] = reply
        try:
            remaining = max(0.0, deadline - time.monotonic())
            outbound.put(cp_pb2.RuntimeFrame(
                correlation_id=correlation_id,
                frame_type=cp_pb2.FRAME_TYPE_REQUEST,
                deadline_unix_ms=int((time.time() + remaining) * 1000) if remaining else 0,
                request=cp_pb2.StrategyRequest(method=method, request=packed),
            ))
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"runtime platform request timed out: {method}")
                frame = reply.get(timeout=remaining)
                if frame.frame_type == cp_pb2.FRAME_TYPE_PROGRESS:
                    continue
                if frame.frame_type == cp_pb2.FRAME_TYPE_ERROR:
                    code = frame.error.code if frame.HasField("error") else "Internal"
                    message = frame.error.message if frame.HasField("error") else "runtime platform request failed"
                    raise RuntimeError(f"{code}: {message}")
                if frame.frame_type != cp_pb2.FRAME_TYPE_RESPONSE:
                    raise RuntimeError(f"unexpected runtime platform frame_type={frame.frame_type}")
                if not frame.HasField("response") or frame.response.response is None:
                    raise RuntimeError("runtime platform response payload is empty")
                response = response_type()
                if not frame.response.response.Unpack(response):
                    raise RuntimeError(f"runtime platform response type mismatch for {method}")
                return response
        finally:
            with self._pending_lock:
                self._pending.pop(correlation_id, None)

    def send_status_patch(
        self,
        *,
        runtime_id: str = "",
        session_id: str = "",
        status: str,
        reason: str = "",
        payload: Any | None = None,
    ) -> None:
        status = str(status or "").strip()
        if not status:
            raise RuntimeError("status patch status is required")
        with self._outbound_lock:
            outbound = self._outbound
        if outbound is None:
            raise RuntimeError("runtime channel is not connected")
        outbound.put(cp_pb2.RuntimeFrame(
            frame_type=cp_pb2.FRAME_TYPE_STATUS_PATCH,
            status_patch=cp_pb2.RuntimeStatusPatch(
                runtime_id=runtime_id or self._current_runtime_id(),
                session_id=session_id,
                status=status,
                reason=reason,
                payload=payload,
            ),
        ))

    def _fail_pending(self, reason: str) -> None:
        with self._pending_lock:
            pending = list(self._pending.items())
            self._pending.clear()
        for correlation_id, reply in pending:
            reply.put(_error_frame(correlation_id, "Unavailable", reason))

    def _run_loop(self) -> None:
        backoff = 1.0
        while not self._stop_event.is_set():
            try:
                self._run_once()
                backoff = 1.0
            except Exception as exc:  # noqa: BLE001
                if self._stop_event.is_set():
                    break
                self.abort_all("runtime channel disconnected")
                self._fail_pending("runtime channel disconnected")
                if _is_terminal_runtime_channel_error(exc):
                    logger.error("RuntimeChannel terminal disconnect: %s", exc)
                    self._stop_event.set()
                    break
                logger.warning("RuntimeChannel disconnected: %s", exc)
                self._stop_event.wait(backoff)
                backoff = min(backoff * 2, 30.0)

    def _run_once(self) -> None:
        channel = self._grpc_channel_factory(self._address)
        with self._channel_lock:
            self._current_channel = channel
        stub = self._stub_factory(channel)
        outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()
        outbound.put(self._build_initial_frame())
        with self._outbound_lock:
            self._outbound = outbound
        self._connected.set()
        heartbeat_stop = threading.Event()
        heartbeat = threading.Thread(
            target=self._heartbeat_loop,
            args=(outbound, heartbeat_stop),
            name="runtime-channel-heartbeat",
            daemon=True,
        )
        heartbeat.start()
        try:
            responses = stub.RuntimeChannel(self._outbound_frames(outbound))
            for frame in responses:
                if self._stop_event.is_set():
                    break
                self._handle_inbound_frame(frame, outbound)
        finally:
            heartbeat_stop.set()
            outbound.put(None)
            self._connected.clear()
            with self._outbound_lock:
                if self._outbound is outbound:
                    self._outbound = None
            self._fail_pending("runtime channel disconnected")
            close = getattr(channel, "close", None)
            if callable(close):
                close()
            with self._channel_lock:
                if self._current_channel is channel:
                    self._current_channel = None

    def _heartbeat_loop(self, outbound: queue.Queue[cp_pb2.RuntimeFrame | None], stop_event: threading.Event) -> None:
        while not stop_event.wait(self._heartbeat_seconds):
            outbound.put(heartbeat_frame(self._current_fingerprint()))

    def _outbound_frames(self, outbound: queue.Queue[cp_pb2.RuntimeFrame | None]):
        while not self._stop_event.is_set():
            frame = outbound.get()
            if frame is None:
                return
            yield frame

    def _handle_inbound_frame(
        self,
        frame: cp_pb2.RuntimeFrame,
        outbound: queue.Queue[cp_pb2.RuntimeFrame | None],
    ) -> None:
        if frame.correlation_id:
            with self._pending_lock:
                pending = self._pending.get(frame.correlation_id)
            if pending is not None and frame.frame_type in (
                cp_pb2.FRAME_TYPE_RESPONSE,
                cp_pb2.FRAME_TYPE_PROGRESS,
                cp_pb2.FRAME_TYPE_ERROR,
            ):
                pending.put(frame)
                return
        if frame.frame_type == cp_pb2.FRAME_TYPE_ABORT:
            cid = frame.correlation_id
            reason = frame.abort.reason if frame.HasField("abort") else "aborted by control plane"
            with self._inflight_lock:
                abort = self._inflight_abort.pop(cid, None)
            if abort is not None:
                abort(reason)
            return
        if frame.frame_type == cp_pb2.FRAME_TYPE_SHUTDOWN:
            reason = frame.shutdown.reason if frame.HasField("shutdown") else "runtime shutdown requested"
            logger.error("RuntimeChannel shutdown requested: %s", reason)
            self.abort_all(reason)
            self._stop_event.set()
            return
        if frame.frame_type == cp_pb2.FRAME_TYPE_HELLO_ACK:
            if frame.HasField("hello_ack"):
                self._remember_resume_material(frame.hello_ack)
            return
        if frame.frame_type == cp_pb2.FRAME_TYPE_HEARTBEAT_ACK:
            if frame.HasField("heartbeat_ack"):
                self._remember_fingerprint(frame.heartbeat_ack.runtime_id, frame.heartbeat_ack.fingerprint, frame.heartbeat_ack.fingerprint_expires_at)
            return
        if frame.frame_type == cp_pb2.FRAME_TYPE_COMMAND:
            self._handle_command_frame(frame, outbound)
            return
        if frame.frame_type in (
            cp_pb2.FRAME_TYPE_DATASET_CHUNK,
            cp_pb2.FRAME_TYPE_LIVE_KLINE_BATCH,
        ):
            self._handle_data_frame(frame, outbound)
            return
        if frame.frame_type == cp_pb2.FRAME_TYPE_REQUEST:
            if frame.deadline_unix_ms and int(time.time() * 1000) > frame.deadline_unix_ms:
                outbound.put(_error_frame(
                    frame.correlation_id,
                    "DeadlineExceeded",
                    "RuntimeChannel request deadline exceeded before dispatch",
                ))
                return
            if self._request_handler is not None:
                def _dispatch_request() -> None:
                    try:
                        outbound.put(self._request_handler(frame))
                    except Exception as exc:  # noqa: BLE001
                        logger.exception("RuntimeChannel request dispatch failed")
                        outbound.put(_error_frame(frame.correlation_id, "Internal", str(exc)))

                threading.Thread(
                    target=_dispatch_request,
                    name=f"runtime-channel-request-{frame.correlation_id[:8]}",
                    daemon=True,
                ).start()
                return
            outbound.put(cp_pb2.RuntimeFrame(
                correlation_id=frame.correlation_id,
                frame_type=cp_pb2.FRAME_TYPE_ERROR,
                error=cp_pb2.StreamError(
                    code="Unimplemented",
                    message="RuntimeChannel request dispatch is not wired yet",
                ),
            ))
            return
        logger.debug("ignoring inbound RuntimeChannel frame_type=%s", frame.frame_type)

    def _build_initial_frame(self) -> cp_pb2.RuntimeFrame:
        resume = self._build_resume_frame_if_valid()
        if resume is not None:
            return resume
        return hello_frame(build_signed_hello_from_credential(self._credential, self._hello_args))

    def _build_resume_frame_if_valid(self) -> cp_pb2.RuntimeFrame | None:
        now_ms = int(time.time() * 1000)
        with self._resume_lock:
            if not self._resume_runtime_id or not self._resume_token:
                return None
            if self._resume_expires_unix_ms <= now_ms:
                self._resume_runtime_id = ""
                self._resume_token = ""
                self._resume_expires_unix_ms = 0
                return None
            return cp_pb2.RuntimeFrame(
                frame_type=cp_pb2.FRAME_TYPE_RESUME,
                resume=cp_pb2.RuntimeResume(
                    runtime_id=self._resume_runtime_id,
                    resume_token=self._resume_token,
                    fingerprint=self._resume_token,
                ),
            )

    def _remember_resume_material(self, ack: cp_pb2.RuntimeHelloAck) -> None:
        runtime_id = str(ack.runtime_id or "").strip()
        token = str(ack.fingerprint or "").strip() or str(ack.resume_token or "").strip()
        expires = ack.fingerprint_expires_at if ack.HasField("fingerprint_expires_at") else ack.resume_token_expires_at
        expires_ms = int(expires.seconds) * 1000 + int(expires.nanos) // 1_000_000
        if not runtime_id or not token or expires_ms <= int(time.time() * 1000):
            return
        with self._resume_lock:
            self._resume_runtime_id = runtime_id
            self._resume_token = token
            self._resume_expires_unix_ms = expires_ms
            self._fingerprint = token
            self._fingerprint_expires_unix_ms = expires_ms

    def _remember_fingerprint(self, runtime_id: str, fingerprint: str, expires) -> None:
        runtime_id = str(runtime_id or "").strip()
        token = str(fingerprint or "").strip()
        expires_ms = int(expires.seconds) * 1000 + int(expires.nanos) // 1_000_000
        if not runtime_id or not token or expires_ms <= int(time.time() * 1000):
            return
        with self._resume_lock:
            self._resume_runtime_id = runtime_id
            self._resume_token = token
            self._resume_expires_unix_ms = expires_ms
            self._fingerprint = token
            self._fingerprint_expires_unix_ms = expires_ms

    def _current_fingerprint(self) -> str:
        now_ms = int(time.time() * 1000)
        with self._resume_lock:
            if self._fingerprint and self._fingerprint_expires_unix_ms > now_ms:
                return self._fingerprint
        return ""

    def _current_runtime_id(self) -> str:
        with self._resume_lock:
            if self._resume_runtime_id:
                return self._resume_runtime_id
        return self._hello_args.runtime_id

    def _handle_command_frame(
        self,
        frame: cp_pb2.RuntimeFrame,
        outbound: queue.Queue[cp_pb2.RuntimeFrame | None],
    ) -> None:
        if not frame.HasField("command") or not frame.command.command_id:
            logger.warning("ignoring command frame without command_id")
            return
        command = frame.command
        outbound.put(_command_ack_frame(command.command_id, "acked"))
        if command.command_type == "shutdown_runtime":
            reason = "runtime shutdown command received"
            self.abort_all(reason)
            self._stop_event.set()
            outbound.put(_command_result_frame(command.command_id, "succeeded"))
            return
        handler = self._command_handler
        if handler is None:
            outbound.put(_command_result_frame(
                command.command_id,
                "failed",
                failure_reason=f"runtime command {command.command_type!r} is not wired to worker dispatch yet",
            ))
            return
        try:
            payload = bytes(command.payload.value) if command.HasField("payload") else b"{}"
            result = handler(command.command_type, payload) or b"{}"
        except Exception as exc:  # noqa: BLE001
            logger.exception("RuntimeChannel command failed: %s", command.command_type)
            outbound.put(_command_result_frame(command.command_id, "failed", failure_reason=str(exc)))
            return
        outbound.put(_command_result_frame(command.command_id, "succeeded", result=result))

    def _handle_data_frame(
        self,
        frame: cp_pb2.RuntimeFrame,
        outbound: queue.Queue[cp_pb2.RuntimeFrame | None],
    ) -> None:
        handler = self._data_handler
        if handler is not None:
            handler(frame)
        if frame.frame_type == cp_pb2.FRAME_TYPE_DATASET_CHUNK and frame.HasField("dataset_chunk"):
            outbound.put(_data_ack_frame(
                frame.dataset_chunk.session_id,
                frame.dataset_chunk.dataset_id,
                frame.dataset_chunk.sequence,
            ))
            return
        if frame.frame_type == cp_pb2.FRAME_TYPE_LIVE_KLINE_BATCH and frame.HasField("live_kline_batch"):
            outbound.put(_data_ack_frame(
                frame.live_kline_batch.session_id,
                frame.live_kline_batch.stream_key,
                frame.live_kline_batch.sequence,
            ))


def _error_frame(correlation_id: str, code: str, message: str) -> cp_pb2.RuntimeFrame:
    return cp_pb2.RuntimeFrame(
        correlation_id=correlation_id,
        frame_type=cp_pb2.FRAME_TYPE_ERROR,
        error=cp_pb2.StreamError(code=code, message=message),
    )


def _command_ack_frame(command_id: str, status: str) -> cp_pb2.RuntimeFrame:
    return cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_COMMAND_ACK,
        command_ack=cp_pb2.RuntimeCommandAck(command_id=command_id, status=status),
    )


def _command_result_frame(command_id: str, status: str, failure_reason: str = "", result: bytes | None = None) -> cp_pb2.RuntimeFrame:
    packed = None
    if result is not None:
        packed = Any(type_url=COMMAND_PAYLOAD_JSON_TYPE_URL, value=bytes(result))
    return cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_COMMAND_RESULT,
        command_result=cp_pb2.RuntimeCommandResult(
            command_id=command_id,
            status=status,
            result=packed,
            failure_reason=failure_reason,
        ),
    )


def _data_ack_frame(session_id: str, stream_key: str, sequence: int) -> cp_pb2.RuntimeFrame:
    return cp_pb2.RuntimeFrame(
        frame_type=cp_pb2.FRAME_TYPE_DATA_ACK,
        data_ack=cp_pb2.RuntimeDataAck(
            session_id=session_id,
            stream_key=stream_key,
            sequence=int(sequence),
        ),
    )


def _is_terminal_runtime_channel_error(exc: BaseException) -> bool:
    code_getter = getattr(exc, "code", None)
    if not callable(code_getter):
        return False
    try:
        code = code_getter()
    except Exception:  # noqa: BLE001
        return False
    return code in {
        grpc.StatusCode.INVALID_ARGUMENT,
        grpc.StatusCode.PERMISSION_DENIED,
        grpc.StatusCode.NOT_FOUND,
        grpc.StatusCode.FAILED_PRECONDITION,
        grpc.StatusCode.UNAUTHENTICATED,
    }


def _response_frame(correlation_id: str, response: Message) -> cp_pb2.RuntimeFrame:
    packed = Any()
    packed.Pack(response)
    return cp_pb2.RuntimeFrame(
        correlation_id=correlation_id,
        frame_type=cp_pb2.FRAME_TYPE_RESPONSE,
        response=cp_pb2.StrategyResponse(response=packed),
    )


_METHODS = {
    "RunStrategy": ("RunStrategy", strategy_pb2.RunStrategyRequest),
    "PreviewRunStrategy": ("PreviewRunStrategy", strategy_pb2.PreviewRunStrategyRequest),
    "StopStrategy": ("StopStrategy", strategy_pb2.StopStrategyRequest),
    "GetStrategyStatus": ("GetStrategyStatus", strategy_pb2.GetStrategyStatusRequest),
}

_GRPC_CODE_TO_STREAM = {
    grpc.StatusCode.INVALID_ARGUMENT: "InvalidArgument",
    grpc.StatusCode.PERMISSION_DENIED: "PermissionDenied",
    grpc.StatusCode.NOT_FOUND: "NotFound",
    grpc.StatusCode.FAILED_PRECONDITION: "FailedPrecondition",
    grpc.StatusCode.DEADLINE_EXCEEDED: "DeadlineExceeded",
    grpc.StatusCode.UNAVAILABLE: "Unavailable",
    grpc.StatusCode.UNIMPLEMENTED: "Unimplemented",
    grpc.StatusCode.INTERNAL: "Internal",
}


class RuntimeChannelStrategyDispatcher:
    """Bridge RuntimeChannel REQUEST frames to the existing gRPC servicer."""

    def __init__(self, servicer) -> None:
        self._servicer = servicer

    def __call__(self, frame: cp_pb2.RuntimeFrame) -> cp_pb2.RuntimeFrame:
        req = frame.request
        method = req.method if req is not None else ""
        target = _METHODS.get(method)
        if target is None:
            return _error_frame(frame.correlation_id, "Unimplemented", f"unsupported strategy method: {method}")
        method_name, request_type = target
        request = request_type()
        if req is None or req.request is None or not req.request.Unpack(request):
            return _error_frame(frame.correlation_id, "InvalidArgument", f"invalid {method} request payload")

        context = _RuntimeChannelContext()
        response = getattr(self._servicer, method_name)(request, context)
        if context.code is not grpc.StatusCode.OK:
            return _error_frame(
                frame.correlation_id,
                _GRPC_CODE_TO_STREAM.get(context.code, "Internal"),
                context.details or context.code.name,
            )
        return _response_frame(frame.correlation_id, response)


class _RuntimeChannelContext:
    def __init__(self) -> None:
        self.code = grpc.StatusCode.OK
        self.details = ""

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details
