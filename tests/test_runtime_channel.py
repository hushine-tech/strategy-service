from __future__ import annotations

import base64
import json
import queue
import threading
import time

import grpc
from google.protobuf.any_pb2 import Any
from google.protobuf.timestamp_pb2 import Timestamp
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
    PublicFormat,
)

from strategy_service.runtime_channel import (
    DEFAULT_HEARTBEAT_SECONDS,
    RuntimeChannelClient,
    RuntimeCredential,
    RuntimeCredentialError,
    RuntimeHelloArgs,
    RuntimeChannelStrategyDispatcher,
    build_signed_hello,
    canonical_hello_payload,
    load_runtime_credential,
)
from strategy_service.gen import control_panel_service_pb2 as cp_pb2
from strategy_service.gen import account_service_pb2, strategy_service_pb2


def test_runtime_channel_default_heartbeat_leaves_watchdog_margin():
    assert DEFAULT_HEARTBEAT_SECONDS == 10


def test_build_signed_hello_signs_canonical_payload():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")

    hello = build_signed_hello(
        RuntimeHelloArgs(
            key_id="key-1",
            private_key_pem=private_pem,
            runtime_id="runtime-1",
            name="custom-steady-river",
            capabilities=("strategy", "futures"),
        ),
        now_ms=1_700_000_000_000,
    )

    assert hello.key_id == "key-1"
    assert hello.runtime_id == "runtime-1"
    assert hello.name == "custom-steady-river"
    assert hello.issued_at_unix_ms == 1_700_000_000_000
    assert hello.nonce
    assert hello.signature

    payload = json.loads(canonical_hello_payload(hello).decode("utf-8"))
    assert payload["name"] == "custom-steady-river"
    assert "service_name" not in payload
    assert canonical_hello_payload(hello).decode("utf-8") == (
        '{"capabilities":["strategy","futures"],'
        '"debug_port":0,'
        '"endpoint_host":"",'
        '"grpc_port":0,'
        '"issued_at_unix_ms":1700000000000,'
        '"key_id":"key-1",'
        f'"nonce":"{hello.nonce}",'
        '"resource_profile":"small",'
        '"runtime_id":"runtime-1",'
        '"name":"custom-steady-river",'
        '"version":"0.1.0"}'
    )

    signature = base64.urlsafe_b64decode(hello.signature + "==")
    public_key = private_key.public_key()
    public_key.verify(signature, canonical_hello_payload(hello))


def test_build_signed_hello_rejects_non_ed25519_key():
    # Use a public key PEM in the private-key field to prove strict parsing.
    private_key = Ed25519PrivateKey.generate()
    public_pem = private_key.public_key().public_bytes(
        encoding=Encoding.PEM,
        format=PublicFormat.SubjectPublicKeyInfo,
    ).decode("utf-8")

    try:
        build_signed_hello(RuntimeHelloArgs(key_id="key-1", private_key_pem=public_pem))
    except ValueError as exc:
        assert "Ed25519 private key" in str(exc)
    else:
        raise AssertionError("expected ValueError")


def test_load_runtime_credential_warns_on_world_readable(tmp_path, caplog):
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    path = tmp_path / "runtime.cred"
    path.write_text(json.dumps({
        "version": 1,
        "key_id": "key-1",
        "private_key_pem": private_pem,
    }), encoding="utf-8")
    path.chmod(0o644)

    cred = load_runtime_credential(str(path))

    assert cred.key_id == "key-1"
    assert cred.path == str(path)
    assert any("permissions" in r.message for r in caplog.records)


def test_load_runtime_credential_reads_inline_env_when_path_not_set(monkeypatch):
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    monkeypatch.delenv("RUNTIME_CREDENTIAL_PATH", raising=False)
    monkeypatch.setenv(
        "RUNTIME_CREDENTIAL_JSON",
        json.dumps({
            "version": 1,
            "key_id": "hosted-key-1",
            "private_key_pem": private_pem,
        }),
    )

    cred = load_runtime_credential(None)

    assert cred.key_id == "hosted-key-1"
    assert cred.path == "env:RUNTIME_CREDENTIAL_JSON"


def test_load_runtime_credential_fails_closed_on_bad_version(tmp_path):
    path = tmp_path / "runtime.cred"
    path.write_text(json.dumps({"version": 2, "key_id": "key-1"}), encoding="utf-8")

    try:
        load_runtime_credential(str(path))
    except RuntimeCredentialError as exc:
        assert "version" in str(exc)
    else:
        raise AssertionError("expected RuntimeCredentialError")


def test_runtime_channel_client_aborts_inflight_callbacks():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
    )
    seen: list[str] = []
    client.register_inflight("corr-1", lambda reason: seen.append(reason))

    client.abort_all("disconnect")

    assert seen == ["disconnect"]


def test_runtime_channel_client_disconnect_aborts_inflight_execution():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    aborts: list[str] = []
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        stub_factory=lambda _channel: _DisconnectingStub(),
    )
    client.register_inflight("corr-1", lambda reason: aborts.append(reason))

    client.start()
    deadline = time.time() + 2
    while not aborts and time.time() < deadline:
        time.sleep(0.01)
    client.stop()

    assert aborts == ["runtime channel disconnected"]


def test_runtime_channel_client_reconnects_after_transient_disconnect():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    stub = _ReconnectOnceStub()
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        stub_factory=lambda _channel: stub,
    )

    client.start()
    deadline = time.time() + 2.5
    while stub.calls < 2 and time.time() < deadline:
        time.sleep(0.01)
    client.stop()

    assert stub.calls >= 2
    assert stub.hello_count >= 2


def test_runtime_channel_client_reconnects_with_resume_after_hello_ack():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    stub = _AckThenDisconnectThenResumeStub()
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        stub_factory=lambda _channel: stub,
    )

    client.start()
    deadline = time.time() + 2.5
    while stub.calls < 2 and time.time() < deadline:
        time.sleep(0.01)
    client.stop()

    assert stub.first_frame_types[:2] == [
        cp_pb2.FRAME_TYPE_HELLO,
        cp_pb2.FRAME_TYPE_RESUME,
    ]
    assert stub.resume_runtime_id == "runtime-ack"
    assert stub.resume_token == "resume-token-1"


def test_runtime_channel_client_stops_after_terminal_disconnect():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    stub = _TerminalDisconnectStub()
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        stub_factory=lambda _channel: stub,
    )

    client.start()
    deadline = time.time() + 2
    while not client._stop_event.is_set() and time.time() < deadline:
        time.sleep(0.01)
    client.stop()

    assert client._stop_event.is_set()
    assert stub.calls == 1


def test_runtime_channel_client_stops_after_terminal_credential_error():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    stub = _CredentialTerminalDisconnectStub()
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        stub_factory=lambda _channel: stub,
    )

    client.start()
    deadline = time.time() + 2
    while not client._stop_event.is_set() and time.time() < deadline:
        time.sleep(0.01)
    client.stop()

    assert client._stop_event.is_set()
    assert stub.calls == 1


def test_runtime_channel_heartbeat_loop_keeps_idle_stream_alive():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        heartbeat_seconds=1,
    )
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()
    stop = threading.Event()
    thread = threading.Thread(target=client._heartbeat_loop, args=(outbound, stop), daemon=True)

    thread.start()
    frame = outbound.get(timeout=1.5)
    stop.set()
    thread.join(timeout=1)

    assert frame.frame_type == cp_pb2.FRAME_TYPE_HEARTBEAT
    assert frame.heartbeat.sent_at_unix_ms > 0


def test_runtime_channel_heartbeat_uses_latest_ack_fingerprint():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        heartbeat_seconds=1,
    )
    expires = Timestamp()
    expires.FromSeconds(int(time.time()) + 60)
    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            frame_type=cp_pb2.FRAME_TYPE_HELLO_ACK,
            hello_ack=cp_pb2.RuntimeHelloAck(
                runtime_id="runtime-ack",
                fingerprint="fingerprint-1",
                fingerprint_expires_at=expires,
            ),
        ),
        queue.Queue(),
    )
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()
    stop = threading.Event()
    thread = threading.Thread(target=client._heartbeat_loop, args=(outbound, stop), daemon=True)

    thread.start()
    frame = outbound.get(timeout=1.5)
    stop.set()
    thread.join(timeout=1)

    assert frame.frame_type == cp_pb2.FRAME_TYPE_HEARTBEAT
    assert frame.heartbeat.fingerprint == "fingerprint-1"


def test_runtime_channel_client_invokes_platform_unary():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
    )
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()
    with client._outbound_lock:
        client._outbound = outbound
    client._connected.set()

    result: list[object] = []

    def call():
        result.append(client.invoke_platform_unary(
            "account.SaveSession",
            account_service_pb2.SaveSessionRequest(session_id="sess-1", account_id=7),
            account_service_pb2.SaveSessionResponse,
            timeout_seconds=1,
        ))

    thread = threading.Thread(target=call, daemon=True)
    thread.start()
    frame = outbound.get(timeout=1)

    assert frame.frame_type == cp_pb2.FRAME_TYPE_REQUEST
    assert frame.request.method == "account.SaveSession"
    unpacked = account_service_pb2.SaveSessionRequest()
    assert frame.request.request.Unpack(unpacked)
    assert unpacked.session_id == "sess-1"

    packed = Any()
    packed.Pack(account_service_pb2.SaveSessionResponse())
    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            correlation_id=frame.correlation_id,
            frame_type=cp_pb2.FRAME_TYPE_RESPONSE,
            response=cp_pb2.StrategyResponse(response=packed),
        ),
        outbound,
    )
    thread.join(timeout=1)

    assert len(result) == 1
    assert isinstance(result[0], account_service_pb2.SaveSessionResponse)


def test_runtime_channel_client_injects_trace_context_into_platform_request():
    try:
        from opentelemetry import context as otel_context
        from opentelemetry import propagate, trace
        from opentelemetry.propagate import get_global_textmap, set_global_textmap
        from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags, TraceState
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    except ImportError:
        return

    old_textmap = get_global_textmap()
    set_global_textmap(TraceContextTextMapPropagator())
    span_context = SpanContext(
        trace_id=int("4bf92f3577b34da6a3ce929d0e0e4736", 16),
        span_id=int("00f067aa0ba902b7", 16),
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
        trace_state=TraceState(),
    )
    try:
        private_key = Ed25519PrivateKey.generate()
        private_pem = private_key.private_bytes(
            encoding=Encoding.PEM,
            format=PrivateFormat.PKCS8,
            encryption_algorithm=NoEncryption(),
        ).decode("utf-8")
        client = RuntimeChannelClient(
            "control-panel:50054",
            RuntimeCredential(
                key_id="key-1",
                private_key_pem=private_pem,
                private_key=private_key,
                path="/tmp/runtime.cred",
            ),
            RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        )
        outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()
        with client._outbound_lock:
            client._outbound = outbound
        client._connected.set()

        result: list[object] = []
        errors: list[BaseException] = []

        def call_with_span():
            token = otel_context.attach(trace.set_span_in_context(NonRecordingSpan(span_context)))
            try:
                result.append(client.invoke_platform_unary(
                    "account.SaveSession",
                    account_service_pb2.SaveSessionRequest(session_id="sess-1", account_id=7),
                    account_service_pb2.SaveSessionResponse,
                    timeout_seconds=1,
                ))
            except BaseException as exc:
                errors.append(exc)
            finally:
                otel_context.detach(token)

        thread = threading.Thread(
            target=call_with_span,
            daemon=True,
        )
        thread.start()
        frame = outbound.get(timeout=1)
        assert frame.request.trace_context["traceparent"].startswith("00-4bf92f3577b34da6a3ce929d0e0e4736-")

        packed = Any()
        packed.Pack(account_service_pb2.SaveSessionResponse())
        client._handle_inbound_frame(
            cp_pb2.RuntimeFrame(
                correlation_id=frame.correlation_id,
                frame_type=cp_pb2.FRAME_TYPE_RESPONSE,
                response=cp_pb2.StrategyResponse(response=packed),
            ),
            outbound,
        )
        thread.join(timeout=1)
        if errors:
            raise errors[0]
        assert len(result) == 1
    finally:
        set_global_textmap(old_textmap)
        # Keep import referenced even if assertion changes later.
        assert propagate is not None


def test_runtime_channel_request_handler_can_call_platform_proxy_without_deadlock():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()

    def handler(frame):
        client.invoke_platform_unary(
            "account.SaveSession",
            account_service_pb2.SaveSessionRequest(session_id="sess-1", account_id=7),
            account_service_pb2.SaveSessionResponse,
            timeout_seconds=1,
        )
        packed = Any()
        packed.Pack(strategy_service_pb2.RunStrategyResponse(session_id="sess-1"))
        return cp_pb2.RuntimeFrame(
            correlation_id=frame.correlation_id,
            frame_type=cp_pb2.FRAME_TYPE_RESPONSE,
            response=cp_pb2.StrategyResponse(response=packed),
        )

    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        request_handler=handler,
    )
    with client._outbound_lock:
        client._outbound = outbound
    client._connected.set()

    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            correlation_id="strategy-corr",
            frame_type=cp_pb2.FRAME_TYPE_REQUEST,
            request=cp_pb2.StrategyRequest(method="RunStrategy"),
        ),
        outbound,
    )
    platform_req = outbound.get(timeout=1)
    assert platform_req.frame_type == cp_pb2.FRAME_TYPE_REQUEST
    assert platform_req.request.method == "account.SaveSession"

    packed = Any()
    packed.Pack(account_service_pb2.SaveSessionResponse())
    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            correlation_id=platform_req.correlation_id,
            frame_type=cp_pb2.FRAME_TYPE_RESPONSE,
            response=cp_pb2.StrategyResponse(response=packed),
        ),
        outbound,
    )
    strategy_resp = outbound.get(timeout=1)
    assert strategy_resp.correlation_id == "strategy-corr"
    assert strategy_resp.frame_type == cp_pb2.FRAME_TYPE_RESPONSE


def test_runtime_channel_request_deadline_is_honored_before_dispatch():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    called = False

    def handler(_frame):
        nonlocal called
        called = True
        raise AssertionError("expired requests must not dispatch")

    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
        request_handler=handler,
    )
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()
    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            correlation_id="corr-deadline",
            frame_type=cp_pb2.FRAME_TYPE_REQUEST,
            deadline_unix_ms=int(time.time() * 1000) - 1,
            request=cp_pb2.StrategyRequest(method="RunStrategy"),
        ),
        outbound,
    )

    frame = outbound.get_nowait()
    assert not called
    assert frame.correlation_id == "corr-deadline"
    assert frame.frame_type == cp_pb2.FRAME_TYPE_ERROR
    assert frame.error.code == "DeadlineExceeded"


def test_runtime_channel_client_replies_unimplemented_before_dispatch_is_wired():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
    )
    import queue

    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()
    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            correlation_id="corr-1",
            frame_type=cp_pb2.FRAME_TYPE_REQUEST,
            request=cp_pb2.StrategyRequest(method="RunStrategy"),
        ),
        outbound,
    )

    frame = outbound.get_nowait()
    assert frame.correlation_id == "corr-1"
    assert frame.frame_type == cp_pb2.FRAME_TYPE_ERROR
    assert frame.error.code == "Unimplemented"


def test_runtime_channel_client_handles_shutdown_command_without_worker():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
    )
    outbound: queue.Queue[cp_pb2.RuntimeFrame | None] = queue.Queue()

    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            frame_type=cp_pb2.FRAME_TYPE_COMMAND,
            command=cp_pb2.RuntimeCommandFrame(
                command_id="cmd-shutdown",
                command_type="shutdown_runtime",
                runtime_id="runtime-1",
            ),
        ),
        outbound,
    )

    ack = outbound.get_nowait()
    result = outbound.get_nowait()
    assert ack.frame_type == cp_pb2.FRAME_TYPE_COMMAND_ACK
    assert ack.command_ack.command_id == "cmd-shutdown"
    assert result.frame_type == cp_pb2.FRAME_TYPE_COMMAND_RESULT
    assert result.command_result.status == "succeeded"
    assert client._stop_event.is_set()


def test_runtime_channel_client_handles_shutdown_frame():
    private_key = Ed25519PrivateKey.generate()
    private_pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.PKCS8,
        encryption_algorithm=NoEncryption(),
    ).decode("utf-8")
    client = RuntimeChannelClient(
        "control-panel:50054",
        RuntimeCredential(
            key_id="key-1",
            private_key_pem=private_pem,
            private_key=private_key,
            path="/tmp/runtime.cred",
        ),
        RuntimeHelloArgs(key_id="key-1", private_key_pem=private_pem),
    )

    client._handle_inbound_frame(
        cp_pb2.RuntimeFrame(
            frame_type=cp_pb2.FRAME_TYPE_SHUTDOWN,
            shutdown=cp_pb2.RuntimeShutdown(reason="runtime cancelled"),
        ),
        queue.Queue(),
    )

    assert client._stop_event.is_set()


def test_runtime_channel_dispatcher_calls_existing_servicer_path():
    class FakeContextServicer:
        def RunStrategy(self, request, context):
            assert request.account_id == 7
            return strategy_pb2.RunStrategyResponse(session_id="sess-1")

    from google.protobuf.any_pb2 import Any
    from strategy_service.gen import strategy_service_pb2 as strategy_pb2

    packed = Any()
    packed.Pack(strategy_pb2.RunStrategyRequest(account_id=7, user_id=42))
    dispatcher = RuntimeChannelStrategyDispatcher(FakeContextServicer())

    frame = dispatcher(cp_pb2.RuntimeFrame(
        correlation_id="corr-1",
        frame_type=cp_pb2.FRAME_TYPE_REQUEST,
        request=cp_pb2.StrategyRequest(method="RunStrategy", request=packed),
    ))

    assert frame.frame_type == cp_pb2.FRAME_TYPE_RESPONSE
    response = strategy_pb2.RunStrategyResponse()
    assert frame.response.response.Unpack(response)
    assert response.session_id == "sess-1"


def test_runtime_channel_dispatcher_extracts_trace_context_for_servicer_call():
    try:
        from opentelemetry import propagate, trace
        from opentelemetry.propagate import get_global_textmap, set_global_textmap
        from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    except ImportError:
        return

    old_textmap = get_global_textmap()
    set_global_textmap(TraceContextTextMapPropagator())
    seen: list[str] = []

    class FakeContextServicer:
        def RunStrategy(self, request, context):
            span_context = trace.get_current_span().get_span_context()
            seen.append(f"{span_context.trace_id:032x}")
            return strategy_pb2.RunStrategyResponse(session_id="sess-1")

    from google.protobuf.any_pb2 import Any
    from strategy_service.gen import strategy_service_pb2 as strategy_pb2

    packed = Any()
    packed.Pack(strategy_pb2.RunStrategyRequest(account_id=7, user_id=42))
    dispatcher = RuntimeChannelStrategyDispatcher(FakeContextServicer())

    try:
        frame = dispatcher(cp_pb2.RuntimeFrame(
            correlation_id="corr-1",
            frame_type=cp_pb2.FRAME_TYPE_REQUEST,
            request=cp_pb2.StrategyRequest(
                method="RunStrategy",
                request=packed,
                trace_context={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
            ),
        ))
    finally:
        set_global_textmap(old_textmap)
        assert propagate is not None

    assert frame.frame_type == cp_pb2.FRAME_TYPE_RESPONSE
    assert seen == ["4bf92f3577b34da6a3ce929d0e0e4736"]


class _DisconnectingStub:
    def RuntimeChannel(self, _frames):
        raise grpc.RpcError("stream disconnected")


class _ReconnectOnceStub:
    def __init__(self) -> None:
        self.calls = 0
        self.hello_count = 0

    def RuntimeChannel(self, frames):
        self.calls += 1
        first = next(frames)
        if first.frame_type == cp_pb2.FRAME_TYPE_HELLO:
            self.hello_count += 1
        if self.calls == 1:
            raise grpc.RpcError("transient disconnect")
        return
        yield


class _AckThenDisconnectThenResumeStub:
    def __init__(self) -> None:
        self.calls = 0
        self.first_frame_types: list[int] = []
        self.resume_runtime_id = ""
        self.resume_token = ""

    def RuntimeChannel(self, frames):
        self.calls += 1
        first = next(frames)
        self.first_frame_types.append(first.frame_type)
        if first.frame_type == cp_pb2.FRAME_TYPE_RESUME:
            self.resume_runtime_id = first.resume.runtime_id
            self.resume_token = first.resume.resume_token
            return
            yield
        if self.calls == 1:
            expires = Timestamp()
            expires.FromSeconds(int(time.time()) + 3600)
            yield cp_pb2.RuntimeFrame(
                frame_type=cp_pb2.FRAME_TYPE_HELLO_ACK,
                hello_ack=cp_pb2.RuntimeHelloAck(
                    runtime_id="runtime-ack",
                    resume_token="resume-token-1",
                    resume_token_expires_at=expires,
                ),
            )
            raise grpc.RpcError("transient disconnect")
        return
        yield


class _TerminalRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.FAILED_PRECONDITION

    def details(self):
        return "runtime already ended"


class _TerminalDisconnectStub:
    def __init__(self) -> None:
        self.calls = 0

    def RuntimeChannel(self, _frames):
        self.calls += 1
        raise _TerminalRpcError()


class _CredentialTerminalRpcError(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.PERMISSION_DENIED

    def details(self):
        return "credential consumed; stop retrying with this credential"


class _CredentialTerminalDisconnectStub:
    def __init__(self) -> None:
        self.calls = 0

    def RuntimeChannel(self, _frames):
        self.calls += 1
        raise _CredentialTerminalRpcError()
