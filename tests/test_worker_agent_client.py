import threading
import time

import pytest
from google.protobuf.any_pb2 import Any

from strategy_service.gen import strategy_service_pb2 as strategy_pb2
from strategy_service.gen import runtime_worker_pb2 as worker_pb2
from strategy_service.worker_agent_client import (
    FinalStatusRejected,
    WorkerAgentClient,
    WorkerEnv,
    build_worker_hello_frame,
    load_worker_env,
)


class _FinalAckStub:
    def __init__(self, *, error: str = ""):
        self.sent = []
        self.final_seen = threading.Event()
        self.allow_ack = threading.Event()
        self.error = error

    def Connect(self, frames):
        for frame in frames:
            self.sent.append(frame)
            if frame.WhichOneof("payload") != "final_status":
                continue
            self.final_seen.set()
            assert self.allow_ack.wait(timeout=1.0)
            if self.error:
                yield worker_pb2.AgentFrame(
                    reply_to=frame.frame_id,
                    error=worker_pb2.AgentError(
                        code="INDICATOR_FINALIZATION_FAILED",
                        message=self.error,
                    ),
                )
            else:
                yield worker_pb2.AgentFrame(reply_to=frame.frame_id)
            return


def test_send_final_status_waits_until_matching_reply_to():
    stub = _FinalAckStub()
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=stub,
        call_id_factory=lambda: "final-1",
    )
    client.start()
    for index in range(1440):
        client._outbound.put(worker_pb2.WorkerFrame(
            indicator_frame=worker_pb2.IndicatorFrame(
                session_id="sess-1",
                stream_key="binance:perpetual_futures:TESTUSDT:1m",
                market_time_ms=index * 60_000,
            ),
        ))
    done = threading.Event()
    failure = []

    def send():
        try:
            client.send_final_status(
                session_id="sess-1",
                status="finished",
                bars_processed=1440,
                timeout_seconds=1.0,
            )
        except BaseException as exc:  # noqa: BLE001
            failure.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=send)
    thread.start()
    assert stub.final_seen.wait(timeout=1.0)
    assert not done.is_set()
    assert sum(frame.WhichOneof("payload") == "indicator_frame" for frame in stub.sent) == 1440
    stub.allow_ack.set()
    assert done.wait(timeout=1.0)
    thread.join(timeout=1.0)
    client.close()
    assert failure == []


def test_send_final_status_raises_when_agent_returns_error():
    stub = _FinalAckStub(error="indicator finalization failed: database unavailable")
    stub.allow_ack.set()
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=stub,
        call_id_factory=lambda: "final-1",
    )
    client.start()
    with pytest.raises(FinalStatusRejected, match="database unavailable"):
        client.send_final_status(session_id="sess-1", status="finished", timeout_seconds=1.0)
    client.close()


def test_send_final_status_times_out_without_ack():
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=_FakeWorkerStub([]),
        call_id_factory=lambda: "final-1",
    )
    client.start()
    with pytest.raises(TimeoutError, match="final status ack"):
        client.send_final_status(session_id="sess-1", status="finished", timeout_seconds=0.01)
    client.close()


def test_load_worker_env_requires_agent_addr(monkeypatch):
    monkeypatch.delenv("HUSHINE_AGENT_ADDR", raising=False)
    monkeypatch.setenv("HUSHINE_WORKER_TOKEN", "token")
    monkeypatch.setenv("HUSHINE_SESSION_ID", "sess-1")

    with pytest.raises(RuntimeError, match="HUSHINE_AGENT_ADDR"):
        load_worker_env()


def test_load_worker_env_accepts_required_values(monkeypatch):
    monkeypatch.setenv("HUSHINE_AGENT_ADDR", "127.0.0.1:50000")
    monkeypatch.setenv("HUSHINE_WORKER_TOKEN", "token")
    monkeypatch.setenv("HUSHINE_SESSION_ID", "sess-1")
    monkeypatch.setenv("HUSHINE_DEBUGPY_PORT", "5678")

    env = load_worker_env()

    assert env == WorkerEnv(
        agent_addr="127.0.0.1:50000",
        token="token",
        session_id="sess-1",
        debugpy_port=5678,
    )


def test_build_worker_hello_frame_contains_env_identity():
    frame = build_worker_hello_frame(
        WorkerEnv(agent_addr="127.0.0.1:50000", token="token", session_id="sess-1"),
        pid=123,
    )

    assert frame.WhichOneof("payload") == "hello"
    assert frame.hello == worker_pb2.WorkerHello(
        session_id="sess-1",
        token="token",
        worker_version="0.1.0",
        pid=123,
    )


def test_worker_agent_client_sends_hello_and_receives_start_session():
    start = worker_pb2.AgentFrame(
        start_session=worker_pb2.StartSession(session_id="sess-1", user_id=6, runtime_id="rt-1")
    )
    stub = _FakeWorkerStub([start])
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=stub,
    )

    client.start()
    received = client.wait_for_start_session(timeout_seconds=1.0)
    client.close()

    assert received.session_id == "sess-1"
    assert stub.sent[0].WhichOneof("payload") == "hello"
    assert stub.sent[0].hello.token == "token"


def test_worker_agent_client_invokes_platform_call():
    packed_response = Any()
    packed_response.Pack(strategy_pb2.GetStrategyStatusResponse(status="running"))
    stub = _FakeWorkerStub([
        worker_pb2.AgentFrame(
            platform_call_result=worker_pb2.PlatformCallResult(
                call_id="call-1",
                ok=True,
                response=packed_response,
            )
        )
    ])
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=stub,
        call_id_factory=lambda: "call-1",
    )
    request = strategy_pb2.GetStrategyStatusRequest(session_id="sess-1")

    client.start()
    response = client.invoke_platform_unary(
        "GetStrategyStatus",
        request,
        strategy_pb2.GetStrategyStatusResponse,
        timeout_seconds=1.0,
    )
    client.close()

    assert response.status == "running"
    platform_calls = [frame.platform_call for frame in stub.sent if frame.WhichOneof("payload") == "platform_call"]
    assert platform_calls[0].call_id == "call-1"
    assert platform_calls[0].method == "GetStrategyStatus"


def test_worker_agent_client_waits_for_agent_return_grace_after_platform_timeout_window():
    packed_response = Any()
    packed_response.Pack(strategy_pb2.GetStrategyStatusResponse(status="running"))
    stub = _DelayedPlatformResultStub(
        worker_pb2.PlatformCallResult(
            call_id="call-1",
            ok=True,
            response=packed_response,
        ),
        delay_seconds=0.2,
    )
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=stub,
        call_id_factory=lambda: "call-1",
    )

    client.start()
    response = client.invoke_platform_unary(
        "GetStrategyStatus",
        strategy_pb2.GetStrategyStatusRequest(session_id="sess-1"),
        strategy_pb2.GetStrategyStatusResponse,
        timeout_seconds=0.1,
    )
    client.close()

    assert response.status == "running"
    platform_calls = [frame.platform_call for frame in stub.sent if frame.WhichOneof("payload") == "platform_call"]
    assert platform_calls[0].timeout_ms == 100


def test_worker_agent_client_dispatches_agent_platform_call():
    packed_request = Any()
    packed_request.Pack(strategy_pb2.GetStrategyStatusRequest(session_id="sess-1"))
    packed_response = Any()
    packed_response.Pack(strategy_pb2.GetStrategyStatusResponse(status="running", bars_processed=9))
    stub = _FakeWorkerStub([
        worker_pb2.AgentFrame(
            platform_call=worker_pb2.PlatformCall(
                call_id="agent-call-1",
                method="GetStrategyStatus",
                request=packed_request,
                timeout_ms=1000,
            )
        )
    ])
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=stub,
    )

    def handle_call(call):
        assert call.method == "GetStrategyStatus"
        return packed_response

    client.set_agent_platform_call_handler(handle_call)
    client.start()
    deadline = time.time() + 1.0
    while time.time() < deadline:
        if any(frame.WhichOneof("payload") == "platform_call_result" for frame in stub.sent):
            break
        time.sleep(0.01)
    client.close()

    results = [frame.platform_call_result for frame in stub.sent if frame.WhichOneof("payload") == "platform_call_result"]
    assert len(results) == 1
    assert results[0].call_id == "agent-call-1"
    assert results[0].ok is True


def test_worker_agent_client_sends_indicator_frame_with_definitions():
    client = WorkerAgentClient(
        WorkerEnv(agent_addr="127.0.0.1:1", token="token", session_id="sess-1"),
        stub=_FakeWorkerStub([]),
    )
    definition = type("Definition", (), {
        "key": "bb_mid",
        "name": "BB Mid",
        "type": "line",
        "pane": "price",
        "color": "#22c55e",
        "unit": "USDT",
        "description": "middle band",
        "config": {"width": 2},
    })()
    frame = type("Frame", (), {
        "values": {"bb_mid": 100.5},
        "markers": {},
    })()

    client.send_indicator_frame(
        session_id="sess-1",
        user_id=6,
        strategy_id=12,
        stream_key="futures:ZECUSDT:1m",
        market_time_ms=123000,
        interval_ms=60000,
        definitions=[definition],
        frame=frame,
    )

    sent = client._outbound.get_nowait()
    indicator = sent.indicator_frame
    assert indicator.session_id == "sess-1"
    assert indicator.user_id == 6
    assert indicator.strategy_id == 12
    assert indicator.definitions[0].indicator_key == "bb_mid"
    assert indicator.values[0].indicator_key == "bb_mid"
    assert indicator.values[0].value == 100.5


class _FakeWorkerStub:
    def __init__(self, responses):
        self.responses = list(responses)
        self.sent = []

    def Connect(self, frames):
        self.sent.append(next(frames))
        for response in self.responses:
            if response.WhichOneof("payload") == "platform_call_result":
                while True:
                    frame = next(frames)
                    self.sent.append(frame)
                    if frame.WhichOneof("payload") == "platform_call":
                        break
            yield response
            if response.WhichOneof("payload") == "platform_call":
                frame = next(frames)
                self.sent.append(frame)


class _DelayedPlatformResultStub:
    def __init__(self, result, *, delay_seconds):
        self.result = result
        self.delay_seconds = delay_seconds
        self.sent = []

    def Connect(self, frames):
        self.sent.append(next(frames))
        while True:
            frame = next(frames)
            self.sent.append(frame)
            if frame.WhichOneof("payload") == "platform_call":
                break
        time.sleep(self.delay_seconds)
        yield worker_pb2.AgentFrame(platform_call_result=self.result)
