"""Tests for Phase D1 hosted strategy-runtime self-registration.

Covers `runtime_client.ControlPlaneClient` register + heartbeat behavior
with a stubbed gRPC stub, so we don't need a running control panel.
"""

from __future__ import annotations

import time
import threading
from typing import Optional
from unittest.mock import MagicMock

import grpc
import pytest

from strategy_service.gen import control_panel_service_pb2 as cp_pb2
from strategy_service.runtime_client import (
    RUNTIME_TOKEN_METADATA_KEY,
    ControlPlaneClient,
    RuntimeIdentity,
)


def _make_register_response(*, runtime_id="rt_xyz", user_id=42, name="hosted-steady-river", token="reg_tok_abc"):
    return cp_pb2.RegisterRuntimeResponse(
        runtime=cp_pb2.Runtime(
            runtime_id=runtime_id,
            user_id=user_id,
            name=name,
            source="hosted",
            endpoint_host="10.0.0.5",
            grpc_port=50053,
            status="paired",
        ),
        registration_token=token,
    )


def _make_client_with_stub(stub):
    """Build a ControlPlaneClient and replace its stub with a mock.

    Avoids the constructor's grpc.insecure_channel side-effect by passing
    a placeholder address, then immediately swapping the stub.
    """
    c = ControlPlaneClient("placeholder:50054")
    c._stub = stub
    return c


def test_register_caches_identity_and_returns_token():
    stub = MagicMock()
    stub.RegisterRuntime.return_value = _make_register_response(
        runtime_id="rt_42_default",
        user_id=42,
        name="hosted-steady-river",
        token="reg_tok_abc",
    )
    c = _make_client_with_stub(stub)

    identity = c.register(
        bind_user_id=42,
        name="hosted-steady-river",
        endpoint_host="10.0.0.5",
        grpc_port=50053,
        capabilities=["strategy", "spot", "futures"],
        resource_profile="small",
        version="0.1.0",
    )

    assert identity.runtime_id == "rt_42_default"
    assert identity.user_id == 42
    assert identity.name == "hosted-steady-river"
    assert identity.registration_token == "reg_tok_abc"
    assert c.identity is not None
    assert c.identity.runtime_id == "rt_42_default"

    # Verify request was constructed with source="hosted" and bind_user_id=42.
    args, kwargs = stub.RegisterRuntime.call_args
    req = args[0]
    assert req.source == "hosted"
    assert req.bind_user_id == 42
    assert req.name == "hosted-steady-river"
    assert req.endpoint_host == "10.0.0.5"
    assert req.grpc_port == 50053


def test_register_rejects_zero_user_id():
    stub = MagicMock()
    c = _make_client_with_stub(stub)

    with pytest.raises(ValueError, match="bind_user_id"):
        c.register(
            bind_user_id=0,
            name="hosted-steady-river",
            endpoint_host="10.0.0.5",
            grpc_port=50053,
            capabilities=[],
            resource_profile="small",
            version="0.1.0",
        )
    stub.RegisterRuntime.assert_not_called()


def test_register_rejects_empty_endpoint():
    stub = MagicMock()
    c = _make_client_with_stub(stub)
    with pytest.raises(ValueError, match="endpoint_host"):
        c.register(
            bind_user_id=42,
            name="hosted-steady-river",
            endpoint_host="",
            grpc_port=50053,
            capabilities=[],
            resource_profile="small",
            version="0.1.0",
        )


def test_heartbeat_loop_calls_with_registration_token_metadata():
    stub = MagicMock()
    stub.RegisterRuntime.return_value = _make_register_response(token="reg_tok_xyz")
    stub.HeartbeatRuntime.return_value = cp_pb2.HeartbeatRuntimeResponse()

    c = _make_client_with_stub(stub)
    c.register(
        bind_user_id=42,
        name="hosted-steady-river",
        endpoint_host="10.0.0.5",
        grpc_port=50053,
        capabilities=[],
        resource_profile="small",
        version="0.1.0",
    )

    # interval=1 is the production minimum (the client clamps anything
    # smaller). The first iteration fires the heartbeat before sleeping,
    # so we just need to wait long enough for that one call.
    c.start_heartbeat(interval_seconds=1)
    deadline = time.time() + 1.0
    while time.time() < deadline and stub.HeartbeatRuntime.call_count == 0:
        time.sleep(0.01)
    c.stop()

    assert stub.HeartbeatRuntime.call_count >= 1
    args, kwargs = stub.HeartbeatRuntime.call_args
    req = args[0]
    assert req.runtime_id == "rt_xyz"
    assert req.status == "active"
    metadata = kwargs.get("metadata", ())
    assert (RUNTIME_TOKEN_METADATA_KEY, "reg_tok_xyz") in metadata


def test_heartbeat_survives_transient_errors():
    stub = MagicMock()
    stub.RegisterRuntime.return_value = _make_register_response()

    # First two heartbeats raise, third succeeds.
    rpc_err = grpc.RpcError()
    rpc_err.code = lambda: grpc.StatusCode.UNAVAILABLE
    rpc_err.details = lambda: "transient"
    side_effects = [rpc_err, rpc_err, cp_pb2.HeartbeatRuntimeResponse()]
    stub.HeartbeatRuntime.side_effect = side_effects

    c = _make_client_with_stub(stub)
    c.register(
        bind_user_id=42, name="hosted-steady-river", endpoint_host="h", grpc_port=1,
        capabilities=[], resource_profile="small", version="0.1.0",
    )
    # 1s interval × 3 attempts → ~2.5s upper bound; allow 4s.
    c.start_heartbeat(interval_seconds=1)
    deadline = time.time() + 4.0
    while time.time() < deadline and stub.HeartbeatRuntime.call_count < 3:
        time.sleep(0.05)
    c.stop()

    assert stub.HeartbeatRuntime.call_count >= 3


def test_heartbeat_stops_when_control_panel_requests_shutdown():
    stub = MagicMock()
    stub.RegisterRuntime.return_value = _make_register_response()
    stub.HeartbeatRuntime.return_value = cp_pb2.HeartbeatRuntimeResponse(
        shutdown_requested=True,
        terminal_reason="user_cancelled",
    )

    c = _make_client_with_stub(stub)
    c.register(
        bind_user_id=42, name="hosted-steady-river", endpoint_host="h", grpc_port=1,
        capabilities=[], resource_profile="small", version="0.1.0",
    )
    c.start_heartbeat(interval_seconds=1)
    deadline = time.time() + 2.0
    while time.time() < deadline and not c._stop_event.is_set():
        time.sleep(0.01)
    c.stop()

    assert c._stop_event.is_set()
    assert stub.HeartbeatRuntime.call_count == 1


# Fix #6: misconfigured interval should be clamped, not let the loop spin.
def test_heartbeat_clamps_zero_interval(caplog):
    stub = MagicMock()
    stub.RegisterRuntime.return_value = _make_register_response()
    c = _make_client_with_stub(stub)
    c.register(
        bind_user_id=42, name="hosted-steady-river", endpoint_host="h", grpc_port=1,
        capabilities=[], resource_profile="small", version="0.1.0",
    )
    import logging
    with caplog.at_level(logging.WARNING, logger="strategy_service.runtime_client"):
        c.start_heartbeat(interval_seconds=0)
        c.stop()
    # Warning is logged, and the loop didn't crash.
    assert any("clamped to 1s" in r.message for r in caplog.records)


def test_stop_before_register_is_safe():
    stub = MagicMock()
    c = _make_client_with_stub(stub)
    # No register call. stop() should not raise.
    c.stop()


def test_start_heartbeat_without_register_raises():
    stub = MagicMock()
    c = _make_client_with_stub(stub)
    with pytest.raises(RuntimeError, match="register"):
        c.start_heartbeat(interval_seconds=10)
