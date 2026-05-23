"""Phase D1 section 6.5 tests for CallerTokenInterceptor.

Strategy: stub the control-panel ControlPanelServiceStub with a
configurable fake; build a fake `handler_call_details` with metadata;
verify the interceptor's accept/reject + cache behavior.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
import time
from unittest.mock import MagicMock

import grpc
import pytest

from strategy_service.caller_token_interceptor import (
    CALLER_TOKEN_METADATA_KEY,
    CallerTokenInterceptor,
)
from strategy_service.gen import control_panel_service_pb2 as cp_pb2


@dataclass
class FakeHandlerCallDetails:
    method: str
    invocation_metadata: tuple = ()


def make_continuation(intercepted: list):
    """Returns a continuation function that records the handler_call_details
    it was invoked with, and returns a sentinel handler. Tests assert
    on len(intercepted) to verify the inner handler was reached."""
    def _continuation(details):
        intercepted.append(details)
        return "PASSTHROUGH_HANDLER"
    return _continuation


def _md(token: str) -> tuple:
    return ((CALLER_TOKEN_METADATA_KEY, token),)


def test_valid_token_passes_through():
    stub = MagicMock()
    stub.ValidateCallerToken.return_value = cp_pb2.ValidateCallerTokenResponse(
        valid=True, user_id=42, reason="",
    )
    interceptor = CallerTokenInterceptor(runtime_id="rt_x", control_panel_stub=stub)
    intercepted = []
    cont = make_continuation(intercepted)

    handler = interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/RunStrategy",
                                invocation_metadata=_md("good_token")),
    )

    assert handler == "PASSTHROUGH_HANDLER"
    assert len(intercepted) == 1
    stub.ValidateCallerToken.assert_called_once()


def test_missing_token_is_rejected():
    stub = MagicMock()
    interceptor = CallerTokenInterceptor(runtime_id="rt_x", control_panel_stub=stub)
    intercepted = []
    cont = make_continuation(intercepted)

    handler = interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/RunStrategy",
                                invocation_metadata=()),
    )

    # Should NOT have reached the inner handler.
    assert intercepted == []
    # Calling the rejecting handler aborts with PermissionDenied.
    ctx = MagicMock(spec=grpc.ServicerContext)
    handler.unary_unary(None, ctx)
    ctx.abort.assert_called_once()
    args, _ = ctx.abort.call_args
    assert args[0] == grpc.StatusCode.PERMISSION_DENIED


def test_unknown_token_is_rejected():
    stub = MagicMock()
    stub.ValidateCallerToken.return_value = cp_pb2.ValidateCallerTokenResponse(
        valid=False, user_id=0, reason="unknown",
    )
    interceptor = CallerTokenInterceptor(runtime_id="rt_x", control_panel_stub=stub)
    intercepted = []
    cont = make_continuation(intercepted)

    handler = interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/RunStrategy",
                                invocation_metadata=_md("bogus")),
    )
    assert intercepted == []
    ctx = MagicMock(spec=grpc.ServicerContext)
    handler.unary_unary(None, ctx)
    ctx.abort.assert_called_once()


def test_runtime_mismatch_is_rejected():
    stub = MagicMock()
    stub.ValidateCallerToken.return_value = cp_pb2.ValidateCallerTokenResponse(
        valid=False, user_id=0, reason="runtime_mismatch",
    )
    interceptor = CallerTokenInterceptor(runtime_id="rt_x", control_panel_stub=stub)
    intercepted = []
    cont = make_continuation(intercepted)
    interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/StopStrategy",
                                invocation_metadata=_md("token_for_other_runtime")),
    )
    assert intercepted == []


def test_log_only_mode_does_not_block():
    """enforce=False: log a warning but pass through to inner handler."""
    stub = MagicMock()
    stub.ValidateCallerToken.return_value = cp_pb2.ValidateCallerTokenResponse(
        valid=False, user_id=0, reason="unknown",
    )
    interceptor = CallerTokenInterceptor(
        runtime_id="rt_x", control_panel_stub=stub, enforce=False,
    )
    intercepted = []
    cont = make_continuation(intercepted)
    handler = interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/RunStrategy",
                                invocation_metadata=_md("bogus")),
    )
    # Inner handler reached even though token is bad — log-only mode.
    assert handler == "PASSTHROUGH_HANDLER"
    assert len(intercepted) == 1


def test_control_panel_unavailable_fails_closed():
    """When ValidateCallerToken raises (control-panel down), enforce
    mode rejects with PermissionDenied. Don't fall through silently."""
    stub = MagicMock()
    stub.ValidateCallerToken.side_effect = grpc.RpcError()
    interceptor = CallerTokenInterceptor(runtime_id="rt_x", control_panel_stub=stub)
    intercepted = []
    cont = make_continuation(intercepted)
    handler = interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/RunStrategy",
                                invocation_metadata=_md("any")),
    )
    assert intercepted == []
    ctx = MagicMock(spec=grpc.ServicerContext)
    handler.unary_unary(None, ctx)
    ctx.abort.assert_called_once_with(
        grpc.StatusCode.PERMISSION_DENIED,
        "caller_token: control_panel_unavailable",
    )


def test_health_check_method_skipped():
    """Health-check RPCs (grpc.health.v1.*) bypass token validation
    entirely; otherwise control-panel probing the runtime would fail
    its own health check."""
    stub = MagicMock()
    interceptor = CallerTokenInterceptor(runtime_id="rt_x", control_panel_stub=stub)
    intercepted = []
    cont = make_continuation(intercepted)
    handler = interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/grpc.health.v1.Health/Check",
                                invocation_metadata=()),
    )
    assert handler == "PASSTHROUGH_HANDLER"
    assert len(intercepted) == 1
    stub.ValidateCallerToken.assert_not_called()


def test_cache_avoids_redundant_validate_calls():
    """Two RPCs with the same token within cache_ttl → only one
    ValidateCallerToken call to control-panel."""
    stub = MagicMock()
    stub.ValidateCallerToken.return_value = cp_pb2.ValidateCallerTokenResponse(
        valid=True, user_id=42, reason="",
    )
    interceptor = CallerTokenInterceptor(
        runtime_id="rt_x", control_panel_stub=stub, cache_ttl_seconds=10.0,
    )
    intercepted = []
    cont = make_continuation(intercepted)
    interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/RunStrategy",
                                invocation_metadata=_md("hot_token")),
    )
    interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/GetStrategyStatus",
                                invocation_metadata=_md("hot_token")),
    )
    assert stub.ValidateCallerToken.call_count == 1
    assert len(intercepted) == 2


def test_cache_expires():
    """After cache_ttl elapses, the next call re-validates."""
    stub = MagicMock()
    stub.ValidateCallerToken.return_value = cp_pb2.ValidateCallerTokenResponse(
        valid=True, user_id=42, reason="",
    )
    interceptor = CallerTokenInterceptor(
        runtime_id="rt_x", control_panel_stub=stub, cache_ttl_seconds=0.05,
    )
    intercepted = []
    cont = make_continuation(intercepted)
    interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/RunStrategy",
                                invocation_metadata=_md("token")),
    )
    time.sleep(0.1)  # past cache TTL
    interceptor.intercept_service(
        cont,
        FakeHandlerCallDetails(method="/strategy.v1.StrategyService/RunStrategy",
                                invocation_metadata=_md("token")),
    )
    assert stub.ValidateCallerToken.call_count == 2


def test_constructor_rejects_empty_runtime_id():
    stub = MagicMock()
    with pytest.raises(ValueError, match="runtime_id"):
        CallerTokenInterceptor(runtime_id="", control_panel_stub=stub)
