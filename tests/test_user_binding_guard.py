"""Phase D1 section 6.5 cross-check tests for `_enforce_user_binding`.

Verify that when a strategy-runtime is bound to a user_id, RPC requests
carrying a different user_id get rejected with PermissionDenied. When
bound_user_id=0 (legacy / unregistered mode) the check is skipped.

The full StrategyServiceServicer __init__ requires a live account-service
(via _restore_running_sessions); these tests patch that out so we can
construct a servicer in isolation.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import grpc

from strategy_service.grpc_server import StrategyServiceServicer


def _build_servicer(bound_user_id: int) -> StrategyServiceServicer:
    """Construct a StrategyServiceServicer with `_restore_running_sessions`
    patched to a no-op so tests don't need a live account-service."""
    with patch.object(StrategyServiceServicer, "_restore_running_sessions", lambda self: None):
        return StrategyServiceServicer(
            account_service_addr="acct:1",
            order_service_addr="order:1",
            timescale_config={},
            kafka_brokers="kafka:9092",
            bound_user_id=bound_user_id,
        )


def _ctx() -> MagicMock:
    """A fake grpc.ServicerContext that records set_code / set_details."""
    ctx = MagicMock(spec=grpc.ServicerContext)
    return ctx


def test_bound_runtime_accepts_matching_user_id():
    servicer = _build_servicer(bound_user_id=42)
    ctx = _ctx()
    ok = servicer._enforce_user_binding(42, ctx)
    assert ok is True
    ctx.set_code.assert_not_called()
    ctx.set_details.assert_not_called()


def test_bound_runtime_rejects_mismatching_user_id():
    servicer = _build_servicer(bound_user_id=42)
    ctx = _ctx()
    ok = servicer._enforce_user_binding(99, ctx)
    assert ok is False
    ctx.set_code.assert_called_once_with(grpc.StatusCode.PERMISSION_DENIED)
    args, _ = ctx.set_details.call_args
    detail = args[0]
    assert "user_id mismatch" in detail
    assert "42" in detail
    assert "99" in detail


def test_legacy_mode_skips_check():
    """bound_user_id=0 means the runtime is in legacy mode (no
    control-panel registration). The check is bypassed so any positive
    user_id passes — the legacy direct-dial trust model still applies."""
    servicer = _build_servicer(bound_user_id=0)
    ctx = _ctx()
    assert servicer._enforce_user_binding(42, ctx) is True
    assert servicer._enforce_user_binding(99, ctx) is True
    ctx.set_code.assert_not_called()


def test_bound_user_id_zero_when_constructor_omits_kwarg():
    """Backwards compatibility: existing callers don't pass bound_user_id."""
    with patch.object(StrategyServiceServicer, "_restore_running_sessions", lambda self: None):
        s = StrategyServiceServicer(
            account_service_addr="acct:1",
            order_service_addr="order:1",
            timescale_config={},
            kafka_brokers="kafka:9092",
        )
    assert s._bound_user_id == 0
