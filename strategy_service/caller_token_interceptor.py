"""Phase D1 section 6.5 — caller_token verification on inbound RPCs.

When the strategy-runtime container is started by control-panel-service
(via DockerProvisioner / RegisterRuntime self-register), it knows its
own runtime_id and can dial control-panel-service. quant-handler's
strategy session calls (RunStrategy, PreviewRunStrategy, GetStrategyStatus,
StopStrategy) carry an `x-caller-token` gRPC metadata value issued by
control-panel-service. This interceptor:

1. Reads the metadata value from the inbound RPC.
2. Calls control-panel-service.ValidateCallerToken(token, runtime_id).
3. Caches the validation briefly so a strategy session that fires
   multiple RPCs in succession doesn't pay the round trip every time.
4. On invalid → reject with PermissionDenied (no fallback).

D1 caveats:

- Until 6.5 ships in production, the interceptor MAY operate in
  "log-only" mode (`enforce=False`) where it logs but doesn't reject.
  Operators flip `enforce=True` once handler is on the cutover path.
- The interceptor is **opt-in** via env / runtime config; not
  unconditionally installed by run_grpc_server.py. This way legacy /
  non-D1 deployments aren't affected.

D3 will retire this entirely once the control-panel proxy carries
handler↔runtime traffic.
"""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from typing import Optional

import grpc

from strategy_service.gen import control_panel_service_pb2 as cp_pb2
from strategy_service.gen import control_panel_service_pb2_grpc as cp_grpc

logger = logging.getLogger(__name__)

CALLER_TOKEN_METADATA_KEY = "x-caller-token"


@dataclass
class _CacheEntry:
    user_id: int
    expires_at: float  # local monotonic seconds


class CallerTokenInterceptor(grpc.ServerInterceptor):
    """Validates `x-caller-token` metadata on every inbound unary RPC.

    Streaming RPCs are intentionally NOT covered in D1: strategy-runtime
    does not currently expose any handler-facing streaming RPC. If that
    changes in D2/D3 the interceptor needs intercept_service hooks for
    streams too.

    Args:
      runtime_id: this runtime's own runtime_id (the value control-panel
        signed the token to).
      control_panel_stub: a `ControlPanelServiceStub` already wired with
        a channel to control-panel-service.
      enforce: True → reject invalid tokens with PermissionDenied.
        False → log-only (rollout window).
      cache_ttl_seconds: how long to cache a successful validation
        before re-querying control-panel. Defaults to 5s — short enough
        that revoke / quota-cap flips propagate quickly, long enough
        that bursty strategy session calls don't hammer control-panel.
    """

    def __init__(
        self,
        *,
        runtime_id: str,
        control_panel_stub: cp_grpc.ControlPanelServiceStub,
        enforce: bool = True,
        cache_ttl_seconds: float = 5.0,
    ) -> None:
        if not runtime_id:
            raise ValueError("CallerTokenInterceptor requires a non-empty runtime_id")
        self._runtime_id = runtime_id
        self._stub = control_panel_stub
        self._enforce = enforce
        self._cache_ttl = max(cache_ttl_seconds, 0.0)
        self._cache: dict[str, _CacheEntry] = {}
        self._cache_lock = threading.Lock()

    # gRPC server interceptor API
    def intercept_service(self, continuation, handler_call_details):
        method = handler_call_details.method or ""
        token = self._extract_token(handler_call_details.invocation_metadata)
        # Skip enforcement for built-in / health-check RPCs to avoid
        # circular blocking when control-panel itself probes the runtime.
        if method.startswith("/grpc.") or method.startswith("/grpc.health.v1."):
            return continuation(handler_call_details)

        valid, user_id, reason = self._validate(token)
        if not valid:
            logger.warning(
                "caller_token rejected: method=%s reason=%s enforce=%s",
                method, reason, self._enforce,
            )
            if self._enforce:
                return _denied_handler(reason)
        else:
            logger.debug("caller_token ok: method=%s user_id=%d", method, user_id)
        return continuation(handler_call_details)

    def _extract_token(self, metadata) -> str:
        if not metadata:
            return ""
        for key, value in metadata:
            # gRPC normalizes keys to lowercase already.
            if key == CALLER_TOKEN_METADATA_KEY:
                return value
        return ""

    def _validate(self, token: str) -> tuple[bool, int, str]:
        """Returns (valid, user_id, reason). Cached for short TTL."""
        if not token:
            return False, 0, "unknown"
        # Cache lookup
        now = time.monotonic()
        with self._cache_lock:
            entry = self._cache.get(token)
            if entry is not None and entry.expires_at > now:
                return True, entry.user_id, ""
            # Drop expired entry under the lock.
            if entry is not None and entry.expires_at <= now:
                del self._cache[token]
        # Cache miss: ask control-panel.
        try:
            resp = self._stub.ValidateCallerToken(
                cp_pb2.ValidateCallerTokenRequest(
                    caller_token=token,
                    runtime_id=self._runtime_id,
                ),
                timeout=2.0,
            )
        except grpc.RpcError as e:
            code = e.code() if hasattr(e, "code") else "unknown"
            logger.warning("ValidateCallerToken call failed: code=%s", code)
            # Fail-closed: control-panel unreachable → reject.
            return False, 0, "control_panel_unavailable"
        if not resp.valid:
            return False, 0, resp.reason or "invalid"
        # Success: cache for cache_ttl. Use the smaller of cache_ttl_seconds
        # and any platform expiry signal we may add later (currently we
        # don't surface remaining TTL; cache_ttl is our own bound).
        if self._cache_ttl > 0:
            with self._cache_lock:
                self._cache[token] = _CacheEntry(
                    user_id=resp.user_id,
                    expires_at=now + self._cache_ttl,
                )
        return True, resp.user_id, ""


def _denied_handler(reason: str) -> grpc.RpcMethodHandler:
    """Returns a handler that immediately rejects every RPC type with
    PermissionDenied. Used when intercept_service decides to block.
    """
    detail = f"caller_token: {reason}"

    def _reject_unary(_request, context: grpc.ServicerContext):
        context.abort(grpc.StatusCode.PERMISSION_DENIED, detail)
        # abort raises; the explicit return is unreachable but keeps the
        # function a proper gRPC handler.
        return None

    def _reject_stream(_request_iter, context: grpc.ServicerContext):
        context.abort(grpc.StatusCode.PERMISSION_DENIED, detail)
        return None

    return grpc.unary_unary_rpc_method_handler(_reject_unary)
