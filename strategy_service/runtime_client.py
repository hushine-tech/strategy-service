"""Phase D1 hosted strategy-runtime control-plane client.

Provides:
  * `register(...)` — on startup, calls `RegisterRuntime` with
    `source="hosted"`, caches `(runtime_id, registration_token)`.
  * `start_heartbeat(...)` — spawns a daemon thread that calls
    `HeartbeatRuntime` every `interval_seconds` until `stop()` is called.
  * `stop()` — graceful shutdown of the heartbeat loop and the gRPC channel.

Hosted-only: `RegisterRuntime` now accepts only `source="hosted"` and
requires `bind_user_id`. D3 self-hosted runtimes use the separate
RuntimeChannel client in `strategy_service.runtime_channel` with a signed
credential HELLO; there is no pairing-code client path.

Auth model (D1 token-only — see Phase D1 design.md Resolved Decisions):
  * `registration_token` returned by RegisterRuntime, presented via the
    `x-runtime-token` gRPC metadata key on every Heartbeat call.
  * `caller_token` mechanism (handler→runtime per-call attestation) is
    landing in D1b section 6 and is verified by `grpc_server.py`, not
    here. This module only handles runtime↔control-plane auth.
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

# gRPC metadata key the control panel checks on Heartbeat.
RUNTIME_TOKEN_METADATA_KEY = "x-runtime-token"


@dataclass
class RuntimeIdentity:
    """Cached state from a successful RegisterRuntime call."""

    runtime_id: str
    registration_token: str
    name: str
    user_id: int


class ControlPlaneClient:
    """Thin wrapper around the control-panel-service gRPC client.

    One instance per process. The constructor only opens a channel; call
    `register(...)` exactly once on startup, then `start_heartbeat(...)`,
    then `stop()` on shutdown.
    """

    def __init__(self, address: str) -> None:
        if not address:
            raise ValueError("control-panel-service address is empty")
        self._address = address
        # Insecure channel for D1 — TLS / mTLS deferred to D3+ per design.md.
        self._channel = grpc.insecure_channel(address)
        self._stub = cp_grpc.ControlPanelServiceStub(self._channel)
        self._identity: Optional[RuntimeIdentity] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    # ------------------------------------------------------------------
    # Register
    # ------------------------------------------------------------------

    def register(
        self,
        *,
        bind_user_id: int,
        name: str,
        endpoint_host: str,
        grpc_port: int,
        capabilities: list[str],
        resource_profile: str,
        version: str,
        runtime_id: str = "",
        debug_port: int = 0,
        timeout_seconds: float = 10.0,
    ) -> RuntimeIdentity:
        """Register this runtime as `source=hosted` and cache the token.

        Raises grpc.RpcError on failure; caller decides whether to retry,
        fail-fast, or keep running as a standalone direct-gRPC service.
        """
        if bind_user_id <= 0:
            raise ValueError(
                "bind_user_id is required for hosted-runtime registration "
                "(Phase D1 hosted-only; self-hosted lives in D3)"
            )
        if not endpoint_host:
            raise ValueError("endpoint_host is required for runtime registration")
        if grpc_port <= 0:
            raise ValueError("grpc_port must be > 0 for runtime registration")

        request = cp_pb2.RegisterRuntimeRequest(
            runtime_id=runtime_id,
            source="hosted",
            bind_user_id=bind_user_id,
            name=name,
            endpoint_host=endpoint_host,
            grpc_port=grpc_port,
            debug_port=debug_port,
            capabilities=capabilities,
            resource_profile=resource_profile,
            version=version,
        )
        response = self._stub.RegisterRuntime(request, timeout=timeout_seconds)
        rt = response.runtime
        identity = RuntimeIdentity(
            runtime_id=rt.runtime_id,
            registration_token=response.registration_token,
            name=rt.name,
            user_id=rt.user_id,
        )
        self._identity = identity
        logger.info(
            "registered with control-panel-service: runtime_id=%s user_id=%d name=%s",
            identity.runtime_id, identity.user_id, identity.name,
        )
        return identity

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def start_heartbeat(self, interval_seconds: int = 10) -> None:
        """Spawn the daemon heartbeat loop. Must be called after register().

        ``interval_seconds`` is clamped to a 1s floor: misconfiguring it to
        0 (or negative) would let `Event.wait(0)` return immediately and
        the loop would saturate the control panel.
        """
        if self._identity is None:
            raise RuntimeError("call register() before start_heartbeat()")
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return
        if interval_seconds < 1:
            logger.warning(
                "heartbeat interval=%d clamped to 1s minimum", interval_seconds
            )
            interval_seconds = 1
        self._stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            args=(interval_seconds,),
            name="runtime-heartbeat",
            daemon=True,
        )
        self._heartbeat_thread.start()
        logger.info("heartbeat loop started (interval=%ds)", interval_seconds)

    def _heartbeat_loop(self, interval_seconds: int) -> None:
        assert self._identity is not None
        identity = self._identity
        metadata = ((RUNTIME_TOKEN_METADATA_KEY, identity.registration_token),)
        # Tight loop — control-plane decides what status it wants to see.
        # Transient errors are logged and retried; on token mismatch /
        # not-found we surface but keep retrying (operator may have
        # cancelled and re-registered).
        while not self._stop_event.is_set():
            try:
                resp = self._stub.HeartbeatRuntime(
                    cp_pb2.HeartbeatRuntimeRequest(
                        runtime_id=identity.runtime_id,
                        status="active",
                    ),
                    metadata=metadata,
                    timeout=5.0,
                )
                if getattr(resp, "shutdown_requested", False):
                    logger.error(
                        "control-panel requested runtime shutdown: %s",
                        getattr(resp, "terminal_reason", "") or "terminal_runtime",
                    )
                    self._stop_event.set()
                    break
            except grpc.RpcError as e:
                code = e.code() if hasattr(e, "code") else "unknown"
                logger.warning("heartbeat failed: code=%s detail=%s", code, e.details() if hasattr(e, "details") else "")
            except Exception as e:  # noqa: BLE001
                logger.warning("heartbeat unexpected error: %s", e)
            # Use stop_event.wait so shutdown is immediate, not delayed by interval.
            self._stop_event.wait(interval_seconds)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    def stop(self, timeout_seconds: float = 5.0) -> None:
        """Stop the heartbeat loop and close the channel."""
        self._stop_event.set()
        thread = self._heartbeat_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout_seconds)
        try:
            self._channel.close()
        except Exception as e:  # noqa: BLE001
            logger.debug("channel close failed: %s", e)

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    @property
    def identity(self) -> Optional[RuntimeIdentity]:
        return self._identity

    @property
    def stub(self) -> cp_grpc.ControlPanelServiceStub:
        """The underlying control-panel-service gRPC stub. Exposed so the
        Phase D1 caller_token interceptor can reuse the same channel
        instead of opening a second connection.
        """
        return self._stub
