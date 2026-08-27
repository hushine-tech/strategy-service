from __future__ import annotations

import logging
import os
import threading
import time

import grpc
from google.protobuf.any_pb2 import Any as ProtoAny

from strategy_service.gen import strategy_service_pb2 as strategy_pb2
from strategy_service.grpc_server import PLATFORM_ACCESS_PROXY_ONLY, StrategyServiceServicer
from strategy_service.platform_proxy import RuntimeChannelPlatformProxy
from strategy_service.worker_agent_client import (
    FinalStatusRejected,
    WorkerAgentClient,
    WorkerAgentDataSource,
    WorkerPlatformCallError,
    WorkerRuntimeChannelAdapter,
    load_worker_env,
)

logger = logging.getLogger("hushine-session-worker")

_TERMINAL_STATUSES = {"finished", "failed", "stopped", "stop_failed", "recoverable"}


def _validated_start_bootstrap(start):
    packed = getattr(start, "session_bootstrap", None)
    has_bootstrap = bool(packed is not None and str(getattr(packed, "type_url", "") or ""))
    if not has_bootstrap:
        raise RuntimeError("strategy Session bootstrap is required for this protocol start")
    bootstrap = strategy_pb2.StrategySessionBootstrap()
    if not packed.Unpack(bootstrap):
        raise RuntimeError("StartSession.session_bootstrap is not a StrategySessionBootstrap")
    if str(bootstrap.session_id or "").strip() != str(getattr(start, "session_id", "") or "").strip():
        raise RuntimeError("StrategySessionBootstrap session_id mismatch")
    if not str(bootstrap.launch_operation_id or "").strip():
        raise RuntimeError("StrategySessionBootstrap launch_operation_id is required")
    digest = str(bootstrap.strategy_source_sha256 or "")
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise RuntimeError("StrategySessionBootstrap strategy_source_sha256 is invalid")
    return bootstrap


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    env = load_worker_env()
    _start_debugpy_if_requested(env.debugpy_port)
    client = WorkerAgentClient(env)
    client.start()
    try:
        first = client.next_agent_frame(timeout_seconds=60.0)
        if first is None:
            raise TimeoutError("timed out waiting for StartSession")
        if first.WhichOneof("payload") == "platform_call":
            _handle_agent_platform_call(client, None, first.platform_call)
            return 0
        if first.WhichOneof("payload") != "start_session":
            raise RuntimeError(f"unexpected initial agent frame: {first.WhichOneof('payload')}")
        start = first.start_session
        request = strategy_pb2.RunStrategyRequest()
        if not start.run_strategy_request.Unpack(request):
            raise RuntimeError("StartSession.run_strategy_request is not a RunStrategyRequest")
        runtime_id = start.runtime_id or request.runtime_id
        request.runtime_id = runtime_id
        if start.user_id and not request.user_id:
            request.user_id = start.user_id

        session_bootstrap = _validated_start_bootstrap(start)

        servicer = _build_servicer(
            client,
            bound_user_id=int(start.user_id or request.user_id or 0),
            runtime_id=runtime_id,
            start_session_id=start.session_id,
            session_bootstrap=session_bootstrap,
        )
        client.set_agent_platform_call_handler(lambda call: _invoke_servicer_platform_call(servicer, call))
        context = _WorkerContext(start_session_id=start.session_id)
        response = servicer.RunStrategy(request, context)
        if context.code is not grpc.StatusCode.OK:
            _report_start_rejection(client, start.session_id, context)
            logger.error(
                "SESSION_START_REJECTED session=%s code=%s",
                start.session_id,
                context.code.name,
            )
            return 1
        session_id = response.session_id
        if not session_id:
            client.send_progress(session_id=start.session_id, status="failed", error="RunStrategy returned empty session_id")
            return 1
        if session_id != start.session_id:
            client.send_progress(
                session_id=start.session_id,
                status="failed",
                error="RunStrategy returned mismatched canonical session_id",
            )
            return 1
        if not _publish_running_session(servicer, client, context, session_id):
            state = servicer._sessions.get(session_id)
            error = "strategy session terminated before running publication"
            bars = 0
            if state is not None:
                error = state.error or error
                bars = state.bars_processed
            client.send_progress(
                session_id=session_id,
                status="failed",
                bars_processed=bars,
                error=error,
            )
            return 1
        return _poll_until_terminal(servicer, client, session_id, int(request.user_id or start.user_id or 0), runtime_id)
    except WorkerPlatformCallError as exc:
        _report_worker_failure(
            client,
            env.session_id,
            dependency_error=exc.dependency_error,
        )
        return 1
    except Exception:  # noqa: BLE001
        _report_worker_failure(client, env.session_id)
        return 1
    finally:
        client.close()


def _report_start_rejection(
    client: WorkerAgentClient,
    session_id: str,
    context: "_WorkerContext",
) -> None:
    detail = context.details or context.code.name
    progress = dict(
        session_id=str(session_id or ""),
        status="failed",
        error=detail,
    )
    if context.runtime_dependency_error is not None:
        progress["dependency_error"] = context.runtime_dependency_error
    client.send_progress(**progress)


def _report_worker_failure(
    client: WorkerAgentClient,
    session_id: str,
    *,
    dependency_error=None,
) -> None:
    safe_session_id = str(session_id or "")
    logger.error("SESSION_WORKER_FATAL session=%s", safe_session_id)
    safe_error = (
        "strategy runtime dependency validation failed"
        if dependency_error is not None
        else "session worker terminated"
    )
    try:
        progress = dict(
            session_id=safe_session_id,
            status="failed",
            error=safe_error,
        )
        if dependency_error is not None:
            progress["dependency_error"] = dependency_error
        client.send_progress(**progress)
    except BaseException:
        return


def _poll_until_terminal(
    servicer: StrategyServiceServicer,
    client: WorkerAgentClient,
    session_id: str,
    user_id: int,
    runtime_id: str,
) -> int:
    while True:
        context = _WorkerContext()
        status = servicer.GetStrategyStatus(
            strategy_pb2.GetStrategyStatusRequest(
                session_id=session_id,
                user_id=user_id,
                runtime_id=runtime_id,
            ),
            context,
        )
        if context.code is not grpc.StatusCode.OK:
            detail = context.details or context.code.name
            client.send_progress(session_id=session_id, status="failed", error=detail)
            return 1
        if status.status in _TERMINAL_STATUSES:
            try:
                final_kwargs = dict(
                    session_id=session_id,
                    status=status.status,
                    bars_processed=status.bars_processed,
                    error=status.error,
                    timeout_seconds=35.0,
                )
                sessions = getattr(servicer, "_sessions", None)
                state = sessions.get(session_id) if sessions is not None else None
                reconciliation_run_id = str(
                    getattr(state, "reconciliation_run_id", "") or ""
                ).strip()
                if reconciliation_run_id:
                    final_kwargs["reconciliation_run_id"] = reconciliation_run_id
                client.send_final_status(**final_kwargs)
            except (FinalStatusRejected, TimeoutError) as exc:
                logger.error("final session status was not acknowledged: %s", exc)
                return 1
            return 0 if status.status in {"finished", "stopped"} else 1
        client.send_progress(
            session_id=session_id,
            status=status.status,
            bars_processed=status.bars_processed,
            error=status.error,
        )
        time.sleep(1.0)


def _publish_running_session(
    servicer: StrategyServiceServicer,
    client: WorkerAgentClient,
    context: "_WorkerContext",
    session_id: str,
) -> bool:
    binding = context.take_running_publication()
    if binding is None:
        raise RuntimeError("RunStrategy did not bind running publication")
    bound_session_id, state = binding
    if bound_session_id != session_id or servicer._sessions.get(session_id) is not state:
        raise RuntimeError("RunStrategy bound mismatched running publication")
    startup = state.startup_result()
    if startup is None or not hasattr(startup, "release") or not hasattr(startup, "abort"):
        raise RuntimeError("RunStrategy did not bind session startup result")
    if not servicer._sessions.claim_running_publication(session_id, state):
        servicer._fail_running_publication(
            session_id,
            state,
            "strategy session terminated before running publication",
        )
        return False
    try:
        client.send_progress(session_id=session_id, status="running")
    except BaseException:
        servicer._fail_running_publication(
            session_id,
            state,
            "running publication submission failed",
        )
        raise
    if not state.complete_running_publication_submission(startup.release):
        servicer._fail_running_publication(
            session_id,
            state,
            "strategy session terminated during running publication",
        )
        return False
    return True


def _start_debugpy_if_requested(port: int) -> None:
    if int(port or 0) <= 0:
        return
    import debugpy  # type: ignore

    host = os.environ.get("HUSHINE_DEBUGPY_HOST", "127.0.0.1")
    wait_requested = os.environ.get("DEBUG_WAIT", "0").strip().lower() not in {
        "",
        "0",
        "false",
        "no",
        "off",
    }
    debugpy.configure(subProcess=False)
    try:
        debugpy.listen((host, int(port)))
    except RuntimeError:
        if wait_requested:
            raise
        logger.warning("debugger unavailable on %s:%s; continuing without attach", host, port)
        return
    if wait_requested:
        logger.info("waiting for debugger attach on %s:%s", host, port)
        debugpy.wait_for_client()


def _build_servicer(
    client: WorkerAgentClient,
    *,
    bound_user_id: int,
    runtime_id: str,
    start_session_id: str = "",
    session_bootstrap=None,
) -> StrategyServiceServicer:
    platform_proxy = RuntimeChannelPlatformProxy(WorkerRuntimeChannelAdapter(client))
    servicer = StrategyServiceServicer(
        portfolio_service_addr="",
        order_service_addr="",
        timescale_config={},
        kafka_brokers="",
        bound_user_id=int(bound_user_id or 0),
        runtime_id=runtime_id,
        runtime_source=os.environ.get("HUSHINE_RUNTIME_SOURCE", "bare"),
        runtime_name=os.environ.get("HUSHINE_RUNTIME_NAME", ""),
        platform_access_mode=PLATFORM_ACCESS_PROXY_ONLY,
        restore_running_sessions=False,
        platform_proxy=platform_proxy,
        notification_client=platform_proxy.notification_client(),
        start_session_id=start_session_id,
        session_bootstrap=session_bootstrap,
    )
    data_source = WorkerAgentDataSource(client)
    servicer.set_runtime_data_source(data_source)
    servicer.set_indicator_frame_sink(client.send_indicator_frame)
    return servicer


def _handle_agent_platform_call(
    client: WorkerAgentClient,
    servicer: StrategyServiceServicer | None,
    call,
) -> None:
    try:
        response = _invoke_servicer_platform_call(servicer, call, client=client)
        packed = ProtoAny()
        packed.Pack(response)
        client.send_platform_call_result(call_id=call.call_id, ok=True, response=packed)
    except WorkerPlatformCallError as exc:
        client.send_platform_call_result(
            call_id=call.call_id,
            ok=False,
            error=str(exc),
            dependency_error=exc.dependency_error,
        )
    except Exception as exc:  # noqa: BLE001
        client.send_platform_call_result(call_id=call.call_id, ok=False, error=str(exc))


def _invoke_servicer_platform_call(
    servicer: StrategyServiceServicer | None,
    call,
    *,
    client: WorkerAgentClient | None = None,
):
    method = str(getattr(call, "method", "") or "").strip()
    if method == "PreviewRunStrategy":
        request = strategy_pb2.PreviewRunStrategyRequest()
    elif method == "PrepareRunStrategyStart":
        request = strategy_pb2.PrepareRunStrategyStartRequest()
    elif method == "ValidateStrategySource":
        request = strategy_pb2.ValidateStrategySourceRequest()
    elif method == "GetStrategyStatus":
        request = strategy_pb2.GetStrategyStatusRequest()
    elif method == "StopStrategy":
        request = strategy_pb2.StopStrategyRequest()
    else:
        raise RuntimeError(f"unsupported runtime method: {method}")
    if not call.request.Unpack(request):
        raise RuntimeError(f"{method} request type mismatch")
    active_servicer = servicer
    if active_servicer is None:
        if client is None:
            raise RuntimeError("servicer is not available")
        active_servicer = _build_servicer(
            client,
            bound_user_id=int(getattr(request, "user_id", 0) or 0),
            runtime_id=str(getattr(request, "runtime_id", "") or os.environ.get("HUSHINE_RUNTIME_ID", "")),
        )
    context = _WorkerContext()
    handler = getattr(active_servicer, method)
    response = handler(request, context)
    if context.runtime_dependency_error is not None:
        raise WorkerPlatformCallError(
            context.details or "strategy dependency validation failed",
            context.runtime_dependency_error,
        )
    if context.code is not grpc.StatusCode.OK:
        raise RuntimeError(context.details or context.code.name)
    return response


class _WorkerContext:
    def __init__(self, *, start_session_id: str = "") -> None:
        self.code = grpc.StatusCode.OK
        self.details = ""
        self.start_session_id = str(start_session_id or "")
        self.runtime_dependency_error = None
        self._publication_lock = threading.Lock()
        self._running_publication = None

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details

    def set_trailing_metadata(self, metadata) -> None:
        del metadata

    def set_runtime_dependency_error(self, detail) -> None:
        self.runtime_dependency_error = detail

    def bind_running_publication(self, session_id: str, state) -> None:
        binding = (str(session_id or ""), state)
        with self._publication_lock:
            if self._running_publication is not None:
                raise RuntimeError("running publication is already bound")
            self._running_publication = binding

    def take_running_publication(self):
        with self._publication_lock:
            binding = self._running_publication
            self._running_publication = None
            return binding


if __name__ == "__main__":
    raise SystemExit(main())
