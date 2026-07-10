from __future__ import annotations

import logging
import os
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
    WorkerRuntimeChannelAdapter,
    load_worker_env,
)

logger = logging.getLogger("hushine-session-worker")

_TERMINAL_STATUSES = {"finished", "completed", "failed", "stopped", "stop_failed", "recoverable"}


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

        servicer = _build_servicer(
            client,
            bound_user_id=int(start.user_id or request.user_id or 0),
            runtime_id=runtime_id,
        )
        client.set_agent_platform_call_handler(lambda call: _invoke_servicer_platform_call(servicer, call))
        context = _WorkerContext()
        response = servicer.RunStrategy(request, context)
        if context.code is not grpc.StatusCode.OK:
            detail = context.details or context.code.name
            client.send_progress(session_id=start.session_id, status="failed", error=detail)
            logger.error("session start failed: %s", detail)
            return 1
        session_id = response.session_id
        if not session_id:
            client.send_progress(session_id=start.session_id, status="failed", error="RunStrategy returned empty session_id")
            return 1
        client.send_progress(session_id=session_id, status="running")
        return _poll_until_terminal(servicer, client, session_id, int(request.user_id or start.user_id or 0), runtime_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("session worker failed")
        try:
            client.send_progress(session_id=env.session_id, status="failed", error=str(exc))
        except Exception:  # noqa: BLE001
            pass
        return 1
    finally:
        client.close()


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
                client.send_final_status(
                    session_id=session_id,
                    status=status.status,
                    bars_processed=status.bars_processed,
                    error=status.error,
                    timeout_seconds=35.0,
                )
            except (FinalStatusRejected, TimeoutError) as exc:
                logger.error("final session status was not acknowledged: %s", exc)
                return 1
            return 0 if status.status in {"finished", "completed", "stopped"} else 1
        client.send_progress(
            session_id=session_id,
            status=status.status,
            bars_processed=status.bars_processed,
            error=status.error,
        )
        time.sleep(1.0)


def _start_debugpy_if_requested(port: int) -> None:
    if int(port or 0) <= 0:
        return
    import debugpy  # type: ignore

    host = os.environ.get("HUSHINE_DEBUGPY_HOST", "127.0.0.1")
    debugpy.listen((host, int(port)))
    if os.environ.get("DEBUG_WAIT", "0").lower() not in {"0", "false", "no"}:
        logger.info("waiting for debugger attach on %s:%s", host, port)
        debugpy.wait_for_client()


def _build_servicer(
    client: WorkerAgentClient,
    *,
    bound_user_id: int,
    runtime_id: str,
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
    if context.code is not grpc.StatusCode.OK:
        raise RuntimeError(context.details or context.code.name)
    return response


class _WorkerContext:
    def __init__(self) -> None:
        self.code = grpc.StatusCode.OK
        self.details = ""

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details

    def set_trailing_metadata(self, metadata) -> None:
        del metadata


if __name__ == "__main__":
    raise SystemExit(main())
