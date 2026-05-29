"""Debugger replay runner for self-hosted runtime agent.

The CLI only triggers this runner through the local control socket. The active
dataset stays in the runtime agent process and never crosses the socket.
"""

from __future__ import annotations

import ast
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any

from strategy_service.data_loop import _adapt_kline
from strategy_service.debug_control_server import DebugReplayRequest
from strategy_service.gen import control_panel_service_pb2 as cp_pb2
from strategy_service.notification import StrategyNotifier
from strategy_service.runtime_agent import DebugDataset, RuntimeBusyError
from strategy_service.runtime_profile import RUNTIME_VERSION
from strategy_service.service import StrategyEngine
from strategy_service.wallet_adapter import proto_to_account_spec
from strategy_service.wallet_factory import build_wallet_from_account

logger = logging.getLogger(__name__)

DEFAULT_WORKSPACE_PATH = "/workspace"
DEFAULT_STRATEGY_FILE = "self_hosted_strategy.py"
SNAPSHOT_REASON_ORDER_FILL = 1
SNAPSHOT_REASON_STRATEGY_START = 2
SNAPSHOT_REASON_STRATEGY_END = 3
_DEBUGPY_LISTEN_ENDPOINT: tuple[str, int] | None = None


@dataclass(frozen=True)
class DebugReplayResult:
    session_id: str
    session_name: str
    status: str
    bars_processed: int
    dataset_id: str
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "session_name": self.session_name,
            "status": self.status,
            "bars_processed": self.bars_processed,
            "dataset_id": self.dataset_id,
            "error": self.error,
        }


class DebugReplayRunner:
    def __init__(
        self,
        *,
        agent: Any,
        platform_proxy: Any,
        workspace_path: str = DEFAULT_WORKSPACE_PATH,
        strategy_filename: str = DEFAULT_STRATEGY_FILE,
        progress_every_bars: int = 20,
    ) -> None:
        if agent is None:
            raise ValueError("agent is required")
        if platform_proxy is None:
            raise ValueError("platform_proxy is required")
        self._agent = agent
        self._platform_proxy = platform_proxy
        self._workspace_path = workspace_path
        self._strategy_filename = strategy_filename
        self._progress_every_bars = max(1, int(progress_every_bars or 20))

    def replay(self, request: DebugReplayRequest) -> dict[str, Any]:
        result = self.run(request)
        return result.to_dict()

    def run(self, request: DebugReplayRequest) -> DebugReplayResult:
        dataset = self._require_active_dataset()
        if not self._agent.try_acquire_debug_replay():
            raise RuntimeBusyError("runtime is replaying")

        session_id = ""
        session_name = ""
        bars_processed = 0
        status = "failed"
        error = ""
        account_client = self._platform_proxy.account_client()
        try:
            start_resp = self._start_platform_debug_replay(dataset, request)
            session_id = start_resp.session_id
            session_name = start_resp.session_name
            if not session_id:
                raise RuntimeError("control-plane returned empty debug session_id")
            if start_resp.dataset and start_resp.dataset.dataset_id:
                if start_resp.dataset.dataset_id != dataset.dataset_id:
                    raise RuntimeError(
                        "control-plane debug dataset changed before replay: "
                        f"runtime={dataset.dataset_id} platform={start_resp.dataset.dataset_id}"
                    )

            info = account_client.get_online_account_info(dataset.account_id, dataset.user_id)
            if info is None:
                raise RuntimeError(f"account {dataset.account_id} not found or core-service unreachable")
            if int(getattr(info, "mode", 0) or 0) != 0:
                raise RuntimeError("debug replay only supports mode=0 accounts")
            wallet = build_wallet_from_account(proto_to_account_spec(info))

            strategy_code = self._read_strategy_code(dataset)
            self._save_debug_session(
                account_client=account_client,
                dataset=dataset,
                session_id=session_id,
                session_name=session_name,
            )
            account_client.update_account_wallet_state(
                dataset.account_id,
                wallet.futures,
                wallet.spot,
                snapshot_reason=SNAPSHOT_REASON_STRATEGY_START,
                strategy_id=0,
                session_id=session_id,
            )
            self._activate_debugger(request)

            order_client = self._platform_proxy.order_client()
            engine = StrategyEngine()
            user_strategy = engine.create_strategy(
                user_id=f"debug:{dataset.user_id}:session:{session_id}",
                strategy_path=self._strategy_path(),
                wallet=wallet,
                order_client=order_client,
                account_id=dataset.account_id,
                strategy_id=0,
                session_id=session_id,
                strategy_code=strategy_code,
                notifier=StrategyNotifier(self._platform_proxy.notification_client()),
            )

            def _on_order_sync() -> None:
                account_client.update_account_wallet_state(
                    dataset.account_id,
                    wallet.futures,
                    wallet.spot,
                    snapshot_reason=SNAPSHOT_REASON_ORDER_FILL,
                    strategy_id=0,
                    session_id=session_id,
                )

            user_strategy.on_order_callback = _on_order_sync

            for kline in dataset.klines:
                engine.running_strategy(_adapt_kline(kline, getattr(kline, "market", None)))
                bars_processed += 1
                if bars_processed % self._progress_every_bars == 0:
                    account_client.update_session(
                        session_id=session_id,
                        status="running",
                        bars_processed=bars_processed,
                        runtime_id=dataset.runtime_id,
                    )
            status = "finished"
            return DebugReplayResult(
                session_id=session_id,
                session_name=session_name,
                status=status,
                bars_processed=bars_processed,
                dataset_id=dataset.dataset_id,
            )
        except Exception as exc:  # noqa: BLE001
            error = str(exc)
            logger.exception("debug replay failed: session=%s dataset=%s", session_id, dataset.dataset_id)
            raise
        finally:
            if session_id:
                try:
                    account_client.update_account_wallet_state(
                        dataset.account_id,
                        getattr(locals().get("wallet", None), "futures", None),
                        getattr(locals().get("wallet", None), "spot", None),
                        snapshot_reason=SNAPSHOT_REASON_STRATEGY_END,
                        strategy_id=0,
                        session_id=session_id,
                    )
                    account_client.update_session(
                        session_id=session_id,
                        status=status,
                        bars_processed=bars_processed,
                        error=error,
                        runtime_id=dataset.runtime_id,
                    )
                except Exception:  # noqa: BLE001
                    logger.warning("debug replay finalize failed: session=%s", session_id, exc_info=True)
            self._deactivate_debugger(request)
            self._agent.release_debug_replay()

    def _require_active_dataset(self) -> DebugDataset:
        dataset = self._agent.active_debug_dataset()
        if dataset is None:
            raise RuntimeError("no active debug dataset loaded")
        if not dataset.klines:
            raise RuntimeError("active debug dataset has no klines")
        if int(dataset.user_id or 0) <= 0:
            raise RuntimeError("active debug dataset is missing user_id")
        if int(dataset.account_id or 0) <= 0:
            raise RuntimeError("active debug dataset is missing account_id")
        if not str(dataset.runtime_id or "").strip():
            raise RuntimeError("active debug dataset is missing runtime_id")
        return dataset

    def _start_platform_debug_replay(
        self,
        dataset: DebugDataset,
        request: DebugReplayRequest,
    ) -> cp_pb2.StartDebugReplayResponse:
        req = cp_pb2.StartDebugReplayRequest(
            runtime_id=dataset.runtime_id,
            user_id=int(dataset.user_id),
            dataset_id=dataset.dataset_id,
            requested_name=request.name,
        )
        return self._platform_proxy.invoke(
            "debug.StartDebugReplay",
            req,
            cp_pb2.StartDebugReplayResponse,
        )

    def _save_debug_session(
        self,
        *,
        account_client: Any,
        dataset: DebugDataset,
        session_id: str,
        session_name: str,
    ) -> None:
        account_client.require_save_session(
            session_id=session_id,
            account_id=dataset.account_id,
            strategy_id=0,
            mode=0,
            interval=dataset.interval,
            start_time_ms=dataset.start_time_ms,
            end_time_ms=dataset.end_time_ms,
            runtime_id=dataset.runtime_id,
            runtime_source="self_hosted",
            runtime_name="",
            session_type="debugging",
            runtime_version=RUNTIME_VERSION,
            session_name=session_name,
        )

    def _strategy_path(self) -> str:
        return os.path.join(self._workspace_path, self._strategy_filename)

    def _read_strategy_code(self, dataset: DebugDataset) -> str:
        path = self._strategy_path()
        try:
            with open(path, "r", encoding="utf-8") as f:
                code = f.read()
        except FileNotFoundError as exc:
            raise RuntimeError("debug strategy file not found; run Prepare Debugging first") from exc
        if not code.strip():
            raise RuntimeError("debug strategy file is empty")
        return _with_debug_inputs_if_missing(
            code,
            market=dataset.market,
            symbol=dataset.symbol,
            interval=dataset.interval,
        )

    @staticmethod
    def _activate_debugger(request: DebugReplayRequest) -> None:
        debugger = str(request.debugger or "").strip().lower()
        if debugger in ("", "none"):
            return
        host = request.host or "0.0.0.0"
        port = int(request.port or 0)
        if port <= 0:
            port = 5678 if debugger == "debugpy" else 12345
        if debugger in ("debugpy", "vscode"):
            import debugpy  # type: ignore

            global _DEBUGPY_LISTEN_ENDPOINT
            endpoint = (host, port)
            if _DEBUGPY_LISTEN_ENDPOINT is None:
                try:
                    debugpy.listen(endpoint)
                    _DEBUGPY_LISTEN_ENDPOINT = endpoint
                except RuntimeError as exc:
                    if "listen() has already been called" not in str(exc):
                        raise
                    _DEBUGPY_LISTEN_ENDPOINT = endpoint
            elif _DEBUGPY_LISTEN_ENDPOINT != endpoint:
                raise RuntimeError(
                    "debugpy debugger is already listening on "
                    f"{_DEBUGPY_LISTEN_ENDPOINT[0]}:{_DEBUGPY_LISTEN_ENDPOINT[1]}; "
                    "restart the runtime or use the same host/port"
                )
            if request.wait:
                debugpy.wait_for_client()
            return
        if debugger == "pycharm":
            import pydevd_pycharm  # type: ignore

            DebugReplayRunner._reset_pycharm_debugger_state()
            restore_set_trace_to_threads = None
            pydevd_tracing = None
            try:
                try:
                    import pydevd_tracing as _pydevd_tracing  # type: ignore
                except ModuleNotFoundError:
                    _pydevd_tracing = None
                pydevd_tracing = _pydevd_tracing

                if pydevd_tracing is not None:
                    restore_set_trace_to_threads = getattr(pydevd_tracing, "set_trace_to_threads", None)
                    if restore_set_trace_to_threads is not None:
                        pydevd_tracing.set_trace_to_threads = lambda _tracing_func: 0

                pydevd_pycharm.settrace(
                    host,
                    port=port,
                    stdout_to_server=True,
                    stderr_to_server=True,
                    suspend=bool(request.wait),
                    trace_only_current_thread=True,
                )
            finally:
                if pydevd_tracing is not None and restore_set_trace_to_threads is not None:
                    pydevd_tracing.set_trace_to_threads = restore_set_trace_to_threads
            return
        raise RuntimeError(f"unsupported debugger adapter: {request.debugger}")

    @staticmethod
    def _reset_pycharm_debugger_state() -> None:
        try:
            import pydevd  # type: ignore
        except Exception:  # noqa: BLE001
            return
        try:
            global_debugger = None
            get_global_debugger = getattr(pydevd, "get_global_debugger", None)
            if callable(get_global_debugger):
                global_debugger = get_global_debugger()
            if bool(getattr(pydevd, "connected", False)):
                if global_debugger is not None:
                    try:
                        pydevd.stoptrace()
                    except Exception:  # noqa: BLE001
                        logger.debug("pycharm debugger stoptrace before reconnect failed", exc_info=True)
                pydevd.connected = False
        except Exception:  # noqa: BLE001
            logger.debug("pycharm debugger state reset failed", exc_info=True)

    @staticmethod
    def _deactivate_debugger(request: DebugReplayRequest) -> None:
        debugger = str(request.debugger or "").strip().lower()
        if debugger != "pycharm":
            return
        try:
            import pydevd  # type: ignore
        except Exception:  # noqa: BLE001
            return
        try:
            global_debugger = None
            get_global_debugger = getattr(pydevd, "get_global_debugger", None)
            if callable(get_global_debugger):
                global_debugger = get_global_debugger()
            if bool(getattr(pydevd, "connected", False)) or global_debugger is not None:
                try:
                    pydevd.stoptrace()
                finally:
                    pydevd.connected = False
        except Exception:  # noqa: BLE001
            logger.debug("pycharm debugger cleanup failed", exc_info=True)


def _with_debug_inputs_if_missing(code: str, *, market: str, symbol: str, interval: str) -> str:
    if _my_strategy_declares_inputs(code):
        return code
    inputs = [{"exchange": "binance", "market": market, "symbol": symbol, "interval": interval}]
    suffix = (
        "\n\n"
        "# Hushine debugger injects the page-selected dataset universe when the\n"
        "# scratch strategy does not declare INPUTS itself.\n"
        "_hushine_debug_inputs = "
        f"{json.dumps(inputs, separators=(',', ':'))}\n"
        "if 'MyStrategy' in globals() and not hasattr(MyStrategy, 'INPUTS') "
        "and not hasattr(MyStrategy, 'inputs') and not hasattr(MyStrategy, 'declared_inputs'):\n"
        "    MyStrategy.INPUTS = _hushine_debug_inputs\n"
    )
    return code.rstrip() + suffix


def _my_strategy_declares_inputs(code: str) -> bool:
    try:
        tree = ast.parse(code)
    except SyntaxError:
        return False
    for node in tree.body:
        if not isinstance(node, ast.ClassDef) or node.name != "MyStrategy":
            continue
        for child in node.body:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) and child.name in (
                "inputs",
                "declared_inputs",
            ):
                return True
            if isinstance(child, ast.Assign):
                for target in child.targets:
                    if isinstance(target, ast.Name) and target.id == "INPUTS":
                        return True
            if isinstance(child, ast.AnnAssign):
                target = child.target
                if isinstance(target, ast.Name) and target.id == "INPUTS":
                    return True
    return False
