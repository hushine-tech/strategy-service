import importlib
import sys
import threading
from types import SimpleNamespace

import grpc

from strategy_service.gen import strategy_service_pb2 as strategy_pb2
from strategy_service.grpc_server import StrategyServiceServicer
from strategy_service.session import SessionState
from strategy_service.session_worker_entry import (
    _WorkerContext,
    _build_servicer,
    _poll_until_terminal,
    _publish_running_session,
    _report_worker_failure,
    _report_start_rejection,
    _start_debugpy_if_requested,
)
from strategy_service.worker_agent_client import FinalStatusRejected


class _TerminalServicer:
    def __init__(
        self, status: str, bars: int, error: str = "", reconciliation_run_id: str = "",
        error_code: str = "", error_message: str = "", error_detail_json: str = "{}",
    ):
        self.status = status
        self.bars = bars
        self.error = error
        self.reconciliation_run_id = reconciliation_run_id
        self.error_code = error_code
        self.error_message = error_message
        self.error_detail_json = error_detail_json
        self._sessions = SimpleNamespace(
            get=lambda _session_id: SimpleNamespace(
                reconciliation_run_id=self.reconciliation_run_id,
                error_code=self.error_code,
                error_message=self.error_message,
                error_detail_json=self.error_detail_json,
            )
        )

    def GetStrategyStatus(self, request, context):
        del request, context
        return strategy_pb2.GetStrategyStatusResponse(
            status=self.status,
            bars_processed=self.bars,
            error=self.error,
        )


class _FinalClient:
    def __init__(self, reject: bool = False):
        self.reject = reject
        self.progress = []
        self.final = []

    def send_progress(self, **kwargs):
        self.progress.append(kwargs)

    def send_final_status(self, **kwargs):
        self.final.append(kwargs)
        if self.reject:
            raise FinalStatusRejected("indicator finalization failed: unavailable")

    def send_indicator_frame(self, **kwargs):
        del kwargs


class _FakeDebugpy:
    def __init__(self, listen_error=None):
        self.configured = []
        self.listened = []
        self.wait_calls = 0
        self.listen_error = listen_error

    def configure(self, **kwargs):
        self.configured.append(kwargs)

    def listen(self, address):
        self.listened.append(address)
        if self.listen_error is not None:
            raise self.listen_error

    def wait_for_client(self):
        self.wait_calls += 1


def test_local_bare_runtime_can_import_real_debugpy():
    debugpy = importlib.import_module("debugpy")

    assert callable(debugpy.listen)


def test_start_debugpy_does_not_wait_when_debug_wait_is_false(monkeypatch):
    fake_debugpy = _FakeDebugpy()
    monkeypatch.setitem(sys.modules, "debugpy", fake_debugpy)
    monkeypatch.setenv("DEBUG_WAIT", "false")

    _start_debugpy_if_requested(5678)

    assert fake_debugpy.configured == [{"subProcess": False}]
    assert fake_debugpy.listened == [("127.0.0.1", 5678)]
    assert fake_debugpy.wait_calls == 0


def test_start_debugpy_waits_when_debug_wait_is_true(monkeypatch):
    fake_debugpy = _FakeDebugpy()
    monkeypatch.setitem(sys.modules, "debugpy", fake_debugpy)
    monkeypatch.setenv("DEBUG_WAIT", "true")

    _start_debugpy_if_requested(5678)

    assert fake_debugpy.configured == [{"subProcess": False}]
    assert fake_debugpy.listened == [("127.0.0.1", 5678)]
    assert fake_debugpy.wait_calls == 1


def test_start_debugpy_does_not_block_non_waiting_worker_when_port_is_busy(monkeypatch):
    fake_debugpy = _FakeDebugpy(RuntimeError("address already in use"))
    monkeypatch.setitem(sys.modules, "debugpy", fake_debugpy)
    monkeypatch.setenv("DEBUG_WAIT", "false")

    _start_debugpy_if_requested(5678)

    assert fake_debugpy.configured == [{"subProcess": False}]
    assert fake_debugpy.listened == [("127.0.0.1", 5678)]
    assert fake_debugpy.wait_calls == 0


def test_poll_until_terminal_sends_final_status_and_waits_for_ack():
    client = _FinalClient()
    result = _poll_until_terminal(
        _TerminalServicer("finished", 1440),
        client,
        "sess-1",
        6,
        "rt-1",
    )
    assert result == 0
    assert client.progress == []
    assert client.final == [{
        "session_id": "sess-1",
        "status": "finished",
        "bars_processed": 1440,
        "error": "",
        "timeout_seconds": 35.0,
    }]


def test_poll_until_terminal_returns_failure_when_final_status_rejected():
    client = _FinalClient(reject=True)
    assert _poll_until_terminal(
        _TerminalServicer("finished", 1440), client, "sess-1", 6, "rt-1",
    ) == 1
    assert len(client.final) == 1
    assert client.progress == []


def test_poll_until_terminal_preserves_failed_terminal_status():
    client = _FinalClient()
    assert _poll_until_terminal(
        _TerminalServicer("failed", 17, "strategy error"),
        client,
        "sess-1",
        6,
        "rt-1",
    ) == 1
    assert client.final[0]["status"] == "failed"
    assert client.final[0]["error"] == "strategy error"


def test_poll_until_terminal_copies_typed_session_error_intact():
    client = _FinalClient()
    result = _poll_until_terminal(
        _TerminalServicer(
            "failed", 9, "legacy display text",
            error_code="ORDER_REQUEST_REJECTED",
            error_message="order request was rejected",
            error_detail_json='{"venue_id":17}',
        ),
        client,
        "sess-typed",
        7,
        "rt-1",
    )

    assert result == 1
    assert client.final[0]["error_code"] == "ORDER_REQUEST_REJECTED"
    assert client.final[0]["error_message"] == "order request was rejected"
    assert client.final[0]["error_detail_json"] == '{"venue_id":17}'


def test_poll_until_terminal_forwards_stop_reconciliation_identity():
    client = _FinalClient()

    assert _poll_until_terminal(
        _TerminalServicer(
            "stop_failed",
            17,
            "Spot close requires reconciliation",
            reconciliation_run_id="recon-123",
        ),
        client,
        "sess-1",
        6,
        "rt-1",
    ) == 1

    assert client.final[0]["reconciliation_run_id"] == "recon-123"


class _StatusPortfolioClient:
    def __init__(self):
        self.updates = []

    def update_session(self, **kwargs):
        self.updates.append(kwargs)
        return True


def test_worker_servicer_defers_terminal_but_not_running_persistence():
    servicer = StrategyServiceServicer(
        portfolio_service_addr="",
        order_service_addr="",
        timescale_config={},
        kafka_brokers="",
        restore_running_sessions=False,
    )
    portfolio = _StatusPortfolioClient()
    servicer._portfolio_client = lambda: portfolio
    state = SessionState(status="finished", bars_processed=1440, runtime_id="rt-1")

    assert servicer._persist_session_status("sess-1", state) is True
    assert portfolio.updates == []

    state.status = "running"
    assert servicer._persist_session_status("sess-1", state) is True
    assert portfolio.updates == [{
        "session_id": "sess-1",
        "status": "running",
        "bars_processed": 1440,
        "error": "",
        "runtime_id": "rt-1",
    }]


def test_worker_context_binds_exact_running_publication_once():
    context = _WorkerContext()
    state = SessionState(session_id="1" * 32, status="running")

    context.bind_running_publication("1" * 32, state)

    assert context.take_running_publication() == ("1" * 32, state)
    assert context.take_running_publication() is None


def test_worker_context_retains_typed_dependency_error():
    context = _WorkerContext()
    detail = strategy_pb2.RuntimeDependencyError(
        code="STRATEGY_DEPENDENCY_UNAVAILABLE",
        module="google.cloud",
    )

    context.set_runtime_dependency_error(detail)

    assert context.runtime_dependency_error == detail


def test_publish_running_claims_enqueues_then_releases_user_loop():
    servicer = StrategyServiceServicer(
        "", "", {}, "", restore_running_sessions=False,
    )
    session_id, state = servicer._sessions.prepare(
        session_id="1" * 32,
        initial_status="pending",
    )
    servicer._sessions.register(session_id, state)
    startup = SimpleNamespace(
        release=threading.Event(),
        abort=threading.Event(),
    )
    state.bind_startup_result(startup)
    assert state.mark_running_publication_ready()
    context = _WorkerContext(start_session_id=session_id)
    context.bind_running_publication(session_id, state)
    client = _FinalClient()

    assert _publish_running_session(servicer, client, context, session_id) is True

    assert client.progress == [{"session_id": session_id, "status": "running"}]
    assert state.publication_state() == "RELEASED"
    assert startup.release.is_set()
    assert not startup.abort.is_set()


def test_publish_running_fatal_first_sends_no_running(monkeypatch):
    servicer = StrategyServiceServicer(
        "", "", {}, "", restore_running_sessions=False,
    )
    session_id, state = servicer._sessions.prepare(
        session_id="2" * 32,
        initial_status="pending",
    )
    servicer._sessions.register(session_id, state)
    startup = SimpleNamespace(
        release=threading.Event(),
        abort=threading.Event(),
    )
    state.bind_startup_result(startup)
    assert state.mark_running_publication_ready()
    assert state.latch_user_code_fatal("callback")
    context = _WorkerContext(start_session_id=session_id)
    context.bind_running_publication(session_id, state)
    client = _FinalClient()
    failures = []
    monkeypatch.setattr(
        servicer,
        "_fail_running_publication",
        lambda *args: failures.append(args) or True,
    )

    assert _publish_running_session(servicer, client, context, session_id) is False

    assert client.progress == []
    assert failures == [
        (session_id, state, "strategy session terminated before running publication")
    ]
    assert not startup.release.is_set()


def test_worker_outer_failure_report_is_fixed_and_does_not_leak_exception(caplog):
    client = _FinalClient()

    with caplog.at_level("ERROR"):
        _report_worker_failure(client, "3" * 32)

    assert client.progress == [{
        "session_id": "3" * 32,
        "status": "failed",
        "error": "session worker terminated",
    }]
    assert [record.getMessage() for record in caplog.records] == [
        f"SESSION_WORKER_FATAL session={'3' * 32}"
    ]
    assert "Traceback" not in caplog.text


def test_start_rejection_preserves_typed_dependency_detail():
    client = _FinalClient()
    context = _WorkerContext(start_session_id="4" * 32)
    context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
    context.set_details("strategy dependency validation failed")
    detail = strategy_pb2.RuntimeDependencyError(
        code="STRATEGY_DEPENDENCY_UNAVAILABLE",
        module="google.cloud",
        runtime_profile="platform-python-3.13",
        runtime_profile_version="1.0.0",
        image_build_id="build-1",
    )
    context.set_runtime_dependency_error(detail)

    _report_start_rejection(client, "4" * 32, context)

    assert client.progress == [{
        "session_id": "4" * 32,
        "status": "failed",
        "error": "strategy dependency validation failed",
        "dependency_error": detail,
    }]


def test_start_rejection_preserves_typed_platform_error_fields():
    client = _FinalClient()
    context = _WorkerContext(start_session_id="5" * 32)
    dependency = strategy_pb2.RuntimeDependencyError(
        code="STRATEGY_DEPENDENCY_UNAVAILABLE",
        module="portfolio.v1",
    )
    context.set_code(grpc.StatusCode.UNAVAILABLE)
    context.set_details("portfolio route is unavailable")
    context.set_runtime_error(
        code="PLATFORM_ROUTE_UNAVAILABLE",
        message="portfolio route is unavailable",
        detail_json='{"runtime_id":"rt-test","retryable":true}',
        dependency_error=dependency,
    )

    _report_start_rejection(client, "5" * 32, context)

    assert client.progress == [{
        "session_id": "5" * 32,
        "status": "failed",
        "error": "portfolio route is unavailable",
        "error_code": "PLATFORM_ROUTE_UNAVAILABLE",
        "error_message": "portfolio route is unavailable",
        "error_detail_json": '{"runtime_id":"rt-test","retryable":true}',
        "dependency_error": dependency,
    }]
