from strategy_service.gen import strategy_service_pb2 as strategy_pb2
from strategy_service.grpc_server import StrategyServiceServicer
from strategy_service.session import SessionState
from strategy_service.session_worker_entry import _build_servicer, _poll_until_terminal
from strategy_service.worker_agent_client import FinalStatusRejected


class _TerminalServicer:
    def __init__(self, status: str, bars: int, error: str = ""):
        self.status = status
        self.bars = bars
        self.error = error

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


class _StatusPortfolioClient:
    def __init__(self):
        self.updates = []

    def update_session(self, **kwargs):
        self.updates.append(kwargs)
        return True


def test_agent_managed_servicer_defers_terminal_but_not_running_persistence():
    servicer = StrategyServiceServicer(
        portfolio_service_addr="",
        order_service_addr="",
        timescale_config={},
        kafka_brokers="",
        restore_running_sessions=False,
        agent_managed_final_status=True,
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


def test_session_worker_builds_agent_managed_final_status_servicer():
    servicer = _build_servicer(_FinalClient(), bound_user_id=6, runtime_id="rt-1")
    assert servicer._agent_managed_final_status is True
