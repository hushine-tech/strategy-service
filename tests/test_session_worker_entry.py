from strategy_service.gen import strategy_service_pb2 as strategy_pb2
from strategy_service.session_worker_entry import _poll_until_terminal
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
