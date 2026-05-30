from __future__ import annotations

from strategy_service.account_client import AccountClient
from strategy_service.gen import account_service_pb2


def test_account_client_save_session_sends_runtime_binding():
    captured: dict[str, object] = {}

    class FakeStub:
        def SaveSession(self, req):
            captured["req"] = req
            return account_service_pb2.SaveSessionResponse()

    client = AccountClient("")
    client._stub = FakeStub()

    ok = client.save_session(
        session_id="sess-1",
        account_id=11,
        strategy_id=22,
        mode=2,
        runtime_id="rt-1",
        runtime_source="hosted",
        runtime_name="default",
    )

    assert ok is True
    req = captured["req"]
    assert req.runtime_id == "rt-1"
    assert req.runtime_source == "hosted"
    assert req.runtime_name == "default"


def test_account_client_update_session_sends_runtime_guard():
    captured: dict[str, object] = {}

    class FakeStub:
        def UpdateSession(self, req):
            captured["req"] = req
            return account_service_pb2.UpdateSessionResponse()

    client = AccountClient("")
    client._stub = FakeStub()

    ok = client.update_session("sess-1", "stopped", runtime_id="rt-1")

    assert ok is True
    req = captured["req"]
    assert req.session_id == "sess-1"
    assert req.status == "stopped"
    assert req.runtime_id == "rt-1"


def test_account_client_list_running_sessions_filters_by_runtime():
    captured: dict[str, object] = {}

    class FakeStub:
        def ListRunningSessions(self, req):
            captured["req"] = req
            return account_service_pb2.ListRunningSessionsResponse()

    client = AccountClient("")
    client._stub = FakeStub()

    sessions = client.require_running_sessions(runtime_id="rt-1")

    assert sessions == []
    req = captured["req"]
    assert req.runtime_id == "rt-1"


def test_account_client_get_portfolio_snapshot_uses_portfolio_api():
    captured: dict[str, object] = {}

    class FakeStub:
        def GetPortfolioSnapshot(self, req):
            captured["req"] = req
            return account_service_pb2.GetPortfolioSnapshotResponse(
                snapshot=account_service_pb2.PortfolioSnapshot(account_id=11, user_id=5)
            )

    client = AccountClient("")
    client._stub = FakeStub()

    snapshot = client.get_portfolio_snapshot(account_id=11, user_id=5)

    req = captured["req"]
    assert req.account_id == 11
    assert req.user_id == 5
    assert snapshot.account_id == 11


def test_account_client_update_portfolio_snapshot_uses_portfolio_api():
    captured: dict[str, object] = {}

    class FakeStub:
        def UpdatePortfolioSnapshot(self, req):
            captured["req"] = req
            return account_service_pb2.UpdatePortfolioSnapshotResponse(
                snapshot=account_service_pb2.PortfolioSnapshot(account_id=11, user_id=5)
            )

    client = AccountClient("")
    client._stub = FakeStub()

    snapshot = client.update_portfolio_snapshot(
        account_id=11,
        user_id=5,
        snapshot_reason=2,
        strategy_id=22,
        session_id="sess-1",
    )

    req = captured["req"]
    assert req.account_id == 11
    assert req.user_id == 5
    assert req.snapshot_reason == 2
    assert req.strategy_id == 22
    assert req.session_id == "sess-1"
    assert snapshot.account_id == 11
