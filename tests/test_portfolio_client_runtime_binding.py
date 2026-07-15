from __future__ import annotations

from datetime import datetime, timezone

from strategy_service.portfolio_client import PortfolioClient
from strategy_service.gen import portfolio_service_pb2


def test_save_session_initial_status_descriptor_is_field_15():
    request = portfolio_service_pb2.SaveSessionRequest.DESCRIPTOR
    field = request.fields_by_name.get("initial_status")

    assert field is not None
    assert field.number == 15
    assert request.fields_by_name["leverage"].number == 14
    assert request.fields_by_name["user_id"].number == 100


def test_portfolio_client_save_session_sends_runtime_binding():
    captured: dict[str, object] = {}

    class FakeStub:
        def SaveSession(self, req):
            captured["req"] = req
            return portfolio_service_pb2.SaveSessionResponse()

    client = PortfolioClient("")
    client._stub = FakeStub()

    ok = client.save_session(
        session_id="sess-1",
        portfolio_id=11,
        strategy_id=22,
        environment=1,
        runtime_id="rt-1",
        runtime_source="hosted",
        runtime_name="default",
        leverage=5,
        initial_status="pending",
    )

    assert ok is True
    req = captured["req"]
    assert req.runtime_id == "rt-1"
    assert req.runtime_source == "hosted"
    assert req.runtime_name == "default"
    assert req.leverage == 5
    assert req.initial_status == "pending"


def test_portfolio_client_update_session_sends_runtime_guard():
    captured: dict[str, object] = {}

    class FakeStub:
        def UpdateSession(self, req):
            captured["req"] = req
            return portfolio_service_pb2.UpdateSessionResponse()

    client = PortfolioClient("")
    client._stub = FakeStub()

    ok = client.update_session("sess-1", "stopped", runtime_id="rt-1")

    assert ok is True
    req = captured["req"]
    assert req.session_id == "sess-1"
    assert req.status == "stopped"
    assert req.runtime_id == "rt-1"


def test_portfolio_client_list_running_sessions_filters_by_runtime():
    captured: dict[str, object] = {}

    class FakeStub:
        def ListRunningSessions(self, req):
            captured["req"] = req
            return portfolio_service_pb2.ListRunningSessionsResponse()

    client = PortfolioClient("")
    client._stub = FakeStub()

    sessions = client.require_running_sessions(runtime_id="rt-1")

    assert sessions == []
    req = captured["req"]
    assert req.runtime_id == "rt-1"


def test_portfolio_client_get_portfolio_snapshot_uses_portfolio_api():
    captured: dict[str, object] = {}

    class FakeStub:
        def GetPortfolioSnapshot(self, req):
            captured["req"] = req
            return portfolio_service_pb2.GetPortfolioSnapshotResponse(
                snapshot=portfolio_service_pb2.PortfolioSnapshot(portfolio_id=11, user_id=5)
            )

    client = PortfolioClient("")
    client._stub = FakeStub()

    snapshot = client.get_portfolio_snapshot(portfolio_id=11, user_id=5)

    req = captured["req"]
    assert req.portfolio_id == 11
    assert req.user_id == 5
    assert snapshot.portfolio_id == 11


def test_portfolio_client_get_portfolio_snapshot_sends_required_symbols():
    captured: dict[str, object] = {}

    class FakeStub:
        def GetPortfolioSnapshot(self, req):
            captured["req"] = req
            return portfolio_service_pb2.GetPortfolioSnapshotResponse(
                snapshot=portfolio_service_pb2.PortfolioSnapshot(portfolio_id=11, user_id=5)
            )

    client = PortfolioClient("")
    client._stub = FakeStub()

    snapshot = client.get_portfolio_snapshot(
        portfolio_id=11,
        user_id=5,
        required_symbols={("binance", "perpetual_futures", "ethusdt")},
    )

    req = captured["req"]
    assert snapshot.portfolio_id == 11
    assert len(req.required_symbols) == 1
    assert req.required_symbols[0].exchange == 1
    assert req.required_symbols[0].market == 2
    assert req.required_symbols[0].symbol == "ETHUSDT"


def test_portfolio_client_update_portfolio_snapshot_uses_portfolio_api():
    captured: dict[str, object] = {}

    class FakeStub:
        def UpdatePortfolioSnapshot(self, req):
            captured["req"] = req
            return portfolio_service_pb2.UpdatePortfolioSnapshotResponse(
                snapshot=portfolio_service_pb2.PortfolioSnapshot(portfolio_id=11, user_id=5)
            )

    client = PortfolioClient("")
    client._stub = FakeStub()

    snapshot = client.update_portfolio_snapshot(
        portfolio_id=11,
        user_id=5,
        snapshot_reason=2,
        strategy_id=22,
        session_id="sess-1",
        snapshot_time=datetime(2026, 6, 1, 0, 43, tzinfo=timezone.utc),
    )

    req = captured["req"]
    assert req.portfolio_id == 11
    assert req.user_id == 5
    assert req.snapshot_reason == 2
    assert req.strategy_id == 22
    assert req.session_id == "sess-1"
    assert req.snapshot_time.ToDatetime(tzinfo=timezone.utc) == datetime(2026, 6, 1, 0, 43, tzinfo=timezone.utc)
    assert snapshot.portfolio_id == 11


def test_portfolio_client_preflight_sends_session_metadata():
    captured: dict[str, object] = {}

    class FakeStub:
        def PreflightStrategySession(self, req):
            captured["req"] = req
            return portfolio_service_pb2.PreflightStrategySessionResponse(ok=True)

    client = PortfolioClient("")
    client._stub = FakeStub()

    resp = client.preflight_strategy_session(
        portfolio_id=11,
        user_id=5,
        required_routes={("binance", "perpetual_futures")},
        required_symbols={("binance", "perpetual_futures", "ethusdt")},
        session_id="preflight-session-1",
        strategy_id=22,
        leverage=1,
    )

    req = captured["req"]
    assert resp.ok is True
    assert req.session_id == "preflight-session-1"
    assert req.strategy_id == 22
    assert req.leverage == 1
