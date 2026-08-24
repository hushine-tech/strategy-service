from __future__ import annotations

from datetime import datetime, timezone

import pytest

from strategy_service.inputs import StrategyOrderTarget
from strategy_service.portfolio_client import PortfolioClient
from strategy_service.gen import portfolio_service_pb2


def test_save_session_initial_status_descriptor_is_field_15():
    request = portfolio_service_pb2.SaveSessionRequest.DESCRIPTOR
    field = request.fields_by_name.get("initial_status")

    assert field is not None
    assert field.number == 15
    assert request.fields_by_name["user_id"].number == 100


def test_portfolio_client_update_session_sends_runtime_guard():
    captured: dict[str, object] = {}

    class FakeStub:
        def UpdateSession(self, req):
            captured["req"] = req
            return portfolio_service_pb2.UpdateSessionResponse()

    client = PortfolioClient("")
    client._stub = FakeStub()

    ok = client.update_session(
        "sess-1", "running", runtime_id="rt-1", expected_status="pending"
    )

    assert ok is True
    req = captured["req"]
    assert req.session_id == "sess-1"
    assert req.status == "running"
    assert req.runtime_id == "rt-1"
    assert req.expected_status == "pending"


def test_portfolio_client_strict_update_propagates_transport_error():
    class FailingStub:
        def UpdateSession(self, _req):
            raise RuntimeError("response lost")

    client = PortfolioClient("")
    client._stub = FailingStub()

    with pytest.raises(RuntimeError, match="response lost"):
        client.update_session(
            "sess-1",
            "running",
            runtime_id="rt-1",
            expected_status="pending",
            strict=True,
        )


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
        required_routes={("binance", "perpetual_futures"), ("binance", "spot")},
        required_symbols={
            ("binance", "perpetual_futures", "ethusdt"),
            ("binance", "perpetual_futures", "solusdt"),
            ("binance", "spot", "btcusdt"),
        },
        order_targets=[
            StrategyOrderTarget(
                exchange="binance",
                market="perpetual_futures",
                symbol="ETHUSDT",
                leverage=7,
                effective_leverage=7,
                leverage_source="order_target",
            ),
            StrategyOrderTarget(
                exchange="binance",
                market="spot",
                symbol="BTCUSDT",
            ),
        ],
        session_id="preflight-session-1",
        strategy_id=22,
    )

    req = captured["req"]
    assert resp.ok is True
    assert req.session_id == "preflight-session-1"
    assert req.strategy_id == 22
    symbols = {
        (item.exchange, item.market, item.symbol): item
        for item in req.required_symbols
    }
    futures_target = symbols[(
        1,
        2,
        "ETHUSDT",
    )]
    assert futures_target.order_target is True
    assert list(futures_target.required_order_types) == ["MARKET", "LIMIT"]
    assert futures_target.effective_leverage == 7
    assert futures_target.leverage_source == "order_target"

    futures_input = symbols[(
        1,
        2,
        "SOLUSDT",
    )]
    assert futures_input.order_target is False
    assert futures_input.effective_leverage == 0
    assert futures_input.leverage_source == ""

    spot_target = symbols[(
        1,
        1,
        "BTCUSDT",
    )]
    assert spot_target.order_target is True
    assert list(spot_target.required_order_types) == ["MARKET", "LIMIT"]
    assert spot_target.effective_leverage == 0
    assert spot_target.leverage_source == ""
def test_portfolio_client_commit_strategy_session_start_forwards_typed_contract_and_timeout():
    captured: dict[str, object] = {}
    expected = portfolio_service_pb2.CommitStrategySessionStartResponse(
        ok=False,
        issues=[
            portfolio_service_pb2.PreflightIssue(
                code="LEVERAGE_CONFIRMATION_FAILED",
                message="readback mismatch",
                exchange=1,
                market=2,
                symbol="BTCUSDT",
                venue_id=41,
                retryable=True,
                source="exchange",
            )
        ],
        confirmed_target_facts=[
            portfolio_service_pb2.SessionTargetLeverageFact(
                session_id="sess-commit-1",
                venue_id=41,
                exchange=1,
                environment=1,
                market=2,
                symbol="ETHUSDT",
                effective_leverage=3,
                leverage_source="order_target",
                previous_leverage=2,
                confirmed_leverage=3,
            )
        ],
        target_results=[
            portfolio_service_pb2.FuturesLeverageTargetResult(
                venue_id=41,
                exchange=1,
                market=2,
                symbol="BTCUSDT",
                effective_leverage=5,
                leverage_source="strategy_default",
                previous_leverage=2,
                current_leverage=2,
                confirmed_leverage=5,
                change_required=True,
                status="rolled_back",
                error_code="LEVERAGE_CONFIRMATION_FAILED",
                error_message="readback mismatch",
                retryable=True,
            )
        ],
        rollback_failed=True,
        code="LEVERAGE_ROLLBACK_FAILED",
    )

    class FakeStub:
        def CommitStrategySessionStart(self, req, *, timeout):
            captured["req"] = req
            captured["timeout"] = timeout
            return expected

    request = portfolio_service_pb2.CommitStrategySessionStartRequest(
        launch_operation_id="launch-commit-1",
        session=portfolio_service_pb2.SaveSessionRequest(
            session_id="sess-commit-1",
            portfolio_id=11,
            strategy_id=22,
            environment=1,
            interval="5m",
            start_time_ms=1000,
            end_time_ms=2000,
            runtime_id="runtime-hosted-1",
            runtime_source="hosted",
            runtime_name="default",
            session_type="live",
            runtime_version="v2",
            session_name="momentum",
            initial_status="pending",
            user_id=5,
        ),
        required_routes=[portfolio_service_pb2.RequiredRoute(exchange=1, market=2)],
        required_symbols=[
            portfolio_service_pb2.RequiredSymbol(
                exchange=1,
                market=2,
                symbol="BTCUSDT",
                order_target=True,
                required_order_types=["MARKET", "LIMIT"],
                effective_leverage=5,
                leverage_source="strategy_default",
            )
        ],
    )
    request_bytes = request.SerializeToString()
    client = PortfolioClient("")
    client._stub = FakeStub()

    response = client.commit_strategy_session_start(request, timeout_seconds=17.5)

    assert captured["req"].SerializeToString() == request_bytes
    assert captured["timeout"] == 17.5
    assert response.SerializeToString() == expected.SerializeToString()
