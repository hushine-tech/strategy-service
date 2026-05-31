from __future__ import annotations

import grpc

from strategy_service.gen import strategy_service_pb2 as pb2
from strategy_service.grpc_server import StrategyServiceServicer


class _FakeContext:
    def __init__(self) -> None:
        self.code = None
        self.details = ""

    def set_code(self, code) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _servicer(bound_user_id: int = 0) -> StrategyServiceServicer:
    return StrategyServiceServicer(
        "acct:1",
        "order:1",
        {},
        "127.0.0.1:9092",
        bound_user_id=bound_user_id,
        restore_running_sessions=False,
    )


def test_validate_strategy_code_returns_runtime_profile():
    context = _FakeContext()
    resp = _servicer().ValidateStrategyCode(
        pb2.ValidateStrategyCodeRequest(
            user_id=17,
            code="""
import math

class MyStrategy:
    INPUTS = [{"exchange": "binance", "market": "perpetual_futures", "symbol": "BTCUSDT", "interval": "1m"}]
    ORDER_TARGETS = []

    def on_market_data(self, data, wallet):
        return None
""",
        ),
        context,
    )

    assert resp.ok is True
    assert resp.runtime_version == "1.0.0"
    assert resp.runtime_profile == "platform-python-3.13"
    assert context.code is None


def test_validate_strategy_code_rejects_unsupported_import():
    context = _FakeContext()
    resp = _servicer().ValidateStrategyCode(
        pb2.ValidateStrategyCodeRequest(
            user_id=17,
            code="""
import talib

class MyStrategy:
    def on_market_data(self, data, wallet):
        return None
""",
        ),
        context,
    )

    assert resp.ok is False
    assert resp.issues[0].code == "unsupported_dependency"
    assert resp.issues[0].module == "talib"
    assert context.code is None


def test_validate_strategy_code_requires_user_id():
    context = _FakeContext()
    resp = _servicer().ValidateStrategyCode(
        pb2.ValidateStrategyCodeRequest(code="class MyStrategy: pass"),
        context,
    )

    assert resp.ok is False
    assert context.code == grpc.StatusCode.INVALID_ARGUMENT
    assert context.details == "user_id is required"


def test_validate_strategy_code_enforces_bound_user():
    context = _FakeContext()
    resp = _servicer(bound_user_id=22).ValidateStrategyCode(
        pb2.ValidateStrategyCodeRequest(user_id=17, code="class MyStrategy: pass"),
        context,
    )

    assert resp.ok is False
    assert context.code == grpc.StatusCode.PERMISSION_DENIED
