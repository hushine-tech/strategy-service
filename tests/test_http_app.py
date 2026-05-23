"""HTTP layer smoke tests (no live DB when backtest runner is mocked)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from strategy_service.http_app import create_app  # noqa: E402


def test_health() -> None:
    client = TestClient(create_app())
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


def test_run_backtest_mocked_no_db() -> None:
    body = {
        "user_id": "u1",
        "strategy_path": "tests.strategies.noop",
        "symbol": "BTCUSDT",
        "symbol_type": "futures",
        "market": "futures",
        "interval": "1m",
        "start_time_ms": 0,
        "end_time_ms": 1,
        "account": {
            "futures": {
                "margin_mode": "isolated",
                "position_mode": "one_way",
                "positions": [
                    {
                        "symbol": "BTCUSDT",
                        "direction": 0,
                        "initial_balance": 10_000.0,
                        "leverage": 20.0,
                        "fee_rate": 0.0004,
                    }
                ],
            },
            "spot": {"free": 0.0, "locked": 0.0},
        },
    }
    with patch("strategy_service.http_app._run_backtest_simple", return_value=42):
        client = TestClient(create_app())
        r = client.post("/run-backtest", json=body)
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    assert data["bars_processed"] == 42
