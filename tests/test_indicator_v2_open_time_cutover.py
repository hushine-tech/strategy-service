from __future__ import annotations

import importlib.util
import json
import os
import threading
import time
from pathlib import Path
from types import SimpleNamespace


FIXTURE = Path(__file__).parent / "strategies" / "indicator_v2_open_time_cutover.py"


def _load_strategy_type():
    spec = importlib.util.spec_from_file_location("indicator_v2_open_time_cutover", FIXTURE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.MyStrategy


class _Indicators:
    def __init__(self) -> None:
        self.values: list[tuple[str, float | None]] = []
        self.markers: list[tuple[str, dict[str, object]]] = []

    def set(self, key: str, value: float | None) -> None:
        self.values.append((key, value))

    def mark(self, key: str, **marker: object) -> None:
        self.markers.append((key, marker))


class _Node:
    def __init__(self, child: object) -> None:
        self._child = child

    def __getitem__(self, _key: str) -> object:
        return self._child


def _data(sequence: int):
    open_time = (sequence + 1) * 60_000
    tick = SimpleNamespace(
        price=100.0 + sequence,
        klines={
            "open_time": open_time,
            "close_time": open_time + 59_999,
            "timestamp": open_time + 59_999,
        },
    )
    interval = _Node(tick)
    symbol = SimpleNamespace(interval=interval)
    market = SimpleNamespace(symbol=_Node(symbol))
    exchange = SimpleNamespace(market=_Node(market))
    return SimpleNamespace(exchange=_Node(exchange))


def test_fixture_uses_open_time_shape_and_marks_the_decision_bar(monkeypatch):
    for name in (
        "HUSHINE_INDICATOR_V2_BARRIER_FILE",
        "HUSHINE_INDICATOR_V2_BARRIER_OWNER_TOKEN",
        "HUSHINE_INDICATOR_V2_BARRIER_GENERATION",
    ):
        monkeypatch.delenv(name, raising=False)
    strategy = _load_strategy_type()()
    strategy.indicators = _Indicators()

    decisions = [strategy.on_market_data(_data(sequence), object()) for sequence in range(10)]

    assert [value for _key, value in strategy.indicators.values] == [
        float(sequence) for sequence in range(10)
    ]
    assert [index for index, decision in enumerate(decisions) if decision is not None] == [4, 9]
    assert [marker["text"] for _key, marker in strategy.indicators.markers] == [
        "BUY",
        "SELL",
    ]
    assert [marker["price"] for _key, marker in strategy.indicators.markers] == [
        104.0,
        109.0,
    ]


def test_fixture_barrier_acknowledges_completed_callback_before_advancing(
    tmp_path: Path,
    monkeypatch,
):
    strategy = _load_strategy_type()()
    strategy.indicators = _Indicators()
    strategy._completed = 1023
    strategy._last_open_time_ms = 1023 * 60_000

    owner = "a" * 64
    generation = "generation-1"
    runtime_id = "rt-acceptance"
    session_id = "session-acceptance"
    control = tmp_path / "control.json"
    acknowledgement = tmp_path / "ack.json"
    control.write_text(
        json.dumps(
            {
                "schema": 1,
                "owner_token": owner,
                "generation": generation,
                "runtime_id": runtime_id,
                "session_id": session_id,
                "target_completed": 1023,
                "ack_file": str(acknowledgement),
            }
        ),
        encoding="utf-8",
    )
    os.chmod(control, 0o600)
    monkeypatch.setenv("HUSHINE_INDICATOR_V2_BARRIER_FILE", str(control))
    monkeypatch.setenv("HUSHINE_INDICATOR_V2_BARRIER_OWNER_TOKEN", owner)
    monkeypatch.setenv("HUSHINE_INDICATOR_V2_BARRIER_GENERATION", generation)

    result: list[object] = []
    worker = threading.Thread(
        target=lambda: result.append(strategy.on_market_data(_data(1023), object())),
        daemon=True,
    )
    worker.start()
    deadline = time.monotonic() + 2
    while not acknowledgement.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert acknowledgement.exists()
    ack = json.loads(acknowledgement.read_text(encoding="utf-8"))
    assert ack == {
        "schema": 1,
        "owner_token": owner,
        "generation": generation,
        "runtime_id": runtime_id,
        "session_id": session_id,
        "completed": 1023,
        "last_open_time_ms": 1023 * 60_000,
    }
    assert worker.is_alive(), "next callback must remain blocked at the barrier"
    assert strategy.indicators.values == []

    control.write_text(
        json.dumps(
            {
                "schema": 1,
                "owner_token": owner,
                "generation": generation,
                "runtime_id": runtime_id,
                "session_id": session_id,
                "target_completed": 1025,
                "ack_file": str(acknowledgement),
            }
        ),
        encoding="utf-8",
    )
    os.chmod(control, 0o600)
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert len(result) == 1
    assert strategy._completed == 1024


def test_fixture_acceptance_constants_drive_barrier_without_runtime_env(
    tmp_path: Path,
    monkeypatch,
):
    for name in (
        "HUSHINE_INDICATOR_V2_BARRIER_FILE",
        "HUSHINE_INDICATOR_V2_BARRIER_OWNER_TOKEN",
        "HUSHINE_INDICATOR_V2_BARRIER_GENERATION",
    ):
        monkeypatch.delenv(name, raising=False)

    strategy_type = _load_strategy_type()
    owner = "b" * 64
    generation = "generation-constant"
    runtime_id = "rt-constant"
    session_id = "session-constant"
    control = tmp_path / "control.json"
    acknowledgement = tmp_path / "ack.json"
    control.write_text(
        json.dumps(
            {
                "schema": 1,
                "owner_token": owner,
                "generation": generation,
                "runtime_id": runtime_id,
                "session_id": session_id,
                "target_completed": 1023,
                "ack_file": str(acknowledgement),
            }
        ),
        encoding="utf-8",
    )
    os.chmod(control, 0o600)
    strategy_type.ACCEPTANCE_BARRIER_FILE = str(control)
    strategy_type.ACCEPTANCE_BARRIER_OWNER_TOKEN = owner
    strategy_type.ACCEPTANCE_BARRIER_GENERATION = generation

    strategy = strategy_type()
    strategy.indicators = _Indicators()
    strategy._completed = 1023
    strategy._last_open_time_ms = 1023 * 60_000
    worker = threading.Thread(
        target=lambda: strategy.on_market_data(_data(1023), object()),
        daemon=True,
    )
    worker.start()

    deadline = time.monotonic() + 2
    while not acknowledgement.exists() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert acknowledgement.exists()
    assert worker.is_alive()

    control.write_text(
        json.dumps(
            {
                "schema": 1,
                "owner_token": owner,
                "generation": generation,
                "runtime_id": runtime_id,
                "session_id": session_id,
                "target_completed": 1025,
                "ack_file": str(acknowledgement),
            }
        ),
        encoding="utf-8",
    )
    os.chmod(control, 0o600)
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert strategy._completed == 1024
