"""Deterministic real-chain fixture for the Runtime Indicator V2 cutover.

The fixture deliberately uses K-line open time for indicators while returning
an order on selected bars so acceptance can prove the existing order path keeps
the K-line close-time fact.  Optional file barriers let the acceptance owner
pause the next callback after 1023, 1025, and 2049 completed callbacks without
leaving a partial indicator frame behind.
"""

from __future__ import annotations

import json
import os
import stat
import time
from pathlib import Path
from typing import Any

from strategy_service.types import OrderDecision


_BARRIER_COUNTS = frozenset({1023, 1025, 2049})
_BARRIER_POLL_SECONDS = 0.05


class MyStrategy:
    # The real-chain acceptance harness patches these test-fixture constants
    # into the stored strategy source. This keeps the hosted Runtime env
    # allowlist closed while still allowing the worker to coordinate through
    # its private /coverage mount.
    ACCEPTANCE_BARRIER_FILE = ""
    ACCEPTANCE_BARRIER_OWNER_TOKEN = ""
    ACCEPTANCE_BARRIER_GENERATION = ""

    INPUTS = [
        {
            "exchange": "binance",
            "market": "perpetual_futures",
            "symbol": "TESTUSDT",
            "interval": "1m",
        },
    ]
    ORDER_TARGETS = [
        {
            "exchange": "binance",
            "market": "perpetual_futures",
            "symbol": "TESTUSDT",
        },
    ]
    INDICATORS = {
        "cutover_scalar": {
            "name": "Cutover Scalar",
            "type": "line",
            "pane": "strategy",
            "color": "#2563eb",
        },
        "cutover_signal": {
            "name": "Cutover Signal",
            "type": "marker",
            "pane": "price",
        },
    }

    def __init__(self) -> None:
        self._completed = 0
        self._last_open_time_ms = 0

    @staticmethod
    def _tick(data: Any) -> Any:
        return (
            data.exchange["binance"]
            .market["perpetual_futures"]
            .symbol["TESTUSDT"]
            .interval["1m"]
        )

    @staticmethod
    def _positive_int(value: Any, name: str) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"{name} must be an integer") from exc
        if parsed <= 0:
            raise RuntimeError(f"{name} must be positive")
        return parsed

    @staticmethod
    def _kline_field(tick: Any, name: str) -> Any:
        klines = getattr(tick, "klines", None)
        if isinstance(klines, dict) and name in klines:
            return klines[name]
        return getattr(tick, name, 0)

    @staticmethod
    def _read_private_json(path: Path) -> dict[str, Any]:
        if not path.is_absolute() or path.is_symlink():
            raise RuntimeError("indicator V2 barrier path must be absolute and not a symlink")
        info = path.stat()
        if not stat.S_ISREG(info.st_mode) or stat.S_IMODE(info.st_mode) != 0o600:
            raise RuntimeError("indicator V2 barrier file must be a mode-0600 regular file")
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise RuntimeError("indicator V2 barrier payload must be a JSON object")
        return payload

    @staticmethod
    def _atomic_private_json(path: Path, payload: dict[str, Any]) -> None:
        if not path.is_absolute() or path.is_symlink():
            raise RuntimeError("indicator V2 acknowledgement path is unsafe")
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, path)
        finally:
            try:
                temporary.unlink()
            except FileNotFoundError:
                pass

    def _barrier_before_next_callback(self) -> None:
        if self._completed not in _BARRIER_COUNTS:
            return
        configured = (
            str(self.ACCEPTANCE_BARRIER_FILE).strip(),
            str(self.ACCEPTANCE_BARRIER_OWNER_TOKEN).strip(),
            str(self.ACCEPTANCE_BARRIER_GENERATION).strip(),
        )
        if any(configured):
            if not all(configured):
                raise RuntimeError(
                    "indicator V2 acceptance barrier constants must be set together"
                )
            raw_path, expected_owner, expected_generation = configured
        else:
            raw_path = os.environ.get(
                "HUSHINE_INDICATOR_V2_BARRIER_FILE",
                "",
            ).strip()
            expected_owner = os.environ.get(
                "HUSHINE_INDICATOR_V2_BARRIER_OWNER_TOKEN",
                "",
            ).strip()
            expected_generation = os.environ.get(
                "HUSHINE_INDICATOR_V2_BARRIER_GENERATION",
                "",
            ).strip()
        if not raw_path:
            return
        path = Path(raw_path)
        if not expected_owner or not expected_generation:
            raise RuntimeError("indicator V2 barrier owner and generation are required")

        acknowledged = False
        while True:
            control = self._read_private_json(path)
            owner = str(control.get("owner_token") or "").strip()
            generation = str(control.get("generation") or "").strip()
            runtime_id = str(control.get("runtime_id") or "").strip()
            session_id = str(control.get("session_id") or "").strip()
            target = self._positive_int(
                control.get("target_completed"),
                "target_completed",
            )
            if owner != expected_owner or generation != expected_generation:
                raise RuntimeError("indicator V2 barrier ownership changed")
            if not runtime_id or not session_id:
                raise RuntimeError(
                    "indicator V2 barrier runtime_id and session_id are required"
                )
            raw_ack = str(control.get("ack_file") or "").strip()
            if not raw_ack:
                raise RuntimeError("indicator V2 barrier acknowledgement path is required")
            ack_path = Path(raw_ack)
            if not acknowledged:
                self._atomic_private_json(
                    ack_path,
                    {
                        "schema": 1,
                        "owner_token": owner,
                        "generation": generation,
                        "runtime_id": runtime_id,
                        "session_id": session_id,
                        "completed": self._completed,
                        "last_open_time_ms": self._last_open_time_ms,
                    },
                )
                acknowledged = True
            if target > self._completed:
                return
            time.sleep(_BARRIER_POLL_SECONDS)

    def on_market_data(self, data: Any, wallet: Any) -> OrderDecision | None:
        del wallet
        self._barrier_before_next_callback()
        tick = self._tick(data)
        sequence = self._completed
        open_time_ms = self._positive_int(
            self._kline_field(tick, "open_time"),
            "open_time",
        )
        close_time_ms = self._positive_int(
            self._kline_field(tick, "close_time"),
            "close_time",
        )
        timestamp_ms = self._positive_int(
            self._kline_field(tick, "timestamp"),
            "timestamp",
        )
        if close_time_ms != open_time_ms + 59_999 or timestamp_ms != close_time_ms:
            raise RuntimeError("cutover fixture requires production-shaped one-minute bars")

        price = float(getattr(tick, "price"))
        self.indicators.set("cutover_scalar", float(sequence))
        decision = None
        if sequence in {4, 9, 1438}:
            side = "BUY" if sequence in {4, 1438} else "SELL"
            self.indicators.mark(
                "cutover_signal",
                text=side,
                price=price,
                color="#16a34a" if side == "BUY" else "#dc2626",
                position="belowBar" if side == "BUY" else "aboveBar",
                shape="arrowUp" if side == "BUY" else "arrowDown",
            )
            decision = OrderDecision(
                exchange="binance",
                market="perpetual_futures",
                symbol="TESTUSDT",
                side=side,
                qty="0.001",
                order_type="MARKET",
            )

        self._completed += 1
        self._last_open_time_ms = open_time_ms
        return decision
