"""Acceptance fixture for proving a blocked callback cannot stop runtime heartbeats."""

from __future__ import annotations

import os
import time
from pathlib import Path


class MyStrategy:
    INPUTS = [
        {
            "exchange": "binance",
            "market": "perpetual_futures",
            "symbol": "BTCUSDT",
            "interval": "1m",
        },
    ]
    ORDER_TARGETS = []
    INDICATORS = {
        "blocked_probe": {
            "name": "Blocked worker probe",
            "type": "line",
            "pane": "strategy",
        },
    }

    def __init__(self) -> None:
        self._callbacks = 0

    def on_market_data(self, data, wallet):
        del data, wallet
        self._callbacks += 1
        self.indicators.set("blocked_probe", float(self._callbacks))
        if self._callbacks != 2:
            return None

        marker = Path(os.environ["HUSHINE_BLOCKED_WORKER_MARKER"])
        marker.parent.mkdir(parents=True, exist_ok=True)
        temporary = marker.with_name(f".{marker.name}.{os.getpid()}.tmp")
        temporary.write_text("blocked\n", encoding="utf-8")
        os.replace(temporary, marker)

        deadline = time.monotonic() + float(
            os.environ["HUSHINE_BLOCKED_WORKER_SECONDS"]
        )
        while time.monotonic() < deadline:
            time.sleep(0.1)
        return None
