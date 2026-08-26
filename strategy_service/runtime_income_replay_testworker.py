"""Integration-test Worker that durably barriers Income before explicit ACK."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

from strategy_service.gen import strategy_service_pb2 as strategy_pb2
from strategy_service.grpc_server import StrategyServiceServicer
from strategy_service.platform_proxy import RuntimeChannelPlatformProxy
from strategy_service.session import SessionState
from strategy_service.wallet.portfolio_adapter import (
    build_portfolio_wallet_from_snapshot,
)
from strategy_service.worker_agent_client import (
    WorkerAgentClient,
    WorkerAgentDataSource,
    load_worker_env,
)


def _path(name: str) -> Path:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required")
    return Path(value)


def _write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(payload)
    os.replace(temporary, path)


def _wait(path: Path) -> None:
    while not path.exists():
        time.sleep(0.01)


class _CapturingWorkerAgentClient(WorkerAgentClient):
    def __init__(self, *args, income_batch_path: Path, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._income_batch_path = income_batch_path

    def _handle_agent_frame(self, frame) -> None:
        if frame.WhichOneof("payload") == "income_batch":
            _write(
                self._income_batch_path,
                frame.income_batch.SerializeToString(deterministic=True),
            )
        super()._handle_agent_frame(frame)


def main() -> None:
    env = load_worker_env()
    start_path = _path("HUSHINE_INCOME_REPLAY_START")
    batch_path = _path("HUSHINE_INCOME_REPLAY_BATCH")
    events_path = _path("HUSHINE_INCOME_REPLAY_EVENTS")
    apply_path = _path("HUSHINE_INCOME_REPLAY_APPLY")
    ack_release_path = _path("HUSHINE_INCOME_REPLAY_ACK_RELEASE")
    ack_enqueued_path = _path("HUSHINE_INCOME_REPLAY_ACK_ENQUEUED")
    client = _CapturingWorkerAgentClient(env, income_batch_path=batch_path)
    client.start()
    try:
        start = client.wait_for_start_session(timeout_seconds=15.0)
        _write(start_path, start.SerializeToString(deterministic=True))
        request = strategy_pb2.RunStrategyRequest()
        if not start.run_strategy_request.Unpack(request):
            raise RuntimeError("StartSession run request is invalid")
        snapshot = RuntimeChannelPlatformProxy(client).portfolio_client().get_portfolio_snapshot(
            portfolio_id=request.portfolio_id,
            user_id=request.user_id or start.user_id,
        )
        if snapshot is None:
            raise RuntimeError("replacement Worker portfolio snapshot is unavailable")
        wallet = build_portfolio_wallet_from_snapshot(
            snapshot,
            allowed_routes={("binance", "perpetual_futures")},
        )
        route_wallet = wallet.wallets[("binance", "perpetual_futures", 71)]
        state = SessionState(environment=1)
        state.configure_stop_runtime(wallet=wallet)
        balance_before = str(route_wallet.futures.wallet_balance)
        cursor_before = int(route_wallet.futures.last_applied_income_entry_id)
        source = WorkerAgentDataSource(client)
        decoded = []
        final_event = None
        for event in source.iter_session_events(
            session_id=env.session_id,
            required_streams=[],
            stop_event=client._closed,
            idle_timeout_seconds=0.05,
        ):
            if event.kind != "income":
                continue
            decoded.append(
                {
                    "session_id": event.session_id,
                    "stream_key": event.stream_key,
                    "sequence": event.sequence,
                    "batch_end": event.batch_end,
                    "entry_hex": event.payload.SerializeToString(
                        deterministic=True
                    ).hex(),
                }
            )
            StrategyServiceServicer._apply_runtime_income_event(
                env.session_id,
                state,
                event,
            )
            if event.batch_end:
                final_event = event
                break
        if final_event is None:
            raise RuntimeError("Income batch ended without a final event")
        _write(
            events_path,
            json.dumps(decoded, separators=(",", ":")).encode("utf-8"),
        )
        _write(
            apply_path,
            json.dumps(
                {
                    "balance_before": balance_before,
                    "balance_after": str(route_wallet.futures.wallet_balance),
                    "cursor_before": cursor_before,
                    "cursor_after": int(
                        route_wallet.futures.last_applied_income_entry_id
                    ),
                    "entry_count": len(decoded),
                    "final_sequence": final_event.sequence,
                },
                separators=(",", ":"),
            ).encode("utf-8"),
        )
        _wait(ack_release_path)
        source.acknowledge_income_applied(final_event)
        _write(ack_enqueued_path, b"ack-enqueued\n")
        for _event in source.iter_session_events(
            session_id=env.session_id,
            required_streams=[],
            stop_event=client._closed,
            idle_timeout_seconds=0.05,
        ):
            pass
    finally:
        client.close()


if __name__ == "__main__":
    main()
