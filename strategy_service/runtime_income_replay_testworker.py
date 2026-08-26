"""Integration-test Worker that durably barriers Income before explicit ACK."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

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
    ack_release_path = _path("HUSHINE_INCOME_REPLAY_ACK_RELEASE")
    ack_enqueued_path = _path("HUSHINE_INCOME_REPLAY_ACK_ENQUEUED")
    client = _CapturingWorkerAgentClient(env, income_batch_path=batch_path)
    client.start()
    try:
        start = client.wait_for_start_session(timeout_seconds=15.0)
        _write(start_path, start.SerializeToString(deterministic=True))
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
            if event.batch_end:
                final_event = event
                break
        if final_event is None:
            raise RuntimeError("Income batch ended without a final event")
        _write(
            events_path,
            json.dumps(decoded, separators=(",", ":")).encode("utf-8"),
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
