from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class WorkerEnv:
    agent_addr: str
    token: str
    session_id: str
    debugpy_port: int = 0


def load_worker_env() -> WorkerEnv:
    agent_addr = os.environ.get("HUSHINE_AGENT_ADDR", "").strip()
    token = os.environ.get("HUSHINE_WORKER_TOKEN", "").strip()
    session_id = os.environ.get("HUSHINE_SESSION_ID", "").strip()
    if not agent_addr:
        raise RuntimeError("HUSHINE_AGENT_ADDR is required")
    if not token:
        raise RuntimeError("HUSHINE_WORKER_TOKEN is required")
    if not session_id:
        raise RuntimeError("HUSHINE_SESSION_ID is required")
    debugpy_port_raw = os.environ.get("HUSHINE_DEBUGPY_PORT", "").strip()
    debugpy_port = int(debugpy_port_raw) if debugpy_port_raw else 0
    return WorkerEnv(
        agent_addr=agent_addr,
        token=token,
        session_id=session_id,
        debugpy_port=debugpy_port,
    )
