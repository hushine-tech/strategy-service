from __future__ import annotations

import logging

from strategy_service.worker_agent_client import load_worker_env

logger = logging.getLogger("hushine-session-worker")


def main() -> int:
    env = load_worker_env()
    logger.info("session worker starting: session_id=%s agent=%s", env.session_id, env.agent_addr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
