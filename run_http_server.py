#!/usr/bin/env python3
"""Start HTTP API for backtest runs (see ``strategy_service.http_app``)."""

from __future__ import annotations

import sys

# strategy-library 以符号链接形式挂载在当前目录，需手动加入 sys.path
# 使得 utils.log.tracer 等模块可正常导入
sys.path.insert(0, ".")
sys.path.insert(0, "./strategy-library")

if __name__ == "__main__":
    try:
        import uvicorn
    except ImportError as e:
        raise SystemExit(
            "uvicorn is required. Install with: pip install uvicorn fastapi"
        ) from e

    host = "0.0.0.0"
    port = 8000
    if len(sys.argv) >= 2:
        port = int(sys.argv[1])

    uvicorn.run(
        "strategy_service.http_app:create_app",
        factory=True,
        host=host,
        port=port,
    )
