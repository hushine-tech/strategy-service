from __future__ import annotations

from pathlib import Path


def test_strategy_runtime_image_builds_executor_only():
    dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
    content = dockerfile.read_text(encoding="utf-8")

    assert "FROM python:3.13-slim AS runtime-base" in content
    assert "FROM runtime-base AS executor" in content
    assert "FROM executor AS default" in content
    assert "AS debugger" not in content
    assert "debugpy" not in content
    assert "pydevd-pycharm" not in content
