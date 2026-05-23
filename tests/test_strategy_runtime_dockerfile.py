from __future__ import annotations

from pathlib import Path


def test_debugger_image_uses_python_312_without_downgrading_executor():
    dockerfile = Path(__file__).resolve().parents[1] / "Dockerfile"
    content = dockerfile.read_text(encoding="utf-8")

    assert "FROM python:3.13-slim AS runtime-base" in content
    assert "FROM runtime-base AS executor" in content
    assert "FROM python:3.12-slim AS debugger-base" in content
    assert "FROM debugger-base AS debugger" in content
    assert '"pydevd-pycharm~=${PYDEVD_PYCHARM_VERSION}"' in content
