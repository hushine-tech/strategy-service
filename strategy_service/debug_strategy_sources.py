from __future__ import annotations

import os
import re
from pathlib import Path


ENV_DEBUG_STRATEGY_SOURCE_DIR = "RUNTIME_DEBUG_STRATEGY_SOURCE_DIR"
DEFAULT_DEBUG_STRATEGY_SOURCE_DIR = ".hushine-runtime/strategies"

_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9]+")


class DebugStrategySourceError(RuntimeError):
    pass


def materialize_bare_strategy_source(
    *,
    user_id: int,
    strategy_id: int,
    name: str,
    version: str,
    strategy_code: str,
    root_dir: str | Path | None = None,
) -> Path:
    if not strategy_code:
        raise DebugStrategySourceError("strategy_code is required")
    path = bare_strategy_source_path(
        root_dir=root_dir,
        user_id=user_id,
        strategy_id=strategy_id,
        name=name,
        version=version,
    )
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(strategy_code, encoding="utf-8")
        tmp.replace(path)
    except OSError as exc:
        raise DebugStrategySourceError(f"write {path}: {exc}") from exc
    return path


def ensure_bare_strategy_source(
    *,
    user_id: int,
    strategy_id: int,
    name: str,
    version: str,
    strategy_code: str,
    root_dir: str | Path | None = None,
) -> Path:
    path = bare_strategy_source_path(
        root_dir=root_dir,
        user_id=user_id,
        strategy_id=strategy_id,
        name=name,
        version=version,
    )
    if path.exists():
        return path
    return materialize_bare_strategy_source(
        root_dir=root_dir,
        user_id=user_id,
        strategy_id=strategy_id,
        name=name,
        version=version,
        strategy_code=strategy_code,
    )


def bare_strategy_source_path(
    *,
    user_id: int,
    strategy_id: int,
    name: str,
    version: str,
    root_dir: str | Path | None = None,
) -> Path:
    try:
        user_id_int = int(user_id)
        strategy_id_int = int(strategy_id)
    except (TypeError, ValueError) as exc:
        raise DebugStrategySourceError("user_id and strategy_id must be integers") from exc
    if user_id_int <= 0 or strategy_id_int <= 0:
        raise DebugStrategySourceError("user_id and strategy_id must be positive")

    root = _debug_source_root(root_dir)
    return (
        root
        / f"user-{user_id_int}"
        / f"strategy-{strategy_id_int}-{_slug(name, 'strategy')}-{_slug(version, 'version')}.py"
    )


def _debug_source_root(root_dir: str | Path | None) -> Path:
    configured = root_dir or os.environ.get(ENV_DEBUG_STRATEGY_SOURCE_DIR) or DEFAULT_DEBUG_STRATEGY_SOURCE_DIR
    return Path(configured).expanduser().resolve()


def _slug(value: str, fallback: str) -> str:
    slug = _UNSAFE_FILENAME_CHARS.sub("-", str(value or "").strip().lower()).strip("-")
    return slug or fallback
