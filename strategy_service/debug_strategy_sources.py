from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path


ENV_DEBUG_STRATEGY_SOURCE_DIR = "RUNTIME_DEBUG_STRATEGY_SOURCE_DIR"
DEFAULT_DEBUG_STRATEGY_SOURCE_DIR = ".hushine-runtime/strategies"

_UNSAFE_FILENAME_CHARS = re.compile(r"[^A-Za-z0-9]+")


class DebugStrategySourceError(RuntimeError):
    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__("bare debug strategy source is invalid")


def materialize_bare_strategy_source(
    *,
    user_id: int,
    strategy_id: int,
    name: str,
    version: str,
    strategy_code: str,
    root_dir: str | Path | None = None,
) -> Path:
    if type(strategy_code) is not str or not strategy_code:
        raise DebugStrategySourceError("missing_code")
    path: Path | None = None
    temporary_path: Path | None = None
    raw_fd: int | None = None
    materialization_failed = False
    try:
        path = bare_strategy_source_path(
            root_dir=root_dir,
            user_id=user_id,
            strategy_id=strategy_id,
            name=name,
            version=version,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        raw_fd, raw_temporary_path = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary_path = Path(raw_temporary_path)
        temporary = os.fdopen(raw_fd, "w", encoding="utf-8", newline="")
        raw_fd = None
        with temporary:
            temporary.write(strategy_code)
            temporary.flush()
            os.fsync(temporary.fileno())
        temporary_path.replace(path)
        temporary_path = None
    except OSError:
        materialization_failed = True
    finally:
        if raw_fd is not None:
            try:
                os.close(raw_fd)
            except OSError:
                pass
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
    if materialization_failed:
        raise DebugStrategySourceError("materialization_failed")
    assert path is not None
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
    lookup_failed = False
    path: Path | None = None
    try:
        path = bare_strategy_source_path(
            root_dir=root_dir,
            user_id=user_id,
            strategy_id=strategy_id,
            name=name,
            version=version,
        )
        if path.exists():
            return path
    except DebugStrategySourceError:
        raise
    except OSError:
        lookup_failed = True
    if lookup_failed:
        raise DebugStrategySourceError("materialization_failed")
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
    invalid_identity = False
    user_id_int = 0
    strategy_id_int = 0
    try:
        user_id_int = int(user_id)
        strategy_id_int = int(strategy_id)
    except (TypeError, ValueError):
        invalid_identity = True
    if invalid_identity or user_id_int <= 0 or strategy_id_int <= 0:
        raise DebugStrategySourceError("invalid_identity")

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
