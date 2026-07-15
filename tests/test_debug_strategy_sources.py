import threading
import sys
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from strategy_service.grpc_server import StrategyServiceServicer
from strategy_service.service import StrategyEngine
from strategy_service.strategy_imports import (
    gate_strategy_source,
    prepare_strategy,
    resolve_strategy_source,
)
from strategy_service.debug_strategy_sources import (
    DebugStrategySourceError,
    materialize_bare_strategy_source,
)
from strategy_service.wallet.portfolio import PortfolioWalletRuntime


_CODE = (
    "class MyStrategy:\n"
    "    INPUTS = [{\"exchange\": \"binance\", \"market\": \"perpetual_futures\", \"symbol\": \"ETHUSDT\", \"interval\": \"1m\"}]\n"
    "    ORDER_TARGETS = []\n"
    "    def on_market_data(self, data, wallet):\n"
    "        return None\n"
)


def test_materialize_bare_strategy_source_writes_stable_python_file(tmp_path: Path):
    path = materialize_bare_strategy_source(
        root_dir=tmp_path,
        user_id=6,
        strategy_id=123,
        name="Mean Reversion / Demo",
        version="1.0.0",
        strategy_code=_CODE,
    )

    assert path == tmp_path / "user-6" / "strategy-123-mean-reversion-demo-1-0-0.py"
    assert path.read_text(encoding="utf-8") == _CODE
    assert stat.S_IMODE(path.stat().st_mode) == 0o600

    gate = gate_strategy_source(
        resolve_strategy_source(str(path), None, hot_reload=True),
        python_invocation_path=sys.executable,
    )
    assert gate.ok and gate.gated_source is not None
    strategy = StrategyEngine().create_strategy(
        "materialized",
        prepare_strategy(gate.gated_source),
        PortfolioWalletRuntime(
            1,
            {("binance", "perpetual_futures")},
            {("binance", "perpetual_futures", 1): object()},
        ),
    )._get_strategy()
    assert strategy.on_market_data.__code__.co_filename == str(path)


def test_bare_materialization_is_atomic_under_concurrency(monkeypatch, tmp_path: Path):
    original_replace = Path.replace
    replace_barrier = threading.Barrier(2)
    temporary_names: list[str] = []
    names_lock = threading.Lock()

    def replace_together(path: Path, target: Path):
        with names_lock:
            temporary_names.append(path.name)
        replace_barrier.wait(timeout=2)
        return original_replace(path, target)

    monkeypatch.setattr(Path, "replace", replace_together)

    def materialize() -> Path:
        return materialize_bare_strategy_source(
            root_dir=tmp_path,
            user_id=6,
            strategy_id=123,
            name="Atomic Demo",
            version="1.0.0",
            strategy_code=_CODE,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        paths = tuple(pool.map(lambda _: materialize(), range(2)))

    assert len(set(temporary_names)) == 2
    assert paths[0] == paths[1]
    assert paths[0].read_text(encoding="utf-8") == _CODE
    assert not tuple(paths[0].parent.glob(f".{paths[0].name}.*.tmp"))


@pytest.mark.parametrize("failure_stage", ["permission", "write", "fsync", "replace"])
def test_bare_materialization_failure_is_closed_and_cleans_temp(
    monkeypatch,
    tmp_path: Path,
    failure_stage: str,
):
    original_fdopen = os.fdopen

    if failure_stage == "permission":
        monkeypatch.setattr(
            Path,
            "mkdir",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                PermissionError("permission-canary-path")
            ),
        )
    elif failure_stage == "write":
        class FailingWriter:
            def __init__(self, fd, *args, **kwargs):
                self._file = original_fdopen(fd, *args, **kwargs)

            def __enter__(self):
                self._file.__enter__()
                return self

            def __exit__(self, *args):
                return self._file.__exit__(*args)

            def write(self, _value):
                raise OSError("write-canary-path")

        monkeypatch.setattr(os, "fdopen", FailingWriter)
    elif failure_stage == "fsync":
        monkeypatch.setattr(
            os,
            "fsync",
            lambda _fd: (_ for _ in ()).throw(OSError("fsync-canary-path")),
        )
    else:
        monkeypatch.setattr(
            Path,
            "replace",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("replace-canary-path")
            ),
        )

    with pytest.raises(DebugStrategySourceError) as captured:
        materialize_bare_strategy_source(
            root_dir=tmp_path,
            user_id=6,
            strategy_id=123,
            name="Failure Canary",
            version="1.0.0",
            strategy_code=_CODE,
        )

    assert captured.value.reason == "materialization_failed"
    assert str(captured.value) == "bare debug strategy source is invalid"
    assert captured.value.__cause__ is None
    assert captured.value.__context__ is None
    assert "canary" not in str(captured.value)
    assert not tuple(tmp_path.rglob("*.tmp"))


def test_bare_servicer_materializes_db_strategy_source(monkeypatch, tmp_path: Path, caplog):
    monkeypatch.setenv("RUNTIME_DEBUG_STRATEGY_SOURCE_DIR", str(tmp_path))
    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        runtime_source="bare",
        restore_running_sessions=False,
    )

    with caplog.at_level("INFO"):
        path, code, hot_reload = servicer._debug_strategy_source_for_db_code(
            user_id=6,
            strategy_id=123,
            strategy_name="Mean Reversion / Demo",
            strategy_version="1.0.0",
            strategy_path="<db:Mean Reversion / Demo@1.0.0>",
            strategy_code=_CODE,
        )

    assert path == str(tmp_path / "user-6" / "strategy-123-mean-reversion-demo-1-0-0.py")
    assert code is None
    assert hot_reload is True
    assert Path(path).read_text(encoding="utf-8") == _CODE
    assert [record.getMessage() for record in caplog.records] == [
        "BARE_STRATEGY_SOURCE_READY user_id=6 strategy_id=123"
    ]
    assert str(tmp_path) not in caplog.text
    assert "Mean Reversion" not in caplog.text


def test_bare_servicer_uses_existing_local_strategy_source(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("RUNTIME_DEBUG_STRATEGY_SOURCE_DIR", str(tmp_path))
    local_code = _CODE.replace("return None", "self.marker = 'local'\n        return None")
    local_path = tmp_path / "user-6" / "strategy-123-mean-reversion-demo-1-0-0.py"
    local_path.parent.mkdir(parents=True)
    local_path.write_text(local_code, encoding="utf-8")
    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        runtime_source="bare",
        restore_running_sessions=False,
    )

    path, code, hot_reload = servicer._debug_strategy_source_for_db_code(
        user_id=6,
        strategy_id=123,
        strategy_name="Mean Reversion / Demo",
        strategy_version="1.0.0",
        strategy_path="<db:Mean Reversion / Demo@1.0.0>",
        strategy_code=_CODE,
    )

    assert path == str(local_path)
    assert code is None
    assert hot_reload is True
    assert local_path.read_text(encoding="utf-8") == local_code


def test_non_bare_servicer_keeps_db_strategy_virtual_path(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("RUNTIME_DEBUG_STRATEGY_SOURCE_DIR", str(tmp_path))
    servicer = StrategyServiceServicer(
        "",
        "",
        {},
        "",
        runtime_source="hosted",
        restore_running_sessions=False,
    )

    path, code, hot_reload = servicer._debug_strategy_source_for_db_code(
        user_id=6,
        strategy_id=123,
        strategy_name="Mean Reversion / Demo",
        strategy_version="1.0.0",
        strategy_path="<db:Mean Reversion / Demo@1.0.0>",
        strategy_code=_CODE,
    )

    assert path == "<db:Mean Reversion / Demo@1.0.0>"
    assert code == _CODE
    assert hot_reload is False
    assert not any(tmp_path.rglob("*.py"))
