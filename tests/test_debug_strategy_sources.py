from pathlib import Path

from strategy_service.grpc_server import StrategyServiceServicer
from strategy_service.strategy.base import _load_strategy_instance
from strategy_service.debug_strategy_sources import materialize_bare_strategy_source


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

    strategy = _load_strategy_instance(str(path), strategy_code=_CODE)
    assert strategy.on_market_data.__code__.co_filename == str(path)


def test_bare_servicer_materializes_db_strategy_source(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("RUNTIME_DEBUG_STRATEGY_SOURCE_DIR", str(tmp_path))
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

    assert path == str(tmp_path / "user-6" / "strategy-123-mean-reversion-demo-1-0-0.py")
    assert code is None
    assert hot_reload is True
    assert Path(path).read_text(encoding="utf-8") == _CODE


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
