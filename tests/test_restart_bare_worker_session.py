from __future__ import annotations

import importlib.util
import os
import tempfile
from pathlib import Path


def _load_script():
    script = Path(__file__).resolve().parents[1] / "scripts" / "restart_bare_worker_session.py"
    spec = importlib.util.spec_from_file_location("restart_bare_worker_session", script)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_load_state_file_parses_shell_exports(tmp_path: Path) -> None:
    mod = _load_script()
    state = tmp_path / "runtime.env"
    state.write_text(
        """
export USER_ID='6'
export RUNTIME_RUNTIME_ID="bare-6-debug"
QUANT_HANDLER_URL=http://192.168.88.6:8090
DEBUG_PORT=5678
""".strip(),
        encoding="utf-8",
    )

    got = mod.load_state_file(state)

    assert got["USER_ID"] == "6"
    assert got["RUNTIME_RUNTIME_ID"] == "bare-6-debug"
    assert got["QUANT_HANDLER_URL"] == "http://192.168.88.6:8090"
    assert got["DEBUG_PORT"] == "5678"


def test_choose_runtime_id_prefers_explicit_then_state_then_session() -> None:
    mod = _load_script()

    assert mod.choose_runtime_id(
        "rt-explicit",
        {"RUNTIME_RUNTIME_ID": "rt-state"},
        {"runtime_id": "rt-session"},
    ) == "rt-explicit"
    assert mod.choose_runtime_id(
        "",
        {"RUNTIME_RUNTIME_ID": "rt-state"},
        {"runtime_id": "rt-session"},
    ) == "rt-state"
    assert mod.choose_runtime_id(
        "", {}, {"runtime_id": "rt-session"}
    ) == "rt-session"


def test_choose_control_url_prefers_explicit_then_state_url_then_state_addr() -> None:
    mod = _load_script()

    assert mod.choose_control_url(
        "http://127.0.0.1:5707",
        {"RUNTIME_AGENT_CONTROL_URL": "http://127.0.0.1:5706"},
    ) == "http://127.0.0.1:5707"
    assert mod.choose_control_url(
        "", {"RUNTIME_AGENT_CONTROL_URL": "http://127.0.0.1:5706"}
    ) == "http://127.0.0.1:5706"
    assert mod.choose_control_url(
        "", {"RUNTIME_AGENT_CONTROL_ADDR": "127.0.0.1:5706"}
    ) == "http://127.0.0.1:5706"


def test_build_restart_payload_omits_empty_optional_values() -> None:
    mod = _load_script()

    payload = mod.build_restart_payload(
        session_id="", max_loss_close_pct=0.0, leverage=3.0
    )
    assert payload == {
        "leverage": 3.0,
    }


def test_default_state_file_discovers_latest_runtime_when_user_id_is_empty(tmp_path: Path) -> None:
    mod = _load_script()
    old = tmp_path / "hushine-bare-debugpy-user-1" / "runtime.env"
    new = tmp_path / "hushine-bare-debugpy-user-9" / "runtime.env"
    old.parent.mkdir(parents=True)
    new.parent.mkdir(parents=True)
    old.write_text('export USER_ID="1"\n', encoding="utf-8")
    new.write_text('export USER_ID="9"\n', encoding="utf-8")
    os.utime(old, (1, 1))
    os.utime(new, (2, 2))

    assert mod.default_state_file("", base_dir=tmp_path) == new


def test_default_state_file_uses_platform_temp_directory(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("RUNTIME_BARE_STATE_FILE", raising=False)
    monkeypatch.delenv("USER_ID", raising=False)
    monkeypatch.setattr(tempfile, "gettempdir", lambda: str(tmp_path))
    mod = _load_script()

    assert mod.default_state_file("42") == (
        tmp_path / "hushine-bare-debugpy-user-42" / "runtime.env"
    )
