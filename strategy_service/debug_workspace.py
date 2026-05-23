from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

TEMPLATE_NAME = "self_hosted_strategy.py"


@dataclass(frozen=True)
class DebugWorkspaceResult:
    host_path: str
    container_path: str
    template_path: str
    archived_template_path: str
    vscode_launch_created: bool
    vscode_launch_preserved: bool
    pycharm_doc_created: bool
    pycharm_doc_preserved: bool
    prepared_at_ms: int
    last_error: str = ""

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def prepare_debug_workspace(container_path: str, host_path: str = "") -> DebugWorkspaceResult:
    root = Path(container_path or "/workspace").resolve()
    if not root.exists():
        raise RuntimeError(f"workspace path does not exist: {root}")
    if not root.is_dir():
        raise RuntimeError(f"workspace path is not a directory: {root}")
    probe = root / ".hushine_write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink(missing_ok=True)
    except OSError as exc:
        raise RuntimeError(f"workspace path is not writable: {root}") from exc

    template = root / TEMPLATE_NAME
    archived = ""
    if template.exists():
        suffix = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
        archived_path = root / f"self_hosted_strategy-{suffix}.py"
        template.rename(archived_path)
        archived = str(archived_path)

    template.write_text(_strategy_template(), encoding="utf-8")

    vscode_dir = root / ".vscode"
    vscode_dir.mkdir(exist_ok=True)
    launch = vscode_dir / "launch.json"
    vscode_preserved = launch.exists()
    vscode_created = False
    if not launch.exists():
        launch.write_text(_vscode_launch_json(), encoding="utf-8")
        vscode_created = True

    pycharm = root / "PYCHARM_DEBUG.md"
    pycharm_preserved = pycharm.exists()
    pycharm_created = False
    if not pycharm.exists():
        pycharm.write_text(_pycharm_doc(), encoding="utf-8")
        pycharm_created = True

    return DebugWorkspaceResult(
        host_path=host_path,
        container_path=str(root),
        template_path=str(template),
        archived_template_path=archived,
        vscode_launch_created=vscode_created,
        vscode_launch_preserved=vscode_preserved,
        pycharm_doc_created=pycharm_created,
        pycharm_doc_preserved=pycharm_preserved,
        prepared_at_ms=int(datetime.now(timezone.utc).timestamp() * 1000),
    )


def _strategy_template() -> str:
    return '''class MyStrategy:
    def on_market_data(self, data, wallet):
        # Write your strategy logic here. Return an order decision or None.
        return None
'''


def _vscode_launch_json() -> str:
    return '''{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Hushine Debugger Attach",
      "type": "debugpy",
      "request": "attach",
      "connect": {
        "host": "localhost",
        "port": 5678
      },
      "pathMappings": [
        {
          "localRoot": "${workspaceFolder}",
          "remoteRoot": "/workspace"
        }
      ]
    }
  ]
}
'''


def _pycharm_doc() -> str:
    return '''# PyCharm Debugging

1. Start a Python Debug Server in PyCharm.
2. Use host `host.docker.internal` from Docker Desktop on macOS.
3. Run `hushine-debug replay --pycharm --host host.docker.internal --port 5680`.
4. Keep `/workspace` mapped to your local debug workspace.
'''
