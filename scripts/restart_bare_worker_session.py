#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def load_state_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        try:
            parsed = shlex.split(value, posix=True)
        except ValueError:
            parsed = [value.strip().strip("\"'")]
        values[key] = parsed[0] if parsed else ""
    return values


def choose_runtime_id(explicit: str, state: dict[str, str], session: dict[str, Any]) -> str:
    for value in (explicit, state.get("RUNTIME_RUNTIME_ID", ""), str(session.get("runtime_id", "") or "")):
        value = value.strip()
        if value:
            return value
    return ""


def choose_control_url(explicit: str, state: dict[str, str]) -> str:
    explicit = explicit.strip()
    if explicit:
        return explicit.rstrip("/")
    state_url = state.get("RUNTIME_AGENT_CONTROL_URL", "").strip()
    if state_url:
        return state_url.rstrip("/")
    state_addr = state.get("RUNTIME_AGENT_CONTROL_ADDR", "").strip()
    if state_addr:
        return f"http://{state_addr}".rstrip("/")
    return ""


def build_restart_payload(session_id: str, max_loss_close_pct: float) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    session_id = session_id.strip()
    if session_id:
        payload["session_id"] = session_id
    if max_loss_close_pct > 0:
        payload["max_loss_close_pct"] = max_loss_close_pct
    return payload


def validate_restart_result(
    requested_session_id: str,
    result: dict[str, Any],
) -> dict[str, Any]:
    old_session_id = str(result.get("old_session_id", "") or "").strip()
    new_session_id = str(result.get("new_session_id", "") or "").strip()
    requested_session_id = str(requested_session_id or "").strip()
    if not old_session_id or (requested_session_id and old_session_id != requested_session_id):
        raise ValueError("restart result old Session does not match the request")
    if not new_session_id or new_session_id == old_session_id:
        raise ValueError("restart result must create a new Session")
    return result


def discover_latest_state_file(base_dir: Path | None = None) -> Path | None:
    if base_dir is None:
        base_dir = Path(tempfile.gettempdir())
    candidates = list(base_dir.glob("hushine-bare-debugpy-user-*/runtime.env"))
    if not candidates:
        return None
    return max(candidates, key=lambda item: item.stat().st_mtime)


def default_state_file(user_id: str, base_dir: Path | None = None) -> Path:
    explicit = os.environ.get("RUNTIME_BARE_STATE_FILE", "").strip()
    if explicit:
        return Path(explicit)
    if base_dir is None:
        base_dir = Path(tempfile.gettempdir())
    user_id = (user_id or os.environ.get("USER_ID") or "").strip()
    if user_id:
        return base_dir / f"hushine-bare-debugpy-user-{user_id}" / "runtime.env"
    latest = discover_latest_state_file(base_dir)
    if latest is not None:
        return latest
    return base_dir / "hushine-bare-debugpy-user-6" / "runtime.env"


def post_restart(control_url: str, payload: dict[str, Any], timeout: float) -> dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{control_url}/restart-worker-session",
        data=data,
        headers={"content-type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Restart the current local bare runtime worker session.")
    parser.add_argument("session_id_pos", nargs="?", help="optional session_id to restart")
    parser.add_argument("--session-id", default="", help="session_id to restart; default uses latest running/recoverable session for this runtime")
    parser.add_argument("--user-id", default=os.environ.get("USER_ID", ""), help="debug user id used to locate the default state file")
    parser.add_argument("--state-file", default="", help="path to runtime.env written by start-bare-runtime-debugpy.sh")
    parser.add_argument("--control-url", default=os.environ.get("RUNTIME_AGENT_CONTROL_URL", ""), help="local runtime-agent control URL")
    parser.add_argument("--max-loss-close-pct", type=float, default=0.0, help="optional override passed to the restarted run")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(list(argv or sys.argv[1:]))
    state_file = Path(args.state_file) if args.state_file else default_state_file(str(args.user_id))
    state = load_state_file(state_file)
    control_url = choose_control_url(str(args.control_url or ""), state)
    if not control_url:
        print(f"runtime-agent control URL not found; start bare runtime first or pass --control-url. state_file={state_file}", file=sys.stderr)
        return 2
    payload = build_restart_payload(
        session_id=str(args.session_id or args.session_id_pos or ""),
        max_loss_close_pct=float(args.max_loss_close_pct or 0.0),
    )
    try:
        result = post_restart(control_url, payload, timeout=float(args.timeout))
        result = validate_restart_result(str(payload.get("session_id", "")), result)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"restart request failed: HTTP {exc.code} {body}", file=sys.stderr)
        return 1
    except ValueError as exc:
        print(f"restart request failed: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"restart request failed: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
