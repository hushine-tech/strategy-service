from __future__ import annotations

import argparse
import json
import os
import socket
import sys

DEFAULT_SOCKET = "/tmp/hushine-debug.sock"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="hushine-debug")
    sub = parser.add_subparsers(dest="command", required=True)
    replay = sub.add_parser("replay")
    replay.add_argument("--name", default="")
    replay.add_argument("--debugpy", action="store_true")
    replay.add_argument("--pycharm", action="store_true")
    replay.add_argument("--wait", action="store_true")
    replay.add_argument("--host", default="host.docker.internal")
    replay.add_argument("--port", type=int, default=5678)
    args = parser.parse_args(argv)

    debugger = ""
    if args.debugpy:
        debugger = "debugpy"
    if args.pycharm:
        debugger = "pycharm"
    if not debugger:
        debugger = os.getenv("HUSHINE_DEBUGGER", "")

    request = {
        "method": "replay",
        "params": {
            "name": args.name,
            "debugger": debugger,
            "wait": args.wait or os.getenv("HUSHINE_DEBUG_WAIT", "") == "1",
            "host": args.host,
            "port": args.port,
        },
    }
    try:
        response = _call_socket(os.getenv("HUSHINE_DEBUG_SOCKET", DEFAULT_SOCKET), request)
    except OSError as exc:
        print(f"hushine-debug control socket unavailable: {exc}", file=sys.stderr)
        return 1
    if not response.get("ok"):
        print(response.get("error", "debug replay failed"), file=sys.stderr)
        return 1
    print(json.dumps(response.get("result") or {}, indent=2, sort_keys=True))
    return 0


def _call_socket(socket_path: str, request: dict) -> dict:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
        client.connect(socket_path)
        client.sendall(json.dumps(request, separators=(",", ":")).encode("utf-8"))
        return json.loads(client.recv(1024 * 1024).decode("utf-8"))


if __name__ == "__main__":
    raise SystemExit(main())
