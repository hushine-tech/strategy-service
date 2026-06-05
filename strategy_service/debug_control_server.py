from __future__ import annotations

import json
import os
import socket
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class DebugReplayRequest:
    name: str = ""
    debugger: str = ""
    wait: bool = False
    host: str = "host.docker.internal"
    port: int = 5678


class DebugControlServer:
    def __init__(self, socket_path: str, replay_handler: Callable[[DebugReplayRequest], dict]):
        self.socket_path = socket_path
        self._replay_handler = replay_handler
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._startup_error: OSError | None = None
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._stop.clear()
        self._ready.clear()
        self._startup_error = None
        Path(self.socket_path).parent.mkdir(parents=True, exist_ok=True)
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass
        self._thread = threading.Thread(target=self._serve, name="hushine-debug-control", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=1.0):
            raise RuntimeError(f"debug control socket did not start: {self.socket_path}")
        if self._startup_error is not None:
            raise RuntimeError(f"debug control socket failed to start: {self._startup_error}") from self._startup_error

    def stop(self) -> None:
        self._stop.set()
        try:
            os.unlink(self.socket_path)
        except FileNotFoundError:
            pass

    def _serve(self) -> None:
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            try:
                server.bind(self.socket_path)
                server.listen(4)
            except OSError as exc:
                self._startup_error = exc
                self._ready.set()
                return
            self._ready.set()
            server.settimeout(0.2)
            while not self._stop.is_set():
                try:
                    conn, _ = server.accept()
                except TimeoutError:
                    continue
                try:
                    with conn:
                        response = self._handle_connection(conn)
                        conn.sendall(json.dumps(response, separators=(",", ":")).encode("utf-8"))
                except OSError:
                    continue

    def _handle_connection(self, conn: socket.socket) -> dict:
        try:
            request = json.loads(conn.recv(1024 * 1024).decode("utf-8"))
            if request.get("method") != "replay":
                return {"ok": False, "error": "unsupported debug control method"}
            params = request.get("params") or {}
            if not isinstance(params, dict):
                return {"ok": False, "error": "debug replay params must be an object"}
            return {"ok": True, "result": self._replay_handler(DebugReplayRequest(**params))}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": str(exc)}
