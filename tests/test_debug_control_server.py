import os
import json
import socket
import time
import uuid

from strategy_service.cli.hushine_debug import main
from strategy_service.debug_control_server import DebugControlServer


def test_cli_calls_debug_control_socket(monkeypatch, capsys):
    socket_path = f"/tmp/hushine-debug-test-{os.getpid()}-{uuid.uuid4().hex[:8]}.sock"

    def handler(req):
        assert req.name == "manual"
        return {"session_id": "s1", "status": "finished"}

    server = DebugControlServer(socket_path, handler)
    server.start()
    deadline = time.time() + 1.0
    while not os.path.exists(socket_path) and time.time() < deadline:
        time.sleep(0.01)
    monkeypatch.setenv("HUSHINE_DEBUG_SOCKET", socket_path)
    try:
        rc = main(["replay", "--name", "manual"])
    finally:
        server.stop()

    assert rc == 0
    assert "s1" in capsys.readouterr().out


def test_cli_debugpy_flag_uses_debugpy_adapter(monkeypatch, capsys):
    socket_path = f"/tmp/hushine-debug-test-{os.getpid()}-{uuid.uuid4().hex[:8]}.sock"

    def handler(req):
        assert req.debugger == "debugpy"
        assert req.host == "host.docker.internal"
        assert req.port == 5678
        return {"session_id": "s-debugpy", "status": "finished"}

    server = DebugControlServer(socket_path, handler)
    server.start()
    deadline = time.time() + 1.0
    while not os.path.exists(socket_path) and time.time() < deadline:
        time.sleep(0.01)
    monkeypatch.setenv("HUSHINE_DEBUG_SOCKET", socket_path)
    try:
        rc = main(["replay", "--debugpy"])
    finally:
        server.stop()

    assert rc == 0
    assert "s-debugpy" in capsys.readouterr().out


def test_cli_reports_socket_error(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("HUSHINE_DEBUG_SOCKET", str(tmp_path / "missing.sock"))

    rc = main(["replay"])

    assert rc == 1
    assert "control socket unavailable" in capsys.readouterr().err


def test_debug_control_server_survives_client_disconnect(monkeypatch, capsys):
    socket_path = f"/tmp/hushine-debug-test-{os.getpid()}-{uuid.uuid4().hex[:8]}.sock"
    calls = 0

    def handler(req):
        nonlocal calls
        calls += 1
        time.sleep(0.05)
        return {"session_id": f"s{calls}", "status": "finished"}

    server = DebugControlServer(socket_path, handler)
    server.start()
    deadline = time.time() + 1.0
    while not os.path.exists(socket_path) and time.time() < deadline:
        time.sleep(0.01)
    try:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(socket_path)
        client.sendall(json.dumps({"method": "replay", "params": {}}).encode("utf-8"))
        client.shutdown(socket.SHUT_RDWR)
        client.close()

        deadline = time.time() + 1.0
        while calls < 1 and time.time() < deadline:
            time.sleep(0.01)

        monkeypatch.setenv("HUSHINE_DEBUG_SOCKET", socket_path)
        rc = main(["replay"])
    finally:
        server.stop()

    assert rc == 0
    assert calls == 2
    assert "s2" in capsys.readouterr().out
